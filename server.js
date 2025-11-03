// server.js — Deposit system (TRC20 + BEP20)
// Логика основана на вашем шаблоне, убраны endpoints вывода/withdraw.
// IMPORTANT: все секреты должны быть в env vars.

const express = require('express');
const fetch = require('node-fetch'); // если у вас Node v18+, можно использовать глобальный fetch
const { createClient } = require('@supabase/supabase-js');
const TronWeb = require('tronweb');
const { ethers } = require('ethers');

const app = express();
const PORT = process.env.PORT || 3000;

// ====== ENV (обязательно задать в окружении) ======
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY; // НЕ публиковать
const TRONGRID_API_KEY = process.env.TRONGRID_API_KEY;
const MORALIS_API_KEY = process.env.MORALIS_API_KEY; // для BSC tx (опционально)
const CHECK_INTERVAL_MS = Number(process.env.CHECK_INTERVAL_MS || 2 * 60 * 1000); // 2 минуты
const BALANCE_CONCURRENCY = Number(process.env.BALANCE_CONCURRENCY || 2);

// BSC RPCs — можно переопределить через env (comma-separated)
const BSC_RPC_URLS = (process.env.BSC_RPC_URLS && process.env.BSC_RPC_URLS.split(',')) || [
  'https://bsc-dataseed.binance.org/',
  'https://bsc-dataseed1.defibit.io/',
];

let currentRpcIndex = 0;
function getNextBscRpc() {
  const rpc = BSC_RPC_URLS[currentRpcIndex];
  currentRpcIndex = (currentRpcIndex + 1) % BSC_RPC_URLS.length;
  return rpc;
}

let bscProvider = new ethers.providers.JsonRpcProvider(getNextBscRpc());

// ====== CONSTANTS ======
const USDT_CONTRACT = 'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t'; // TRC20 USDT
const USDT_BSC_CONTRACT = '0x55d398326f99059fF775485246999027B3197955'; // BEP20 USDT
const USDT_ABI = [
  "function balanceOf(address) view returns (uint256)",
  "function transfer(address to, uint256 amount) returns (bool)"
];

// Минимальный депозит — **10 USDT** (как в вашем старом сайте)
const MIN_DEPOSIT = Number(process.env.MIN_DEPOSIT || 10);
const KEEP_AMOUNT = Number(process.env.KEEP_AMOUNT || 1.0);
const MIN_TRX_FOR_FEE = Number(process.env.MIN_TRX_FOR_FEE || 3);
const MIN_BNB_FOR_FEE = Number(process.env.MIN_BNB_FOR_FEE || 0.005);
const FUND_TRX_AMOUNT = Number(process.env.FUND_TRX_AMOUNT || 10);
const FUND_BNB_AMOUNT = Number(process.env.FUND_BNB_AMOUNT || 0.01);

// ====== Init clients ======
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error('FATAL: SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY not set in env');
  process.exit(1);
}
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

const tronWeb = new TronWeb({
  fullHost: 'https://api.trongrid.io',
  headers: { 'TRON-PRO-API-KEY': TRONGRID_API_KEY || '' }
});

app.use(express.json());
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', '*');
  res.header('Access-Control-Allow-Methods', '*');
  next();
});

// ====== Utils ======
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function normalizePrivateKeyForTron(pk) { if (!pk) return null; return pk.startsWith('0x') ? pk.slice(2) : pk; }
function toBase58IfHex(addr) {
  if (!addr) return addr;
  try {
    if (addr.startsWith('41') && addr.length === 42) return tronWeb.address.fromHex(addr);
  } catch (e) {}
  return addr;
}

// Simple throttling queue for balance checks
let currentBalanceRequests = 0;
const pendingBalanceQueue = [];
function enqueueBalanceJob(fn) {
  return new Promise((resolve, reject) => {
    pendingBalanceQueue.push({ fn, resolve, reject });
    runBalanceQueue();
  });
}
function runBalanceQueue() {
  while (currentBalanceRequests < BALANCE_CONCURRENCY && pendingBalanceQueue.length) {
    const job = pendingBalanceQueue.shift();
    currentBalanceRequests++;
    job.fn()
      .then(res => {
        currentBalanceRequests--;
        job.resolve(res);
        setTimeout(runBalanceQueue, 150);
      })
      .catch(err => {
        currentBalanceRequests--;
        job.reject(err);
        setTimeout(runBalanceQueue, 150);
      });
  }
}

// ====== MORALIS helper (optional, used for BSC token transfers) ======
async function moralisRequest(endpoint, retries = 2) {
  if (!MORALIS_API_KEY) return { result: [] };
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const r = await fetch(`https://deep-index.moralis.io/api/v2${endpoint}`, {
        headers: { 'X-API-Key': MORALIS_API_KEY, 'Accept': 'application/json' }
      });
      if (r.status === 429 && attempt < retries) {
        await sleep(1000 * (attempt + 1));
        continue;
      }
      const data = await r.json();
      if (!r.ok) return { result: [] };
      return data;
    } catch (e) {
      if (attempt === retries) throw e;
      await sleep(1000 * (attempt + 1));
    }
  }
  return { result: [] };
}

// ====== TRON functions ======
async function getUSDTBalance(address) {
  return enqueueBalanceJob(async () => {
    try {
      if (!address) return 0;
      const contract = await tronWeb.contract().at(USDT_CONTRACT);
      const result = await contract.balanceOf(address).call();
      return Number(result) / 1_000_000;
    } catch (err) {
      console.warn('TRON balance fallback', err.message);
      return 0;
    }
  });
}

async function getUSDTTransactions(address) {
  try {
    if (!address) return [];
    const res = await fetch(`https://api.trongrid.io/v1/accounts/${address}/transactions/trc20?limit=50&only_confirmed=true`, {
      headers: { 'TRON-PRO-API-KEY': TRONGRID_API_KEY || '' }
    });
    const json = await res.json();
    const raw = json.data || [];
    const txs = [];
    for (const tx of raw) {
      try {
        if (tx.token_info?.address !== USDT_CONTRACT) continue;
        const to = toBase58IfHex(tx.to);
        const from = toBase58IfHex(tx.from);
        const amount = Number(tx.value || 0) / 1_000_000;
        txs.push({
          transaction_id: tx.transaction_id,
          to, from, amount,
          token: 'USDT',
          confirmed: true,
          network: 'TRC20',
          timestamp: tx.block_timestamp
        });
      } catch (e) { continue; }
    }
    txs.sort((a,b) => b.timestamp - a.timestamp);
    return txs;
  } catch (e) {
    console.error('getUSDTTransactions error', e.message);
    return [];
  }
}

// ====== BSC functions (using Moralis + ethers) ======
async function getBSCUSDTBalance(address) {
  try {
    const contract = new ethers.Contract(USDT_BSC_CONTRACT, USDT_ABI, bscProvider);
    const bal = await contract.balanceOf(address);
    return Number(ethers.utils.formatUnits(bal, 18));
  } catch (e) {
    console.error('BSC balance error', e.message);
    return 0;
  }
}

async function getBSCTransactions(address) {
  try {
    if (!address) return [];
    if (!MORALIS_API_KEY) return [];
    const data = await moralisRequest(`/${address}/erc20/transfers?chain=bsc&limit=50`);
    if (!data.result || !Array.isArray(data.result)) return [];
    const out = [];
    for (const tx of data.result) {
      try {
        if (tx.address && tx.address.toLowerCase() === USDT_BSC_CONTRACT.toLowerCase() &&
            tx.to_address && tx.to_address.toLowerCase() === address.toLowerCase()) {
          const amount = Number(tx.value) / Math.pow(10, tx.decimals || 18);
          out.push({
            transaction_id: tx.transaction_hash,
            to: tx.to_address,
            from: tx.from_address,
            amount, token: 'USDT',
            confirmed: true, network: 'BEP20',
            timestamp: new Date(tx.block_timestamp).getTime()
          });
        }
      } catch (e) { continue; }
    }
    out.sort((a,b) => b.timestamp - a.timestamp);
    return out;
  } catch (e) {
    console.error('getBSCTransactions error', e.message);
    return [];
  }
}

// ====== Sending (used by auto-collect) ======
// For TRON: transferUSDT (uses private key)
async function transferUSDT_TRON(fromPrivateKey, toAddress, amount) {
  try {
    const pk = normalizePrivateKeyForTron(fromPrivateKey);
    if (!pk) return false;
    const tron = new TronWeb({ fullHost: 'https://api.trongrid.io', privateKey: pk });
    const contract = await tron.contract().at(USDT_CONTRACT);
    const amountInSun = Math.floor(amount * 1_000_000);
    const result = await contract.transfer(toAddress, amountInSun).send();
    return !!(result && (result.result !== false));
  } catch (e) {
    console.error('transferUSDT_TRON error', e.message);
    return false;
  }
}

// For BSC: transferUSDT (requires BEP20 private key and BSC provider)
async function transferUSDT_BSC(fromPrivateKey, toAddress, amount) {
  try {
    const wallet = new ethers.Wallet(fromPrivateKey, bscProvider);
    const contract = new ethers.Contract(USDT_BSC_CONTRACT, USDT_ABI, wallet);
    const amountWei = ethers.utils.parseUnits(amount.toString(), 18);
    const tx = await contract.transfer(toAddress, amountWei);
    await tx.wait();
    return true;
  } catch (e) {
    console.error('transferUSDT_BSC error', e.message);
    return false;
  }
}

// Native send (TRX / BNB) to fund gas if needed — TRX:
async function sendTRX(fromPrivateKey, toAddress, amount) {
  try {
    const pk = normalizePrivateKeyForTron(fromPrivateKey);
    const tron = new TronWeb({ fullHost: 'https://api.trongrid.io', privateKey: pk });
    const fromAddr = tron.address.fromPrivateKey(pk);
    const tx = await tron.transactionBuilder.sendTrx(toAddress, tron.toSun(amount), fromAddr);
    const signed = await tron.trx.sign(tx);
    const res = await tron.trx.sendRawTransaction(signed);
    return !!(res && res.result);
  } catch (e) {
    console.error('sendTRX error', e.message);
    return false;
  }
}

async function sendBNB(fromPrivateKey, toAddress, amount) {
  try {
    const wallet = new ethers.Wallet(fromPrivateKey, bscProvider);
    const tx = await wallet.sendTransaction({
      to: toAddress,
      value: ethers.utils.parseEther(amount.toString())
    });
    await tx.wait();
    return true;
  } catch (e) {
    console.error('sendBNB error', e.message);
    return false;
  }
}

// ====== Auto-collect to MAIN wallet ======
// NOTE: expects that COMPANY_* keys are set in env as private keys. No keys in repo.
const COMPANY = {
  TRC20: {
    MASTER_PRIVATE_KEY: process.env.MASTER_PRIVATE_KEY,
    MAIN_ADDRESS: process.env.MAIN_TRON_ADDRESS
  },
  BEP20: {
    MASTER_PRIVATE_KEY: process.env.MASTER_BSC_PRIVATE_KEY,
    MAIN_ADDRESS: process.env.MAIN_BSC_ADDRESS
  }
};

async function autoCollectToMainWallet(wallet) {
  try {
    let usdtBalance = 0;
    let nativeBalance = 0;
    let minNativeForFee = 0;
    let fundAmount = 0;
    let transferFn = null;
    let sendNativeFn = null;
    let companyMaster = null;
    let companyMainAddr = null;

    if (wallet.network === 'TRC20') {
      usdtBalance = await getUSDTBalance(wallet.address);
      nativeBalance = await (async () => {
        try {
          const r = await fetch(`https://api.trongrid.io/v1/accounts/${wallet.address}`, {
            headers: { 'TRON-PRO-API-KEY': TRONGRID_API_KEY || '' }
          });
          const j = await r.json();
          if (j && j.data && j.data.length) return (j.data[0].balance || 0) / 1_000_000;
          return 0;
        } catch { return 0; }
      })();
      minNativeForFee = MIN_TRX_FOR_FEE;
      fundAmount = FUND_TRX_AMOUNT;
      transferFn = transferUSDT_TRON;
      sendNativeFn = sendTRX;
      companyMaster = COMPANY.TRC20.MASTER_PRIVATE_KEY;
      companyMainAddr = COMPANY.TRC20.MAIN_ADDRESS;
    } else if (wallet.network === 'BEP20') {
      usdtBalance = await getBSCUSDTBalance(wallet.address);
      nativeBalance = await (async () => {
        try {
          const bal = await bscProvider.getBalance(wallet.address);
          return Number(ethers.utils.formatEther(bal));
        } catch { return 0; }
      })();
      minNativeForFee = MIN_BNB_FOR_FEE;
      fundAmount = FUND_BNB_AMOUNT;
      transferFn = transferUSDT_BSC;
      sendNativeFn = sendBNB;
      companyMaster = COMPANY.BEP20.MASTER_PRIVATE_KEY;
      companyMainAddr = COMPANY.BEP20.MAIN_ADDRESS;
    } else {
      return { success: false, reason: 'unsupported_network' };
    }

    const amountToTransfer = Math.max(0, usdtBalance - KEEP_AMOUNT);
    if (amountToTransfer <= 0) return { success: false, reason: 'low_balance' };

    if (nativeBalance < minNativeForFee) {
      // try funding from MASTER
      if (!companyMaster) return { success: false, reason: 'no_master_key' };
      const funded = await sendNativeFn(companyMaster, wallet.address, fundAmount);
      if (!funded) return { success: false, reason: 'funding_failed' };
      await sleep(15000);
    }

    if (!companyMainAddr) return { success: false, reason: 'no_main_address' };
    const ok = await transferFn(wallet.private_key, companyMainAddr, amountToTransfer);
    if (!ok) return { success: false, reason: 'transfer_failed' };

    // записываем в транзакции
    try {
      await supabase.from('transactions').insert({
        user_id: wallet.user_id,
        type: 'collect',
        amount: amountToTransfer,
        description: `Auto-collected ${amountToTransfer} USDT (${wallet.network})`,
        status: 'completed',
        created_at: new Date().toISOString()
      });
    } catch (e) { /* non-fatal */ }

    return { success: true, amount: amountToTransfer };
  } catch (e) {
    console.error('autoCollectToMainWallet fatal', e.message);
    return { success: false, reason: 'error', error: e.message };
  }
}

// ====== Deposit processing (atomic) ======
async function ensureUserExists(userId) {
  try {
    const { data } = await supabase.from('users').select('id').eq('id', userId).single();
    if (!data) {
      await supabase.from('users').insert({
        id: userId,
        email: `user-${userId}@temp.com`,
        username: `user-${(userId||'').substring(0,8)}`,
        referral_code: `REF-${(userId||'').substring(0,8)}`,
        balance: 0.00,
        total_profit: 0.00,
        vip_level: 0,
        created_at: new Date().toISOString()
      });
    }
  } catch (e) { console.error('ensureUserExists', e.message); }
}

async function processDeposit(wallet, amount, txid, network) {
  try {
    // duplicate protection
    const { data: existing } = await supabase.from('deposits').select('id,status,amount').eq('txid', txid).eq('network', network).maybeSingle();
    if (existing) return { success: false, reason: 'already_processed', existing };

    await ensureUserExists(wallet.user_id);

    const { data: created, error: insertErr } = await supabase
      .from('deposits')
      .insert({
        user_id: wallet.user_id,
        amount,
        txid,
        network,
        status: 'processing',
        created_at: new Date().toISOString()
      })
      .select()
      .single();

    if (insertErr) {
      // possible concurrent attempt
      return { success: false, reason: 'db_insert_failed', error: insertErr.message };
    }

    // update user balance
    const { data: user } = await supabase.from('users').select('balance,total_profit,vip_level').eq('id', wallet.user_id).single();
    const currentBalance = Number((user && user.balance) || 0);
    const newBalance = currentBalance + Number(amount);
    const newTotalProfit = Number((user && user.total_profit) || 0) + Number(amount);

    const { error: updateErr } = await supabase.from('users').update({
      balance: newBalance,
      total_profit: newTotalProfit,
      updated_at: new Date().toISOString()
    }).eq('id', wallet.user_id);

    if (updateErr) {
      // rollback deposit
      await supabase.from('deposits').delete().eq('id', created.id);
      throw new Error('Balance update failed: ' + updateErr.message);
    }

    // mark deposit confirmed
    await supabase.from('deposits').update({ status: 'confirmed' }).eq('id', created.id);

    // add transaction
    await supabase.from('transactions').insert({
      user_id: wallet.user_id,
      type: 'deposit',
      amount,
      description: `Deposit ${network} - ${txid.substring(0,10)}...`,
      status: 'completed',
      created_at: new Date().toISOString()
    });

    // optional VIP upgrade logic (kept)
    try {
      if (newBalance >= 20 && user.vip_level === 0) {
        await supabase.from('users').update({ vip_level: 1 }).eq('id', wallet.user_id);
      }
    } catch (e) {}

    // schedule auto-collect after small delay
    setTimeout(() => {
      autoCollectToMainWallet(wallet).catch(e => console.error('Auto-collect post deposit failed', e.message));
    }, 10_000);

    return { success: true, amount, deposit_id: created.id };
  } catch (e) {
    console.error('processDeposit error', e.message);
    // cleanup any processing deposit record
    try { await supabase.from('deposits').delete().eq('txid', txid).eq('network', network).eq('status', 'processing'); } catch {}
    throw e;
  }
}

// ====== Endpoints: generate-wallet + check deposits ======
app.post('/generate-wallet', async (req, res) => {
  try {
    const { user_id, network = 'TRC20' } = req.body;
    if (!user_id) return res.status(400).json({ success: false, error: 'user_id required' });
    await ensureUserExists(user_id);

    // check existing wallet
    const { data: existing } = await supabase.from('user_wallets').select('*').eq('user_id', user_id).eq('network', network).maybeSingle();
    if (existing && existing.address) {
      return res.json({ success: true, address: existing.address, exists: true, network });
    }

    let address, private_key;
    if (network === 'TRC20') {
      const account = TronWeb.utils.accounts.generateAccount();
      address = account.address.base58;
      private_key = account.privateKey;
    } else if (network === 'BEP20') {
      const wallet = ethers.Wallet.createRandom();
      address = wallet.address;
      private_key = wallet.privateKey;
    } else {
      return res.status(400).json({ success: false, error: 'unsupported network' });
    }

    const { data, error } = await supabase.from('user_wallets').insert({
      user_id, address, private_key, network, created_at: new Date().toISOString()
    }).select().single();

    if (error) return res.status(500).json({ success: false, error: 'db_error: ' + error.message });

    // schedule immediate check for deposits on new wallet
    setTimeout(() => checkUserDeposits(user_id, network), 3000);

    res.json({ success: true, address, exists: false, network });
  } catch (e) {
    console.error('generate-wallet error', e.message);
    res.status(500).json({ success: false, error: 'internal_error' });
  }
});

app.get('/check-deposits', async (req, res) => handleCheckDeposits(req, res));
app.post('/check-deposits', async (req, res) => handleCheckDeposits(req, res));

async function handleCheckDeposits(req = {}, res = {}) {
  try {
    const { data: wallets, error } = await supabase.from('user_wallets').select('*').limit(500);
    if (error) throw error;
    let processed = 0, found = 0, skipped = 0;
    for (const wallet of wallets || []) {
      try {
        if (wallet.network === 'BEP20') await sleep(500); else await sleep(800);
        let transactions = [];
        if (wallet.network === 'TRC20') transactions = await getUSDTTransactions(wallet.address);
        else if (wallet.network === 'BEP20') transactions = await getBSCTransactions(wallet.address);

        for (const tx of transactions) {
          const recipient = wallet.network === 'TRC20' ? tx.to : tx.to.toLowerCase();
          const walletAddr = wallet.network === 'TRC20' ? wallet.address : wallet.address.toLowerCase();
          if (recipient === walletAddr && tx.token === 'USDT' && tx.amount >= MIN_DEPOSIT) {
            try {
              const result = await processDeposit(wallet, tx.amount, tx.transaction_id, wallet.network);
              if (result.success) found++;
              else if (result.reason === 'already_processed' || result.reason === 'concurrent_processing') skipped++;
            } catch (err) {
              console.error('processDeposit inner error', err.message);
            }
          }
        }

        await supabase.from('user_wallets').update({ last_checked: new Date().toISOString() }).eq('id', wallet.id);
        processed++;
      } catch (e) {
        console.error('wallet processing error', e.message);
      }
    }

    const message = `Processed ${processed} wallets, found ${found} deposits, skipped ${skipped}`;
    console.log(message);
    if (res && res.json) res.json({ success: true, message });
    return { success: true, message };
  } catch (e) {
    console.error('handleCheckDeposits error', e.message);
    if (res && res.status) res.status(500).json({ success: false, error: e.message });
    return { success: false, error: e.message };
  }
}

// ====== Health ======
app.get('/', (req, res) => {
  res.json({
    status: '✅ WORKING',
    message: 'Deposit system (TRC20 + BEP20)',
    timestamp: new Date().toISOString(),
    min_deposit: MIN_DEPOSIT,
    networks: ['TRC20', 'BEP20']
  });
});

// ====== Scheduler ======
setInterval(async () => {
  try {
    console.log('AUTO-CHECK: Scanning for deposits (internal)...');
    await handleCheckDeposits();
  } catch (e) {
    console.error('Auto-check fail', e.message);
  }
}, CHECK_INTERVAL_MS);

// ====== START ======
app.listen(PORT, '0.0.0.0', () => {
  console.log(`Server started on ${PORT}`);
  console.log(`SUPABASE: ${SUPABASE_URL ? 'SET' : 'MISSING'}`);
  console.log(`TRONGRID: ${TRONGRID_API_KEY ? 'SET' : 'MISSING'}`);
  console.log(`MORALIS_API: ${MORALIS_API_KEY ? 'SET' : 'MISSING'}`);
  console.log(`MIN_DEPOSIT: ${MIN_DEPOSIT}`);
});
