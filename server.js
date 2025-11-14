// server.js - full ready-to-run file (NO MORALIS) 
// Dependencies: express, @supabase/supabase-js, tronweb, ethers, node-fetch
// Node 18+ recommended.

const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const TronWeb = require('tronweb');
const { ethers } = require('ethers');

// fetch fallback for Node <18 (node-fetch)
const fetch = global.fetch || require('node-fetch');

// ========== STARTUP LOGS ==========
console.log('🚀 STARTING SERVER - BEP20/TRC20 payments (NO MORALIS) - getLogs variant');
console.log('📅', new Date().toISOString());
console.log('🔧 Node.js version:', process.version);

// ========== ENV / CONFIG ==========
const PORT = process.env.PORT || 3000;

// Defaults from your previous file (you can override via env)
const SUPABASE_URL = process.env.SUPABASE_URL || 'https://eqzfivdckzrkkncahlyn.supabase.co';
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVxemZpdmRja3pya2tuY2FobHluIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MTYwNTg2NSwiZXhwIjoyMDc3MTgxODY1fQ.AuGqzDDMzWS1COhHdBMchHarYmd1gNC_9PfRfJWPTxc';
const TRONGRID_API_KEY = process.env.TRONGRID_API_KEY || '33759ca3-ffb8-41bc-9036-25a32601eae2';

// BSC RPC cluster (free/public)
const BSC_RPC_URLS = [
  process.env.BSC_RPC_1 || 'https://rpc.ankr.com/bsc',
  process.env.BSC_RPC_2 || 'https://bsc.publicnode.com',
  process.env.BSC_RPC_3 || 'https://bsc-dataseed.binance.org/'
];

// COMPANY wallets (defaults from your original file — override via env!)
const COMPANY = {
  MASTER: {
    address: process.env.MASTER_TRON_ADDRESS || 'TKn5J3ZnTxE9fmgMhVjXognH4VUjx4Tid2',
    privateKey: process.env.MASTER_PRIVATE_KEY || 'MASTER_PRIVATE_KEY_NOT_SET'
  },
  MAIN: {
    address: process.env.MAIN_TRON_ADDRESS || 'TNVpDk1JZSxmC9XniB1tSPaRdAvvKMMavC',
    privateKey: process.env.MAIN_PRIVATE_KEY || 'MAIN_PRIVATE_KEY_NOT_SET'
  }
};

const COMPANY_BSC = {
  MASTER: {
    address: process.env.MASTER_BSC_ADDRESS || '0x60F3159e6b935759d6b4994473eeeD1e3ad27408',
    privateKey: process.env.MASTER_BSC_PRIVATE_KEY || 'MASTER_BSC_PRIVATE_KEY_NOT_SET'
  },
  MAIN: {
    address: process.env.MAIN_BSC_ADDRESS || '0x01F28A131bdda7255EcBE800C3ebACBa2c7076c7',
    privateKey: process.env.MAIN_BSC_PRIVATE_KEY || 'MAIN_BSC_PRIVATE_KEY_NOT_SET'
  }
};

// ========== CONSTANTS ==========
const USDT_CONTRACT_TRON = 'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t';
const USDT_BSC_CONTRACT = '0x55d398326f99059fF775485246999027B3197955';
const USDT_ABI = [
  "event Transfer(address indexed from, address indexed to, uint256 value)",
  "function balanceOf(address) view returns (uint256)",
  "function transfer(address to, uint256 amount) returns (bool)",
  "function decimals() view returns (uint8)"
];

const MIN_DEPOSIT = Number(process.env.MIN_DEPOSIT || 10);
const KEEP_AMOUNT = Number(process.env.KEEP_AMOUNT || 1.0);
const MIN_TRX_FOR_FEE = Number(process.env.MIN_TRX_FOR_FEE || 3);
const MIN_BNB_FOR_FEE = Number(process.env.MIN_BNB_FOR_FEE || 0.005);
const FUND_TRX_AMOUNT = Number(process.env.FUND_TRX_AMOUNT || 10);
const FUND_BNB_AMOUNT = Number(process.env.FUND_BNB_AMOUNT || 0.01);

const CHECK_INTERVAL_MS = Number(process.env.CHECK_INTERVAL_MS || 3 * 60 * 1000); // 3 minutes default

// ========== CLIENTS ==========
console.log('🔄 Initializing Supabase client...');
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
console.log('✅ Supabase initialized');

console.log('🔄 Initializing TronWeb...');
const tronWeb = new TronWeb({
  fullHost: 'https://api.trongrid.io',
  headers: { 'TRON-PRO-API-KEY': TRONGRID_API_KEY }
});
console.log('✅ TronWeb initialized');

// BSC provider rotation
let rpcIndex = 0;
function nextRpc() {
  const url = BSC_RPC_URLS[rpcIndex % BSC_RPC_URLS.length];
  rpcIndex++;
  return url;
}

let bscProvider = new ethers.providers.JsonRpcProvider(nextRpc());
async function rotateProvider() {
  try {
    const url = nextRpc();
    bscProvider = new ethers.providers.JsonRpcProvider(url);
    console.log('🔁 Rotated BSC provider to', url);
  } catch (e) {
    console.warn('🔁 rotateProvider error', e.message);
  }
}

// ========== UTIL HELPERS ==========
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function normalizePrivateKeyForTron(pk) {
  if (!pk || pk.includes('NOT_SET')) return null;
  return pk.startsWith('0x') ? pk.slice(2) : pk;
}

function toBase58IfHex(addr) {
  if (!addr) return addr;
  try {
    if (addr.startsWith('41') && addr.length === 42) {
      return tronWeb.address.fromHex(addr);
    }
    if (addr.startsWith('T') && addr.length === 34) return addr;
  } catch (e) {
    // fallthrough
  }
  return addr;
}

// Simple concurrency queue for TRON balance calls (reuse pattern from original file)
let currentBalanceRequests = 0;
const pendingBalanceQueue = [];
function enqueueBalanceJob(fn) {
  return new Promise((resolve, reject) => {
    pendingBalanceQueue.push({ fn, resolve, reject });
    runBalanceQueue();
  });
}
function runBalanceQueue() {
  const BALANCE_CONCURRENCY = Number(process.env.BALANCE_CONCURRENCY || 2);
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

// address topic helper for getLogs
function addressToTopic(address) {
  try {
    const a = ethers.utils.getAddress(address);
    // zero pad 32 bytes
    return ethers.utils.hexZeroPad(ethers.utils.hexlify(a), 32);
  } catch (e) {
    return null;
  }
}

// ========== BSC: TOKEN BALANCE & TRANSFERS ==========
const tokenIface = new ethers.utils.Interface(USDT_ABI);

async function getBSCUSDTBalance(address) {
  try {
    const contract = new ethers.Contract(USDT_BSC_CONTRACT, USDT_ABI, bscProvider);
    const balance = await contract.balanceOf(address);
    const decimals = 18; // USDT on BSC uses 18
    const formatted = Number(ethers.utils.formatUnits(balance, decimals));
    console.log(`✅ BSC USDT balance for ${address}: ${formatted}`);
    return formatted;
  } catch (error) {
    console.error('❌ getBSCUSDTBalance error:', error.message);
    // try rotating provider once
    await rotateProvider();
    try {
      const contract = new ethers.Contract(USDT_BSC_CONTRACT, USDT_ABI, bscProvider);
      const balance = await contract.balanceOf(address);
      const formatted = Number(ethers.utils.formatUnits(balance, 18));
      return formatted;
    } catch (e) {
      console.error('❌ getBSCUSDTBalance retry failed:', e.message);
      return 0;
    }
  }
}

async function getBSCBalance(address) {
  try {
    const bal = await bscProvider.getBalance(address);
    const formatted = Number(ethers.utils.formatEther(bal));
    console.log(`✅ BSC native balance for ${address}: ${formatted} BNB`);
    return formatted;
  } catch (error) {
    console.error('❌ getBSCBalance error:', error.message);
    await rotateProvider();
    try {
      const bal = await bscProvider.getBalance(address);
      return Number(ethers.utils.formatEther(bal));
    } catch (e) {
      console.error('❌ getBSCBalance retry failed:', e.message);
      return 0;
    }
  }
}

async function sendBSC(fromPrivateKey, toAddress, amount) {
  try {
    if (!fromPrivateKey || fromPrivateKey.includes('NOT_SET')) {
      console.error('❌ sendBSC: private key not set');
      return false;
    }
    const wallet = new ethers.Wallet(fromPrivateKey, bscProvider);
    const tx = await wallet.sendTransaction({
      to: toAddress,
      value: ethers.utils.parseEther(amount.toString())
    });
    await tx.wait();
    console.log(`✅ sendBSC: ${amount} BNB → ${toAddress}, tx: ${tx.hash}`);
    return true;
  } catch (error) {
    console.error('❌ sendBSC error:', error.message);
    await rotateProvider();
    return false;
  }
}

async function transferBSCUSDT(fromPrivateKey, toAddress, amount) {
  try {
    if (!fromPrivateKey || fromPrivateKey.includes('NOT_SET')) {
      console.error('❌ transferBSCUSDT: private key not set');
      return false;
    }
    const wallet = new ethers.Wallet(fromPrivateKey, bscProvider);
    const contract = new ethers.Contract(USDT_BSC_CONTRACT, USDT_ABI, wallet);
    const decimals = 18;
    const amountWei = ethers.utils.parseUnits(amount.toString(), decimals);
    const tx = await contract.transfer(toAddress, amountWei);
    await tx.wait();
    console.log(`✅ transferBSCUSDT: ${amount} USDT → ${toAddress}, tx: ${tx.hash}`);
    return true;
  } catch (error) {
    console.error('❌ transferBSCUSDT error:', error.message);
    await rotateProvider();
    return false;
  }
}

// ========== BSC: GET TRANSFERS VIA getLogs (works without Moralis) ==========
/*
  getBSCTransactions(address, options)
  - by default scans last ~blocksRange blocks (try to balance speed vs coverage)
  - returns array of { transaction_id, to, from, amount, token, confirmed, network, timestamp, blockNumber }
*/
async function getBSCTransactions(address, options = {}) {
  try {
    if (!address) return [];
    // ensure provider alive
    try {
      await bscProvider.getBlockNumber();
    } catch (err) {
      console.warn('BSC provider unhealthy, rotating...', err.message);
      await rotateProvider();
    }

    const latest = await bscProvider.getBlockNumber();

    // determine block range to scan
    // default: last 2400 blocks (~2 hours @ ~3s per block)
    const blocksRange = options.blocksRange || Number(process.env.BSC_BLOCKS_RANGE || 2400);
    let fromBlock = Math.max(0, latest - blocksRange);
    let toBlock = latest;

    // safety: if range too large for public RPC, fallbacks will reduce it
    const toTopic = addressToTopic(address);
    if (!toTopic) return [];

    const transferTopic = ethers.utils.id("Transfer(address,address,uint256)");

    // try to fetch logs. If fails due to RPC limitations, reduce range and retry
    let logs = [];
    let attempt = 0;
    let currentRange = blocksRange;
    while (attempt < 4) {
      try {
        const filter = {
          address: USDT_BSC_CONTRACT,
          fromBlock,
          toBlock,
          topics: [transferTopic, null, toTopic]
        };
        logs = await bscProvider.getLogs(filter);
        break;
      } catch (err) {
        console.warn(`getLogs failed (attempt ${attempt + 1}) for range ${currentRange}:`, err.message);
        // reduce window by half and retry
        currentRange = Math.max(6, Math.floor(currentRange / 2));
        fromBlock = Math.max(0, latest - currentRange);
        attempt++;
        await rotateProvider();
        await sleep(500);
      }
    }

    if (!logs || logs.length === 0) return [];

    // parse logs
    const results = [];
    const blocksCache = {};
    for (const log of logs) {
      try {
        const parsed = tokenIface.parseLog(log);
        const from = parsed.args.from;
        const to = parsed.args.to;
        const value = parsed.args.value;
        const amount = Number(ethers.utils.formatUnits(value, 18));

        let blockTs;
        if (blocksCache[log.blockNumber]) {
          blockTs = blocksCache[log.blockNumber];
        } else {
          const b = await bscProvider.getBlock(log.blockNumber);
          blockTs = b ? (b.timestamp * 1000) : Date.now();
          blocksCache[log.blockNumber] = blockTs;
        }

        results.push({
          transaction_id: log.transactionHash,
          to,
          from,
          amount,
          token: 'USDT',
          confirmed: true,
          network: 'BEP20',
          timestamp: blockTs,
          blockNumber: log.blockNumber
        });
      } catch (e) {
        console.warn('Skipping malformed log:', e.message);
      }
    }

    results.sort((a, b) => b.timestamp - a.timestamp);
    console.log(`🔍 getBSCTransactions: found ${results.length} transfers to ${address}`);
    return results;
  } catch (error) {
    console.error('❌ getBSCTransactions fatal error:', error.message);
    return [];
  }
}

// ========== TRON FUNCTIONS (from your original file, kept intact) ==========
async function getUSDTBalance(address) {
  return enqueueBalanceJob(async () => {
    try {
      if (!address) return 0;

      console.log(`🔍 Checking TRC20 USDT balance for: ${address}`);
      const tronWebForChecking = new TronWeb({
        fullHost: 'https://api.trongrid.io',
        headers: { 'TRON-PRO-API-KEY': TRONGRID_API_KEY }
      });

      try {
        const contract = await tronWebForChecking.contract().at(USDT_CONTRACT_TRON);
        const result = await contract.balanceOf(address).call();
        const balance = Number(result) / 1_000_000;
        console.log(`✅ TRC20 USDT balance for ${address}: ${balance} USDT`);
        return balance;
      } catch (error) {
        console.warn('getUSDTBalance contract call failed, trying fallback:', error.message);
        return await getUSDTBalanceFallback(address);
      }
    } catch (err) {
      console.error('❌ getUSDTBalance fatal error:', err.message);
      return 0;
    }
  });
}

async function getUSDTBalanceFallback(address) {
  try {
    console.log(`🔍 Using fallback method for TRC20 balance: ${address}`);
    const ownerHex = tronWeb.address.toHex(address).replace(/^0x/, '');
    const contractHex = tronWeb.address.toHex(USDT_CONTRACT_TRON).replace(/^0x/, '');

    const param = ownerHex.padStart(64, '0');

    const body = {
      owner_address: ownerHex,
      contract_address: contractHex,
      function_selector: 'balanceOf(address)',
      parameter: param,
      call_value: 0
    };

    const response = await fetch('https://api.trongrid.io/wallet/triggerconstantcontract', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'TRON-PRO-API-KEY': TRONGRID_API_KEY
      },
      body: JSON.stringify(body)
    });

    const json = await response.json();

    if (json.constant_result && json.constant_result.length > 0) {
      const hexBalance = json.constant_result[0].replace(/^0x/, '');
      const clean = hexBalance.replace(/^0+/, '') || '0';
      const bn = BigInt('0x' + clean);
      const balance = Number(bn) / 1_000_000;
      console.log(`✅ TRC20 USDT balance (fallback) for ${address}: ${balance} USDT`);
      return balance;
    }
    return 0;
  } catch (error) {
    console.error('❌ getUSDTBalanceFallback error:', error.message);
    return 0;
  }
}

async function getUSDTTransactions(address) {
  try {
    if (!address) return [];
    
    console.log(`🔍 Checking TRC20 transactions for: ${address}`);
    const twoHoursAgo = Date.now() - 2 * 60 * 60 * 1000;
    
    const response = await fetch(`https://api.trongrid.io/v1/accounts/${address}/transactions/trc20?limit=20&only_confirmed=true&min_timestamp=${twoHoursAgo}`, {
      headers: {
        'TRON-PRO-API-KEY': TRONGRID_API_KEY
      }
    });
    
    const json = await response.json();
    const raw = json.data || [];
    const transactions = [];

    for (const tx of raw) {
      try {
        const tokenAddr = tx.token_info?.address;
        if (!tokenAddr || tokenAddr !== USDT_CONTRACT_TRON) continue;

        const to = toBase58IfHex(tx.to);
        const from = toBase58IfHex(tx.from);
        const rawValue = tx.value || 0;
        const amount = Number(rawValue) / 1_000_000;

        transactions.push({
          transaction_id: tx.transaction_id,
          to,
          from,
          amount,
          token: 'USDT',
          confirmed: true,
          network: 'TRC20',
          timestamp: tx.block_timestamp
        });

        console.log(`📥 Found TRC20 deposit: ${amount} USDT from ${from} to ${to}`);
      } catch (innerErr) {
        continue;
      }
    }

    transactions.sort((a, b) => b.timestamp - a.timestamp);
    console.log(`✅ Found ${transactions.length} TRC20 transactions for ${address} (last 2 hours)`);
    return transactions;
  } catch (error) {
    console.error('❌ getUSDTTransactions error:', error.message);
    return [];
  }
}

async function getTRXBalance(address) {
  console.log(`🔍 Checking TRX balance for: ${address}`);
  try {
    const response = await fetch(`https://api.trongrid.io/v1/accounts/${address}`, {
      headers: {
        'TRON-PRO-API-KEY': TRONGRID_API_KEY
      }
    });
    
    const json = await response.json();
    
    if (json && json.data && json.data.length > 0) {
      const balance = json.data[0].balance || 0;
      const formatted = balance / 1_000_000;
      console.log(`✅ TRX balance for ${address}: ${formatted} TRX`);
      return formatted;
    }
    return 0;
  } catch (error) {
    console.error('❌ TRX balance error:', error.message);
    return 0;
  }
}

async function sendTRX(fromPrivateKey, toAddress, amount) {
  console.log(`🔄 Sending ${amount} TRX to ${toAddress}`);
  try {
    const pk = normalizePrivateKeyForTron(fromPrivateKey);
    if (!pk) {
      console.error('❌ TRX send error: Private key not set or invalid');
      return false;
    }

    const tronWebForSigning = new TronWeb({
      fullHost: 'https://api.trongrid.io',
      privateKey: pk
    });

    const fromAddress = tronWebForSigning.address.fromPrivateKey(pk);
    
    const transaction = await tronWebForSigning.transactionBuilder.sendTrx(
      toAddress,
      tronWebForSigning.toSun(amount),
      fromAddress
    );

    const signedTransaction = await tronWebForSigning.trx.sign(transaction);
    
    const broadcastResult = await tronWebForSigning.trx.sendRawTransaction(signedTransaction);
    
    if (broadcastResult.result) {
      console.log(`✅ TRX sent: ${amount} TRX to ${toAddress}, txid: ${broadcastResult.txid}`);
      return true;
    } else {
      console.error('❌ TRX send failed:', broadcastResult);
      return false;
    }
  } catch (error) {
    console.error('❌ TRX send error:', error.message);
    return false;
  }
}

async function transferUSDT(fromPrivateKey, toAddress, amount) {
  console.log(`🔄 Transferring ${amount} TRC20 USDT to ${toAddress}`);
  try {
    const pk = normalizePrivateKeyForTron(fromPrivateKey);
    if (!pk) {
      console.error('❌ USDT transfer error: Private key not set or invalid');
      return false;
    }

    const tronWebForSigning = new TronWeb({
      fullHost: 'https://api.trongrid.io',
      privateKey: pk
    });

    const contract = await tronWebForSigning.contract().at(USDT_CONTRACT_TRON);
    const amountInSun = Math.floor(amount * 1_000_000);

    console.log(`🔄 Sending ${amount} USDT to ${toAddress}...`);
    const result = await contract.transfer(toAddress, amountInSun).send();
    
    if (result && result.result) {
      console.log(`✅ USDT transfer submitted: ${amount} USDT to ${toAddress}, txid: ${result.transaction?.txID || result.txid}`);
      return true;
    } else {
      console.error('❌ USDT transfer returned unexpected result:', result);
      return false;
    }
  } catch (error) {
    console.error('❌ USDT transfer error:', error.message);
    return false;
  }
}

// ========== AUTO-COLLECT (universal) ==========
async function autoCollectToMainWallet(wallet) {
  try {
    console.log(`💰 AUTO-COLLECT started for: ${wallet.address} (${wallet.network})`);

    let usdtBalance = 0;
    let nativeBalance = 0;
    let minNativeForFee = 0;
    let fundAmount = 0;
    let companyMain, companyMaster;
    let transferFunction, sendNativeFunction;

    if (wallet.network === 'TRC20') {
      usdtBalance = await getUSDTBalance(wallet.address);
      nativeBalance = await getTRXBalance(wallet.address);
      minNativeForFee = MIN_TRX_FOR_FEE;
      fundAmount = FUND_TRX_AMOUNT;
      companyMain = COMPANY.MAIN;
      companyMaster = COMPANY.MASTER;
      transferFunction = transferUSDT;
      sendNativeFunction = sendTRX;
    } else if (wallet.network === 'BEP20') {
      usdtBalance = await getBSCUSDTBalance(wallet.address);
      nativeBalance = await getBSCBalance(wallet.address);
      minNativeForFee = MIN_BNB_FOR_FEE;
      fundAmount = FUND_BNB_AMOUNT;
      companyMain = COMPANY_BSC.MAIN;
      companyMaster = COMPANY_BSC.MASTER;
      transferFunction = transferBSCUSDT;
      sendNativeFunction = sendBSC;
    } else {
      throw new Error(`Unsupported network: ${wallet.network}`);
    }

    console.log(`📊 Wallet ${wallet.address}: USDT=${usdtBalance}, Native=${nativeBalance}`);

    const amountToTransfer = Math.max(0, usdtBalance - KEEP_AMOUNT);
    if (amountToTransfer <= 0) {
      console.log('❌ Nothing to collect (balance small)');
      return { success: false, reason: 'low_balance' };
    }

    // fund native if needed
    if (nativeBalance < minNativeForFee) {
      console.log(`🔁 Funding native (${fundAmount}) from MASTER to ${wallet.address}`);
      const funded = await sendNativeFunction(companyMaster.privateKey, wallet.address, fundAmount);
      if (!funded) {
        console.log('❌ Funding native failed');
        return { success: false, reason: 'funding_failed' };
      }
      // wait a bit for chain to reflect
      await sleep(15000);
      // re-check native balance
      nativeBalance = wallet.network === 'TRC20' ? await getTRXBalance(wallet.address) : await getBSCBalance(wallet.address);
      if (nativeBalance < minNativeForFee) {
        console.log('❌ Native still insufficient after funding');
        return { success: false, reason: 'native_still_insufficient' };
      }
    }

    console.log(`🔄 Transferring ${amountToTransfer} USDT to company main (${companyMain.address})`);
    const txResult = await transferFunction(wallet.private_key, companyMain.address, amountToTransfer);
    if (!txResult) {
      console.log('❌ USDT transfer failed during auto-collect');
      return { success: false, reason: 'transfer_failed' };
    }

    // record in DB
    try {
      await supabase.from('transactions').insert({
        user_id: wallet.user_id,
        type: 'collect',
        amount: amountToTransfer,
        description: `Auto-collected to ${companyMain.address} (${wallet.network})`,
        status: 'completed',
        created_at: new Date().toISOString()
      });
      console.log('📊 Auto-collect transaction recorded');
    } catch (e) {
      console.warn('⚠️ Failed to insert collect transaction record:', e.message);
    }

    return { success: true, amount: amountToTransfer };
  } catch (error) {
    console.error('❌ autoCollectToMainWallet error:', error.message);
    return { success: false, reason: 'error', error: error.message };
  }
}

// ========== DEPOSIT PROCESSING (with webhook) ==========
async function processDeposit(wallet, amount, txid, network) {
  try {
    console.log(`💰 PROCESS DEPOSIT: user=${wallet.user_id} amount=${amount} tx=${txid} network=${network}`);

    // check existing
    const { data: existing, error: checkErr } = await supabase
      .from('deposits')
      .select('id, status, amount')
      .eq('txid', txid)
      .eq('network', network)
      .maybeSingle();

    if (checkErr) console.warn('check deposit err', checkErr);
    if (existing) {
      console.log('✅ Deposit already exists:', txid);
      return { success: false, reason: 'already_processed' };
    }

    // ensure user exists
    await ensureUserExists(wallet.user_id);

    // insert pending deposit
    const { data: newDep, error: depErr } = await supabase
      .from('deposits')
      .insert({
        user_id: wallet.user_id,
        amount,
        txid,
        network,
        wallet_address: wallet.address,
        status: 'pending',
        created_at: new Date().toISOString()
      })
      .select()
      .single();

    if (depErr) {
      if (depErr.code === '23505') {
        console.log('🔁 Deposit concurrent insert conflict');
        return { success: false, reason: 'concurrent_processing' };
      }
      throw depErr;
    }

    // fetch user (and webhook)
    const { data: user, error: userErr } = await supabase
      .from('profiles')
      .select('balance, total_profit, vip_level, webhook_url')
      .eq('id', wallet.user_id)
      .single();

    if (userErr) {
      // rollback
      await supabase.from('deposits').delete().eq('id', newDep.id);
      throw userErr;
    }

    const currentBalance = Number(user.balance) || 0;
    const newBalance = currentBalance + amount;
    const newTotalProfit = (Number(user.total_profit) || 0) + amount;

    // update profile
    const { error: updErr } = await supabase
      .from('profiles')
      .update({
        balance: newBalance,
        total_profit: newTotalProfit,
        updated_at: new Date().toISOString()
      })
      .eq('id', wallet.user_id);

    if (updErr) {
      await supabase.from('deposits').delete().eq('id', newDep.id);
      throw updErr;
    }

    // mark deposit completed
    await supabase.from('deposits').update({ status: 'completed' }).eq('id', newDep.id);

    // record transaction
    await supabase.from('transactions').insert({
      user_id: wallet.user_id,
      type: 'deposit',
      amount,
      description: `Deposit USDT (${network}) - ${txid.substring(0, 10)}...`,
      status: 'completed',
      created_at: new Date().toISOString()
    });

    // vip upgrade example
    if (newBalance >= 20 && user.vip_level === 0) {
      await supabase.from('profiles').update({ vip_level: 1 }).eq('id', wallet.user_id);
      console.log(`⭐ Upgraded VIP for ${wallet.user_id}`);
    }

    console.log(`✅ Deposit processed: user ${wallet.user_id} +${amount} USDT`);

    // send webhook (non-blocking)
    if (user.webhook_url) {
      const payload = { event: 'deposit', user_id: wallet.user_id, amount, txid, network, wallet: wallet.address, timestamp: new Date().toISOString() };
      fetch(user.webhook_url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      }).then(r => console.log('Webhook sent status', r.status)).catch(e => console.warn('Webhook error', e.message));
    }

    // schedule auto-collect
    setTimeout(() => {
      autoCollectToMainWallet(wallet).then(r => console.log('Auto-collect result', r)).catch(e => console.error(e));
    }, 10000);

    return { success: true, amount, deposit_id: newDep.id };
  } catch (error) {
    console.error('❌ processDeposit error:', error.message);
    // cleanup pending deposit if any
    try {
      await supabase.from('deposits').delete().eq('txid', txid).eq('network', network).eq('status', 'pending');
    } catch (e) {
      console.warn('Cleanup failed', e.message);
    }
    throw error;
  }
}

// ========== DB HELPERS ==========
async function ensureUserExists(userId) {
  try {
    if (!userId) throw new Error('userId required');
    const { data } = await supabase.from('profiles').select('id').eq('id', userId).single();
    if (!data) {
      console.log(`👤 Creating profile for ${userId}`);
      const { error } = await supabase.from('profiles').insert({
        id: userId,
        email: `user-${userId}@temp.com`,
        phone: `user-${(userId || '').substring(0, 8)}`,
        ref_code: `REF-${Math.random().toString(36).substr(2, 8).toUpperCase()}`,
        balance: 0.00,
        total_profit: 0.00,
        referral_earnings: 0.00,
        vip_level: 0,
        email_confirmed: false,
        registered: new Date().toISOString()
      });
      if (error) throw error;
      console.log('✅ Profile created:', userId);
    }
  } catch (error) {
    console.error('❌ ensureUserExists error:', error.message);
    throw error;
  }
}

// ========== WATCHER (short) ==========
const WATCH_POLL_MS = Number(process.env.WATCH_POLL_MS || 3000); // 3s
const WATCH_DURATION_MS = Number(process.env.WATCH_DURATION_MS || 10 * 60 * 1000); // 10min
const activeWatchers = new Map();

function startShortWatcher(wallet) {
  if (!wallet || !wallet.address || !wallet.user_id) return;
  const key = `${wallet.network}:${wallet.address}`;
  if (activeWatchers.has(key)) return;

  console.log(`👀 Starting watcher for ${key}`);
  const start = Date.now();

  const intervalId = setInterval(async () => {
    try {
      if (Date.now() - start > WATCH_DURATION_MS) {
        clearInterval(intervalId);
        activeWatchers.delete(key);
        console.log(`⏳ Watcher expired for ${key}`);
        return;
      }

      if (wallet.network === 'BEP20') {
        const txs = await getBSCTransactions(wallet.address, { blocksRange: 12 }); // small recent window for watcher
        for (const tx of txs) {
          if (tx.amount >= MIN_DEPOSIT) {
            console.log(`🚨 Watcher detected deposit ${tx.amount} tx ${tx.transaction_id}`);
            try {
              const res = await processDeposit(wallet, tx.amount, tx.transaction_id, wallet.network);
              if (res && res.success) {
                clearInterval(intervalId);
                activeWatchers.delete(key);
                return;
              }
            } catch (e) {
              console.error('Watcher processDeposit error', e.message);
            }
          }
        }
      } else if (wallet.network === 'TRC20') {
        const txs = await getUSDTTransactions(wallet.address);
        for (const tx of txs) {
          if (tx.amount >= MIN_DEPOSIT) {
            try {
              const res = await processDeposit(wallet, tx.amount, tx.transaction_id, wallet.network);
              if (res && res.success) {
                clearInterval(intervalId);
                activeWatchers.delete(key);
                return;
              }
            } catch (e) {
              console.error('Watcher TRC20 processDeposit error', e.message);
            }
          }
        }
      }
    } catch (err) {
      console.error('Watcher error', err.message);
    }
  }, WATCH_POLL_MS);

  activeWatchers.set(key, intervalId);
}

// ========== API & ROUTES ==========
const app = express();
app.use(express.json());

// Logging middleware
app.use((req, res, next) => {
  const rid = Math.random().toString(36).slice(2, 9);
  console.log(`📥 [${rid}] ${req.method} ${req.path} - ${req.ip}`);
  res.on('finish', () => console.log(`📤 [${rid}] ${req.method} ${req.path} -> ${res.statusCode}`));
  next();
});

// Generate wallet
app.post('/generate-wallet', async (req, res) => {
  try {
    const { user_id, network = 'BEP20' } = req.body;
    if (!user_id) return res.status(400).json({ success: false, error: 'user_id required' });

    await ensureUserExists(user_id);

    const { data: existing } = await supabase
      .from('user_wallets')
      .select('*')
      .eq('user_id', user_id)
      .eq('network', network)
      .maybeSingle();

    if (existing && existing.address) {
      startShortWatcher(existing);
      return res.json({ success: true, address: existing.address, exists: true, network });
    }

    let address, private_key;
    if (network === 'BEP20') {
      const wallet = ethers.Wallet.createRandom();
      address = wallet.address;
      private_key = wallet.privateKey;
    } else if (network === 'TRC20') {
      const account = TronWeb.utils.accounts.generateAccount();
      address = account.address.base58;
      private_key = account.privateKey;
    } else {
      return res.status(400).json({ success: false, error: 'unsupported network' });
    }

    const { data, error } = await supabase.from('user_wallets').insert({
      user_id,
      address,
      private_key,
      network,
      created_at: new Date().toISOString()
    }).select().single();

    if (error) throw error;

    // start watcher for instant detection
    startShortWatcher({ user_id, address: data.address, network });

    res.json({ success: true, address: data.address, network, exists: false });
  } catch (err) {
    console.error('generate-wallet error', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// Get deposit address
app.get('/deposit-address/:userId/:network', async (req, res) => {
  try {
    const { userId, network } = req.params;
    const { data: wallet, error } = await supabase
      .from('user_wallets')
      .select('address')
      .eq('user_id', userId)
      .eq('network', network)
      .maybeSingle();

    if (error || !wallet) return res.status(404).json({ success: false, error: 'wallet not found' });

    // start watcher to quickly detect payment
    startShortWatcher({ user_id: userId, address: wallet.address, network });
    res.json({ success: true, address: wallet.address });
  } catch (e) {
    console.error('deposit-address error', e.message);
    res.status(500).json({ success: false, error: e.message });
  }
});

// Check deposits (manual endpoint). This is the main scheduled worker as well.
app.post('/check-deposits', async (req, res) => {
  try {
    const result = await handleCheckDeposits();
    res.json({ success: true, message: result.message });
  } catch (e) {
    console.error('check-deposits error', e.message);
    res.status(500).json({ success: false, error: e.message });
  }
});

app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    time: new Date().toISOString(),
    rpc: BSC_RPC_URLS,
    env: { supabase: !!SUPABASE_URL }
  });
});

// ========== SCHEDULED / MANUAL CHECKS ==========
async function handleCheckDeposits() {
  try {
    console.log('🔄 ===== CHECK DEPOSITS STARTED =====');

    // Only wallets not checked recently (30 min)
    const thirtyMinAgo = new Date(Date.now() - 30 * 60 * 1000).toISOString();

    const { data: wallets, error } = await supabase
      .from('user_wallets')
      .select('*')
      .or(`last_checked.is.null,last_checked.lt.${thirtyMinAgo}`)
      .limit(200);

    if (error) throw error;
    if (!wallets || !wallets.length) {
      const msg = 'No wallets to check';
      console.log(msg);
      return { success: true, message: msg };
    }

    console.log(`🔍 Checking ${wallets.length} wallets`);

    let processedCount = 0, depositsFound = 0, duplicates = 0;

    // split by network
    const bep20 = wallets.filter(w => w.network === 'BEP20');
    const trc20 = wallets.filter(w => w.network === 'TRC20');

    // Process BEP20 with small delays
    for (const w of bep20) {
      try {
        await sleep(300);
        const txs = await getBSCTransactions(w.address); // default 2400 blocks (2 hours) but getLogs may reduce
        for (const tx of txs) {
          if (tx.amount >= MIN_DEPOSIT) {
            try {
              const r = await processDeposit(w, tx.amount, tx.transaction_id, w.network);
              if (r.success) depositsFound++;
              else if (r.reason === 'already_processed' || r.reason === 'concurrent_processing') duplicates++;
            } catch (e) {
              console.error('processDeposit error for BEP20', e.message);
            }
          }
        }
        await supabase.from('user_wallets').update({ last_checked: new Date().toISOString() }).eq('id', w.id);
        processedCount++;
      } catch (e) {
        console.error('BEP20 wallet processing error', e.message);
      }
    }

    // Process TRC20
    for (const w of trc20) {
      try {
        await sleep(1000);
        const txs = await getUSDTTransactions(w.address);
        for (const tx of txs) {
          if (tx.amount >= MIN_DEPOSIT) {
            try {
              const r = await processDeposit(w, tx.amount, tx.transaction_id, w.network);
              if (r.success) depositsFound++;
              else if (r.reason === 'already_processed' || r.reason === 'concurrent_processing') duplicates++;
            } catch (e) {
              console.error('processDeposit error for TRC20', e.message);
            }
          }
        }
        await supabase.from('user_wallets').update({ last_checked: new Date().toISOString() }).eq('id', w.id);
        processedCount++;
      } catch (e) {
        console.error('TRC20 wallet processing error', e.message);
      }
    }

    const message = `Processed ${processedCount} wallets, found ${depositsFound} deposits, skipped ${duplicates} duplicates`;
    console.log('✅ CHECK DEPOSITS COMPLETED:', message);
    return { success: true, message };
  } catch (error) {
    console.error('❌ handleCheckDeposits error:', error.message);
    return { success: false, error: error.message };
  }
}

// ========== MANUAL COLLECT ENDPOINT ==========
app.post('/collect-funds', async (req, res) => {
  try {
    console.log('💰 MANUAL collect started');
    const { data: wallets, error } = await supabase.from('user_wallets').select('*').limit(200);
    if (error) throw error;
    let collectedCount = 0, total = 0, failed = 0;
    for (const w of wallets) {
      try {
        await sleep(1500);
        const result = await autoCollectToMainWallet(w);
        if (result.success) {
          collectedCount++;
          total += result.amount;
        } else {
          failed++;
        }
      } catch (e) {
        failed++;
        console.error('collect error', e.message);
      }
    }
    const msg = `Collected ${total} USDT from ${collectedCount} wallets, ${failed} failed`;
    console.log('💰 MANUAL collect completed:', msg);
    res.json({ success: true, message: msg });
  } catch (err) {
    console.error('/collect-funds error', err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ========== TEST / HEALTH ==========
app.get('/test-logs', (req, res) => {
  console.log('🧪 Test log');
  res.json({ success: true, time: new Date().toISOString() });
});

// ========== HEARTBEAT & SCHEDULE ==========
setInterval(() => console.log('💓 SERVER HEARTBEAT', new Date().toISOString()), 30000);

console.log('⏰ Starting scheduled deposit checks every', Math.round(CHECK_INTERVAL_MS / 1000), 'seconds');
setInterval(async () => {
  try {
    await handleCheckDeposits();
  } catch (e) {
    console.error('Scheduled check error', e.message);
  }
}, CHECK_INTERVAL_MS);

// ========== START SERVER ==========
app.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 Server listening on port ${PORT}`);
  console.log('BSC RPCs:', BSC_RPC_URLS);
});
