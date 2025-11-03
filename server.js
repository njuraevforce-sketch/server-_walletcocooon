const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const TronWeb = require('tronweb');
const { ethers } = require('ethers');

const app = express();
const PORT = process.env.PORT || 3000;

// ========== ENVIRONMENT VARIABLES ==========
// УБИРАЕМ ДЕФОЛТНЫЕ ЗНАЧЕНИЯ - ТОЛЬКО ИЗ ПЕРЕМЕННЫХ ОКРУЖЕНИЯ
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const TRONGRID_API_KEY = process.env.TRONGRID_API_KEY;
const MORALIS_API_KEY = process.env.MORALIS_API_KEY;

// Проверка обязательных переменных
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error('❌ CRITICAL: Missing Supabase environment variables');
  console.error('SUPABASE_URL:', SUPABASE_URL ? 'SET' : 'MISSING');
  console.error('SUPABASE_SERVICE_ROLE_KEY:', SUPABASE_SERVICE_ROLE_KEY ? 'SET' : 'MISSING');
  process.exit(1);
}

// ========== BSC RPC CONFIGURATION ==========
const BSC_RPC_URLS = [
  'https://bsc-dataseed.binance.org/',
  'https://bsc-dataseed1.defibit.io/',
  'https://bsc-dataseed1.ninicoin.io/',
  'https://bsc-dataseed2.ninicoin.io/',
];

let currentRpcIndex = 0;
function getNextBscRpc() {
  const rpc = BSC_RPC_URLS[currentRpcIndex];
  currentRpcIndex = (currentRpcIndex + 1) % BSC_RPC_URLS.length;
  return rpc;
}

let bscProvider = new ethers.providers.JsonRpcProvider(getNextBscRpc());

// COMPANY wallets - TRC20
const COMPANY = {
  MASTER: {
    address: 'TKn5J3ZnTxE9fmgMhVjXognH4VUjx4Tid2',
    privateKey: process.env.MASTER_PRIVATE_KEY
  },
  MAIN: {
    address: 'TNVpDk1JZSxmC9XniB1tSPaRdAvvKMMavC',
    privateKey: process.env.MAIN_PRIVATE_KEY
  }
};

// COMPANY wallets - BEP20
const COMPANY_BSC = {
  MASTER: {
    address: '0x60F3159e6b935759d6b4994473eeeD1e3ad27408',
    privateKey: process.env.MASTER_BSC_PRIVATE_KEY
  },
  MAIN: {
    address: '0x01F28A131bdda7255EcBE800C3ebACBa2c7076c7',
    privateKey: process.env.MAIN_BSC_PRIVATE_KEY
  }
};

// Проверка приватных ключей
if (!process.env.MASTER_PRIVATE_KEY || !process.env.MAIN_PRIVATE_KEY) {
  console.warn('⚠️ WARNING: TRC20 private keys not set');
}
if (!process.env.MASTER_BSC_PRIVATE_KEY || !process.env.MAIN_BSC_PRIVATE_KEY) {
  console.warn('⚠️ WARNING: BSC private keys not set');
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

// Проверка подключения к Supabase
async function testSupabaseConnection() {
  try {
    const { data, error } = await supabase.from('users').select('count').limit(1);
    if (error) throw error;
    console.log('✅ Supabase connection: SUCCESS');
    return true;
  } catch (error) {
    console.error('❌ Supabase connection failed:', error.message);
    return false;
  }
}

const tronWeb = new TronWeb({
  fullHost: 'https://api.trongrid.io',
  headers: { 'TRON-PRO-API-KEY': TRONGRID_API_KEY }
});

app.use(express.json());
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', '*');
  res.header('Access-Control-Allow-Methods', '*');
  next();
});

// ========== CONSTANTS ==========
const USDT_CONTRACT = 'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t';
const USDT_BSC_CONTRACT = '0x55d398326f99059fF775485246999027B3197955';
const USDT_ABI = [
  "function balanceOf(address) view returns (uint256)",
  "function transfer(address to, uint256 amount) returns (bool)"
];

const MIN_DEPOSIT = 10;
const KEEP_AMOUNT = 1.0;
const MIN_TRX_FOR_FEE = 3;
const MIN_BNB_FOR_FEE = 0.005;
const FUND_TRX_AMOUNT = 10;
const FUND_BNB_AMOUNT = 0.01;

// Throttling / concurrency
const BALANCE_CONCURRENCY = Number(process.env.BALANCE_CONCURRENCY || 2);
const CHECK_INTERVAL_MS = Number(process.env.CHECK_INTERVAL_MS || 2 * 60 * 1000);

// ========== HELPERS ==========
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function normalizePrivateKeyForTron(pk) {
  if (!pk) return null;
  return pk.startsWith('0x') ? pk.slice(2) : pk;
}

function toBase58IfHex(addr) {
  if (!addr) return addr;
  if (addr.startsWith('41') && addr.length === 42) {
    try {
      return tronWeb.address.fromHex(addr);
    } catch (e) {
      return addr;
    }
  }
  if (addr.startsWith('T') && addr.length === 34) return addr;
  return addr;
}

// ========== MORALIS API FUNCTIONS ==========
async function moralisRequest(endpoint, retries = 3) {
  if (!MORALIS_API_KEY) {
    console.error('❌ Moralis API key not configured');
    return { result: [] };
  }

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const response = await fetch(`https://deep-index.moralis.io/api/v2${endpoint}`, {
        headers: {
          'X-API-Key': MORALIS_API_KEY,
          'Accept': 'application/json'
        }
      });

      if (response.status === 429) {
        if (attempt < retries) {
          const backoff = 2000 * Math.pow(2, attempt);
          console.warn(`⚠️ Moralis rate limit, waiting ${backoff}ms...`);
          await sleep(backoff);
          continue;
        }
      }

      const data = await response.json();
      
      if (response.ok) {
        return data;
      } else {
        console.log(`❌ Moralis API error: ${data.message || response.statusText}`);
        return { result: [] };
      }
    } catch (error) {
      console.error(`❌ Moralis request attempt ${attempt + 1} failed:`, error.message);
      if (attempt === retries) throw error;
      await sleep(1000 * (attempt + 1));
    }
  }
  return { result: [] };
}

// ========== BSC FUNCTIONS ==========
async function getBSCUSDTBalance(address) {
  try {
    const contract = new ethers.Contract(USDT_BSC_CONTRACT, USDT_ABI, bscProvider);
    const balance = await contract.balanceOf(address);
    return Number(ethers.utils.formatUnits(balance, 18));
  } catch (error) {
    console.error('❌ BSC USDT balance error:', error.message);
    return 0;
  }
}

async function getBSCTransactions(address) {
  try {
    if (!address) return [];

    console.log(`🔍 Checking BSC transactions via Moralis API: ${address}`);
    
    const data = await moralisRequest(`/${address}/erc20/transfers?chain=bsc&limit=50`);
    
    if (data.result && Array.isArray(data.result)) {
      console.log(`✅ Moralis API: Found ${data.result.length} token transfers for ${address}`);
      
      const transactions = [];
      for (const tx of data.result) {
        try {
          if (tx.address && tx.address.toLowerCase() === USDT_BSC_CONTRACT.toLowerCase() &&
              tx.to_address && tx.to_address.toLowerCase() === address.toLowerCase()) {
            
            const amount = Number(tx.value) / Math.pow(10, tx.decimals || 18);
            
            transactions.push({
              transaction_id: tx.transaction_hash,
              to: tx.to_address,
              from: tx.from_address,
              amount: amount,
              token: 'USDT',
              confirmed: true,
              network: 'BEP20',
              timestamp: new Date(tx.block_timestamp).getTime()
            });
          }
        } catch (e) { 
          console.warn('Skipping malformed BSC transaction:', e.message);
          continue; 
        }
      }
      
      transactions.sort((a, b) => b.timestamp - a.timestamp);
      return transactions;
    } else {
      console.log(`ℹ️ Moralis API: No transactions found for ${address}`);
      return [];
    }
  } catch (error) {
    console.error('❌ BSC transactions error:', error.message);
    return [];
  }
}

// ========== TRON FUNCTIONS ==========
async function getUSDTTransactions(address) {
  try {
    if (!address) return [];
    
    const response = await fetch(`https://api.trongrid.io/v1/accounts/${address}/transactions/trc20?limit=50&only_confirmed=true`, {
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
        if (!tokenAddr || tokenAddr !== USDT_CONTRACT) continue;

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
      } catch (innerErr) {
        continue;
      }
    }

    transactions.sort((a, b) => b.timestamp - a.timestamp);
    return transactions;
  } catch (error) {
    console.error('❌ getUSDTTransactions error:', error.message);
    return [];
  }
}

async function transferUSDT(fromPrivateKey, toAddress, amount) {
  try {
    const pk = normalizePrivateKeyForTron(fromPrivateKey);
    if (!pk) return false;

    const tronWebForSigning = new TronWeb({
      fullHost: 'https://api.trongrid.io',
      privateKey: pk
    });

    const contract = await tronWebForSigning.contract().at(USDT_CONTRACT);
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

// ========== UNIVERSAL DEPOSIT PROCESSING ==========
async function processDeposit(wallet, amount, txid, network) {
  try {
    console.log(`💰 PROCESSING DEPOSIT: ${amount} USDT for user ${wallet.user_id}, txid: ${txid}, network: ${network}`);

    const { data: existingDeposit, error: checkError } = await supabase
      .from('deposits')
      .select('id, status, amount')
      .eq('txid', txid)
      .eq('network', network)
      .maybeSingle();

    if (checkError) {
      console.error('Error checking existing deposit:', checkError);
      throw checkError;
    }

    if (existingDeposit) {
      console.log(`✅ Deposit already processed: ${txid}, status: ${existingDeposit.status}, amount: ${existingDeposit.amount}`);
      return { success: false, reason: 'already_processed', existing: existingDeposit };
    }

    await ensureUserExists(wallet.user_id);

    const { data: newDeposit, error: depositError } = await supabase
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

    if (depositError) {
      if (depositError.code === '23505') {
        console.log(`🔄 Deposit already being processed by another thread: ${txid}`);
        return { success: false, reason: 'concurrent_processing' };
      }
      throw new Error(`Deposit insert failed: ${depositError.message}`);
    }

    const { data: user, error: userError } = await supabase
      .from('users')
      .select('balance, total_profit, vip_level')
      .eq('id', wallet.user_id)
      .single();

    if (userError) {
      await supabase.from('deposits').delete().eq('id', newDeposit.id);
      throw new Error(`user fetch error: ${userError.message}`);
    }

    const currentBalance = Number(user.balance) || 0;
    const newBalance = currentBalance + amount;
    const newTotalProfit = (Number(user.total_profit) || 0) + amount;

    const { error: updateError } = await supabase
      .from('users')
      .update({
        balance: newBalance,
        total_profit: newTotalProfit,
        updated_at: new Date().toISOString()
      })
      .eq('id', wallet.user_id);

    if (updateError) {
      await supabase.from('deposits').delete().eq('id', newDeposit.id);
      throw new Error(`Balance update failed: ${updateError.message}`);
    }

    await supabase
      .from('deposits')
      .update({ status: 'confirmed' })
      .eq('id', newDeposit.id);

    await supabase.from('transactions').insert({
      user_id: wallet.user_id,
      type: 'deposit',
      amount,
      description: `Депозит USDT (${network}) - ${txid.substring(0, 10)}...`,
      status: 'completed',
      created_at: new Date().toISOString()
    });

    if (newBalance >= 20 && user.vip_level === 0) {
      await supabase
        .from('users')
        .update({ vip_level: 1 })
        .eq('id', wallet.user_id);
      console.log(`⭐ VIP Level upgraded to 1 for user ${wallet.user_id}`);
    }

    console.log(`✅ DEPOSIT PROCESSED: ${amount} USDT for user ${wallet.user_id}`);
    console.log(`💰 New balance: ${newBalance} USDT`);

    return { success: true, amount, deposit_id: newDeposit.id };

  } catch (error) {
    console.error('❌ Error processing deposit:', error.message);
    
    try {
      await supabase
        .from('deposits')
        .delete()
        .eq('txid', txid)
        .eq('network', network)
        .eq('status', 'processing');
    } catch (cleanupError) {
      console.error('Cleanup error:', cleanupError);
    }
    
    throw error;
  }
}

// ========== API Endpoints ==========

// Endpoint для получения адреса пополнения
app.get('/deposit-address/:userId/:network', async (req, res) => {
  try {
    const { userId, network } = req.params;

    console.log(`🔍 Getting deposit address for user: ${userId}, network: ${network}`);

    const { data: wallet, error } = await supabase
      .from('user_wallets')
      .select('address')
      .eq('user_id', userId)
      .eq('network', network.toUpperCase())
      .single();

    if (error || !wallet) {
      console.log(`❌ Wallet not found for user ${userId}, network ${network}`);
      return res.status(404).json({ success: false, error: 'Wallet not found' });
    }

    console.log(`✅ Found wallet: ${wallet.address}`);
    res.json({ 
      success: true, 
      address: wallet.address
    });
  } catch (error) {
    console.error('❌ Get deposit address error:', error.message);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

app.post('/generate-wallet', async (req, res) => {
  try {
    const { user_id, network = 'TRC20' } = req.body;
    if (!user_id) return res.status(400).json({ success: false, error: 'User ID is required' });

    console.log(`🔐 Generating ${network} wallet for user: ${user_id}`);
    await ensureUserExists(user_id);

    // Сначала проверяем существующий кошелек
    const { data: existingWallet, error: checkError } = await supabase
      .from('user_wallets')
      .select('address')
      .eq('user_id', user_id)
      .eq('network', network)
      .single();

    if (existingWallet) {
      console.log(`✅ Wallet already exists: ${existingWallet.address} (${network})`);
      return res.json({ success: true, address: existingWallet.address, exists: true, network });
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
      return res.status(400).json({ success: false, error: 'Unsupported network' });
    }

    console.log(`🆕 Generated new ${network} wallet: ${address}`);

    // Сохраняем в базу
    const { data, error } = await supabase.from('user_wallets').insert({
      user_id,
      address,
      private_key,
      network,
      created_at: new Date().toISOString()
    }).select().single();

    if (error) {
      console.error('❌ Database error:', error);
      return res.status(500).json({ success: false, error: 'Failed to save wallet: ' + error.message });
    }

    console.log(`✅ New ${network} wallet created: ${address}`);
    
    // Запускаем проверку депозитов
    setTimeout(() => checkUserDeposits(user_id, network), 5000);

    res.json({ success: true, address, exists: false, network });
  } catch (error) {
    console.error('❌ Generate wallet error:', error.message);
    res.status(500).json({ success: false, error: 'Internal server error: ' + error.message });
  }
});

// ========== helper DB functions ==========
async function ensureUserExists(userId) {
  try {
    const { data } = await supabase.from('users').select('id').eq('id', userId).single();
    if (!data) {
      const { error: insertError } = await supabase.from('users').insert({
        id: userId,
        email: `user-${userId}@temp.com`,
        username: `user-${(userId || '').substring(0, 8)}`,
        referral_code: `REF-${(userId || '').substring(0, 8)}`,
        balance: 0.00,
        total_profit: 0.00,
        vip_level: 0,
        created_at: new Date().toISOString()
      });
      
      if (insertError) {
        console.error('❌ Error creating user:', insertError);
        throw insertError;
      } else {
        console.log(`✅ User created: ${userId}`);
      }
    }
  } catch (error) {
    console.error('❌ ensureUserExists error:', error.message);
    throw error;
  }
}

async function checkUserDeposits(userId, network) {
  try {
    const { data: wallet } = await supabase
      .from('user_wallets')
      .select('*')
      .eq('user_id', userId)
      .eq('network', network)
      .single();
    
    if (!wallet) return;
    
    console.log(`🔍 Checking ${network} deposits for user ${userId}, wallet: ${wallet.address}`);
    
    if (network === 'BEP20') {
      await sleep(500);
    }
    
    let transactions = [];

    if (network === 'TRC20') {
      transactions = await getUSDTTransactions(wallet.address);
    } else if (network === 'BEP20') {
      transactions = await getBSCTransactions(wallet.address);
    }
    
    for (const tx of transactions) {
      const recipient = network === 'TRC20' ? tx.to : tx.to.toLowerCase();
      const walletAddress = network === 'TRC20' ? wallet.address : wallet.address.toLowerCase();
      
      if (recipient === walletAddress && tx.token === 'USDT' && tx.amount >= MIN_DEPOSIT) {
        try {
          const result = await processDeposit(wallet, tx.amount, tx.transaction_id, network);
          if (result.success) {
            console.log(`💰 FOUND NEW DEPOSIT: ${tx.amount} USDT for user ${userId} (${network})`);
          } else if (result.reason === 'already_processed') {
            console.log(`✅ Deposit already processed: ${tx.transaction_id}`);
          }
        } catch (err) {
          console.error(`❌ Error processing transaction ${tx.transaction_id}:`, err);
        }
      }
    }
  } catch (error) {
    console.error('❌ checkUserDeposits error:', error);
  }
}

// ========== HEALTH CHECK ==========
app.get('/', (req, res) => {
  res.json({
    status: '✅ WORKING',
    message: 'Cocoon AI - Deposit System',
    timestamp: new Date().toISOString(),
    networks: ['TRC20', 'BEP20'],
    features: [
      'Multi-Network Wallet Generation',
      'Deposit Processing',
      'Auto Collection'
    ]
  });
});

// ========== START SERVER ==========
async function startServer() {
  // Тестируем подключение к Supabase перед запуском
  const supabaseConnected = await testSupabaseConnection();
  if (!supabaseConnected) {
    console.error('❌ FAILED TO CONNECT TO SUPABASE. EXITING...');
    process.exit(1);
  }

  app.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 SERVER RUNNING on port ${PORT}`);
    console.log(`✅ SUPABASE: CONNECTED`);
    console.log(`✅ TRONGRID: API KEY ${TRONGRID_API_KEY ? 'SET' : 'MISSING'}`);
    console.log(`✅ MORALIS API: ${MORALIS_API_KEY ? 'AVAILABLE' : 'MISSING'}`);
    console.log(`💰 TRC20 MASTER: ${COMPANY.MASTER.address}`);
    console.log(`💰 TRC20 MAIN: ${COMPANY.MAIN.address}`);
    console.log(`💰 BEP20 MASTER: ${COMPANY_BSC.MASTER.address}`);
    console.log(`💰 BEP20 MAIN: ${COMPANY_BSC.MAIN.address}`);
    console.log('===================================');
  });
}

startServer();
