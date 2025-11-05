const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const TronWeb = require('tronweb');
const { ethers } = require('ethers');

// ========== ENHANCED LOGGING - MUST BE AT THE VERY TOP ==========
console.log('🚀 STARTING SERVER - ENHANCED LOGGING ENABLED');
console.log('📅 Server start time:', new Date().toISOString());

// Environment variables
console.log('🔑 Environment check:');
console.log('GETBLOCK_API_KEY:', process.env.GETBLOCK_API_KEY ? '✅ SET' : '❌ MISSING');

const app = express();
const PORT = process.env.PORT || 3000;

// ========== ENVIRONMENT VARIABLES ==========
const SUPABASE_URL = process.env.SUPABASE_URL || 'https://eqzfivdckzrkkncahlyn.supabase.co';
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVxemZpdmRja3pya2tuY2FobHluIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MTYwNTg2NSwiZXhwIjoyMDc3MTgxODY1fQ.AuGqzDDMzWS1COhHdBMchHarYmd1gNC_9PfRfJWPTxc';
const TRONGRID_API_KEY = process.env.TRONGRID_API_KEY || '33759ca3-ffb8-41bc-9036-25a32601eae2';
const GETBLOCK_API_KEY = process.env.GETBLOCK_API_KEY || 'b124f13a33774dbbb13fa002dd4c831f';

// ========== GETBLOCK CONFIGURATION ==========
const GETBLOCK_BSC_URL = `https://go.getblock.io/${GETBLOCK_API_KEY}`;

console.log('🔄 Initializing Supabase client...');
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

console.log('🔄 Initializing TronWeb...');
const tronWeb = new TronWeb({
  fullHost: 'https://api.trongrid.io',
  headers: { 'TRON-PRO-API-KEY': TRONGRID_API_KEY }
});

// Initialize BSC provider with GetBlock
let bscProvider = new ethers.providers.JsonRpcProvider(GETBLOCK_BSC_URL);

// COMPANY wallets
const COMPANY = {
  MASTER: { address: 'TKn5J3ZnTxE9fmgMhVjXognH4VUjx4Tid2', privateKey: process.env.MASTER_PRIVATE_KEY || 'NOT_SET' },
  MAIN: { address: 'TNVpDk1JZSxmC9XniB1tSPaRdAvvKMMavC', privateKey: process.env.MAIN_PRIVATE_KEY || 'NOT_SET' }
};

const COMPANY_BSC = {
  MASTER: { address: '0x60F3159e6b935759d6b4994473eeeD1e3ad27408', privateKey: process.env.MASTER_BSC_PRIVATE_KEY || 'NOT_SET' },
  MAIN: { address: '0x01F28A131bdda7255EcBE800C3ebACBa2c7076c7', privateKey: process.env.MAIN_BSC_PRIVATE_KEY || 'NOT_SET' }
};

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

const CHECK_INTERVAL_MS = 1 * 60 * 1000; // 1 минута

// ========== HELPERS ==========
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ========== GETBLOCK API FUNCTIONS ==========
async function getBlockRequest(method, params, retries = 3) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const response = await fetch(GETBLOCK_BSC_URL, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          jsonrpc: "2.0",
          method: method,
          params: params,
          id: 1
        })
      });

      if (response.status === 429) {
        if (attempt < retries) {
          await sleep(2000 * Math.pow(2, attempt));
          continue;
        }
      }

      const data = await response.json();
      if (data.result) return data;
      
      return { result: [] };
    } catch (error) {
      if (attempt === retries) throw error;
      await sleep(1000 * (attempt + 1));
    }
  }
  return { result: [] };
}

// ========== BSC FUNCTIONS WITH GETBLOCK ==========
async function getBSCUSDTBalance(address) {
  console.log(`🔍 Checking BSC USDT balance: ${address}`);
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const contract = new ethers.Contract(USDT_BSC_CONTRACT, USDT_ABI, bscProvider);
      const balance = await contract.balanceOf(address);
      const formatted = Number(ethers.utils.formatUnits(balance, 18));
      console.log(`✅ BSC USDT balance: ${formatted} USDT`);
      return formatted;
    } catch (error) {
      console.error(`❌ Balance attempt ${attempt + 1} error:`, error.message);
      await sleep(1000);
    }
  }
  return 0;
}

async function getBSCTransactions(address) {
  try {
    if (!address) return [];

    console.log(`🎯 GETBLOCK ULTIMATE CHECK: ${address}`);
    
    // 1. Получаем текущий блок
    const currentBlockData = await getBlockRequest('eth_blockNumber', []);
    if (!currentBlockData.result) {
      console.log('❌ Failed to get current block');
      return [];
    }
    
    const currentBlock = parseInt(currentBlockData.result, 16);
    
    // 2. Проверяем несколько диапазонов блоков
    const blockRanges = [
      { from: currentBlock - 300, name: '5 минут' },      // ~5 минут
      { from: currentBlock - 1000, name: '15 минут' },    // ~15 минут  
      { from: currentBlock - 5000, name: '1 час' },       // ~1 час
      { from: 'earliest', name: 'все блоки' }             // все исторические блоки
    ];

    for (const range of blockRanges) {
      console.log(`🔍 Checking ${range.name} (blocks: ${range.from} to ${currentBlock})`);
      
      const fromBlockHex = range.from === 'earliest' ? 'earliest' : '0x' + range.from.toString(16);
      
      const data = await getBlockRequest('eth_getLogs', [{
        fromBlock: fromBlockHex,
        toBlock: 'latest',
        address: USDT_BSC_CONTRACT.toLowerCase(),
        topics: [
          '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
          null,
          '0x' + address.toLowerCase().replace('0x', '').padStart(64, '0')
        ]
      }]);
      
      if (data.result && Array.isArray(data.result)) {
        console.log(`✅ GetBlock found ${data.result.length} transfer events in ${range.name}`);
        
        const transactions = [];
        for (const log of data.result) {
          try {
            const from = '0x' + log.topics[1].slice(26);
            const valueHex = log.data;
            const value = parseInt(valueHex, 16) / 1e18;
            
            if (value >= MIN_DEPOSIT) {
              transactions.push({
                transaction_id: log.transactionHash,
                to: address,
                from: from,
                amount: value,
                token: 'USDT',
                confirmed: true,
                network: 'BEP20',
                timestamp: Date.now()
              });

              console.log(`💰 GETBLOCK FOUND DEPOSIT: ${value} USDT from ${from}`);
            }
          } catch (e) { 
            continue; 
          }
        }
        
        if (transactions.length > 0) {
          transactions.sort((a, b) => b.timestamp - a.timestamp);
          return transactions;
        }
      }
      
      await sleep(500); // Небольшая задержка между запросами
    }
    
    console.log(`❌ GetBlock: No transactions found for ${address}`);
    return [];
  } catch (error) {
    console.error('❌ GetBlock transactions error:', error.message);
    return [];
  }
}

async function getBSCBalance(address) {
  console.log(`🔍 Checking BSC native balance: ${address}`);
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const balance = await bscProvider.getBalance(address);
      const formatted = Number(ethers.utils.formatEther(balance));
      console.log(`✅ BSC native balance: ${formatted} BNB`);
      return formatted;
    } catch (error) {
      console.error(`❌ BNB balance attempt ${attempt + 1} error:`, error.message);
      await sleep(1000);
    }
  }
  return 0;
}

async function transferBSCUSDT(fromPrivateKey, toAddress, amount) {
  console.log(`🔄 Transferring ${amount} BSC USDT to ${toAddress}`);
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      if (!fromPrivateKey || fromPrivateKey.includes('NOT_SET')) {
        console.error('❌ BSC USDT transfer error: Private key not set');
        return false;
      }

      const wallet = new ethers.Wallet(fromPrivateKey, bscProvider);
      const contract = new ethers.Contract(USDT_BSC_CONTRACT, USDT_ABI, wallet);
      
      const amountInWei = ethers.utils.parseUnits(amount.toString(), 18);
      const tx = await contract.transfer(toAddress, amountInWei);
      
      await tx.wait();
      console.log(`✅ BSC USDT transfer: ${amount} USDT to ${toAddress}, txid: ${tx.hash}`);
      return true;
    } catch (error) {
      console.error(`❌ BSC USDT transfer attempt ${attempt + 1} error:`, error.message);
      await sleep(1000);
    }
  }
  return false;
}

// ========== TRON FUNCTIONS ==========
async function getUSDTBalance(address) {
  try {
    if (!address) return 0;
    console.log(`🔍 Checking TRC20 USDT balance: ${address}`);
    
    const tronWebForChecking = new TronWeb({
      fullHost: 'https://api.trongrid.io',
      headers: { 'TRON-PRO-API-KEY': TRONGRID_API_KEY }
    });

    const contract = await tronWebForChecking.contract().at(USDT_CONTRACT);
    const result = await contract.balanceOf(address).call();
    const balance = Number(result) / 1_000_000;
    console.log(`✅ TRC20 USDT balance: ${balance} USDT`);
    return balance;
  } catch (error) {
    console.error('❌ TRC20 balance error:', error.message);
    return 0;
  }
}

async function getUSDTTransactions(address) {
  try {
    if (!address) return [];
    
    console.log(`🔍 Checking TRC20 transactions: ${address}`);
    const response = await fetch(`https://api.trongrid.io/v1/accounts/${address}/transactions/trc20?limit=50&only_confirmed=true`, {
      headers: {'TRON-PRO-API-KEY': TRONGRID_API_KEY}
    });
    
    const json = await response.json();
    const raw = json.data || [];
    const transactions = [];

    for (const tx of raw) {
      try {
        const tokenAddr = tx.token_info?.address;
        if (!tokenAddr || tokenAddr !== USDT_CONTRACT) continue;

        const to = tx.to;
        const from = tx.from;
        const amount = Number(tx.value || 0) / 1_000_000;

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
    console.log(`✅ Found ${transactions.length} TRC20 transactions`);
    return transactions;
  } catch (error) {
    console.error('❌ TRC20 transactions error:', error.message);
    return [];
  }
}

async function getTRXBalance(address) {
  console.log(`🔍 Checking TRX balance: ${address}`);
  try {
    const response = await fetch(`https://api.trongrid.io/v1/accounts/${address}`, {
      headers: {'TRON-PRO-API-KEY': TRONGRID_API_KEY}
    });
    
    const json = await response.json();
    
    if (json && json.data && json.data.length > 0) {
      const balance = json.data[0].balance || 0;
      const formatted = balance / 1_000_000;
      console.log(`✅ TRX balance: ${formatted} TRX`);
      return formatted;
    }
    return 0;
  } catch (error) {
    console.error('❌ TRX balance error:', error.message);
    return 0;
  }
}

// ========== UNIVERSAL DEPOSIT PROCESSING ==========
async function processDeposit(wallet, amount, txid, network) {
  try {
    console.log(`💰 PROCESSING DEPOSIT: ${amount} USDT for user ${wallet.user_id}, txid: ${txid}`);

    // Check if deposit already exists
    const { data: existingDeposit } = await supabase
      .from('deposits')
      .select('id, status, amount')
      .eq('txid', txid)
      .eq('network', network)
      .maybeSingle();

    if (existingDeposit) {
      console.log(`✅ Deposit already processed: ${txid}`);
      return { success: false, reason: 'already_processed' };
    }

    await ensureUserExists(wallet.user_id);

    // Insert new deposit
    const { data: newDeposit, error: depositError } = await supabase
      .from('deposits')
      .insert({
        user_id: wallet.user_id,
        amount,
        txid: txid,
        network,
        wallet_address: wallet.address,
        status: 'pending',
        created_at: new Date().toISOString()
      })
      .select()
      .single();

    if (depositError) {
      if (depositError.code === '23505') {
        return { success: false, reason: 'concurrent_processing' };
      }
      throw new Error(`Deposit insert failed: ${depositError.message}`);
    }

    const { data: user } = await supabase
      .from('users')
      .select('balance, total_profit, vip_level')
      .eq('id', wallet.user_id)
      .single();

    const currentBalance = Number(user.balance) || 0;
    const newBalance = currentBalance + amount;
    const newTotalProfit = (Number(user.total_profit) || 0) + amount;

    console.log(`📊 User balance update: ${currentBalance} → ${newBalance} USDT`);

    await supabase
      .from('users')
      .update({
        balance: newBalance,
        total_profit: newTotalProfit,
        updated_at: new Date().toISOString()
      })
      .eq('id', wallet.user_id);

    await supabase
      .from('deposits')
      .update({ status: 'completed' })
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

    return { success: true, amount, deposit_id: newDeposit.id };

  } catch (error) {
    console.error('❌ Error processing deposit:', error.message);
    
    try {
      await supabase
        .from('deposits')
        .delete()
        .eq('txid', txid)
        .eq('network', network)
        .eq('status', 'pending');
    } catch (cleanupError) {
      console.error('Cleanup error:', cleanupError);
    }
    
    throw error;
  }
}

// ========== API Endpoints ==========
app.post('/generate-wallet', async (req, res) => {
  try {
    const { user_id, network = 'TRC20' } = req.body;
    if (!user_id) return res.status(400).json({ success: false, error: 'User ID is required' });

    console.log(`🔐 Generating ${network} wallet for user: ${user_id}`);
    await ensureUserExists(user_id);

    const { data: existingWallet } = await supabase
      .from('user_wallets')
      .select('address')
      .eq('user_id', user_id)
      .eq('network', network)
      .single();

    if (existingWallet) {
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

    const { data, error } = await supabase.from('user_wallets').insert({
      user_id,
      address,
      private_key,
      network,
      created_at: new Date().toISOString()
    }).select().single();

    if (error) {
      return res.status(500).json({ success: false, error: 'Failed to save wallet' });
    }

    console.log(`✅ New ${network} wallet created: ${address}`);
    
    setTimeout(() => {
      checkUserDeposits(user_id, network).catch(err => {
        console.error(`Error in initial deposit check for ${address}:`, err.message);
      });
    }, 5000);

    res.json({ success: true, address, exists: false, network });
  } catch (error) {
    console.error('❌ Generate wallet error:', error.message);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

app.get('/deposit-address/:userId/:network', async (req, res) => {
  try {
    const { userId, network } = req.params;

    const { data: wallet, error } = await supabase
      .from('user_wallets')
      .select('address')
      .eq('user_id', userId)
      .eq('network', network.toUpperCase())
      .single();

    if (error || !wallet) {
      return res.status(404).json({ success: false, error: 'Wallet not found' });
    }

    res.json({ success: true, address: wallet.address });
  } catch (error) {
    console.error('❌ Get deposit address error:', error.message);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

app.post('/check-deposits', async (req, res) => { await handleCheckDeposits(req, res); });
app.get('/check-deposits', async (req, res) => { await handleCheckDeposits(req, res); });

async function handleCheckDeposits(req = {}, res = {}) {
  try {
    console.log('🔄 ===== MANUAL DEPOSIT CHECK STARTED =====');
    const { data: wallets, error } = await supabase.from('user_wallets').select('*').limit(60);
    if (error) throw error;

    console.log(`🔍 Checking ${wallets?.length || 0} wallets`);
    
    let processedCount = 0;
    let depositsFound = 0;

    for (const wallet of wallets || []) {
      try {
        console.log(`🔍 Processing wallet ${wallet.address} (${wallet.network}) for user ${wallet.user_id}`);
        
        await sleep(500);
        
        let transactions = [];

        if (wallet.network === 'TRC20') {
          transactions = await getUSDTTransactions(wallet.address);
        } else if (wallet.network === 'BEP20') {
          transactions = await getBSCTransactions(wallet.address);
        }

        console.log(`📊 Found ${transactions.length} transactions for wallet ${wallet.address}`);

        for (const tx of transactions) {
          const recipient = wallet.network === 'TRC20' ? tx.to : tx.to.toLowerCase();
          const walletAddress = wallet.network === 'TRC20' ? wallet.address : wallet.address.toLowerCase();
          
          if (recipient === walletAddress && tx.token === 'USDT' && tx.amount >= MIN_DEPOSIT) {
            console.log(`💰 Potential deposit found: ${tx.amount} USDT to ${walletAddress}, txid: ${tx.transaction_id}`);
            try {
              const result = await processDeposit(wallet, tx.amount, tx.transaction_id, wallet.network);
              if (result.success) {
                depositsFound++;
                console.log(`✅ Deposit processed successfully: ${tx.amount} USDT for user ${wallet.user_id}`);
              }
            } catch (err) {
              console.error(`❌ Error processing deposit ${tx.transaction_id}:`, err.message);
            }
          }
        }

        await supabase.from('user_wallets').update({ last_checked: new Date().toISOString() }).eq('id', wallet.id);
        processedCount++;
      } catch (err) {
        console.error(`❌ Error processing wallet ${wallet.address}:`, err.message);
      }
    }

    const message = `✅ Deposit check completed: Processed ${processedCount} wallets, found ${depositsFound} new deposits`;
    console.log(`🔄 ===== MANUAL DEPOSIT CHECK COMPLETED =====`);
    
    if (res && typeof res.json === 'function') res.json({ success: true, message });
    return { success: true, message };
  } catch (error) {
    console.error('❌ Deposit check error:', error.message);
    if (res && typeof res.status === 'function') res.status(500).json({ success: false, error: error.message });
    return { success: false, error: error.message };
  }
}

// ========== helper DB functions ==========
async function ensureUserExists(userId) {
  try {
    const { data } = await supabase.from('users').select('id').eq('id', userId).single();
    if (!data) {
      console.log(`👤 Creating new user: ${userId}`);
      await supabase.from('users').insert({
        id: userId,
        email: `user-${userId}@temp.com`,
        username: `user-${(userId || '').substring(0, 8)}`,
        referral_code: `REF-${(userId || '').substring(0, 8)}`,
        balance: 0.00,
        total_profit: 0.00,
        vip_level: 0,
        created_at: new Date().toISOString()
      });
    }
  } catch (error) {
    console.error('❌ ensureUserExists error:', error.message);
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
    
    await sleep(500);
    
    let transactions = [];

    if (network === 'TRC20') {
      transactions = await getUSDTTransactions(wallet.address);
    } else if (network === 'BEP20') {
      transactions = await getBSCTransactions(wallet.address);
    }
    
    console.log(`📊 Found ${transactions.length} transactions for user ${userId}`);
    
    for (const tx of transactions) {
      const recipient = network === 'TRC20' ? tx.to : tx.to.toLowerCase();
      const walletAddress = network === 'TRC20' ? wallet.address : wallet.address.toLowerCase();
      
      if (recipient === walletAddress && tx.token === 'USDT' && tx.amount >= MIN_DEPOSIT) {
        try {
          const result = await processDeposit(wallet, tx.amount, tx.transaction_id, network);
          if (result.success) {
            console.log(`💰 FOUND NEW DEPOSIT: ${tx.amount} USDT for user ${userId} (${network})`);
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

app.get('/health', (req, res) => {
  res.json({
    status: '✅ WORKING',
    timestamp: new Date().toISOString(),
    provider: 'GetBlock ONLY',
    message: 'BSC transactions via GetBlock API'
  });
});

app.get('/', (req, res) => {
  res.json({
    status: '✅ WORKING', 
    message: 'Cocoon AI - GetBlock Deposit System',
    timestamp: new Date().toISOString(),
    provider: 'GetBlock'
  });
});

// ========== SCHEDULED DEPOSIT CHECKS ==========
console.log('⏰ Starting scheduled deposit checks every 1 minute...');
setInterval(async () => {
  try {
    console.log('🕒 ===== SCHEDULED DEPOSIT CHECK STARTED =====');
    await handleCheckDeposits();
    console.log('🕒 ===== SCHEDULED DEPOSIT CHECK COMPLETED =====');
  } catch (err) {
    console.error('❌ Scheduled deposit check error:', err.message);
  }
}, CHECK_INTERVAL_MS);

// ========== START SERVER ==========
console.log('🎯 STARTING SERVER WITH GETBLOCK ONLY...');

app.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 SERVER STARTED on port ${PORT}`);
  console.log(`✅ GETBLOCK: CONFIGURED`);
  console.log(`✅ TRONGRID: CONFIGURED`); 
  console.log(`✅ SUPABASE: CONNECTED`);
  console.log(`⏰ AUTO-CHECK: EVERY ${CHECK_INTERVAL_MS / 1000}s`);
  console.log('===================================');
  console.log('🎉 GETBLOCK SYSTEM READY');
});
