// server.js
const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const TronWeb = require('tronweb');
const { ethers } = require('ethers');

// ========== ENHANCED LOGGING - MUST BE AT THE VERY TOP ==========
console.log('🚀 STARTING SERVER - ENHANCED BEP20 OPTIMIZATION');
require('dotenv').config();

console.log('📅 Server start time:', new Date().toISOString());
console.log('🔧 Node.js version:', process.version);

// Global fetch fallback for Node <18
if (typeof fetch === 'undefined') {
  try {
    global.fetch = require('node-fetch');
    console.log('🔁 polyfill fetch applied (node-fetch).');
  } catch (e) {
    console.warn('⚠️ fetch is not available and node-fetch not installed');
  }
}

process.on('uncaughtException', (error) => {
  console.error('💥 UNCAUGHT EXCEPTION:', error);
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('💥 UNHANDLED REJECTION at:', promise);
  console.error('💥 Reason:', reason);
});

const app = express();
const PORT = process.env.PORT || 3000;

// ========== ENVIRONMENT VARIABLES ==========
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const TRONGRID_API_KEY = process.env.TRONGRID_API_KEY;

console.log('🔄 Initializing Supabase client...');
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
console.log('✅ Supabase client initialized');

console.log('🔄 Initializing TronWeb...');
const tronWeb = new TronWeb({
  fullHost: 'https://api.trongrid.io',
  headers: { 'TRON-PRO-API-KEY': TRONGRID_API_KEY }
});
console.log('✅ TronWeb initialized');

// ========== OPTIMIZED BSC RPC CONFIGURATION ==========
const BSC_RPC_URLS = [
  'https://bsc-dataseed.binance.org/',
  'https://bsc-dataseed1.defibit.io/',
  'https://bsc-dataseed1.ninicoin.io/',
  'https://bsc-dataseed2.defibit.io/',
  'https://bsc-dataseed3.defibit.io/',
  'https://bsc-dataseed4.defibit.io/'
];

console.log(`🔌 ${BSC_RPC_URLS.length} BSC RPC URLs configured`);

let currentRpcIndex = 0;
function getNextBscRpc() {
  const rpc = BSC_RPC_URLS[currentRpcIndex];
  currentRpcIndex = (currentRpcIndex + 1) % BSC_RPC_URLS.length;
  return rpc;
}

let bscProvider = new ethers.providers.JsonRpcProvider(getNextBscRpc());

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

// ========== ENHANCED REQUEST LOGGING MIDDLEWARE ==========
app.use((req, res, next) => {
  const start = Date.now();
  const requestId = Math.random().toString(36).substring(7);
  
  console.log(`📥 [${requestId}] ${req.method} ${req.path}`, {
    ip: req.ip,
    timestamp: new Date().toISOString()
  });

  res.on('finish', () => {
    const duration = Date.now() - start;
    console.log(`📤 [${requestId}] ${req.method} ${req.path} → ${res.statusCode} (${duration}ms)`);
  });

  next();
});

app.use(express.json());
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', '*');
  res.header('Access-Control-Allow-Methods', '*');
  next();
});

// ========== OPTIMIZED BEP20 CONSTANTS ==========
const USDT_BSC_CONTRACT = (process.env.USDT_BSC_CONTRACT || '0x55d398326f99059fF775485246999027B3197955').toLowerCase();
const USDT_ABI = [
  "function balanceOf(address) view returns (uint256)",
  "function transfer(address to, uint256 amount) returns (bool)",
  "event Transfer(address indexed from, address indexed to, uint256 value)"
];

const MIN_DEPOSIT = Number(process.env.MIN_DEPOSIT || 10);
const KEEP_AMOUNT = Number(process.env.KEEP_AMOUNT || 1.0);
const MIN_BNB_FOR_FEE = Number(process.env.MIN_BNB_FOR_FEE || 0.005);
const FUND_BNB_AMOUNT = Number(process.env.FUND_BNB_AMOUNT || 0.01);

// OPTIMIZED: Much smaller lookback for performance
const BSC_BLOCK_LOOKBACK = Number(process.env.BSC_BLOCK_LOOKBACK || 500); // ~2 hours
const BSC_CHUNK_SIZE = Number(process.env.BSC_CHUNK_SIZE || 500); // Smaller chunks
const BSC_WALLET_CONCURRENCY = Number(process.env.BSC_WALLET_CONCURRENCY || 5); // Reduced concurrency

const CHECK_INTERVAL_MS = Number(process.env.CHECK_INTERVAL_MS || 2 * 60 * 1000);

// ========== OPTIMIZED HELPERS ==========
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function rotateProvider() {
  try {
    bscProvider = new ethers.providers.JsonRpcProvider(getNextBscRpc());
    await bscProvider.getBlockNumber();
  } catch (e) {
    console.warn('🔁 rotateProvider warning:', e.message);
  }
}

// ========== OPTIMIZED BSC FUNCTIONS ==========
async function getBSCUSDTBalance(address) {
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const contract = new ethers.Contract(USDT_BSC_CONTRACT, USDT_ABI, bscProvider);
      const balance = await contract.balanceOf(address);
      return Number(ethers.utils.formatUnits(balance, 18));
    } catch (error) {
      if (attempt < 1) {
        await rotateProvider();
        await sleep(1000);
      }
    }
  }
  return 0;
}

async function getBSCBalance(address) {
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const balance = await bscProvider.getBalance(address);
      return Number(ethers.utils.formatEther(balance));
    } catch (error) {
      if (attempt < 1) {
        await rotateProvider();
        await sleep(1000);
      }
    }
  }
  return 0;
}

// OPTIMIZED: Fast BSC transaction scanning with guaranteed progress
async function getBSCTransactions(address) {
  try {
    if (!address) return [];
    
    const currentBlock = await bscProvider.getBlockNumber();
    const fromBlock = Math.max(0, currentBlock - BSC_BLOCK_LOOKBACK);
    
    console.log(`🔍 BSC scan: ${address} (blocks: ${fromBlock}-${currentBlock})`);

    const transferTopic = ethers.utils.id("Transfer(address,address,uint256)");
    let toTopic;
    try {
      toTopic = ethers.utils.hexZeroPad(ethers.utils.getAddress(address), 32);
    } catch (e) {
      const raw = address.toLowerCase().replace(/^0x/, '');
      toTopic = '0x' + raw.padStart(64, '0');
    }

    const filter = {
      address: USDT_BSC_CONTRACT,
      topics: [transferTopic, null, toTopic],
    };

    const logs = await fetchLogsOptimized(filter, fromBlock, currentBlock);
    console.log(`✅ ${address}: ${logs.length} transfers`);

    const transactions = [];

    for (const log of logs) {
      try {
        const fromAddr = ethers.utils.getAddress('0x' + log.topics[1].substring(26));
        const toAddr = ethers.utils.getAddress('0x' + log.topics[2].substring(26));
        const value = ethers.BigNumber.from(log.data);
        const amount = Number(ethers.utils.formatUnits(value, 18));

        transactions.push({
          transaction_id: log.transactionHash,
          to: toAddr,
          from: fromAddr,
          amount: amount,
          token: 'USDT',
          confirmed: true,
          network: 'BEP20',
          timestamp: Date.now() // Simplified for speed
        });

      } catch (e) {
        continue;
      }
    }

    return transactions;
  } catch (error) {
    console.error('❌ BSC transactions error:', error.message);
    await rotateProvider();
    return [];
  }
}

// OPTIMIZED: Fast and reliable log fetching with guaranteed progress
async function fetchLogsOptimized(filter, fromBlock, toBlock) {
  let results = [];
  let chunkSize = BSC_CHUNK_SIZE;
  let consecutiveErrors = 0;
  const MAX_CONSECUTIVE_ERRORS = 3;

  for (let start = fromBlock; start <= toBlock; ) {
    let end = Math.min(start + chunkSize - 1, toBlock);
    
    // Force progress if we're stuck
    if (consecutiveErrors >= MAX_CONSECUTIVE_ERRORS) {
      console.warn(`⏩ FORCE SKIPPING problematic range: ${start}-${end}`);
      start = end + 1;
      consecutiveErrors = 0;
      chunkSize = BSC_CHUNK_SIZE;
      continue;
    }

    try {
      const logs = await bscProvider.getLogs({
        ...filter,
        fromBlock: start,
        toBlock: end
      });
      
      if (logs && logs.length) {
        results.push(...logs);
      }
      
      // Success - move forward
      start = end + 1;
      consecutiveErrors = 0;
      
      // Gradually increase chunk size back to normal
      if (chunkSize < BSC_CHUNK_SIZE) {
        chunkSize = Math.min(BSC_CHUNK_SIZE, chunkSize * 2);
      }
      
      await sleep(100); // Small delay between successful requests

    } catch (err) {
      const msg = (err && err.message) ? err.message.toLowerCase() : '';
      
      if (err && (err.code === -32005 || msg.includes('limit exceeded') || msg.includes('exceeded'))) {
        consecutiveErrors++;
        chunkSize = Math.max(100, Math.floor(chunkSize / 2));
        
        console.warn(`⚠️ Limit exceeded: ${start}-${end}, reducing chunk to ${chunkSize}`);
        
        // Small delay before retry
        await sleep(500);
        
      } else {
        // Other errors - skip this range and continue
        console.error(`❌ Error for ${start}-${end}:`, err.message);
        start = end + 1;
        consecutiveErrors++;
        await rotateProvider();
        await sleep(500);
      }
    }
  }
  
  return results;
}

async function transferBSCUSDT(fromPrivateKey, toAddress, amount) {
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      if (!fromPrivateKey) return false;

      const wallet = new ethers.Wallet(fromPrivateKey, bscProvider);
      const contract = new ethers.Contract(USDT_BSC_CONTRACT, USDT_ABI, wallet);
      
      const amountInWei = ethers.utils.parseUnits(amount.toString(), 18);
      const tx = await contract.transfer(toAddress, amountInWei);
      
      await tx.wait();
      console.log(`✅ BSC USDT transfer: ${amount} USDT to ${toAddress}`);
      return true;
    } catch (error) {
      if (attempt < 1) {
        await rotateProvider();
        await sleep(1000);
      }
    }
  }
  return false;
}

async function sendBSC(fromPrivateKey, toAddress, amount) {
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      if (!fromPrivateKey) return false;

      const wallet = new ethers.Wallet(fromPrivateKey, bscProvider);
      const tx = await wallet.sendTransaction({
        to: toAddress,
        value: ethers.utils.parseEther(amount.toString())
      });
      
      await tx.wait();
      console.log(`✅ BSC sent: ${amount} BNB to ${toAddress}`);
      return true;
    } catch (error) {
      if (attempt < 1) {
        await rotateProvider();
        await sleep(1000);
      }
    }
  }
  return false;
}

// ========== FAST BEP20 DEPOSIT PROCESSING ==========
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
      .from('profiles')
      .select('balance, total_profit, vip_level')
      .eq('id', wallet.user_id)
      .single();

    if (!user) throw new Error('User not found');

    const currentBalance = Number(user.balance) || 0;
    const newBalance = currentBalance + amount;
    const newTotalProfit = (Number(user.total_profit) || 0) + amount;

    const { error: updateError } = await supabase
      .from('profiles')
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

    // Update status to completed
    await supabase
      .from('deposits')
      .update({ status: 'completed' })
      .eq('id', newDeposit.id);

    await supabase.from('transactions').insert({
      user_id: wallet.user_id,
      type: 'deposit',
      amount,
      description: `Deposit USDT (${network}) - ${txid.substring(0, 10)}...`,
      status: 'completed',
      created_at: new Date().toISOString()
    });

    if (newBalance >= 20 && user.vip_level === 0) {
      await supabase
        .from('profiles')
        .update({ vip_level: 1 })
        .eq('id', wallet.user_id);
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

// ========== OPTIMIZED BEP20 DEPOSIT CHECK ==========
app.post('/check-deposits', async (req, res) => { 
  try {
    console.log('🔄 ===== FAST BEP20 DEPOSIT CHECK STARTED =====');
    
    const startTime = Date.now();
    const { data: wallets, error } = await supabase
      .from('user_wallets')
      .select('*')
      .eq('network', 'BEP20')
      .limit(250);

    if (error) throw error;

    console.log(`🔍 Checking ${wallets?.length || 0} BEP20 wallets`);
    
    let processedCount = 0;
    let depositsFound = 0;
    let duplicatesSkipped = 0;

    // Process wallets in optimized batches
    const batchSize = BSC_WALLET_CONCURRENCY;
    
    for (let i = 0; i < wallets.length; i += batchSize) {
      const batch = wallets.slice(i, i + batchSize);
      console.log(`🔁 Processing batch ${Math.floor(i/batchSize)+1}/${Math.ceil(wallets.length/batchSize)} (${batch.length} wallets)`);
      
      const batchPromises = batch.map(async (wallet) => {
        try {
          const transactions = await getBSCTransactions(wallet.address);
          
          for (const tx of transactions) {
            const recipient = ethers.utils.getAddress(tx.to);
            const walletAddrNorm = ethers.utils.getAddress(wallet.address);
            
            if (recipient === walletAddrNorm && tx.amount >= MIN_DEPOSIT) {
              console.log(`🎯 BEP20 DEPOSIT: ${tx.amount} USDT to ${walletAddrNorm}`);
              
              const result = await processDeposit(wallet, tx.amount, tx.transaction_id, 'BEP20');
              if (result.success) {
                depositsFound++;
              } else if (result.reason === 'already_processed' || result.reason === 'concurrent_processing') {
                duplicatesSkipped++;
              }
            }
          }
          
          await supabase
            .from('user_wallets')
            .update({ last_checked: new Date().toISOString() })
            .eq('id', wallet.id);
          
          processedCount++;
          return { success: true };
          
        } catch (err) {
          console.error(`❌ Error processing wallet ${wallet.address}:`, err.message);
          return { success: false, error: err.message };
        }
      });

      await Promise.all(batchPromises);
      
      // Small delay between batches
      if (i + batchSize < wallets.length) {
        await sleep(500);
      }
    }

    const duration = Date.now() - startTime;
    const message = `✅ BEP20 check completed: ${processedCount} wallets, ${depositsFound} new deposits, ${duplicatesSkipped} duplicates in ${(duration/1000).toFixed(1)}s`;
    
    console.log(`🔄 ===== FAST BEP20 DEPOSIT CHECK COMPLETED =====`);
    console.log(message);
    
    res.json({ 
      success: true, 
      message,
      stats: {
        wallets: processedCount,
        newDeposits: depositsFound,
        duplicates: duplicatesSkipped,
        duration: `${(duration/1000).toFixed(1)}s`
      }
    });
    
  } catch (error) {
    console.error('❌ Deposit check error:', error.message);
    res.status(500).json({ success: false, error: error.message });
  }
});

// ========== TRC20 FUNCTIONS (UNCHANGED) ==========
async function getUSDTTransactions(address) {
  try {
    if (!address) return [];
    
    console.log(`🔍 Checking TRC20 transactions for: ${address}`);
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
        if (!tokenAddr || tokenAddr !== 'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t') continue;

        const to = tx.to;
        const from = tx.from;
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
    console.log(`✅ Found ${transactions.length} TRC20 transactions for ${address}`);
    return transactions;
  } catch (error) {
    console.error('❌ getUSDTTransactions error:', error.message);
    return [];
  }
}

// TRC20 deposit check endpoint
app.post('/check-deposits-trc20', async (req, res) => { 
  try {
    console.log('🔄 ===== TRC20 DEPOSIT CHECK STARTED =====');
    
    const { data: wallets, error } = await supabase
      .from('user_wallets')
      .select('*')
      .eq('network', 'TRC20')
      .limit(250);

    if (error) throw error;

    console.log(`🔍 Checking ${wallets?.length || 0} TRC20 wallets`);
    
    let processedCount = 0;
    let depositsFound = 0;

    for (const wallet of wallets) {
      try {
        await sleep(1000); // Rate limiting for TRON
        const transactions = await getUSDTTransactions(wallet.address);
        
        for (const tx of transactions) {
          if (tx.to === wallet.address && tx.amount >= MIN_DEPOSIT) {
            console.log(`🎯 TRC20 DEPOSIT: ${tx.amount} USDT to ${wallet.address}`);
            
            const result = await processDeposit(wallet, tx.amount, tx.transaction_id, 'TRC20');
            if (result.success) {
              depositsFound++;
            }
          }
        }
        
        await supabase
          .from('user_wallets')
          .update({ last_checked: new Date().toISOString() })
          .eq('id', wallet.id);
        
        processedCount++;
        
      } catch (err) {
        console.error(`❌ Error processing TRC20 wallet ${wallet.address}:`, err.message);
      }
    }

    const message = `✅ TRC20 check completed: ${processedCount} wallets, ${depositsFound} new deposits`;
    
    console.log(`🔄 ===== TRC20 DEPOSIT CHECK COMPLETED =====`);
    console.log(message);
    
    res.json({ 
      success: true, 
      message,
      stats: {
        wallets: processedCount,
        newDeposits: depositsFound
      }
    });
    
  } catch (error) {
    console.error('❌ TRC20 deposit check error:', error.message);
    res.status(500).json({ success: false, error: error.message });
  }
});

// ========== HELPER FUNCTIONS ==========
async function ensureUserExists(userId) {
  try {
    const { data } = await supabase.from('profiles').select('id').eq('id', userId).single();
    
    if (!data) {
      const { error: insertError } = await supabase.from('profiles').insert({
        id: userId,
        email: `user-${userId}@temp.com`,
        balance: 0.00,
        total_profit: 0.00,
        vip_level: 0,
        registered: new Date().toISOString()
      });

      if (insertError) throw insertError;
    }
  } catch (error) {
    console.error('❌ ensureUserExists error:', error.message);
    throw error;
  }
}

// ========== BASIC ENDPOINTS ==========
app.get('/health', (req, res) => {
  res.json({
    status: '✅ HEALTHY',
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    features: ['Fast BEP20 Deposit Detection', 'TRC20 Support']
  });
});

app.get('/', (req, res) => {
  res.json({
    status: '✅ WORKING',
    message: 'Optimized BEP20 Deposit System',
    timestamp: new Date().toISOString(),
    performance: {
      bscBlockLookback: BSC_BLOCK_LOOKBACK,
      bscChunkSize: BSC_CHUNK_SIZE,
      walletConcurrency: BSC_WALLET_CONCURRENCY,
      targetSpeed: '1-4 minutes for 250 wallets'
    }
  });
});

// ========== START SERVER ==========
console.log('🎯 BEP20 OPTIMIZATIONS LOADED - STARTING SERVER...');

app.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 SERVER STARTED on port ${PORT}`);
  console.log(`✅ BEP20 OPTIMIZED: ${BSC_BLOCK_LOOKBACK} blocks lookback`);
  console.log(`✅ BEP20 OPTIMIZED: ${BSC_CHUNK_SIZE} blocks chunk size`);
  console.log(`✅ BEP20 OPTIMIZED: ${BSC_WALLET_CONCURRENCY} wallets concurrency`);
  console.log('🎯 Ready for fast BEP20 deposit detection!');
});
