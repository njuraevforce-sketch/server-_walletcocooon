const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const TronWeb = require('tronweb');
const { ethers } = require('ethers');

// ========== ENHANCED LOGGING - MUST BE AT THE VERY TOP ==========
console.log('🚀 STARTING SERVER - ENHANCED LOGGING ENABLED');
console.log('📅 Server start time:', new Date().toISOString());
console.log('🔧 Node.js version:', process.version);
console.log('🌐 Platform:', process.platform, process.arch);
console.log('📊 Process ID:', process.pid);

// Log environment variables (safe version)
console.log('🔑 Environment check:');
console.log('PORT:', process.env.PORT || '3000 (default)');
console.log('NODE_ENV:', process.env.NODE_ENV || 'not set');
console.log('SUPABASE_URL:', process.env.SUPABASE_URL ? '✅ SET' : '❌ MISSING');
console.log('SUPABASE_SERVICE_ROLE_KEY:', process.env.SUPABASE_SERVICE_ROLE_KEY ? '✅ SET' : '❌ MISSING');
console.log('TRONGRID_API_KEY:', process.env.TRONGRID_API_KEY ? '✅ SET' : '❌ MISSING');
console.log('CHAINSTACK_BSC_URL:', process.env.CHAINSTACK_BSC_URL ? '✅ SET' : '❌ MISSING');

// Enhanced error handling
process.on('uncaughtException', (error) => {
  console.error('💥 UNCAUGHT EXCEPTION:', error);
  console.error('💥 Stack trace:', error.stack);
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('💥 UNHANDLED REJECTION at:', promise);
  console.error('💥 Reason:', reason);
});

const app = express();
const PORT = process.env.PORT || 3000;

// ========== ENVIRONMENT VARIABLES ==========
const SUPABASE_URL = process.env.SUPABASE_URL || 'https://eqzfivdckzrkkncahlyn.supabase.co';
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVxemZpdmRja3pya2tuY2FobHluIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MTYwNTg2NSwiZXhwIjoyMDc3MTgxODY1fQ.AuGqzDDMzWS1COhHdBMchHarYmd1gNC_9PfRfJWPTxc';
const TRONGRID_API_KEY = process.env.TRONGRID_API_KEY || '33759ca3-ffb8-41bc-9036-25a32601eae2';

// ========== CHAINSTACK BSC CONFIGURATION ==========
const CHAINSTACK_BSC_URL = process.env.CHAINSTACK_BSC_URL || 'https://bsc-mainnet.core.chainstack.com/5de4f946fec6345ad71e18bf5f6386a5';

console.log('🔄 Initializing Supabase client...');
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
console.log('✅ Supabase client initialized');

console.log('🔄 Initializing TronWeb...');
const tronWeb = new TronWeb({
  fullHost: 'https://api.trongrid.io',
  headers: { 'TRON-PRO-API-KEY': TRONGRID_API_KEY }
});
console.log('✅ TronWeb initialized');

// ========== BSC RPC CONFIGURATION WITH CHAINSTACK ==========
const BSC_RPC_URLS = [
  CHAINSTACK_BSC_URL, // Primary - Chainstack
  'https://bsc-dataseed.binance.org/', // Fallback 1
  'https://bsc-dataseed1.defibit.io/', // Fallback 2
  'https://bsc-dataseed1.ninicoin.io/', // Fallback 3
];

console.log('🔌 BSC RPC URLs configured:');
BSC_RPC_URLS.forEach((url, index) => {
  console.log(`   ${index === 0 ? '🟢 PRIMARY' : '🔵 FALLBACK'} ${index + 1}: ${url.substring(0, 50)}...`);
});

let currentRpcIndex = 0;
function getNextBscRpc() {
  const rpc = BSC_RPC_URLS[currentRpcIndex];
  console.log(`🔌 Using BSC RPC: ${rpc.substring(0, 50)}... (index: ${currentRpcIndex})`);
  currentRpcIndex = (currentRpcIndex + 1) % BSC_RPC_URLS.length;
  return rpc;
}

// Initialize with Chainstack as primary
let bscProvider = new ethers.providers.JsonRpcProvider(getNextBscRpc());

// COMPANY wallets - TRC20
const COMPANY = {
  MASTER: {
    address: 'TKn5J3ZnTxE9fmgMhVjXognH4VUjx4Tid2',
    privateKey: process.env.MASTER_PRIVATE_KEY || 'MASTER_PRIVATE_KEY_NOT_SET'
  },
  MAIN: {
    address: 'TNVpDk1JZSxmC9XniB1tSPaRdAvvKMMavC',
    privateKey: process.env.MAIN_PRIVATE_KEY || 'MAIN_PRIVATE_KEY_NOT_SET'
  }
};

// COMPANY wallets - BEP20
const COMPANY_BSC = {
  MASTER: {
    address: '0x60F3159e6b935759d6b4994473eeeD1e3ad27408',
    privateKey: process.env.MASTER_BSC_PRIVATE_KEY || 'MASTER_BSC_PRIVATE_KEY_NOT_SET'
  },
  MAIN: {
    address: '0x01F28A131bdda7255EcBE800C3ebACBa2c7076c7',
    privateKey: process.env.MAIN_BSC_PRIVATE_KEY || 'MAIN_BSC_PRIVATE_KEY_NOT_SET'
  }
};

// ========== ENHANCED REQUEST LOGGING MIDDLEWARE ==========
app.use((req, res, next) => {
  const start = Date.now();
  const requestId = Math.random().toString(36).substring(7);
  
  console.log(`📥 [${requestId}] INCOMING: ${req.method} ${req.path}`, {
    ip: req.ip,
    userAgent: req.get('User-Agent')?.substring(0, 50) || 'unknown',
    timestamp: new Date().toISOString()
  });

  res.on('finish', () => {
    const duration = Date.now() - start;
    console.log(`📤 [${requestId}] RESPONSE: ${req.method} ${req.path} → ${res.statusCode} (${duration}ms)`);
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

// ========== CONSTANTS ==========
const USDT_CONTRACT = 'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t';
const USDT_BSC_CONTRACT = '0x55d398326f99059fF775485246999027B3197955';
const USDT_ABI = [
  "function balanceOf(address) view returns (uint256)",
  "function transfer(address to, uint256 amount) returns (bool)",
  "event Transfer(address indexed from, address indexed to, uint256 value)"
];

const MIN_DEPOSIT = 10;
const KEEP_AMOUNT = 1.0;
const MIN_TRX_FOR_FEE = 3;
const MIN_BNB_FOR_FEE = 0.005;
const FUND_TRX_AMOUNT = 10;
const FUND_BNB_AMOUNT = 0.01;

// Throttling / concurrency
const BSC_CONCURRENCY = 3; // Chainstack 25 RPS limit
const CHECK_INTERVAL_BEP20_MS = 60 * 1000; // 1 minute for BEP20
const CHECK_INTERVAL_TRC20_MS = 2 * 60 * 1000; // 2 minutes for TRC20

// ========== HELPERS ==========
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function normalizePrivateKeyForTron(pk) {
  if (!pk || pk.includes('NOT_SET')) return null;
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

// Queue for BSC requests
let currentBscRequests = 0;
const pendingBscQueue = [];
function enqueueBscJob(fn) {
  return new Promise((resolve, reject) => {
    pendingBscQueue.push({ fn, resolve, reject });
    runBscQueue();
  });
}

function runBscQueue() {
  while (currentBscRequests < BSC_CONCURRENCY && pendingBscQueue.length) {
    const job = pendingBscQueue.shift();
    currentBscRequests++;
    job.fn()
      .then(res => {
        currentBscRequests--;
        job.resolve(res);
        setTimeout(runBscQueue, 100);
      })
      .catch(err => {
        currentBscRequests--;
        job.reject(err);
        setTimeout(runBscQueue, 100);
      });
  }
}

// Cache for balances
const balanceCache = new Map();
const BALANCE_CACHE_TTL = 30000; // 30 seconds

// ========== BSC FUNCTIONS WITH CHAINSTACK ==========
async function getBSCUSDTBalance(address) {
  return enqueueBscJob(async () => {
    try {
      // Check cache first
      const cacheKey = `usdt_${address}`;
      const cached = balanceCache.get(cacheKey);
      if (cached && Date.now() - cached.timestamp < BALANCE_CACHE_TTL) {
        console.log(`🔍 Using cached BSC USDT balance for: ${address} - ${cached.balance} USDT`);
        return cached.balance;
      }

      console.log(`🔍 Checking BSC USDT balance for: ${address}`);
      for (let attempt = 0; attempt < 2; attempt++) {
        try {
          const contract = new ethers.Contract(USDT_BSC_CONTRACT, USDT_ABI, bscProvider);
          const balance = await contract.balanceOf(address);
          const formatted = Number(ethers.utils.formatUnits(balance, 18));
          console.log(`✅ BSC USDT balance for ${address}: ${formatted} USDT`);
          
          // Save to cache
          balanceCache.set(cacheKey, {
            balance: formatted,
            timestamp: Date.now()
          });
          
          return formatted;
        } catch (error) {
          console.error(`❌ BSC USDT balance attempt ${attempt + 1} error:`, error.message);
          if (attempt < 1) {
            bscProvider = new ethers.providers.JsonRpcProvider(getNextBscRpc());
            await sleep(500);
          }
        }
      }
      return 0;
    } catch (error) {
      console.error('❌ getBSCUSDTBalance fatal error:', error.message);
      return 0;
    }
  });
}

async function getBSCTransactions(address) {
  return enqueueBscJob(async () => {
    try {
      console.log(`🔍 Checking BSC transactions for: ${address}`);
      
      const transactions = [];
      const currentBlock = await bscProvider.getBlockNumber();
      const fromBlock = currentBlock - 10000; // Check last ~2 days
      
      console.log(`📦 Checking blocks ${fromBlock} to ${currentBlock} for ${address}`);
      
      // Create contract for event filtering
      const contract = new ethers.Contract(USDT_BSC_CONTRACT, USDT_ABI, bscProvider);
      
      // Filter Transfer events where to is our address
      const filter = contract.filters.Transfer(null, address);
      
      try {
        const logs = await contract.queryFilter(filter, fromBlock, currentBlock);
        console.log(`✅ Found ${logs.length} USDT transfer events to ${address}`);
        
        for (const log of logs) {
          try {
            const tx = await bscProvider.getTransactionReceipt(log.transactionHash);
            
            if (tx && tx.status === 1) { // Successful transaction
              const amount = Number(ethers.utils.formatUnits(log.args.value, 18));
              
              transactions.push({
                transaction_id: log.transactionHash,
                to: address.toLowerCase(),
                from: log.args.from.toLowerCase(),
                amount: amount,
                token: 'USDT',
                confirmed: true,
                network: 'BEP20',
                timestamp: (await bscProvider.getBlock(log.blockNumber)).timestamp * 1000,
                blockNumber: log.blockNumber
              });
              
              console.log(`📥 Found BSC deposit: ${amount} USDT from ${log.args.from}`);
            }
          } catch (txError) {
            console.warn('Skipping malformed BSC transaction:', txError.message);
            continue;
          }
        }
      } catch (logsError) {
        console.error('❌ Error querying event logs:', logsError.message);
        return [];
      }
      
      // Sort by time (newest first)
      transactions.sort((a, b) => b.timestamp - a.timestamp);
      return transactions.slice(0, 50); // Limit to 50 most recent
      
    } catch (error) {
      console.error('❌ BSC transactions error:', error.message);
      return [];
    }
  });
}

async function getBSCBalance(address) {
  return enqueueBscJob(async () => {
    console.log(`🔍 Checking BSC native balance for: ${address}`);
    for (let attempt = 0; attempt < 2; attempt++) {
      try {
        const balance = await bscProvider.getBalance(address);
        const formatted = Number(ethers.utils.formatEther(balance));
        console.log(`✅ BSC native balance for ${address}: ${formatted} BNB`);
        return formatted;
      } catch (error) {
        console.error(`❌ BSC balance attempt ${attempt + 1} error:`, error.message);
        if (attempt < 1) {
          bscProvider = new ethers.providers.JsonRpcProvider(getNextBscRpc());
          await sleep(500);
        }
      }
    }
    return 0;
  });
}

async function sendBSC(fromPrivateKey, toAddress, amount) {
  console.log(`🔄 Sending ${amount} BNB to ${toAddress}`);
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      if (!fromPrivateKey || fromPrivateKey.includes('NOT_SET')) {
        console.error('❌ BSC send error: Private key not set');
        return false;
      }

      const wallet = new ethers.Wallet(fromPrivateKey, bscProvider);
      const tx = await wallet.sendTransaction({
        to: toAddress,
        value: ethers.utils.parseEther(amount.toString())
      });
      
      await tx.wait();
      console.log(`✅ BSC sent: ${amount} BNB to ${toAddress}, txid: ${tx.hash}`);
      return true;
    } catch (error) {
      console.error(`❌ BSC send attempt ${attempt + 1} error:`, error.message);
      if (attempt < 2) {
        bscProvider = new ethers.providers.JsonRpcProvider(getNextBscRpc());
        await sleep(1000);
      }
    }
  }
  return false;
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
      if (attempt < 2) {
        bscProvider = new ethers.providers.JsonRpcProvider(getNextBscRpc());
        await sleep(1000);
      }
    }
  }
  return false;
}

// ========== TRON FUNCTIONS ==========
async function getUSDTBalance(address) {
  try {
    if (!address) return 0;

    console.log(`🔍 Checking TRC20 USDT balance for: ${address}`);
    const tronWebForChecking = new TronWeb({
      fullHost: 'https://api.trongrid.io',
      headers: { 'TRON-PRO-API-KEY': TRONGRID_API_KEY }
    });

    try {
      const contract = await tronWebForChecking.contract().at(USDT_CONTRACT);
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
}

async function getUSDTBalanceFallback(address) {
  try {
    console.log(`🔍 Using fallback method for TRC20 balance: ${address}`);
    const ownerHex = tronWeb.address.toHex(address).replace(/^0x/, '');
    const contractHex = tronWeb.address.toHex(USDT_CONTRACT).replace(/^0x/, '');

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

        console.log(`📥 Found TRC20 deposit: ${amount} USDT from ${from} to ${to}`);
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

// ========== UNIVERSAL AUTO-COLLECT ==========
async function autoCollectToMainWallet(wallet) {
  try {
    console.log(`💰 AUTO-COLLECT started for: ${wallet.address} (${wallet.network})`);
    
    let usdtBalance, nativeBalance, minNativeForFee, fundAmount, companyMain, companyMaster;
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
    
    console.log(`📊 ${wallet.network} Wallet ${wallet.address}:`);
    console.log(`   USDT Balance: ${usdtBalance} USDT`);
    console.log(`   Native Balance: ${nativeBalance} ${wallet.network === 'TRC20' ? 'TRX' : 'BNB'}`);
    
    const amountToTransfer = Math.max(0, usdtBalance - KEEP_AMOUNT);

    if (amountToTransfer <= 0) {
      console.log(`❌ Nothing to collect: ${usdtBalance} USDT (after keeping ${KEEP_AMOUNT} USDT)`);
      return { success: false, reason: 'low_balance' };
    }

    if (nativeBalance < minNativeForFee) {
      console.log(`🔄 Funding ${fundAmount} ${wallet.network === 'TRC20' ? 'TRX' : 'BNB'} from MASTER to ${wallet.address} for gas`);
      const nativeSent = await sendNativeFunction(companyMaster.privateKey, wallet.address, fundAmount);
      if (!nativeSent) {
        console.log('❌ Failed to fund native currency from MASTER');
        return { success: false, reason: 'funding_failed' };
      }
      
      await sleep(15000);
      const newNativeBalance = wallet.network === 'TRC20' ? await getTRXBalance(wallet.address) : await getBSCBalance(wallet.address);
      console.log(`🔄 New native balance after funding: ${newNativeBalance} ${wallet.network === 'TRC20' ? 'TRX' : 'BNB'}`);
      if (newNativeBalance < minNativeForFee) {
        console.log('❌ Native currency still insufficient after funding');
        return { success: false, reason: 'native_still_insufficient' };
      }
    }

    console.log(`🔄 Transferring ${amountToTransfer} USDT to MAIN wallet...`);
    const transferResult = await transferFunction(wallet.private_key, companyMain.address, amountToTransfer);

    if (transferResult) {
      console.log(`✅ SUCCESS: Collected ${amountToTransfer} USDT from ${wallet.address}`);

      try {
        await supabase.from('transactions').insert({
          user_id: wallet.user_id,
          type: 'collect',
          amount: amountToTransfer,
          description: `Auto-collected to ${companyMain.address} (${wallet.network})`,
          status: 'completed',
          created_at: new Date().toISOString()
        });
        console.log(`📊 Collection transaction recorded in database`);
      } catch (e) {
        console.warn('Warning: failed to insert collect transaction record', e.message);
      }

      return { success: true, amount: amountToTransfer };
    } else {
      console.log(`❌ FAILED: USDT transfer from ${wallet.address}`);
      return { success: false, reason: 'usdt_transfer_failed' };
    }
  } catch (error) {
    console.error('❌ Auto-collection fatal error:', error.message);
    console.error('Stack trace:', error.stack);
    return { success: false, reason: 'error', error: error.stack };
  }
}

// ========== UNIVERSAL DEPOSIT PROCESSING ==========
async function processDeposit(wallet, amount, txid, network) {
  try {
    console.log(`💰 PROCESSING DEPOSIT: ${amount} USDT for user ${wallet.user_id}, txid: ${txid}, network: ${network}, wallet: ${wallet.address}`);

    // Check if deposit already exists
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

    // Insert new deposit with wallet_address
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
        console.log(`🔄 Deposit already being processed by another thread: ${txid}`);
        return { success: false, reason: 'concurrent_processing' };
      }
      throw new Error(`Deposit insert failed: ${depositError.message}`);
    }

    const { data: user, error: userError } = await supabase
      .from('profiles')
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

    console.log(`📊 User ${wallet.user_id} balance update: ${currentBalance} → ${newBalance} USDT`);

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

    // Update status to 'completed'
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
        .from('profiles')
        .update({ vip_level: 1 })
        .eq('id', wallet.user_id);
      console.log(`⭐ VIP Level upgraded to 1 for user ${wallet.user_id}`);
    }

    console.log(`✅ DEPOSIT PROCESSED: ${amount} USDT for user ${wallet.user_id}`);
    console.log(`💰 New balance: ${newBalance} USDT`);

    // Schedule auto-collect after deposit
    setTimeout(() => {
      console.log(`🔄 Scheduling auto-collect for wallet ${wallet.address}`);
      autoCollectToMainWallet(wallet).then(result => {
        if (result.success) {
          console.log(`✅ Auto-collect completed: ${result.amount} USDT collected`);
        } else {
          console.log(`❌ Auto-collect failed: ${result.reason}`);
        }
      }).catch(err => {
        console.error('Auto-collect post-deposit failed:', err.message);
      });
    }, 10000);

    return { success: true, amount, deposit_id: newDeposit.id };

  } catch (error) {
    console.error('❌ Error processing deposit:', error.message);
    console.error('Stack trace:', error.stack);
    
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

    const { data, error } = await supabase.from('user_wallets').insert({
      user_id,
      address,
      private_key,
      network,
      created_at: new Date().toISOString()
    }).select().single();

    if (error) {
      console.error('❌ Database error:', error);
      return res.status(500).json({ success: false, error: 'Failed to save wallet' });
    }

    console.log(`✅ New ${network} wallet created: ${address}`);
    
    // Schedule deposit check for this wallet
    setTimeout(() => {
      console.log(`🔍 Scheduling initial deposit check for new wallet: ${address}`);
      checkUserDeposits(user_id, network).catch(err => {
        console.error(`Error in initial deposit check for ${address}:`, err.message);
      });
    }, 5000);

    res.json({ success: true, address, exists: false, network });
  } catch (error) {
    console.error('❌ Generate wallet error:', error.message);
    console.error('Stack trace:', error.stack);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

// Endpoint для получения адреса пополнения
app.get('/deposit-address/:userId/:network', async (req, res) => {
  try {
    const { userId, network } = req.params;

    console.log(`🔍 Getting deposit address for user ${userId}, network ${network}`);

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

    console.log(`✅ Found wallet for user ${userId}: ${wallet.address}`);
    res.json({ 
      success: true, 
      address: wallet.address
    });
  } catch (error) {
    console.error('❌ Get deposit address error:', error.message);
    console.error('Stack trace:', error.stack);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

// ========== OPTIMIZED BEP20 DEPOSIT CHECK ==========
async function checkBep20Deposits() {
  try {
    console.log('🔄 ===== BEP20 DEPOSIT CHECK STARTED =====');
    
    // Get only BEP20 wallets
    const { data: wallets, error } = await supabase
      .from('user_wallets')
      .select('*')
      .eq('network', 'BEP20')
      .limit(250);
    
    if (error) throw error;

    console.log(`🔍 Checking ${wallets?.length || 0} BEP20 wallets`);
    
    let processedCount = 0;
    let depositsFound = 0;
    let errors = 0;

    // Process wallets in batches for better performance
    const batchSize = 10;
    for (let i = 0; i < wallets.length; i += batchSize) {
      const batch = wallets.slice(i, i + batchSize);
      console.log(`🔍 Processing batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(wallets.length / batchSize)}`);
      
      // Parallel processing of wallets in batch
      const batchPromises = batch.map(async (wallet) => {
        try {
          console.log(`🔍 Processing wallet ${wallet.address} for user ${wallet.user_id}`);
          
          // Small delay between wallets in batch
          await sleep(200);
          
          const transactions = await getBSCTransactions(wallet.address);
          console.log(`📊 Found ${transactions.length} transactions for wallet ${wallet.address}`);

          let walletDeposits = 0;
          
          for (const tx of transactions) {
            const recipient = tx.to.toLowerCase();
            const walletAddress = wallet.address.toLowerCase();
            
            if (recipient === walletAddress && tx.token === 'USDT' && tx.amount >= MIN_DEPOSIT) {
              console.log(`💰 Potential deposit found: ${tx.amount} USDT to ${walletAddress}, txid: ${tx.transaction_id}`);
              try {
                const result = await processDeposit(wallet, tx.amount, tx.transaction_id, wallet.network);
                if (result.success) {
                  walletDeposits++;
                  console.log(`✅ Deposit processed successfully: ${tx.amount} USDT for user ${wallet.user_id}`);
                } else if (result.reason === 'already_processed') {
                  console.log(`ℹ️ Deposit already processed: ${tx.transaction_id}`);
                }
              } catch (err) {
                console.error(`❌ Error processing deposit ${tx.transaction_id}:`, err.message);
                errors++;
              }
            }
          }

          // Update last checked time
          await supabase
            .from('user_wallets')
            .update({ last_checked: new Date().toISOString() })
            .eq('id', wallet.id);

          processedCount++;
          depositsFound += walletDeposits;
          
          console.log(`✅ Completed processing wallet ${wallet.address}, found ${walletDeposits} deposits`);
          
          return { success: true, deposits: walletDeposits };
        } catch (err) {
          console.error(`❌ Error processing wallet ${wallet.address}:`, err.message);
          errors++;
          return { success: false, error: err.message };
        }
      });

      // Wait for current batch to complete
      await Promise.all(batchPromises);
      
      // Delay between batches
      if (i + batchSize < wallets.length) {
        console.log(`⏳ Waiting 2 seconds before next batch...`);
        await sleep(2000);
      }
    }

    const message = `✅ BEP20 check completed: Processed ${processedCount} wallets, found ${depositsFound} new deposits, ${errors} errors`;
    console.log(`🔄 ===== BEP20 DEPOSIT CHECK COMPLETED =====`);
    console.log(message);
    
    return { success: true, processedCount, depositsFound, errors };
  } catch (error) {
    console.error('❌ BEP20 deposit check error:', error.message);
    return { success: false, error: error.message };
  }
}

// ========== TRC20 DEPOSIT CHECK ==========
async function checkTrc20Deposits() {
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
    let errors = 0;

    for (const wallet of wallets || []) {
      try {
        console.log(`🔍 Processing wallet ${wallet.address} for user ${wallet.user_id}`);
        await sleep(1000); // Rate limiting for TRON
        
        const transactions = await getUSDTTransactions(wallet.address);
        console.log(`📊 Found ${transactions.length} transactions for wallet ${wallet.address}`);

        for (const tx of transactions) {
          const recipient = tx.to;
          const walletAddress = wallet.address;
          
          if (recipient === walletAddress && tx.token === 'USDT' && tx.amount >= MIN_DEPOSIT) {
            console.log(`💰 Potential deposit found: ${tx.amount} USDT to ${walletAddress}, txid: ${tx.transaction_id}`);
            try {
              const result = await processDeposit(wallet, tx.amount, tx.transaction_id, wallet.network);
              if (result.success) {
                depositsFound++;
                console.log(`✅ Deposit processed successfully: ${tx.amount} USDT for user ${wallet.user_id}`);
              } else if (result.reason === 'already_processed') {
                console.log(`ℹ️ Deposit already processed: ${tx.transaction_id}`);
              }
            } catch (err) {
              console.error(`❌ Error processing deposit ${tx.transaction_id}:`, err.message);
              errors++;
            }
          }
        }

        await supabase
          .from('user_wallets')
          .update({ last_checked: new Date().toISOString() })
          .eq('id', wallet.id);

        processedCount++;
        console.log(`✅ Completed processing wallet ${wallet.address}`);
      } catch (err) {
        console.error(`❌ Error processing wallet ${wallet.address}:`, err.message);
        errors++;
      }
    }

    const message = `✅ TRC20 check completed: Processed ${processedCount} wallets, found ${depositsFound} new deposits, ${errors} errors`;
    console.log(`🔄 ===== TRC20 DEPOSIT CHECK COMPLETED =====`);
    console.log(message);
    
    return { success: true, processedCount, depositsFound, errors };
  } catch (error) {
    console.error('❌ TRC20 deposit check error:', error.message);
    return { success: false, error: error.message };
  }
}

// Deposit check endpoints
app.post('/check-bep20-deposits', async (req, res) => {
  try {
    console.log('🔄 MANUAL BEP20 DEPOSIT CHECK STARTED');
    const result = await checkBep20Deposits();
    res.json(result);
  } catch (error) {
    console.error('❌ Manual BEP20 check error:', error.message);
    res.status(500).json({ success: false, error: error.message });
  }
});

app.post('/check-trc20-deposits', async (req, res) => {
  try {
    console.log('🔄 MANUAL TRC20 DEPOSIT CHECK STARTED');
    const result = await checkTrc20Deposits();
    res.json(result);
  } catch (error) {
    console.error('❌ Manual TRC20 check error:', error.message);
    res.status(500).json({ success: false, error: error.message });
  }
});

app.post('/check-deposits', async (req, res) => { 
  try {
    console.log('🔄 MANUAL ALL DEPOSITS CHECK STARTED');
    const bep20Result = await checkBep20Deposits();
    await sleep(5000); // Wait between networks
    const trc20Result = await checkTrc20Deposits();
    
    res.json({
      success: true,
      bep20: bep20Result,
      trc20: trc20Result
    });
  } catch (error) {
    console.error('❌ Manual all deposits check error:', error.message);
    res.status(500).json({ success: false, error: error.message });
  }
});

// collect funds endpoints
app.post('/collect-funds', async (req, res) => { 
  try {
    console.log('💰 ===== MANUAL FUNDS COLLECTION STARTED =====');
    const { data: wallets, error } = await supabase.from('user_wallets').select('*').limit(200);
    if (error) throw error;

    console.log(`🔍 Starting collection for ${wallets?.length || 0} wallets`);
    
    let collectedCount = 0;
    let totalCollected = 0;
    let failedCount = 0;
    
    for (const wallet of wallets || []) {
      try {
        console.log(`🔍 Processing collection for wallet ${wallet.address} (${wallet.network})`);
        await sleep(2000); // Rate limiting
        
        const result = await autoCollectToMainWallet(wallet);
        if (result && result.success) {
          collectedCount++;
          totalCollected += result.amount;
          console.log(`✅ Successfully collected ${result.amount} USDT from ${wallet.address}`);
          await sleep(1000);
        } else {
          failedCount++;
          console.log(`❌ Failed to collect from ${wallet.address}: ${result?.reason || 'Unknown error'}`);
        }
      } catch (err) {
        failedCount++;
        console.error(`❌ Error collecting from ${wallet.address}:`, err.message);
      }
    }

    const message = `✅ Collection completed: Collected ${totalCollected.toFixed(6)} USDT from ${collectedCount} wallets, ${failedCount} failed`;
    console.log(`💰 ===== MANUAL FUNDS COLLECTION COMPLETED =====`);
    console.log(message);
    
    res.json({ success: true, message });
  } catch (error) {
    console.error('❌ Funds collection error:', error.message);
    console.error('Stack trace:', error.stack);
    res.status(500).json({ success: false, error: error.message });
  }
});

// ========== helper DB functions ==========
async function ensureUserExists(userId) {
  try {
    // Check if user exists ONLY in profiles
    const { data } = await supabase.from('profiles').select('id').eq('id', userId).single();
    
    if (!data) {
      console.log(`👤 Creating new user in profiles: ${userId}`);
      
      // Create user ONLY in profiles
      const { error: insertError } = await supabase.from('profiles').insert({
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

      if (insertError) {
        console.error('❌ Error creating user profile:', insertError);
        throw insertError;
      }
      
      console.log(`✅ User created in profiles: ${userId}`);
    } else {
      console.log(`✅ User already exists in profiles: ${userId}`);
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
    
    if (!wallet) {
      console.log(`❌ Wallet not found for user ${userId}, network ${network}`);
      return;
    }
    
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
    
    console.log(`📊 Found ${transactions.length} transactions for user ${userId}`);
    
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

// ========== TEST ENDPOINTS ==========
app.get('/test-logs', (req, res) => {
  console.log('🧪 TEST LOG: This should appear in Railway logs');
  console.error('🔴 TEST ERROR LOG: This should appear as error');
  console.warn('🟡 TEST WARN LOG: This should appear as warning');
  
  const testData = {
    message: 'Test log entry',
    timestamp: new Date().toISOString(),
    randomId: Math.random().toString(36).substring(7),
    userAgent: req.get('User-Agent'),
    ip: req.ip
  };
  
  console.log('📊 TEST STRUCTURED LOG:', JSON.stringify(testData, null, 2));
  
  res.json({
    success: true,
    message: 'Test logs generated - check Railway logs',
    testId: testData.randomId,
    timestamp: testData.timestamp
  });
});

app.get('/health', (req, res) => {
  const healthCheck = {
    status: '✅ HEALTHY',
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    memory: process.memoryUsage(),
    env: {
      NODE_ENV: process.env.NODE_ENV,
      PORT: process.env.PORT,
      SUPABASE_URL: process.env.SUPABASE_URL ? 'SET' : 'MISSING',
      CHAINSTACK_BSC_URL: process.env.CHAINSTACK_BSC_URL ? 'SET' : 'MISSING'
    }
  };
  
  console.log('🏥 HEALTH CHECK:', JSON.stringify(healthCheck, null, 2));
  
  res.json(healthCheck);
});

// ========== HEALTH CHECK ==========
app.get('/', (req, res) => {
  const status = {
    status: '✅ WORKING',
    message: 'Cocoon AI - Deposit & Withdrawal System',
    timestamp: new Date().toISOString(),
    networks: ['TRC20', 'BEP20'],
    features: [
      'Multi-Network Wallet Generation',
      'Deposit Processing',
      'Auto Collection',
      'Enhanced Logging',
      'Chainstack BSC Integration'
    ],
    stats: {
      checkIntervalBEP20: `${CHECK_INTERVAL_BEP20_MS / 1000} seconds`,
      checkIntervalTRC20: `${CHECK_INTERVAL_TRC20_MS / 1000} seconds`,
      minDeposit: `${MIN_DEPOSIT} USDT`,
      keepAmount: `${KEEP_AMOUNT} USDT`,
      bscProvider: 'Chainstack (Primary)'
    }
  };
  
  console.log('🏠 Root endpoint called - returning status');
  res.json(status);
});

// ========== HEARTBEAT LOGGING ==========
console.log('💓 Starting heartbeat logger...');
setInterval(() => {
  console.log('💓 SERVER HEARTBEAT - ' + new Date().toISOString());
}, 30000); // Every 30 seconds

// ========== SCHEDULED DEPOSIT CHECKS ==========
console.log('⏰ Starting scheduled BEP20 checks...');
setInterval(async () => {
  try {
    console.log('🕒 ===== SCHEDULED BEP20 CHECK STARTED =====');
    await checkBep20Deposits();
    console.log('🕒 ===== SCHEDULED BEP20 CHECK COMPLETED =====');
  } catch (err) {
    console.error('❌ Scheduled BEP20 check error:', err.message);
  }
}, CHECK_INTERVAL_BEP20_MS);

console.log('⏰ Starting scheduled TRC20 checks...');
setInterval(async () => {
  try {
    console.log('🕒 ===== SCHEDULED TRC20 CHECK STARTED =====');
    await checkTrc20Deposits();
    console.log('🕒 ===== SCHEDULED TRC20 CHECK COMPLETED =====');
  } catch (err) {
    console.error('❌ Scheduled TRC20 check error:', err.message);
  }
}, CHECK_INTERVAL_TRC20_MS);

// ========== START SERVER ==========
console.log('🎯 ALL INITIALIZATION COMPLETE - STARTING SERVER...');

app.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 SERVER SUCCESSFULLY STARTED on port ${PORT}`);
  console.log(`✅ SUPABASE: CONNECTED`);
  console.log(`✅ TRONGRID: API KEY SET`);
  console.log(`🔌 BSC PROVIDER: CHAINSTACK`);
  console.log(`💰 TRC20 MASTER: ${COMPANY.MASTER.address}`);
  console.log(`💰 TRC20 MAIN: ${COMPANY.MAIN.address}`);
  console.log(`💰 BEP20 MASTER: ${COMPANY_BSC.MASTER.address}`);
  console.log(`💰 BEP20 MAIN: ${COMPANY_BSC.MAIN.address}`);
  console.log(`⏰ BEP20 AUTO-CHECK: EVERY ${Math.round(CHECK_INTERVAL_BEP20_MS / 1000)}s`);
  console.log(`⏰ TRC20 AUTO-CHECK: EVERY ${Math.round(CHECK_INTERVAL_TRC20_MS / 1000)}s`);
  console.log('===================================');
  console.log('🎉 APPLICATION READY - BEP20 OPTIMIZED WITH CHAINSTACK');
});
