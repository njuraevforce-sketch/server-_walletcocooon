// server.js
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

// Global fetch fallback for Node <18
if (typeof fetch === 'undefined') {
  try {
    global.fetch = require('node-fetch');
    console.log('🔁 polyfill fetch applied (node-fetch).');
  } catch (e) {
    console.warn('⚠️ fetch is not available and node-fetch not installed — TRON fallback calls may fail on Node <18');
  }
}

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
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVxemZpdmRja3pya2tuY2FobHluIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MTYwNTg2NSwiZXhwIjoyMDc3MTgxODY1fQ.AuGqzDDMzWS1COhHdBMchHarYmd1gNC_9PfRfJWPTxc'; // keep secret in prod
const TRONGRID_API_KEY = process.env.TRONGRID_API_KEY || '33759ca3-ffb8-41bc-9036-25a32601eae2';

console.log('🔄 Initializing Supabase client...');
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
console.log('✅ Supabase client initialized');

console.log('🔄 Initializing TronWeb...');
const tronWeb = new TronWeb({
  fullHost: 'https://api.trongrid.io',
  headers: { 'TRON-PRO-API-KEY': TRONGRID_API_KEY }
});
console.log('✅ TronWeb initialized');

// ========== BSC RPC CONFIGURATION (ROTATION) ==========
const BSC_RPC_URLS = (process.env.BSC_RPC_URLS && process.env.BSC_RPC_URLS.split(',')) || [
  'https://bsc-mainnet.core.chainstack.com/5de4f946fec6345ad71e18bf5f6386a5',
  'https://bsc-dataseed.binance.org/',
  'https://bsc-dataseed1.defibit.io/',
  'https://bsc-dataseed1.ninicoin.io/',
  'https://bsc-dataseed2.defibit.io/',
  'https://bsc-dataseed3.defibit.io/',
  'https://bsc-dataseed4.defibit.io/',
  'https://bsc.rpc.blxrbdn.com',
  'https://bsc-mainnet.public.blastapi.io'
];

console.log('🔌 BSC RPC URLs configured:');
BSC_RPC_URLS.forEach((url, index) => {
  console.log(`   ${index === 0 ? '🟢 PRIMARY' : '🔵 FALLBACK'} ${index + 1}: ${url.substring(0, 60)}...`);
});

let currentRpcIndex = 0;
function getNextBscRpc() {
  const rpc = BSC_RPC_URLS[currentRpcIndex];
  console.log(`🔌 Using BSC RPC: ${rpc.substring(0, 60)}... (index: ${currentRpcIndex})`);
  currentRpcIndex = (currentRpcIndex + 1) % BSC_RPC_URLS.length;
  return rpc;
}

// Initialize provider
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
const USDT_CONTRACT = 'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t'; // TRON
const USDT_BSC_CONTRACT = (process.env.USDT_BSC_CONTRACT || '0x55d398326f99059fF775485246999027B3197955').toLowerCase();
const USDT_ABI = [
  "function balanceOf(address) view returns (uint256)",
  "function transfer(address to, uint256 amount) returns (bool)",
  "event Transfer(address indexed from, address indexed to, uint256 value)"
];

const MIN_DEPOSIT = Number(process.env.MIN_DEPOSIT || 10);
const KEEP_AMOUNT = Number(process.env.KEEP_AMOUNT || 1.0);
const MIN_TRX_FOR_FEE = Number(process.env.MIN_TRX_FOR_FEE || 3);
const MIN_BNB_FOR_FEE = Number(process.env.MIN_BNB_FOR_FEE || 0.005);
const FUND_TRX_AMOUNT = Number(process.env.FUND_TRX_AMOUNT || 10);
const FUND_BNB_AMOUNT = Number(process.env.FUND_BNB_AMOUNT || 0.01);

// New: how many blocks to look back (for performance, not too large)
const BSC_BLOCK_LOOKBACK = Number(process.env.BSC_BLOCK_LOOKBACK || 12000); // ~12k blocks by default
let BSC_CHUNK_SIZE = Number(process.env.BSC_CHUNK_SIZE || 1500); // blocks per getLogs chunk (safe default)
const BSC_WALLET_CONCURRENCY = Number(process.env.BSC_WALLET_CONCURRENCY || 10); // parallel wallets

// Throttling / concurrency
const BALANCE_CONCURRENCY = Number(process.env.BALANCE_CONCURRENCY || 2);
const CHECK_INTERVAL_MS = Number(process.env.CHECK_INTERVAL_MS || 2 * 60 * 1000); // 2 minutes

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

// Simple queue for throttling balance calls
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

// ========== BSC FUNCTIONS WITH CHAINSTACK & ROBUST LOGS CHUNKING ==========
async function rotateProvider() {
  try {
    bscProvider = new ethers.providers.JsonRpcProvider(getNextBscRpc());
    // quick test call
    await bscProvider.getBlockNumber();
  } catch (e) {
    console.warn('🔁 rotateProvider warning:', e.message);
  }
}

async function getBSCUSDTBalance(address) {
  console.log(`🔍 Checking BSC USDT balance for: ${address}`);
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const contract = new ethers.Contract(USDT_BSC_CONTRACT, USDT_ABI, bscProvider);
      const balance = await contract.balanceOf(address);
      const formatted = Number(ethers.utils.formatUnits(balance, 18));
      console.log(`✅ BSC USDT balance for ${address}: ${formatted} USDT`);
      return formatted;
    } catch (error) {
      console.error(`❌ BSC USDT balance attempt ${attempt + 1} error:`, error.message);
      if (attempt < 2) {
        await rotateProvider();
        await sleep(1000 * (attempt + 1));
      }
    }
  }
  return 0;
}

async function getBSCBalance(address) {
  console.log(`🔍 Checking BSC native balance for: ${address}`);
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const balance = await bscProvider.getBalance(address);
      const formatted = Number(ethers.utils.formatEther(balance));
      console.log(`✅ BSC native balance for ${address}: ${formatted} BNB`);
      return formatted;
    } catch (error) {
      console.error(`❌ BSC balance attempt ${attempt + 1} error:`, error.message);
      if (attempt < 2) {
        await rotateProvider();
        await sleep(1000 * (attempt + 1));
      }
    }
  }
  return 0;
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
        await rotateProvider();
        await sleep(1000 * (attempt + 1));
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
        await rotateProvider();
        await sleep(1000 * (attempt + 1));
      }
    }
  }
  return false;
}

// Robust getLogs with chunking to avoid "limit exceeded"
async function fetchLogsInChunks(provider, filter, fromBlock, toBlock, initialChunkSize = BSC_CHUNK_SIZE) {
  let results = [];
  let chunkSize = initialChunkSize;
  const latest = toBlock;

  for (let start = fromBlock; start <= latest; ) {
    let end = Math.min(start + chunkSize - 1, latest);
    try {
      const logs = await provider.getLogs({
        ...filter,
        fromBlock: ethers.utils.hexlify(start),
        toBlock: ethers.utils.hexlify(end)
      });
      if (logs && logs.length) {
        results.push(...logs);
      }
      // small pause to avoid rate limiting
      await sleep(200);
      start = end + 1;
      // gently increase chunkSize back towards initial if we reduced it earlier
      if (chunkSize < initialChunkSize) chunkSize = Math.min(initialChunkSize, Math.floor(chunkSize * 1.5));
    } catch (err) {
      const msg = (err && err.message) ? err.message.toLowerCase() : '';
      // Detect "limit exceeded" or RPC server error and reduce chunk size
      if (err && (err.code === -32005 || msg.includes('limit exceeded') || msg.includes('exceeded'))) {
        console.warn(`⚠️ RPC limit exceeded for range ${start}-${end}. Reducing chunk size (was ${chunkSize}).`);
        // reduce chunk size
        chunkSize = Math.max(200, Math.floor(chunkSize / 2));
        // if chunkSize is small, rotate provider to try another RPC endpoint
        if (chunkSize === 200) {
          console.log('🔁 Rotating RPC provider due to repeated limit exceeded...');
          await rotateProvider();
          // small pause
          await sleep(500);
        }
        // retry same start with reduced chunkSize (do not increment start)
        continue;
      } else {
        console.error(`❌ fetchLogsInChunks error for ${start}-${end}:`, err.message || err);
        // on non-limit error rotate provider and try next chunk
        await rotateProvider();
        start = end + 1;
        await sleep(300);
      }
    }
  }
  return results;
}

async function getBSCTransactions(address) {
  try {
    if (!address) return [];
    console.log(`🔍 Checking BSC transactions via RPC: ${address}`);

    const currentBlock = await bscProvider.getBlockNumber();
    const fromBlock = Math.max(0, currentBlock - BSC_BLOCK_LOOKBACK);
    console.log(`📊 Checking blocks: ${fromBlock} to ${currentBlock} (${currentBlock - fromBlock} blocks)`);

    const transferTopic = ethers.utils.id("Transfer(address,address,uint256)");
    // prepare recipient topic (32 bytes)
    let toTopic;
    try {
      toTopic = ethers.utils.hexZeroPad(ethers.utils.getAddress(address), 32);
    } catch (e) {
      // fallback to lowercased padded
      const raw = address.toLowerCase().replace(/^0x/, '');
      toTopic = '0x' + raw.padStart(64, '0');
    }

    const filter = {
      address: USDT_BSC_CONTRACT,
      topics: [transferTopic, null, toTopic],
    };

    const logs = await fetchLogsInChunks(bscProvider, filter, fromBlock, currentBlock, BSC_CHUNK_SIZE);
    console.log(`✅ RPC: Found ${logs.length} raw transfer events for ${address}`);

    const transactions = [];

    // attempt to fetch timestamp info for each unique block
    const blockCache = {};

    for (const log of logs) {
      try {
        const fromAddr = ethers.utils.getAddress('0x' + log.topics[1].substring(26));
        const toAddr = ethers.utils.getAddress('0x' + log.topics[2].substring(26));
        const value = ethers.BigNumber.from(log.data);
        const amount = Number(ethers.utils.formatUnits(value, 18));

        // Try to get tx details and block timestamp (best-effort)
        let timestamp = Date.now();
        try {
          const tx = await bscProvider.getTransaction(log.transactionHash);
          if (tx && tx.blockNumber) {
            const blockNumber = tx.blockNumber;
            if (!blockCache[blockNumber]) {
              const block = await bscProvider.getBlock(blockNumber);
              blockCache[blockNumber] = block ? (block.timestamp * 1000) : Date.now();
            }
            timestamp = blockCache[blockNumber];
          }
        } catch (blockError) {
          console.warn('Could not get block timestamp for tx', log.transactionHash, blockError.message);
        }

        transactions.push({
          transaction_id: log.transactionHash,
          to: toAddr,
          from: fromAddr,
          amount: amount,
          token: 'USDT',
          confirmed: true,
          network: 'BEP20',
          timestamp: timestamp
        });

        console.log(`💰 FOUND BSC DEPOSIT: ${amount} USDT from ${fromAddr} to ${toAddr} (tx: ${log.transactionHash})`);
      } catch (e) {
        console.warn('❌ Skipping malformed BSC log:', e.message);
        continue;
      }
    }

    transactions.sort((a, b) => b.timestamp - a.timestamp);
    console.log(`📈 Total BSC deposits found: ${transactions.length}`);
    return transactions;
  } catch (error) {
    console.error('❌ BSC transactions error:', error.message || error);
    // rotate provider on fatal error
    await rotateProvider();
    return [];
  }
}

// ========== TRON FUNCTIONS (UNCHANGED) ==========
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
  });
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

// ========== UNIVERSAL AUTO-COLLECT (UNCHANGED LOGIC) ==========
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

    // Insert new deposit with wallet_address - используем 'pending'
    const { data: newDeposit, error: depositError } = await supabase
      .from('deposits')
      .insert({
        user_id: wallet.user_id,
        amount,
        txid: txid,
        network,
        wallet_address: wallet.address,
        status: 'pending', // ← ИСПРАВЛЕНО: используем разрешенный статус
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
      .from('profiles')  // ✅ ТОЛЬКО profiles
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

    // Обновляем статус на 'completed'
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

// ========== API Endpoints & WALLET CHECK WITH PARALLEL BATCHING ==========
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

// Manual check endpoints
app.post('/check-deposits', async (req, res) => { await handleCheckDeposits(req, res); });
app.get('/check-deposits', async (req, res) => { await handleCheckDeposits(req, res); });

async function handleCheckDeposits(req = {}, res = {}) {
  try {
    console.log('🔄 ===== MANUAL DEPOSIT CHECK STARTED =====');
    const { data: wallets, error } = await supabase.from('user_wallets').select('*').limit(2000);
    if (error) throw error;

    console.log(`🔍 Checking ${wallets?.length || 0} wallets across all networks`);
    
    let processedCount = 0;
    let depositsFound = 0;
    let duplicatesSkipped = 0;

    // Split wallets into BEP20 and others
    const bep20Wallets = (wallets || []).filter(w => w.network === 'BEP20');
    const otherWallets = (wallets || []).filter(w => w.network !== 'BEP20');

    // Process non-BEP20 (TRC20) sequentially (unchanged)
    for (const wallet of otherWallets) {
      try {
        console.log(`🔍 Processing wallet ${wallet.address} (${wallet.network}) for user ${wallet.user_id}`);
        await sleep(1000);
        let transactions = [];
        if (wallet.network === 'TRC20') {
          transactions = await getUSDTTransactions(wallet.address);
        }
        console.log(`📊 Found ${transactions.length} transactions for wallet ${wallet.address}`);
        for (const tx of transactions) {
          const recipient = tx.to;
          const walletAddress = wallet.address;
          if (recipient === walletAddress && tx.token === 'USDT' && tx.amount >= MIN_DEPOSIT) {
            console.log(`🎯 CONFIRMED ${wallet.network} DEPOSIT: ${tx.amount} USDT to ${walletAddress}, txid: ${tx.transaction_id}`);
            try {
              const result = await processDeposit(wallet, tx.amount, tx.transaction_id, wallet.network);
              if (result.success) {
                depositsFound++;
              } else if (result.reason === 'already_processed' || result.reason === 'concurrent_processing') {
                duplicatesSkipped++;
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

    // Process BEP20 in parallel batches
    const batchSize = BSC_WALLET_CONCURRENCY;
    for (let i = 0; i < bep20Wallets.length; i += batchSize) {
      const batch = bep20Wallets.slice(i, i + batchSize);
      console.log(`🔁 Processing BEP20 batch ${Math.floor(i/batchSize)+1} (size ${batch.length})`);
      await Promise.all(batch.map(async (wallet) => {
        try {
          console.log(`🔍 Processing wallet ${wallet.address} (BEP20) for user ${wallet.user_id}`);
          // small per-wallet delay
          await sleep(200);
          const transactions = await getBSCTransactions(wallet.address);
          console.log(`📊 Found ${transactions.length} transactions for wallet ${wallet.address}`);
          for (const tx of transactions) {
            try {
              const recipient = ethers.utils.getAddress(tx.to);
              const walletAddrNorm = ethers.utils.getAddress(wallet.address);
              if (recipient === walletAddrNorm && tx.token === 'USDT' && tx.amount >= MIN_DEPOSIT) {
                console.log(`🎯 CONFIRMED BEP20 DEPOSIT: ${tx.amount} USDT to ${walletAddrNorm}, txid: ${tx.transaction_id}`);
                const result = await processDeposit(wallet, tx.amount, tx.transaction_id, wallet.network);
                if (result.success) {
                  depositsFound++;
                } else if (result.reason === 'already_processed' || result.reason === 'concurrent_processing') {
                  duplicatesSkipped++;
                }
              }
            } catch (innerErr) {
              console.warn('⚠️ Error checking tx for wallet', wallet.address, innerErr.message);
            }
          }
          await supabase.from('user_wallets').update({ last_checked: new Date().toISOString() }).eq('id', wallet.id);
          processedCount++;
        } catch (err) {
          console.error(`❌ Error processing BEP20 wallet ${wallet.address}:`, err.message);
          // rotate provider if needed
          await rotateProvider();
        }
      }));
      // small pause between batches
      await sleep(500);
    }

    const message = `✅ Deposit check completed: Processed ${processedCount} wallets, found ${depositsFound} new deposits, skipped ${duplicatesSkipped} duplicates`;
    console.log(`🔄 ===== MANUAL DEPOSIT CHECK COMPLETED =====`);
    console.log(message);
    
    if (res && typeof res.json === 'function') res.json({ success: true, message });
    return { success: true, message };
  } catch (error) {
    console.error('❌ Deposit check error:', error.message);
    console.error('Stack trace:', error.stack);
    if (res && typeof res.status === 'function') res.status(500).json({ success: false, error: error.message });
    return { success: false, error: error.message };
  }
}

// collect funds endpoints
app.post('/collect-funds', async (req, res) => { await handleCollectFunds(req, res); });
app.get('/collect-funds', async (req, res) => { await handleCollectFunds(req, res); });

async function handleCollectFunds(req = {}, res = {}) {
  try {
    console.log('💰 ===== MANUAL FUNDS COLLECTION STARTED =====');
    const { data: wallets, error } = await supabase.from('user_wallets').select('*').limit(2000);
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
    
    if (res && typeof res.json === 'function') res.json({ success: true, message });
    return { success: true, message };
  } catch (error) {
    console.error('❌ Funds collection error:', error.message);
    console.error('Stack trace:', error.stack);
    if (res && typeof res.status === 'function') res.status(500).json({ success: false, error: error.message });
    return { success: false, error: error.message };
  }
}

// ========== helper DB functions ==========
async function ensureUserExists(userId) {
  try {
    const { data } = await supabase.from('profiles').select('id').eq('id', userId).single();
    
    if (!data) {
      console.log(`👤 Creating new user in profiles: ${userId}`);
      
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
      
      const isRecipientMatch = network === 'TRC20' 
        ? recipient === walletAddress
        : recipient.toLowerCase() === walletAddress.toLowerCase();

      if (isRecipientMatch && tx.token === 'USDT' && tx.amount >= MIN_DEPOSIT) {
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

// ========== TEST / HEALTH ENDPOINTS ==========
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
      SUPABASE_URL: process.env.SUPABASE_URL ? 'SET' : 'MISSING'
    }
  };
  
  console.log('🏥 HEALTH CHECK:', JSON.stringify(healthCheck, null, 2));
  
  res.json(healthCheck);
});

app.get('/', (req, res) => {
  const status = {
    status: '✅ WORKING',
    message: 'Cocoon AI - Deposit & Withdrawal System',
    timestamp: new Date().toISOString(),
    networks: ['TRC20', 'BEP20'],
    features: [
      'Multi-Network Wallet Generation',
      'Deposit Processing (BEP20 improved)',
      'Auto Collection',
      'Enhanced Logging',
      'Chainstack BSC Integration'
    ],
    stats: {
      checkInterval: `${CHECK_INTERVAL_MS / 1000} seconds`,
      minDeposit: `${MIN_DEPOSIT} USDT`,
      keepAmount: `${KEEP_AMOUNT} USDT`,
      bscProvider: 'Chainstack (Primary) + rotation',
      blockRangeLookback: `${BSC_BLOCK_LOOKBACK} blocks`,
      bscChunkSize: `${BSC_CHUNK_SIZE} blocks`
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
console.log('⏰ Starting scheduled deposit checks...');
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
console.log('🎯 ALL INITIALIZATION COMPLETE - STARTING SERVER...');

app.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 SERVER SUCCESSFULLY STARTED on port ${PORT}`);
  console.log(`✅ SUPABASE: CONNECTED`);
  console.log(`✅ TRONGRID: API KEY SET`);
  console.log(`🔌 BSC PROVIDER: ROTATING among ${BSC_RPC_URLS.length} endpoints`);
  console.log(`📊 BSC BLOCK LOOKBACK: ${BSC_BLOCK_LOOKBACK} blocks`);
  console.log(`📊 BSC CHUNK SIZE: ${BSC_CHUNK_SIZE} blocks`);
  console.log(`📊 BEP20 WALLET CONCURRENCY: ${BSC_WALLET_CONCURRENCY}`);
  console.log('===================================');
  console.log('🎉 BEP20 deposit detection improvements enabled!');
});
