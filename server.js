// server.js - OPTIMIZED BEP20 WITH YOUR KEYS
const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const TronWeb = require('tronweb');
const { ethers } = require('ethers');

// ========== ENHANCED LOGGING ==========
console.log('🚀 STARTING SERVER - BEP20 OPTIMIZED');
console.log('📅 Server start time:', new Date().toISOString());

// Global fetch fallback
if (typeof fetch === 'undefined') {
  try {
    global.fetch = require('node-fetch');
    console.log('🔁 polyfill fetch applied');
  } catch (e) {
    console.warn('⚠️ fetch not available');
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

// ========== ENVIRONMENT VARIABLES WITH YOUR KEYS ==========
const SUPABASE_URL = 'https://eqzfivdckzrkkncahlyn.supabase.co';
const SUPABASE_SERVICE_ROLE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVxemZpdmRja3pya2tuY2FobHluIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MTYwNTg2NSwiZXhwIjoyMDc3MTgxODY1fQ.AuGqzDDMzWS1COhHdBMchHarYmd1gNC_9PfRfJWPTxc';
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

// ========== BSC RPC CONFIGURATION (OPTIMIZED) ==========
const BSC_RPC_URLS = [
  'https://bsc-dataseed.binance.org/',
  'https://bsc-dataseed1.defibit.io/',
  'https://bsc-dataseed1.ninicoin.io/',
  'https://bsc-dataseed2.defibit.io/',
  'https://bsc-dataseed3.defibit.io/',
  'https://bsc-dataseed4.defibit.io/',
];

console.log('🔌 BSC RPC URLs configured:', BSC_RPC_URLS.length);

let currentRpcIndex = 0;
function getBscProvider() {
  const rpcUrl = BSC_RPC_URLS[currentRpcIndex];
  currentRpcIndex = (currentRpcIndex + 1) % BSC_RPC_URLS.length;
  return new ethers.providers.JsonRpcProvider(rpcUrl);
}

// COMPANY wallets
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

// ========== MIDDLEWARE ==========
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

// ========== CONSTANTS (OPTIMIZED) ==========
const USDT_CONTRACT = 'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t';
const USDT_BSC_CONTRACT = '0x55d398326f99059fF775485246999027B3197955'.toLowerCase();
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

// Новый подход: проверяем только последние 2000 блоков (это ~1 час)
const BSC_BLOCK_LOOKBACK = 2000;
let lastCheckedBlock = 0;

// ========== BSC FUNCTIONS (COMPLETELY REWRITTEN) ==========

async function getLatestBscBlock() {
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const provider = getBscProvider();
      const block = await provider.getBlockNumber();
      return block;
    } catch (error) {
      console.warn(`⚠️ Failed to get block (attempt ${attempt + 1}):`, error.message);
      await sleep(2000);
    }
  }
  return 0;
}

// Основная функция для поиска депозитов BEP20
async function findBscDeposits(walletAddresses) {
  console.log(`🔍 Searching BSC deposits for ${walletAddresses.length} wallets`);
  
  const currentBlock = await getLatestBscBlock();
  if (!currentBlock) {
    console.error('❌ Cannot get current block');
    return [];
  }

  const fromBlock = Math.max(lastCheckedBlock, currentBlock - BSC_BLOCK_LOOKBACK);
  console.log(`📦 Checking blocks: ${fromBlock} to ${currentBlock} (${currentBlock - fromBlock} blocks)`);

  if (fromBlock >= currentBlock) {
    console.log('⏭️ No new blocks to check');
    lastCheckedBlock = currentBlock;
    return [];
  }

  const allDeposits = [];
  
  // Используем батчинг по 20 адресов за раз (меньше = стабильнее)
  const batchSize = 20;
  
  for (let i = 0; i < walletAddresses.length; i += batchSize) {
    const batch = walletAddresses.slice(i, i + batchSize);
    console.log(`🔍 Checking batch ${Math.floor(i/batchSize) + 1} (${batch.length} addresses)`);

    // Создаем массив тем для батча
    const addressTopics = batch.map(address => {
      try {
        return '0x' + address.toLowerCase().replace(/^0x/, '').padStart(64, '0');
      } catch (e) {
        console.warn(`⚠️ Invalid address: ${address}`);
        return null;
      }
    }).filter(Boolean);

    if (addressTopics.length === 0) {
      console.log('⚠️ No valid addresses in batch');
      continue;
    }

    for (let attempt = 0; attempt < BSC_RPC_URLS.length; attempt++) {
      try {
        const provider = getBscProvider();
        
        // Ищем трансферы TO любым из наших адресов в батче
        const logs = await provider.getLogs({
          address: USDT_BSC_CONTRACT,
          topics: [
            ethers.utils.id("Transfer(address,address,uint256)"),
            null,
            addressTopics
          ],
          fromBlock: fromBlock,
          toBlock: currentBlock
        });

        console.log(`📊 Batch ${Math.floor(i/batchSize) + 1}: Found ${logs.length} transfer events`);

        // Обрабатываем логи
        for (const log of logs) {
          try {
            const fromAddr = '0x' + log.topics[1].substring(26);
            const toAddr = '0x' + log.topics[2].substring(26);
            const value = ethers.BigNumber.from(log.data);
            const amount = Number(ethers.utils.formatUnits(value, 18));

            if (amount >= MIN_DEPOSIT) {
              allDeposits.push({
                transaction_id: log.transactionHash,
                to: toAddr.toLowerCase(),
                from: fromAddr.toLowerCase(),
                amount: amount,
                token: 'USDT',
                confirmed: true,
                network: 'BEP20',
                timestamp: Date.now(),
                blockNumber: log.blockNumber
              });

              console.log(`💰 FOUND BSC DEPOSIT: ${amount} USDT to ${toAddr}`);
            }
          } catch (e) {
            console.warn('⚠️ Error parsing log:', e.message);
          }
        }

        break; // Успешно обработали батч, выходим из retry loop

      } catch (batchError) {
        console.error(`❌ Batch ${Math.floor(i/batchSize) + 1} attempt ${attempt + 1} error:`, batchError.message);
        
        if (attempt === BSC_RPC_URLS.length - 1) {
          console.error(`❌ Failed to process batch ${Math.floor(i/batchSize) + 1} after all retries`);
        } else {
          await sleep(1000);
        }
      }
    }

    // Пауза между батчами
    await sleep(500);
  }

  // Обновляем последний проверенный блок
  lastCheckedBlock = currentBlock;

  console.log(`✅ Total BSC deposits found: ${allDeposits.length}`);
  return allDeposits;
}

// Упрощенные функции для балансов
async function getBSCUSDTBalance(address) {
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const provider = getBscProvider();
      const contract = new ethers.Contract(USDT_BSC_CONTRACT, USDT_ABI, provider);
      const balance = await contract.balanceOf(address);
      return Number(ethers.utils.formatUnits(balance, 18));
    } catch (error) {
      console.warn(`⚠️ BSC balance attempt ${attempt + 1} failed:`, error.message);
      await sleep(1000);
    }
  }
  return 0;
}

async function getBSCBalance(address) {
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const provider = getBscProvider();
      const balance = await provider.getBalance(address);
      return Number(ethers.utils.formatEther(balance));
    } catch (error) {
      console.warn(`⚠️ BNB balance attempt ${attempt + 1} failed:`, error.message);
      await sleep(1000);
    }
  }
  return 0;
}

// BSC transfer functions
async function sendBSC(fromPrivateKey, toAddress, amount) {
  try {
    if (!fromPrivateKey || fromPrivateKey.includes('NOT_SET')) {
      console.error('❌ BSC send error: Private key not set');
      return false;
    }

    const provider = getBscProvider();
    const wallet = new ethers.Wallet(fromPrivateKey, provider);
    const tx = await wallet.sendTransaction({
      to: toAddress,
      value: ethers.utils.parseEther(amount.toString())
    });
    
    await tx.wait();
    console.log(`✅ BSC sent: ${amount} BNB to ${toAddress}, txid: ${tx.hash}`);
    return true;
  } catch (error) {
    console.error('❌ BSC send error:', error.message);
    return false;
  }
}

async function transferBSCUSDT(fromPrivateKey, toAddress, amount) {
  try {
    if (!fromPrivateKey || fromPrivateKey.includes('NOT_SET')) {
      console.error('❌ BSC USDT transfer error: Private key not set');
      return false;
    }

    const provider = getBscProvider();
    const wallet = new ethers.Wallet(fromPrivateKey, provider);
    const contract = new ethers.Contract(USDT_BSC_CONTRACT, USDT_ABI, wallet);
    
    const amountInWei = ethers.utils.parseUnits(amount.toString(), 18);
    const tx = await contract.transfer(toAddress, amountInWei);
    
    await tx.wait();
    console.log(`✅ BSC USDT transfer: ${amount} USDT to ${toAddress}, txid: ${tx.hash}`);
    return true;
  } catch (error) {
    console.error('❌ BSC USDT transfer error:', error.message);
    return false;
  }
}

// ========== TRON FUNCTIONS (UNCHANGED - они работают) ==========
async function getUSDTBalance(address) {
  try {
    console.log(`🔍 Checking TRC20 USDT balance for: ${address}`);
    const tronWebForChecking = new TronWeb({
      fullHost: 'https://api.trongrid.io',
      headers: { 'TRON-PRO-API-KEY': TRONGRID_API_KEY }
    });

    const contract = await tronWebForChecking.contract().at(USDT_CONTRACT);
    const result = await contract.balanceOf(address).call();
    const balance = Number(result) / 1_000_000;
    console.log(`✅ TRC20 USDT balance for ${address}: ${balance} USDT`);
    return balance;
  } catch (error) {
    console.error('❌ TRC20 balance error:', error.message);
    return 0;
  }
}

async function getUSDTTransactions(address) {
  try {
    console.log(`🔍 Checking TRC20 transactions for: ${address}`);
    const response = await fetch(`https://api.trongrid.io/v1/accounts/${address}/transactions/trc20?limit=20&only_confirmed=true`, {
      headers: { 'TRON-PRO-API-KEY': TRONGRID_API_KEY }
    });
    
    const json = await response.json();
    const transactions = [];

    for (const tx of json.data || []) {
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
    }

    transactions.sort((a, b) => b.timestamp - a.timestamp);
    console.log(`✅ Found ${transactions.length} TRC20 transactions for ${address}`);
    return transactions;
  } catch (error) {
    console.error('❌ TRC20 transactions error:', error.message);
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

function normalizePrivateKeyForTron(pk) {
  if (!pk || pk.includes('NOT_SET')) return null;
  return pk.startsWith('0x') ? pk.slice(2) : pk;
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

// ========== AUTO-COLLECT FUNCTION ==========
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
      description: `Deposit USDT (${network}) - ${txid.substring(0, 10)}...`,
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

// ========== OPTIMIZED DEPOSIT CHECK ==========
async function checkAllDeposits() {
  try {
    console.log('🔄 ===== OPTIMIZED DEPOSIT CHECK STARTED =====');
    
    // Get all wallets
    const { data: wallets, error } = await supabase
      .from('user_wallets')
      .select('*')
      .limit(1000);

    if (error) throw error;

    console.log(`🔍 Checking ${wallets?.length || 0} wallets`);

    // Separate wallets by network
    const trc20Wallets = (wallets || []).filter(w => w.network === 'TRC20');
    const bep20Wallets = (wallets || []).filter(w => w.network === 'BEP20');

    let processedCount = 0;
    let depositsFound = 0;
    let duplicatesSkipped = 0;

    // Process TRC20 (sequentially, as before)
    for (const wallet of trc20Wallets) {
      try {
        const transactions = await getUSDTTransactions(wallet.address);
        
        for (const tx of transactions) {
          if (tx.to === wallet.address && tx.amount >= MIN_DEPOSIT) {
            console.log(`🎯 FOUND TRC20 DEPOSIT: ${tx.amount} USDT to ${wallet.address}`);
            
            const result = await processDeposit(wallet, tx.amount, tx.transaction_id, 'TRC20');
            if (result.success) {
              depositsFound++;
            } else if (result.reason === 'already_processed' || result.reason === 'concurrent_processing') {
              duplicatesSkipped++;
            }
          }
        }
        
        processedCount++;
        await sleep(500); // Pause between TRC20 checks
        
      } catch (err) {
        console.error(`❌ TRC20 wallet error ${wallet.address}:`, err.message);
      }
    }

    // Process BEP20 (NEW OPTIMIZED METHOD)
    if (bep20Wallets.length > 0) {
      console.log(`🔍 Optimized BEP20 check for ${bep20Wallets.length} wallets`);
      
      // Get all BEP20 wallet addresses
      const bep20Addresses = bep20Wallets.map(w => w.address.toLowerCase());
      
      // Find deposits with one optimized query
      const deposits = await findBscDeposits(bep20Addresses);
      
      console.log(`📊 Found ${deposits.length} potential BEP20 deposits`);
      
      // Process found deposits
      for (const deposit of deposits) {
        try {
          // Find wallet by recipient address
          const wallet = bep20Wallets.find(w => 
            w.address.toLowerCase() === deposit.to.toLowerCase()
          );
          
          if (wallet) {
            console.log(`🎯 PROCESSING BEP20 DEPOSIT: ${deposit.amount} USDT to ${wallet.address}`);
            
            const result = await processDeposit(wallet, deposit.amount, deposit.transaction_id, 'BEP20');
            if (result.success) {
              depositsFound++;
            } else if (result.reason === 'already_processed' || result.reason === 'concurrent_processing') {
              duplicatesSkipped++;
            }
          }
        } catch (err) {
          console.error(`❌ BEP20 deposit processing error:`, err.message);
        }
      }
      
      processedCount += bep20Wallets.length;
    }

    const message = `✅ Deposit check completed: Processed ${processedCount} wallets, found ${depositsFound} new deposits, skipped ${duplicatesSkipped} duplicates`;
    console.log(`🔄 ===== DEPOSIT CHECK COMPLETED =====`);
    console.log(message);
    
    return { success: true, message };
    
  } catch (error) {
    console.error('❌ Deposit check error:', error.message);
    return { success: false, error: error.message };
  }
}

// ========== API ENDPOINTS ==========
app.post('/generate-wallet', async (req, res) => {
  try {
    const { user_id, network = 'TRC20' } = req.body;
    if (!user_id) return res.status(400).json({ success: false, error: 'User ID required' });

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

    if (error) throw error;

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

// Manual check endpoints
app.post('/check-deposits', async (req, res) => {
  try {
    const result = await checkAllDeposits();
    res.json(result);
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get('/check-deposits', async (req, res) => {
  try {
    const result = await checkAllDeposits();
    res.json(result);
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Collect funds endpoints
app.post('/collect-funds', async (req, res) => {
  try {
    const result = await handleCollectFunds();
    res.json(result);
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

app.get('/collect-funds', async (req, res) => {
  try {
    const result = await handleCollectFunds();
    res.json(result);
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

async function handleCollectFunds() {
  try {
    console.log('💰 ===== MANUAL FUNDS COLLECTION STARTED =====');
    const { data: wallets, error } = await supabase.from('user_wallets').select('*').limit(1000);
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
    
    return { success: true, message };
  } catch (error) {
    console.error('❌ Funds collection error:', error.message);
    return { success: false, error: error.message };
  }
}

// ========== HELPER FUNCTIONS ==========
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

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
      // For BEP20, use the new optimized method
      const deposits = await findBscDeposits([wallet.address.toLowerCase()]);
      transactions = deposits.map(deposit => ({
        transaction_id: deposit.transaction_id,
        to: deposit.to,
        from: deposit.from,
        amount: deposit.amount,
        token: deposit.token,
        confirmed: deposit.confirmed,
        network: deposit.network,
        timestamp: deposit.timestamp
      }));
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

// ========== HEALTH ENDPOINTS ==========
app.get('/health', (req, res) => {
  const healthCheck = {
    status: '✅ HEALTHY',
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    memory: process.memoryUsage(),
    env: {
      NODE_ENV: process.env.NODE_ENV,
      PORT: process.env.PORT
    }
  };
  
  console.log('🏥 HEALTH CHECK:', JSON.stringify(healthCheck, null, 2));
  
  res.json(healthCheck);
});

app.get('/', (req, res) => {
  const status = {
    status: '✅ WORKING',
    message: 'Cocoon AI - Optimized BEP20 Deposit System',
    timestamp: new Date().toISOString(),
    networks: ['TRC20', 'BEP20'],
    features: [
      'Fast BEP20 deposits (1-2 minutes)',
      'TRC20 support (unchanged)',
      'Auto-collection',
      'Batch processing for 250+ wallets'
    ],
    stats: {
      minDeposit: `${MIN_DEPOSIT} USDT`,
      bscBlockLookback: `${BSC_BLOCK_LOOKBACK} blocks`,
      rpcProviders: `${BSC_RPC_URLS.length} endpoints`
    }
  };
  
  console.log('🏠 Root endpoint called - returning status');
  res.json(status);
});

// ========== SCHEDULED CHECKS ==========
console.log('⏰ Starting optimized deposit checks...');
const CHECK_INTERVAL = 2 * 60 * 1000; // 2 minutes

setInterval(async () => {
  try {
    console.log('🕒 ===== SCHEDULED CHECK STARTED =====');
    await checkAllDeposits();
    console.log('🕒 ===== SCHEDULED CHECK COMPLETED =====');
  } catch (err) {
    console.error('❌ Scheduled check error:', err.message);
  }
}, CHECK_INTERVAL);

// ========== START SERVER ==========
app.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 SERVER STARTED on port ${PORT}`);
  console.log('✅ SUPABASE: CONNECTED WITH YOUR KEYS');
  console.log('✅ BEP20 OPTIMIZATIONS: ENABLED');
  console.log('✅ Fast deposit detection for 250+ wallets (1-2 minutes)');
  console.log('✅ TRC20: UNCHANGED (working perfectly)');
  console.log('===================================');
});
