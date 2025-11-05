const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const TronWeb = require('tronweb');
const { ethers } = require('ethers');

// ========== ENHANCED LOGGING ==========
console.log('🚀 STARTING SERVER - QUICKNODE 5-BLOCK LIMIT VERSION');

const app = express();
const PORT = process.env.PORT || 3000;

// ========== ENVIRONMENT VARIABLES ==========
const SUPABASE_URL = process.env.SUPABASE_URL || 'https://eqzfivdckzrkkncahlyn.supabase.co';
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVxemZpdmRja3pya2tuY2FobHluIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MTYwNTg2NSwiZXhwIjoyMDc3MTgxODY1fQ.AuGqzDDMzWS1COhHdBMchHarYmd1gNC_9PfRfJWPTxc';
const TRONGRID_API_KEY = process.env.TRONGRID_API_KEY || '33759ca3-ffb8-41bc-9036-25a32601eae2';
const QUICKNODE_BSC_URL = process.env.QUICKNODE_BSC_URL || 'https://thrilling-falling-morning.bsc.quiknode.pro/e634acd5c5a0b08f71e357def772863df6a69cf6/';

console.log('🔄 Initializing Supabase client...');
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
console.log('✅ Supabase client initialized');

console.log('🔄 Initializing TronWeb...');
const tronWeb = new TronWeb({
  fullHost: 'https://api.trongrid.io',
  headers: { 'TRON-PRO-API-KEY': TRONGRID_API_KEY }
});
console.log('✅ TronWeb initialized');

// COMPANY wallets
const COMPANY = {
  MASTER: { address: 'TKn5J3ZnTxE9fmgMhVjXognH4VUjx4Tid2', privateKey: process.env.MASTER_PRIVATE_KEY || 'MASTER_PRIVATE_KEY_NOT_SET' },
  MAIN: { address: 'TNVpDk1JZSxmC9XniB1tSPaRdAvvKMMavC', privateKey: process.env.MAIN_PRIVATE_KEY || 'MAIN_PRIVATE_KEY_NOT_SET' }
};

const COMPANY_BSC = {
  MASTER: { address: '0x60F3159e6b935759d6b4994473eeeD1e3ad27408', privateKey: process.env.MASTER_BSC_PRIVATE_KEY || 'MASTER_BSC_PRIVATE_KEY_NOT_SET' },
  MAIN: { address: '0x01F28A131bdda7255EcBE800C3ebACBa2c7076c7', privateKey: process.env.MAIN_BSC_PRIVATE_KEY || 'MAIN_BSC_PRIVATE_KEY_NOT_SET' }
};

// ========== MIDDLEWARE ==========
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

const CHECK_INTERVAL_MS = 2 * 60 * 1000; // 2 minutes
const BSC_CHECK_INTERVAL_MS = 10000; // 10 seconds для более частой проверки

// ========== GLOBAL VARIABLES ==========
let bscLastCheckedBlocks = new Map(); // Храним последний проверенный блок для каждого кошелька

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

// ========== BSC FUNCTIONS WITH 5-BLOCK LIMIT ==========
async function getBSCUSDTBalance(address) {
  console.log(`🔍 Checking BSC USDT balance for: ${address}`);
  try {
    const provider = new ethers.providers.JsonRpcProvider(QUICKNODE_BSC_URL);
    const contract = new ethers.Contract(USDT_BSC_CONTRACT, USDT_ABI, provider);
    const balance = await contract.balanceOf(address);
    const formatted = Number(ethers.utils.formatUnits(balance, 18));
    console.log(`✅ BSC USDT balance for ${address}: ${formatted} USDT`);
    return formatted;
  } catch (error) {
    console.error('❌ BSC USDT balance error:', error.message);
    return 0;
  }
}

// ОПТИМИЗИРОВАННАЯ ФУНКЦИЯ: Работаем в рамках 5-блочного лимита QuickNode
async function getBSCTransactions(address) {
  try {
    if (!address) return [];

    console.log(`🔍 [QUICKNODE] Checking BSC transactions for: ${address}`);
    
    const quicknodeProvider = new ethers.providers.JsonRpcProvider(QUICKNODE_BSC_URL);
    const contract = new ethers.Contract(USDT_BSC_CONTRACT, [
      "event Transfer(address indexed from, address indexed to, uint256 value)"
    ], quicknodeProvider);

    const currentBlock = await quicknodeProvider.getBlockNumber();
    
    // Получаем последний проверенный блок для этого кошелька
    const lastCheckedBlock = bscLastCheckedBlocks.get(address) || Math.max(0, currentBlock - 5);
    
    // Проверяем только 5 блоков (ограничение QuickNode)
    const fromBlock = Math.max(lastCheckedBlock + 1, currentBlock - 4);
    const toBlock = currentBlock;
    
    // Если диапазон больше 5 блоков, корректируем
    const blockRange = toBlock - fromBlock + 1;
    if (blockRange > 5) {
      console.log(`⚠️ Block range too large (${blockRange}), adjusting to 5 blocks`);
      const adjustedFromBlock = Math.max(toBlock - 4, 0);
      return await getBSCTransactions(address, adjustedFromBlock, toBlock);
    }

    console.log(`📦 Checking blocks ${fromBlock} to ${toBlock} (${blockRange} blocks) for ${address}`);

    try {
      const filter = contract.filters.Transfer(null, address);
      const logs = await contract.queryFilter(filter, fromBlock, toBlock);

      console.log(`✅ [QUICKNODE] Found ${logs.length} transfer events for ${address}`);
      
      // Обновляем последний проверенный блок
      bscLastCheckedBlocks.set(address, toBlock);
      
      const transactions = [];
      for (const log of logs) {
        try {
          const amount = Number(ethers.utils.formatUnits(log.args.value, 18));
          
          let timestamp = Date.now();
          try {
            const block = await quicknodeProvider.getBlock(log.blockNumber);
            timestamp = block.timestamp * 1000;
          } catch (e) {
            console.warn(`Could not get block ${log.blockNumber}:`, e.message);
          }
          
          transactions.push({
            transaction_id: log.transactionHash,
            to: log.args.to,
            from: log.args.from,
            amount: amount,
            token: 'USDT',
            confirmed: true,
            network: 'BEP20',
            timestamp: timestamp
          });

          console.log(`📥 [QUICKNODE] Found BSC deposit: ${amount} USDT from ${log.args.from}`);
        } catch (e) { 
          console.warn('[QUICKNODE] Skipping malformed BSC transaction:', e.message);
          continue; 
        }
      }
      
      transactions.sort((a, b) => b.timestamp - a.timestamp);
      return transactions;
    } catch (queryError) {
      console.error('❌ [QUICKNODE] Query filter error:', queryError.message);
      
      // При ошибке пробуем проверить только последний блок
      console.log('🔄 Retrying with single block...');
      try {
        const singleBlockLogs = await contract.queryFilter(filter, currentBlock, currentBlock);
        bscLastCheckedBlocks.set(address, currentBlock);
        
        const singleBlockTransactions = [];
        for (const log of singleBlockLogs) {
          const amount = Number(ethers.utils.formatUnits(log.args.value, 18));
          singleBlockTransactions.push({
            transaction_id: log.transactionHash,
            to: log.args.to,
            from: log.args.from,
            amount: amount,
            token: 'USDT',
            confirmed: true,
            network: 'BEP20',
            timestamp: Date.now()
          });
        }
        
        console.log(`✅ [QUICKNODE SINGLE BLOCK] Found ${singleBlockTransactions.length} transactions`);
        return singleBlockTransactions;
      } catch (singleBlockError) {
        console.error('❌ [QUICKNODE] Single block also failed:', singleBlockError.message);
        return [];
      }
    }
  } catch (error) {
    console.error('❌ [QUICKNODE] BSC transactions error:', error.message);
    return [];
  }
}

// ФУНКЦИЯ ДЛЯ ПРОВЕРКИ КОНКРЕТНОГО ДИАПАЗОНА БЛОКОВ (для поиска пропущенных транзакций)
async function getBSCTransactionsForBlockRange(address, fromBlock, toBlock) {
  try {
    const quicknodeProvider = new ethers.providers.JsonRpcProvider(QUICKNODE_BSC_URL);
    const contract = new ethers.Contract(USDT_BSC_CONTRACT, [
      "event Transfer(address indexed from, address indexed to, uint256 value)"
    ], quicknodeProvider);

    // Разбиваем большой диапазон на чанки по 5 блоков
    const transactions = [];
    let currentFrom = fromBlock;
    
    while (currentFrom <= toBlock) {
      const currentTo = Math.min(currentFrom + 4, toBlock); // 5 блоков включая начальный
      const chunkSize = currentTo - currentFrom + 1;
      
      console.log(`🔍 Checking blocks ${currentFrom} to ${currentTo} (${chunkSize} blocks) for ${address}`);
      
      try {
        const filter = contract.filters.Transfer(null, address);
        const logs = await contract.queryFilter(filter, currentFrom, currentTo);
        
        for (const log of logs) {
          const amount = Number(ethers.utils.formatUnits(log.args.value, 18));
          transactions.push({
            transaction_id: log.transactionHash,
            to: log.args.to,
            from: log.args.from,
            amount: amount,
            token: 'USDT',
            confirmed: true,
            network: 'BEP20',
            timestamp: Date.now()
          });
          console.log(`📥 Found historical BSC deposit: ${amount} USDT from ${log.args.from}`);
        }
        
        await sleep(500); // Задержка между запросами
      } catch (chunkError) {
        console.error(`❌ Error checking blocks ${currentFrom}-${currentTo}:`, chunkError.message);
      }
      
      currentFrom = currentTo + 1;
    }
    
    console.log(`✅ Found ${transactions.length} historical transactions for ${address}`);
    return transactions;
  } catch (error) {
    console.error('❌ Historical BSC transactions error:', error.message);
    return [];
  }
}

async function getBSCBalance(address) {
  console.log(`🔍 Checking BSC native balance for: ${address}`);
  try {
    const provider = new ethers.providers.JsonRpcProvider(QUICKNODE_BSC_URL);
    const balance = await provider.getBalance(address);
    const formatted = Number(ethers.utils.formatEther(balance));
    console.log(`✅ BSC native balance for ${address}: ${formatted} BNB`);
    return formatted;
  } catch (error) {
    console.error('❌ BSC balance error:', error.message);
    return 0;
  }
}

async function sendBSC(fromPrivateKey, toAddress, amount) {
  console.log(`🔄 Sending ${amount} BNB to ${toAddress}`);
  try {
    if (!fromPrivateKey || fromPrivateKey.includes('NOT_SET')) {
      console.error('❌ BSC send error: Private key not set');
      return false;
    }

    const provider = new ethers.providers.JsonRpcProvider(QUICKNODE_BSC_URL);
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
  console.log(`🔄 Transferring ${amount} BSC USDT to ${toAddress}`);
  try {
    if (!fromPrivateKey || fromPrivateKey.includes('NOT_SET')) {
      console.error('❌ BSC USDT transfer error: Private key not set');
      return false;
    }

    const provider = new ethers.providers.JsonRpcProvider(QUICKNODE_BSC_URL);
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

// ========== TRON FUNCTIONS (БЕЗ ИЗМЕНЕНИЙ) ==========
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

// ========== ОБРАБОТКА ДЕПОЗИТОВ ==========
async function processDeposit(wallet, amount, txid, network) {
  try {
    console.log(`💰 PROCESSING DEPOSIT: ${amount} USDT for user ${wallet.user_id}, txid: ${txid}, network: ${network}, wallet: ${wallet.address}`);

    // Проверяем существующий депозит
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

    // Вставляем депозит
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

    // Получаем данные пользователя
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

    console.log(`📊 User ${wallet.user_id} balance update: ${currentBalance} → ${newBalance} USDT`);

    // Обновляем баланс
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

    // Обновляем статус депозита
    await supabase
      .from('deposits')
      .update({ status: 'completed' })
      .eq('id', newDeposit.id);

    // Добавляем запись в транзакции
    await supabase.from('transactions').insert({
      user_id: wallet.user_id,
      type: 'deposit',
      amount,
      description: `Депозит USDT (${network}) - ${txid.substring(0, 10)}...`,
      status: 'completed',
      created_at: new Date().toISOString()
    });

    // Проверяем VIP уровень
    if (newBalance >= 20 && user.vip_level === 0) {
      await supabase
        .from('users')
        .update({ vip_level: 1 })
        .eq('id', wallet.user_id);
      console.log(`⭐ VIP Level upgraded to 1 for user ${wallet.user_id}`);
    }

    console.log(`✅ DEPOSIT PROCESSED: ${amount} USDT for user ${wallet.user_id}`);
    console.log(`💰 New balance: ${newBalance} USDT`);

    // Запланировать авто-коллекцию после депозита
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
    
    // Инициализируем отслеживание блоков для нового кошелька
    setTimeout(async () => {
      try {
        const provider = new ethers.providers.JsonRpcProvider(QUICKNODE_BSC_URL);
        const currentBlock = await provider.getBlockNumber();
        bscLastCheckedBlocks.set(address, currentBlock - 1);
        console.log(`🔧 Initialized block tracking for new wallet ${address} at block ${currentBlock - 1}`);
      } catch (err) {
        console.error('Error initializing block tracking:', err.message);
      }
    }, 2000);

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

app.post('/check-deposits', async (req, res) => { await handleCheckDeposits(req, res); });
app.get('/check-deposits', async (req, res) => { await handleCheckDeposits(req, res); });

async function handleCheckDeposits(req = {}, res = {}) {
  try {
    console.log('🔄 ===== MANUAL DEPOSIT CHECK STARTED =====');
    const { data: wallets, error } = await supabase.from('user_wallets').select('*').limit(200);
    if (error) throw error;

    console.log(`🔍 Checking ${wallets?.length || 0} wallets across all networks`);
    
    let processedCount = 0;
    let depositsFound = 0;
    let duplicatesSkipped = 0;

    for (const wallet of wallets || []) {
      try {
        console.log(`🔍 Processing wallet ${wallet.address} (${wallet.network}) for user ${wallet.user_id}`);
        
        await sleep(1000); // Задержка между проверками
        
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
              } else if (result.reason === 'already_processed' || result.reason === 'concurrent_processing') {
                duplicatesSkipped++;
                console.log(`ℹ️ Deposit already processed: ${tx.transaction_id}`);
              }
            } catch (err) {
              console.error(`❌ Error processing deposit ${tx.transaction_id}:`, err.message);
            }
          }
        }

        await supabase.from('user_wallets').update({ last_checked: new Date().toISOString() }).eq('id', wallet.id);
        processedCount++;
        console.log(`✅ Completed processing wallet ${wallet.address}`);
      } catch (err) {
        console.error(`❌ Error processing wallet ${wallet.address}:`, err.message);
      }
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
      console.log(`✅ User created: ${userId}`);
    } else {
      console.log(`✅ User already exists: ${userId}`);
    }
  } catch (error) {
    console.error('❌ ensureUserExists error:', error.message);
  }
}

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
      'QuickNode 5-Block Limit Compliance'
    ],
    stats: {
      checkInterval: `${CHECK_INTERVAL_MS / 1000} seconds`,
      bscCheckInterval: `${BSC_CHECK_INTERVAL_MS / 1000} seconds`,
      minDeposit: `${MIN_DEPOSIT} USDT`,
      keepAmount: `${KEEP_AMOUNT} USDT`
    }
  };
  
  console.log('🏠 Root endpoint called - returning status');
  res.json(status);
});

// ========== HEARTBEAT LOGGING ==========
console.log('💓 Starting heartbeat logger...');
setInterval(() => {
  console.log('💓 SERVER HEARTBEAT - ' + new Date().toISOString());
}, 30000);

// ========== SCHEDULED DEPOSIT CHECKS ==========
console.log('⏰ Starting scheduled deposit checks...');

// TRC20 проверка каждые 2 минуты
setInterval(async () => {
  try {
    console.log('🕒 ===== SCHEDULED TRC20 DEPOSIT CHECK STARTED =====');
    const { data: wallets } = await supabase
      .from('user_wallets')
      .select('*')
      .eq('network', 'TRC20')
      .limit(200);
    
    for (const wallet of wallets || []) {
      try {
        await sleep(1500);
        const transactions = await getUSDTTransactions(wallet.address);
        
        for (const tx of transactions) {
          if (tx.to === wallet.address && tx.token === 'USDT' && tx.amount >= MIN_DEPOSIT) {
            await processDeposit(wallet, tx.amount, tx.transaction_id, 'TRC20');
          }
        }
      } catch (err) {
        console.error(`❌ Error processing TRC20 wallet ${wallet.address}:`, err.message);
      }
    }
    console.log('🕒 ===== SCHEDULED TRC20 DEPOSIT CHECK COMPLETED =====');
  } catch (err) {
    console.error('❌ Scheduled TRC20 deposit check error:', err.message);
  }
}, CHECK_INTERVAL_MS);

// BSC проверка каждые 10 секунд с соблюдением 5-блочного лимита
setInterval(async () => {
  try {
    console.log('⏱️ ===== SCHEDULED BSC DEPOSIT CHECK STARTED =====');
    const { data: wallets } = await supabase
      .from('user_wallets')
      .select('*')
      .eq('network', 'BEP20')
      .limit(200);
    
    for (const wallet of wallets || []) {
      try {
        await sleep(500); // Уменьшенная задержка для более частой проверки
        const transactions = await getBSCTransactions(wallet.address);
        
        for (const tx of transactions) {
          if (tx.to.toLowerCase() === wallet.address.toLowerCase() && tx.token === 'USDT' && tx.amount >= MIN_DEPOSIT) {
            await processDeposit(wallet, tx.amount, tx.transaction_id, 'BEP20');
          }
        }
      } catch (err) {
        console.error(`❌ Error processing BSC wallet ${wallet.address}:`, err.message);
      }
    }
    console.log('⏱️ ===== SCHEDULED BSC DEPOSIT CHECK COMPLETED =====');
  } catch (err) {
    console.error('❌ Scheduled BSC deposit check error:', err.message);
  }
}, BSC_CHECK_INTERVAL_MS);

// ========== START SERVER ==========
console.log('🎯 ALL INITIALIZATION COMPLETE - STARTING SERVER...');

app.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 SERVER SUCCESSFULLY STARTED on port ${PORT}`);
  console.log(`✅ SUPABASE: CONNECTED`);
  console.log(`✅ TRONGRID: API KEY SET`);
  console.log(`✅ QUICKNODE: CONFIGURED WITH 5-BLOCK LIMIT`);
  console.log(`💰 TRC20 MASTER: ${COMPANY.MASTER.address}`);
  console.log(`💰 TRC20 MAIN: ${COMPANY.MAIN.address}`);
  console.log(`💰 BEP20 MASTER: ${COMPANY_BSC.MASTER.address}`);
  console.log(`💰 BEP20 MAIN: ${COMPANY_BSC.MAIN.address}`);
  console.log(`⏰ TRC20 AUTO-CHECK: EVERY ${Math.round(CHECK_INTERVAL_MS / 1000)}s`);
  console.log(`⏱️ BSC AUTO-CHECK: EVERY ${Math.round(BSC_CHECK_INTERVAL_MS / 1000)}s`);
  console.log('===================================');
  console.log('🎉 APPLICATION READY - 5-BLOCK QUICKNODE COMPLIANCE');
  console.log('🔧 BSC Transactions: Strict 5-block range checking');
});
