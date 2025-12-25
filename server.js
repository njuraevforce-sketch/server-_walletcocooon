// server-autodeposit.js — ADAPTED FOR FTP QUANT WITH TRC20 & BEP20
const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const TronWeb = require('tronweb');
const { ethers } = require('ethers');

const app = express();
const PORT = process.env.PORT || 8081;

// ========== CONFIGURATION ==========
const SUPABASE_URL = 'https://fctwivbwjoslkejtjxhe.supabase.co';
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZjdHdpdmJ3am9zbGtlanRqeGhlIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2NjE0MzAzNSwiZXhwIjoyMDgxNzE5MDM1fQ.DiAzcqkigZPueh40idz2fIoJ-o-sKDMaRleOagH__B0';
const TRONGRID_API_KEY = process.env.TRONGRID_API_KEY || '8fa63ef4-f010-4ad2-a556-a7124563bafd';
const MORALIS_API_KEY = process.env.MORALIS_API_KEY || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJub25jZSI6IjcxODVlYzdiLTQ3NzctNDFhNS05ZDI4LTE0YjFlZmJkZTA5NSIsIm9yZ2lkIjoiNDg1NjY3IiwidXNlcklkIjoiNDk5NjYxIiwidHlwZUlkIjoiNjdjYjQzMzgtMmY2OC00MmE3LThmMzItYmJiMDljMDkyM2NlIiwidHlwZSI6IlBST0pFQ1QiLCJpYXQiOjE3NjU0OTA0MDMsImV4cCI6NDkyMTI1MDQwM30.0Z_G2u-E8EdfZQzyUZFY4CVbgUqR2H5e4TjQzi9MnzU';

// ========== MIDDLEWARE ==========
app.use(express.json());
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', '*');
  res.header('Access-Control-Allow-Methods', '*');
  next();
});

// ========== HTTP ROUTES ==========
app.get('/', (req, res) => {
  res.json({
    status: '✅ FTP QUANT DEPOSIT SERVER',
    message: 'Auto Deposit Processing System',
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    environment: process.env.NODE_ENV || 'production'
  });
});

app.get('/health', (req, res) => {
  res.json({
    status: '✅ HEALTHY',
    service: 'FTP Quant Deposit Processor',
    timestamp: new Date().toISOString(),
    uptime: process.uptime()
  });
});

// ========== INITIALIZE SERVICES ==========
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
const tronWeb = new TronWeb({
  fullHost: 'https://api.trongrid.io',
  headers: { 'TRON-PRO-API-KEY': TRONGRID_API_KEY }
});

// ========== CONSTANTS ==========
const USDT_TRC20_CONTRACT = 'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t';
const USDT_BEP20_CONTRACT = '0x55d398326f99059fF775485246999027B3197955';
const USDC_BEP20_CONTRACT = '0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d';
const MIN_DEPOSIT = 10;

// ========== OPTIMIZED SETTINGS ==========
const TRC20_CHECK_INTERVAL = 45000; // 45 секунд
const BEP20_CHECK_INTERVAL = 180000; // 3 минуты
const BEP20_DELAY_MS = 500; // 2 запроса/секунду для Moralis
const TRC20_DELAY_MS = 100; // 10 запросов/секунду для TronGrid

// ========== HELPERS ==========
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
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

// ========== WALLET GENERATION ==========
async function generateTRC20Wallet() {
  try {
    const account = await tronWeb.createAccount();
    return {
      address: account.address.base58,
      privateKey: account.privateKey
    };
  } catch (error) {
    console.error('❌ TRC20 wallet generation error:', error);
    throw error;
  }
}

async function generateBEP20Wallet() {
  try {
    const wallet = ethers.Wallet.createRandom();
    return {
      address: wallet.address,
      privateKey: wallet.privateKey
    };
  } catch (error) {
    console.error('❌ BEP20 wallet generation error:', error);
    throw error;
  }
}

// ========== DEPOSIT PROCESSING ==========
async function processDeposit(wallet, amount, txid, network) {
  try {
    console.log(`💰 PROCESSING DEPOSIT: ${amount} ${network.toUpperCase()} for user ${wallet.user_id}, txid: ${txid}`);

    // Проверяем дубликаты по хэшу
    const { data: existingDeposit, error: checkError } = await supabase
      .from('deposits')
      .select('id, status, amount, network')
      .eq('tx_hash', txid)
      .eq('network', network)
      .maybeSingle();

    if (checkError) {
      console.error('Error checking existing deposit:', checkError);
      throw checkError;
    }

    if (existingDeposit) {
      console.log(`✅ Deposit already processed: ${txid}, status: ${existingDeposit.status}, amount: ${existingDeposit.amount}`);
      
      if (existingDeposit.status !== 'completed') {
        await supabase
          .from('deposits')
          .update({ 
            status: 'completed',
            completed_at: new Date().toISOString()
          })
          .eq('id', existingDeposit.id);
      }
      
      return { success: false, reason: 'already_processed', existing: existingDeposit };
    }

    // Обработка депозита через PostgreSQL RPC
    const depositResult = await processDepositTransaction(wallet.user_id, amount, txid, network);
    
    return depositResult;
  } catch (error) {
    console.error('❌ Error in processDeposit:', error.message);
    throw error;
  }
}

// Функция для обработки депозита через PostgreSQL
async function processDepositTransaction(userId, amount, txid, network) {
  try {
    // Получаем текущий баланс пользователя
    const { data: userData, error: userError } = await supabase
      .from('users')
      .select('balance, total_deposit')
      .eq('id', userId)
      .single();

    if (userError) {
      console.error('❌ Error getting user data:', userError);
      throw new Error('User not found');
    }

    // Начинаем транзакцию в Supabase
    const newBalance = (userData.balance || 0) + parseFloat(amount);
    const newTotalDeposit = (userData.total_deposit || 0) + parseFloat(amount);

    // 1. Обновляем баланс пользователя
    const { error: updateError } = await supabase
      .from('users')
      .update({
        balance: newBalance,
        total_deposit: newTotalDeposit,
        updated_at: new Date().toISOString()
      })
      .eq('id', userId);

    if (updateError) {
      console.error('❌ Error updating user balance:', updateError);
      throw updateError;
    }

    // 2. Создаем запись о депозите
    const { data: depositData, error: depositError } = await supabase
      .from('deposits')
      .insert({
        user_id: userId,
        amount: parseFloat(amount),
        network: network,
        tx_hash: txid,
        status: 'completed',
        confirmed_at: new Date().toISOString(),
        completed_at: new Date().toISOString(),
        created_at: new Date().toISOString()
      })
      .select()
      .single();

    if (depositError) {
      console.error('❌ Error creating deposit record:', depositError);
      
      // Откатываем обновление баланса
      await supabase
        .from('users')
        .update({
          balance: userData.balance,
          total_deposit: userData.total_deposit
        })
        .eq('id', userId);
      
      throw depositError;
    }

    // 3. Создаем запись в транзакциях
    const { error: txError } = await supabase
      .from('transactions')
      .insert({
        user_id: userId,
        type: 'deposit',
        amount: parseFloat(amount),
        description: `Deposit via ${network.toUpperCase()}`,
        status: 'completed',
        metadata: {
          tx_hash: txid,
          network: network
        },
        created_at: new Date().toISOString()
      });

    if (txError) {
      console.error('❌ Error creating transaction:', txError);
      // Не откатываем, так как депозит уже зачислен
    }

    console.log(`✅ DEPOSIT PROCESSED: ${amount} ${network.toUpperCase()} for user ${userId}`);
    console.log(`💰 New balance: ${newBalance}`);
    console.log(`📝 Deposit ID: ${depositData.id}`);

    return { 
      success: true, 
      amount, 
      deposit_id: depositData.id,
      new_balance: newBalance
    };
    
  } catch (error) {
    console.error('❌ Error in deposit processing:', error.message);
    throw error;
  }
}

// ========== API Endpoints ==========
app.post('/api/deposit/generate', async (req, res) => {
  try {
    const { user_id, network = 'usdt_trc20' } = req.query;
    if (!user_id) return res.status(400).json({ success: false, error: 'User ID is required' });

    console.log(`🔐 Generating ${network} wallet for user: ${user_id}`);

    // Определяем тип сети для таблицы
    let addressType = '';
    let networkType = '';
    
    if (network === 'usdt_trc20') {
      addressType = 'usdt_trc20_address';
      networkType = 'trc20';
    } else if (network === 'usdt_bep20') {
      addressType = 'usdt_bep20_address';
      networkType = 'bep20';
    } else if (network === 'usdc_bep20') {
      addressType = 'usdc_bep20_address';
      networkType = 'bep20';
    } else {
      return res.status(400).json({ success: false, error: 'Unsupported network' });
    }

    // Проверяем существующий кошелек в user_wallets
    const { data: existingWallet, error: walletError } = await supabase
      .from('user_wallets')
      .select('*')
      .eq('user_id', user_id)
      .single();

    let address, privateKey;

    if (!existingWallet || !existingWallet[addressType]) {
      // Генерируем новый кошелек
      if (networkType === 'trc20') {
        const wallet = await generateTRC20Wallet();
        address = wallet.address;
        privateKey = wallet.privateKey;
      } else if (networkType === 'bep20') {
        const wallet = await generateBEP20Wallet();
        address = wallet.address;
        privateKey = wallet.privateKey;
        
        // Для BEP20 сетей (USDT и USDC) используем один адрес
        const bep20Address = {
          usdt_bep20_address: address,
          usdc_bep20_address: address
        };
        
        // Сохраняем в user_wallets
        if (existingWallet) {
          // Обновляем существующий кошелек
          await supabase
            .from('user_wallets')
            .update(bep20Address)
            .eq('user_id', user_id);
        } else {
          // Создаем новый кошелек
          await supabase
            .from('user_wallets')
            .insert({
              user_id: user_id,
              ...bep20Address,
              default_network: network,
              created_at: new Date().toISOString(),
              updated_at: new Date().toISOString()
            });
        }
      }
      
      // Для TRC20 отдельно сохраняем
      if (networkType === 'trc20') {
        const trc20Data = { [addressType]: address };
        
        if (existingWallet) {
          await supabase
            .from('user_wallets')
            .update(trc20Data)
            .eq('user_id', user_id);
        } else {
          await supabase
            .from('user_wallets')
            .insert({
              user_id: user_id,
              ...trc20Data,
              default_network: network,
              created_at: new Date().toISOString(),
              updated_at: new Date().toISOString()
            });
        }
      }
    } else {
      // Используем существующий адрес
      address = existingWallet[addressType];
      privateKey = 'stored_in_database';
    }

    console.log(`✅ ${network} wallet ready: ${address}`);
    
    // Начинаем проверку депозитов для этого пользователя
    setTimeout(() => {
      if (networkType === 'trc20') {
        checkUserTRC20Deposits(user_id);
      } else {
        checkUserBEP20Deposits(user_id);
      }
    }, 5000);

    res.json({ 
      success: true, 
      address: address,
      network: network,
      exists: !!existingWallet?.[addressType]
    });
  } catch (error) {
    console.error('❌ Generate wallet error:', error.message);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

app.get('/api/deposit/check/:userId', async (req, res) => {
  try {
    const { userId } = req.params;
    
    const trc20Result = await checkUserTRC20Deposits(userId);
    const bep20Result = await checkUserBEP20Deposits(userId);
    
    res.json({
      success: true,
      trc20: trc20Result,
      bep20: bep20Result,
      message: 'Deposit check completed'
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// ========== TRC20 TRANSACTIONS ==========
async function getTRC20Transactions(address) {
  try {
    if (!address) return [];
    
    const response = await fetch(`https://api.trongrid.io/v1/accounts/${address}/transactions/trc20?limit=10&only_confirmed=true`, {
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
        if (!tokenAddr || tokenAddr !== USDT_TRC20_CONTRACT) continue;

        const to = toBase58IfHex(tx.to);
        const from = toBase58IfHex(tx.from);
        const rawValue = tx.value || 0;
        const amount = Number(rawValue) / 1_000_000;

        if (amount >= MIN_DEPOSIT) {
          transactions.push({
            transaction_id: tx.transaction_id,
            to,
            from,
            amount,
            token: 'USDT',
            network: 'usdt_trc20',
            confirmed: true,
            timestamp: tx.block_timestamp
          });
        }
      } catch (innerErr) {
        continue;
      }
    }

    transactions.sort((a, b) => b.timestamp - a.timestamp);
    return transactions;
  } catch (error) {
    console.error('❌ TRC20 transactions error:', error.message);
    return [];
  }
}

// ========== BEP20 TRANSACTIONS ==========
async function getBEP20Transactions(address) {
  try {
    console.log(`🔍 Checking BEP20 via Moralis: ${address}`);
    
    const response = await fetch(
      `https://deep-index.moralis.io/api/v2/${address}/erc20/transfers?chain=bsc&token_addresses=${USDT_BEP20_CONTRACT},${USDC_BEP20_CONTRACT}&limit=10`,
      {
        headers: {
          'X-API-Key': MORALIS_API_KEY,
          'Accept': 'application/json'
        }
      }
    );

    if (!response.ok) {
      throw new Error(`Moralis API error: ${response.status}`);
    }

    const data = await response.json();
    const transactions = [];

    for (const tx of data.result || []) {
      try {
        if (tx.to_address.toLowerCase() === address.toLowerCase()) {
          const amount = Number(tx.value) / Math.pow(10, tx.decimals || 18);
          
          if (amount >= MIN_DEPOSIT) {
            let token = 'USDT';
            let network = 'usdt_bep20';
            
            if (tx.token_address.toLowerCase() === USDC_BEP20_CONTRACT.toLowerCase()) {
              token = 'USDC';
              network = 'usdc_bep20';
            }
            
            transactions.push({
              transaction_id: tx.transaction_hash,
              to: tx.to_address.toLowerCase(),
              from: tx.from_address.toLowerCase(),
              amount: amount,
              token: token,
              network: network,
              confirmed: true,
              timestamp: new Date(tx.block_timestamp).getTime(),
              blockNumber: parseInt(tx.block_number)
            });
          }
        }
      } catch (e) {
        continue;
      }
    }

    console.log(`✅ Found ${transactions.length} BEP20 transactions for ${address}`);
    return transactions;

  } catch (error) {
    console.error('❌ Moralis API error:', error.message);
    return [];
  }
}

// ========== CHECK FUNCTIONS ==========
async function checkUserTRC20Deposits(userId) {
  try {
    // Получаем TRC20 адрес пользователя
    const { data: wallet, error } = await supabase
      .from('user_wallets')
      .select('usdt_trc20_address')
      .eq('user_id', userId)
      .single();
    
    if (error || !wallet?.usdt_trc20_address) {
      console.log(`No TRC20 wallet for user ${userId}`);
      return { success: false, deposits: 0 };
    }

    const transactions = await getTRC20Transactions(wallet.usdt_trc20_address);
    let depositsFound = 0;

    for (const tx of transactions) {
      if (tx.to === wallet.usdt_trc20_address && tx.amount >= MIN_DEPOSIT) {
        try {
          // Проверяем, не обрабатывалась ли уже эта транзакция
          const { data: existing } = await supabase
            .from('deposits')
            .select('id')
            .eq('tx_hash', tx.transaction_id)
            .eq('network', tx.network)
            .maybeSingle();
          
          if (existing) {
            console.log(`⏭️ Skipping duplicate TRC20 transaction: ${tx.transaction_id}`);
            continue;
          }
          
          const result = await processDeposit({
            user_id: userId,
            address: wallet.usdt_trc20_address
          }, tx.amount, tx.transaction_id, tx.network);
          
          if (result.success) {
            depositsFound++;
            console.log(`💰 NEW TRC20 DEPOSIT: ${tx.amount} ${tx.token} for user ${userId}`);
          }
        } catch (err) {
          if (err.message.includes('already_processed')) {
            console.log(`⏭️ Duplicate TRC20 deposit skipped: ${tx.transaction_id}`);
          } else {
            console.error(`❌ Error processing TRC20 deposit ${tx.transaction_id}:`, err.message);
          }
        }
      }
    }

    console.log(`✅ TRC20 check for user ${userId}: found ${depositsFound} deposits`);
    return { success: true, deposits: depositsFound };
    
  } catch (error) {
    console.error('❌ TRC20 check error:', error.message);
    return { success: false, error: error.message };
  }
}

async function checkUserBEP20Deposits(userId) {
  try {
    // Получаем BEP20 адрес пользователя (USDT и USDC один адрес)
    const { data: wallet, error } = await supabase
      .from('user_wallets')
      .select('usdt_bep20_address, usdc_bep20_address')
      .eq('user_id', userId)
      .single();
    
    if (error || !wallet?.usdt_bep20_address) {
      console.log(`No BEP20 wallet for user ${userId}`);
      return { success: false, deposits: 0 };
    }

    const transactions = await getBEP20Transactions(wallet.usdt_bep20_address);
    let depositsFound = 0;

    for (const tx of transactions) {
      if (tx.to.toLowerCase() === wallet.usdt_bep20_address.toLowerCase() && tx.amount >= MIN_DEPOSIT) {
        try {
          // Проверяем, не обрабатывалась ли уже эта транзакция
          const { data: existing } = await supabase
            .from('deposits')
            .select('id')
            .eq('tx_hash', tx.transaction_id)
            .eq('network', tx.network)
            .maybeSingle();
          
          if (existing) {
            console.log(`⏭️ Skipping duplicate BEP20 transaction: ${tx.transaction_id}`);
            continue;
          }
          
          const result = await processDeposit({
            user_id: userId,
            address: wallet.usdt_bep20_address
          }, tx.amount, tx.transaction_id, tx.network);
          
          if (result.success) {
            depositsFound++;
            console.log(`💰 NEW BEP20 DEPOSIT: ${tx.amount} ${tx.token} for user ${userId}`);
          }
        } catch (err) {
          if (err.message.includes('already_processed')) {
            console.log(`⏭️ Duplicate BEP20 deposit skipped: ${tx.transaction_id}`);
          } else {
            console.error(`❌ Error processing BEP20 deposit ${tx.transaction_id}:`, err.message);
          }
        }
      }
    }

    console.log(`✅ BEP20 check for user ${userId}: found ${depositsFound} deposits`);
    return { success: true, deposits: depositsFound };
    
  } catch (error) {
    console.error('❌ BEP20 check error:', error.message);
    return { success: false, error: error.message };
  }
}

// ========== BACKGROUND CHECK ==========
async function checkAllDeposits() {
  try {
    console.log('🔄 Checking all deposits...');
    
    // Получаем всех пользователей с кошельками
    const { data: wallets, error } = await supabase
      .from('user_wallets')
      .select('user_id, usdt_trc20_address, usdt_bep20_address')
      .limit(100);

    if (error) throw error;

    let totalDeposits = 0;

    for (const wallet of wallets || []) {
      try {
        // Проверяем TRC20
        if (wallet.usdt_trc20_address) {
          const trc20Result = await checkUserTRC20Deposits(wallet.user_id);
          totalDeposits += trc20Result.deposits || 0;
          await sleep(TRC20_DELAY_MS);
        }
        
        // Проверяем BEP20
        if (wallet.usdt_bep20_address) {
          const bep20Result = await checkUserBEP20Deposits(wallet.user_id);
          totalDeposits += bep20Result.deposits || 0;
          await sleep(BEP20_DELAY_MS);
        }
      } catch (err) {
        console.error(`❌ Error checking wallet for user ${wallet.user_id}:`, err.message);
      }
    }

    console.log(`✅ Total deposits found: ${totalDeposits}`);
    return { success: true, totalDeposits };
    
  } catch (error) {
    console.error('❌ Error in checkAllDeposits:', error.message);
    return { success: false, error: error.message };
  }
}

// ========== START SERVER ==========
const server = app.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 FTP QUANT DEPOSIT SERVER RUNNING on port ${PORT}`);
  console.log(`✅ Health check: http://0.0.0.0:${PORT}/health`);
  console.log(`✅ SUPABASE: CONNECTED`);
  console.log(`✅ TRONGRID: API KEY SET`);
  console.log(`✅ MORALIS: API KEY SET`);
  console.log(`✅ MINIMUM DEPOSIT: $${MIN_DEPOSIT}`);
  console.log(`✅ SUPPORTED NETWORKS: USDT-TRC20, USDT-BEP20, USDC-BEP20`);
  console.log('=============================================');
});

// ========== BACKGROUND TASKS ==========
let isChecking = false;

// Фоновая проверка депозитов каждые 60 секунд
setInterval(async () => {
  if (isChecking) return;
  
  try {
    isChecking = true;
    await checkAllDeposits();
  } catch (err) {
    console.error('❌ Auto-check error:', err.message);
  } finally {
    isChecking = false;
  }
}, 60000);

// Ручная проверка через API
app.get('/api/check-all', async (req, res) => {
  try {
    if (isChecking) {
      return res.json({ success: false, error: 'Check already in progress' });
    }
    
    isChecking = true;
    const result = await checkAllDeposits();
    isChecking = false;
    
    res.json(result);
  } catch (error) {
    isChecking = false;
    res.status(500).json({ success: false, error: error.message });
  }
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('🛑 Received SIGTERM, shutting down gracefully');
  server.close(() => {
    console.log('✅ Server closed');
    process.exit(0);
  });
});

// Handle uncaught exceptions
process.on('uncaughtException', (error) => {
  console.error('❌ Uncaught Exception:', error);
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('❌ Unhandled Rejection at:', promise, 'reason:', reason);
});
