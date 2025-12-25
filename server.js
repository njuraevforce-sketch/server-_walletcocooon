// server-deposit.js — АДАПТИРОВАННЫЙ ДЛЯ FTP QUANT
const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const TronWeb = require('tronweb');
const { ethers } = require('ethers');

const app = express();
const PORT = process.env.PORT || 8080;

// ========== CONFIGURATION ==========
const SUPABASE_URL = process.env.SUPABASE_URL || 'https://fctwivbwjoslkejtjxhe.supabase.co';
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZjdHdpdmJ3am9zbGtlanRqeGhlIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2NjE0MzAzNSwiZXhwIjoyMDgxNzE5MDM1fQ.DiAzcqkigZPueh40idz2fIoJ-o-sKDMaRleOagH__B0';
const TRONGRID_API_KEY = process.env.TRONGRID_API_KEY || '8fa63ef4-f010-4ad2-a556-a7124563bafd';
const MORALIS_API_KEY = process.env.MORALIS_API_KEY || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJub25jZSI6IjcxODVlYzdiLTQ3NzctNDFhNS05ZDI4LTE0YjFlZmJkZTA5NSIsIm9yZ0lkIjoiNDg1NjY3IiwidXNlcklkIjoiNDk5NjYxIiwidHlwZUlkIjoiNjdjYjQzMzgtMmY2OC00MmE3LThmMzItYmJiMDljMDkyM2NlIiwidHlwZSI6IlBST0pFQ1QiLCJpYXQiOjE3NjU0OTA0MDMsImV4cCI6NDkyMTI1MDQwM30.0Z_G2u-E8EdfZQzyUZFY4CVbgUqR2H5e4TjQzi9MnzU';

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
    status: '✅ FTP QUANT DEPOSIT SERVER IS RUNNING',
    message: 'FTP Quant Deposit Processing System',
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    environment: process.env.NODE_ENV || 'development'
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

app.get('/api/health', (req, res) => {
  res.json({
    status: '✅ API HEALTHY',
    timestamp: new Date().toISOString()
  });
});

// ========== INITIALIZE SERVICES ==========
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
const tronWeb = new TronWeb({
  fullHost: 'https://api.trongrid.io',
  headers: { 'TRON-PRO-API-KEY': TRONGRID_API_KEY }
});

// ========== CONSTANTS ==========
const CONTRACTS = {
  'usdt_trc20': 'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t',
  'usdt_bep20': '0x55d398326f99059fF775485246999027B3197955',
  'usdc_bep20': '0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d'
};

const MIN_DEPOSIT = 10; // Минимальный депозит $10

// ========== OPTIMIZED SETTINGS ==========
const TRC20_CHECK_INTERVAL = 45000; // 45 секунд
const BEP20_CHECK_INTERVAL = 180000; // 3 минуты
const BEP20_DELAY_MS = 500;
const TRC20_DELAY_MS = 100;

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
async function processDeposit(wallet, amount, txid, network, token = 'USDT') {
  try {
    console.log(`💰 PROCESSING DEPOSIT: ${amount} ${token} for user ${wallet.user_id}, txid: ${txid}, network: ${network}`);

    // Проверяем существующий депозит с таким хэшем
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
          .update({ status: 'completed', completed_at: new Date().toISOString() })
          .eq('id', existingDeposit.id);
      }
      
      return { success: false, reason: 'already_processed', existing: existingDeposit };
    }

    // Начинаем обработку депозита
    const depositResult = await processDepositTransaction(wallet, amount, txid, network, token);
    
    return depositResult;
  } catch (error) {
    console.error('❌ Error in processDeposit:', error.message);
    throw error;
  }
}

// Функция для обработки депозита
async function processDepositTransaction(wallet, amount, txid, network, token = 'USDT') {
  try {
    // Начинаем транзакцию
    const { data: userData, error: userError } = await supabase
      .from('users')
      .select('balance, total_deposit')
      .eq('id', wallet.user_id)
      .single();

    if (userError) throw new Error(`User not found: ${userError.message}`);

    const newBalance = Number(userData.balance) + Number(amount);
    const newTotalDeposit = Number(userData.total_deposit) + Number(amount);

    // Создаем запись в deposits
    const { data: depositData, error: depositError } = await supabase
      .from('deposits')
      .insert({
        user_id: wallet.user_id,
        amount: amount,
        network: network,
        tx_hash: txid,
        status: 'completed',
        confirmed_at: new Date().toISOString(),
        completed_at: new Date().toISOString()
      })
      .select()
      .single();

    if (depositError) throw new Error(`Failed to create deposit record: ${depositError.message}`);

    // Обновляем баланс пользователя
    const { error: updateError } = await supabase
      .from('users')
      .update({
        balance: newBalance,
        total_deposit: newTotalDeposit,
        updated_at: new Date().toISOString()
      })
      .eq('id', wallet.user_id);

    if (updateError) throw new Error(`Failed to update user balance: ${updateError.message}`);

    // Создаем транзакцию в истории
    const { error: txError } = await supabase
      .from('transactions')
      .insert({
        user_id: wallet.user_id,
        type: 'deposit',
        amount: amount,
        description: `Deposit ${amount} ${token} via ${network}`,
        status: 'completed',
        metadata: { tx_hash: txid, network: network, token: token }
      });

    if (txError) console.error('Warning: Failed to create transaction record:', txError.message);

    console.log(`✅ DEPOSIT PROCESSED: ${amount} ${token} for user ${wallet.user_id}`);
    console.log(`💰 New balance: ${newBalance} USD`);
    console.log(`📝 Deposit ID: ${depositData.id}`);

    return { 
      success: true, 
      amount, 
      deposit_id: depositData.id,
      new_balance: newBalance,
      token: token
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

    console.log(`🔐 Generating ${network} address for user: ${user_id}`);

    // Проверяем существующий кошелек
    const { data: existingWallet, error: walletError } = await supabase
      .from('user_wallets')
      .select('*')
      .eq('user_id', user_id)
      .single();

    let address, private_key;
    let isNew = false;

    // Генерируем или получаем адрес
    if (network === 'usdt_trc20') {
      if (existingWallet && existingWallet.usdt_trc20_address) {
        address = existingWallet.usdt_trc20_address;
        private_key = existingWallet.usdt_trc20_private_key;
      } else {
        const wallet = await generateTRC20Wallet();
        address = wallet.address;
        private_key = wallet.privateKey;
        isNew = true;
      }
    } else if (network === 'usdt_bep20' || network === 'usdc_bep20') {
      // Для BEP20 используем один адрес для USDT и USDC
      if (existingWallet && existingWallet.usdt_bep20_address) {
        address = existingWallet.usdt_bep20_address;
        private_key = existingWallet.usdt_bep20_private_key;
      } else {
        const wallet = await generateBEP20Wallet();
        address = wallet.address;
        private_key = wallet.privateKey;
        isNew = true;
      }
    } else {
      return res.status(400).json({ success: false, error: 'Unsupported network' });
    }

    // Сохраняем или обновляем в базе данных
    const walletData = {
      user_id: user_id,
      updated_at: new Date().toISOString()
    };

    if (network === 'usdt_trc20') {
      walletData.usdt_trc20_address = address;
      walletData.usdt_trc20_private_key = private_key;
    } else if (network === 'usdt_bep20') {
      walletData.usdt_bep20_address = address;
      walletData.usdt_bep20_private_key = private_key;
      // Также сохраняем для USDC BEP20 (тот же адрес)
      walletData.usdc_bep20_address = address;
      walletData.usdc_bep20_private_key = private_key;
    } else if (network === 'usdc_bep20') {
      // Для USDC используем тот же адрес, что и для USDT BEP20
      if (existingWallet && existingWallet.usdt_bep20_address) {
        address = existingWallet.usdt_bep20_address;
        private_key = existingWallet.usdt_bep20_private_key;
      }
      walletData.usdc_bep20_address = address;
      walletData.usdc_bep20_private_key = private_key;
      // Обновляем также USDT BEP20, если его еще нет
      if (!existingWallet || !existingWallet.usdt_bep20_address) {
        walletData.usdt_bep20_address = address;
        walletData.usdt_bep20_private_key = private_key;
      }
    }

    let result;
    if (existingWallet) {
      // Обновляем существующий кошелек
      const { data, error } = await supabase
        .from('user_wallets')
        .update(walletData)
        .eq('user_id', user_id)
        .select()
        .single();

      if (error) {
        console.error('❌ Database update error:', error);
        return res.status(500).json({ success: false, error: 'Failed to update wallet' });
      }
      result = data;
    } else {
      // Создаем новый кошелек
      walletData.created_at = new Date().toISOString();
      const { data, error } = await supabase
        .from('user_wallets')
        .insert(walletData)
        .select()
        .single();

      if (error) {
        console.error('❌ Database insert error:', error);
        return res.status(500).json({ success: false, error: 'Failed to save wallet' });
      }
      result = data;
    }

    console.log(`✅ ${isNew ? 'New' : 'Existing'} ${network} address: ${address}`);
    
    // Запускаем проверку депозитов для пользователя
    setTimeout(() => {
      if (network === 'usdt_trc20') {
        checkUserTRC20Deposits(user_id);
      } else {
        checkUserBEP20Deposits(user_id);
      }
    }, 5000);

    res.json({ 
      success: true, 
      address: address,
      network: network,
      is_new: isNew,
      contract_address: CONTRACTS[network]
    });
  } catch (error) {
    console.error('❌ Generate address error:', error.message);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

app.get('/api/deposit/history', async (req, res) => {
  try {
    const { user_id, network = 'usdt_trc20' } = req.query;
    if (!user_id) return res.status(400).json({ success: false, error: 'User ID is required' });

    const { data: deposits, error } = await supabase
      .from('deposits')
      .select('*')
      .eq('user_id', user_id)
      .eq('network', network)
      .order('created_at', { ascending: false })
      .limit(20);

    if (error) {
      console.error('❌ Database error:', error);
      return res.status(500).json({ success: false, error: 'Failed to fetch deposit history' });
    }

    res.json({ success: true, deposits: deposits || [] });
  } catch (error) {
    console.error('❌ Deposit history error:', error.message);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

// ========== DEPOSIT CHECKING ==========
app.get('/api/check-deposits', async (req, res) => { 
  try {
    console.log('🔄 Manual deposit check triggered via API');
    const trc20Result = await handleCheckTRC20Deposits();
    const bep20Result = await handleCheckBEP20Deposits();
    
    res.json({
      success: true,
      trc20: trc20Result,
      bep20: bep20Result
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Проверка депозитов для конкретного пользователя
app.get('/api/check-deposits/:user_id', async (req, res) => {
  try {
    const userId = req.params.user_id;
    console.log(`🔄 Checking deposits for user: ${userId}`);
    
    const trc20Result = await checkUserTRC20Deposits(userId);
    const bep20Result = await checkUserBEP20Deposits(userId);
    
    res.json({
      success: true,
      user_id: userId,
      trc20_checked: !!trc20Result,
      bep20_checked: !!bep20Result
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
        if (!tokenAddr || tokenAddr !== CONTRACTS.usdt_trc20) continue;

        const to = toBase58IfHex(tx.to);
        const from = toBase58IfHex(tx.from);
        const rawValue = tx.value || 0;
        const amount = Number(rawValue) / 1_000_000; // USDT TRC20 имеет 6 decimals

        if (amount >= MIN_DEPOSIT) {
          transactions.push({
            transaction_id: tx.transaction_id,
            to,
            from,
            amount,
            token: 'USDT',
            contract_address: tokenAddr,
            confirmed: true,
            network: 'usdt_trc20',
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
async function getBEP20Transactions(address, tokenAddress, tokenSymbol) {
  try {
    console.log(`🔍 Checking BEP20 via Moralis: ${address} for ${tokenSymbol}`);
    
    const response = await fetch(
      `https://deep-index.moralis.io/api/v2/${address}/erc20/transfers?chain=bsc&token_addresses=${tokenAddress}&limit=5`,
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
        if (tx.to_address.toLowerCase() === address.toLowerCase() && tx.token_symbol === tokenSymbol) {
          const decimals = tokenSymbol === 'USDC' ? 18 : 18; // USDC тоже 18 decimals на BSC
          const amount = Number(tx.value) / Math.pow(10, decimals);
          
          if (amount >= MIN_DEPOSIT) {
            transactions.push({
              transaction_id: tx.transaction_hash,
              to: tx.to_address.toLowerCase(),
              from: tx.from_address.toLowerCase(),
              amount: amount,
              token: tokenSymbol,
              contract_address: tokenAddress,
              confirmed: true,
              network: tokenSymbol === 'USDC' ? 'usdc_bep20' : 'usdt_bep20',
              timestamp: new Date(tx.block_timestamp).getTime(),
              blockNumber: parseInt(tx.block_number)
            });
          }
        }
      } catch (e) {
        continue;
      }
    }

    console.log(`✅ Found ${transactions.length} BEP20 ${tokenSymbol} transactions for ${address}`);
    return transactions;

  } catch (error) {
    console.error(`❌ Moralis API error for ${tokenSymbol}:`, error.message);
    return [];
  }
}

// ========== OPTIMIZED TRC20 CHECKING ==========
async function handleCheckTRC20Deposits() {
  try {
    console.log('🔄 Checking TRC20 deposits...');
    
    const { data: wallets, error } = await supabase
      .from('user_wallets')
      .select('user_id, usdt_trc20_address')
      .not('usdt_trc20_address', 'is', null)
      .limit(100);

    if (error) throw error;

    let processedCount = 0;
    let depositsFound = 0;
    let duplicatesSkipped = 0;

    for (const wallet of wallets || []) {
      try {
        await sleep(TRC20_DELAY_MS);
        
        const transactions = await getTRC20Transactions(wallet.usdt_trc20_address);
        
        for (const tx of transactions) {
          if (tx.to === wallet.usdt_trc20_address && tx.token === 'USDT' && tx.amount >= MIN_DEPOSIT) {
            try {
              // Проверяем, не обрабатывалась ли уже эта транзакция
              const { data: existing } = await supabase
                .from('deposits')
                .select('id')
                .eq('tx_hash', tx.transaction_id)
                .eq('network', 'usdt_trc20')
                .maybeSingle();
              
              if (existing) {
                duplicatesSkipped++;
                console.log(`⏭️ Skipping duplicate TRC20 transaction: ${tx.transaction_id}`);
                continue;
              }
              
              const walletData = { user_id: wallet.user_id, address: wallet.usdt_trc20_address };
              const result = await processDeposit(walletData, tx.amount, tx.transaction_id, 'usdt_trc20', 'USDT');
              if (result.success) {
                depositsFound++;
                console.log(`💰 NEW TRC20 DEPOSIT: ${tx.amount} USDT for user ${wallet.user_id}`);
              }
            } catch (err) {
              if (err.message.includes('already_processed') || (err.reason && err.reason === 'already_processed')) {
                duplicatesSkipped++;
                console.log(`⏭️ Duplicate TRC20 deposit skipped: ${tx.transaction_id}`);
              } else {
                console.error(`❌ Error processing TRC20 deposit ${tx.transaction_id}:`, err.message);
              }
            }
          }
        }
        processedCount++;
      } catch (err) {
        console.error(`❌ Error processing TRC20 wallet ${wallet.usdt_trc20_address}:`, err.message);
      }
    }

    console.log(`✅ TRC20: Processed ${processedCount} wallets, found ${depositsFound} new deposits, skipped ${duplicatesSkipped} duplicates`);
    return { success: true, processed: processedCount, deposits: depositsFound, duplicates: duplicatesSkipped };
    
  } catch (error) {
    console.error('❌ TRC20 check error:', error.message);
    return { success: false, error: error.message };
  }
}

// ========== OPTIMIZED BEP20 CHECKING ==========
async function handleCheckBEP20Deposits() {
  try {
    console.log('🔄 Checking BEP20 deposits (USDT & USDC)...');
    
    const { data: wallets, error } = await supabase
      .from('user_wallets')
      .select('user_id, usdt_bep20_address, usdc_bep20_address')
      .not('usdt_bep20_address', 'is', null)
      .limit(100);

    if (error) throw error;

    let processedCount = 0;
    let depositsFound = 0;
    let duplicatesSkipped = 0;

    for (const wallet of wallets || []) {
      try {
        await sleep(BEP20_DELAY_MS);
        
        // Проверяем USDT BEP20
        const usdtTransactions = await getBEP20Transactions(
          wallet.usdt_bep20_address, 
          CONTRACTS.usdt_bep20, 
          'USDT'
        );
        
        // Проверяем USDC BEP20 (используем тот же адрес)
        const usdcTransactions = await getBEP20Transactions(
          wallet.usdt_bep20_address, 
          CONTRACTS.usdc_bep20, 
          'USDC'
        );
        
        const allTransactions = [...usdtTransactions, ...usdcTransactions];
        
        for (const tx of allTransactions) {
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
                duplicatesSkipped++;
                console.log(`⏭️ Skipping duplicate ${tx.network} transaction: ${tx.transaction_id}`);
                continue;
              }
              
              const walletData = { user_id: wallet.user_id, address: wallet.usdt_bep20_address };
              const result = await processDeposit(walletData, tx.amount, tx.transaction_id, tx.network, tx.token);
              if (result.success) {
                depositsFound++;
                console.log(`💰 NEW ${tx.network.toUpperCase()} DEPOSIT: ${tx.amount} ${tx.token} for user ${wallet.user_id}`);
              }
            } catch (err) {
              if (err.message.includes('already_processed') || (err.reason && err.reason === 'already_processed')) {
                duplicatesSkipped++;
                console.log(`⏭️ Duplicate ${tx.network} deposit skipped: ${tx.transaction_id}`);
              } else {
                console.error(`❌ Error processing ${tx.network} deposit ${tx.transaction_id}:`, err.message);
              }
            }
          }
        }
        processedCount++;
      } catch (err) {
        console.error(`❌ Error processing BEP20 wallet ${wallet.usdt_bep20_address}:`, err.message);
      }
    }

    console.log(`✅ BEP20: Processed ${processedCount} wallets, found ${depositsFound} new deposits, skipped ${duplicatesSkipped} duplicates`);
    return { success: true, processed: processedCount, deposits: depositsFound, duplicates: duplicatesSkipped };
    
  } catch (error) {
    console.error('❌ BEP20 check error:', error.message);
    return { success: false, error: error.message };
  }
}

// ========== HELPER FUNCTIONS ==========
async function checkUserTRC20Deposits(userId) {
  try {
    const { data: wallet } = await supabase
      .from('user_wallets')
      .select('usdt_trc20_address')
      .eq('user_id', userId)
      .single();
    
    if (!wallet || !wallet.usdt_trc20_address) return;
    
    const transactions = await getTRC20Transactions(wallet.usdt_trc20_address);
    
    for (const tx of transactions) {
      if (tx.to === wallet.usdt_trc20_address && tx.token === 'USDT' && tx.amount >= MIN_DEPOSIT) {
        try {
          // Проверяем дубликаты
          const { data: existing } = await supabase
            .from('deposits')
            .select('id')
            .eq('tx_hash', tx.transaction_id)
            .eq('network', 'usdt_trc20')
            .maybeSingle();
          
          if (existing) {
            console.log(`⏭️ Skipping duplicate TRC20 transaction for user ${userId}: ${tx.transaction_id}`);
            continue;
          }
          
          const walletData = { user_id: userId, address: wallet.usdt_trc20_address };
          const result = await processDeposit(walletData, tx.amount, tx.transaction_id, 'usdt_trc20', 'USDT');
          if (result.success) {
            console.log(`💰 FOUND NEW TRC20 DEPOSIT: ${tx.amount} USDT for user ${userId}`);
          }
        } catch (err) {
          if (err.message.includes('already_processed') || (err.reason && err.reason === 'already_processed')) {
            console.log(`⏭️ Duplicate TRC20 deposit for user ${userId}: ${tx.transaction_id}`);
          } else {
            console.error(`❌ Error processing transaction ${tx.transaction_id}:`, err);
          }
        }
      }
    }
  } catch (error) {
    console.error('❌ checkUserTRC20Deposits error:', error);
  }
}

async function checkUserBEP20Deposits(userId) {
  try {
    const { data: wallet } = await supabase
      .from('user_wallets')
      .select('usdt_bep20_address, usdc_bep20_address')
      .eq('user_id', userId)
      .single();
    
    if (!wallet || !wallet.usdt_bep20_address) return;
    
    // Проверяем USDT BEP20
    const usdtTransactions = await getBEP20Transactions(
      wallet.usdt_bep20_address, 
      CONTRACTS.usdt_bep20, 
      'USDT'
    );
    
    // Проверяем USDC BEP20
    const usdcTransactions = await getBEP20Transactions(
      wallet.usdt_bep20_address, 
      CONTRACTS.usdc_bep20, 
      'USDC'
    );
    
    const allTransactions = [...usdtTransactions, ...usdcTransactions];
    
    for (const tx of allTransactions) {
      if (tx.to.toLowerCase() === wallet.usdt_bep20_address.toLowerCase() && tx.amount >= MIN_DEPOSIT) {
        try {
          // Проверяем дубликаты
          const { data: existing } = await supabase
            .from('deposits')
            .select('id')
            .eq('tx_hash', tx.transaction_id)
            .eq('network', tx.network)
            .maybeSingle();
          
          if (existing) {
            console.log(`⏭️ Skipping duplicate ${tx.network} transaction for user ${userId}: ${tx.transaction_id}`);
            continue;
          }
          
          const walletData = { user_id: userId, address: wallet.usdt_bep20_address };
          const result = await processDeposit(walletData, tx.amount, tx.transaction_id, tx.network, tx.token);
          if (result.success) {
            console.log(`💰 FOUND NEW ${tx.network.toUpperCase()} DEPOSIT: ${tx.amount} ${tx.token} for user ${userId}`);
          }
        } catch (err) {
          if (err.message.includes('already_processed') || (err.reason && err.reason === 'already_processed')) {
            console.log(`⏭️ Duplicate ${tx.network} deposit for user ${userId}: ${tx.transaction_id}`);
          } else {
            console.error(`❌ Error processing transaction ${tx.transaction_id}:`, err);
          }
        }
      }
    }
  } catch (error) {
    console.error('❌ checkUserBEP20Deposits error:', error);
  }
}

// ========== START SERVER ==========
const server = app.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 FTP QUANT DEPOSIT SERVER RUNNING on port ${PORT}`);
  console.log(`✅ Health check available at: http://0.0.0.0:${PORT}/health`);
  console.log(`✅ API Health check: http://0.0.0.0:${PORT}/api/health`);
  console.log(`✅ SUPABASE: CONNECTED`);
  console.log(`✅ TRONGRID: API KEY SET`);
  console.log(`✅ MORALIS: API KEY SET`);
  console.log(`✅ MINIMUM DEPOSIT: $${MIN_DEPOSIT}`);
  console.log(`✅ SUPPORTED NETWORKS: USDT-TRC20, USDT-BEP20, USDC-BEP20`);
  console.log(`✅ TRC20: Checking every 45 seconds`);
  console.log(`✅ BEP20: Checking every 3 minutes`);
  console.log('===================================');
});

// ========== BACKGROUND TASKS ==========
let isCheckingTRC20 = false;
let isCheckingBEP20 = false;

// TRC20 Background Check (Fast)
setInterval(async () => {
  if (isCheckingTRC20) return;
  
  try {
    isCheckingTRC20 = true;
    await handleCheckTRC20Deposits();
  } catch (err) {
    console.error('❌ TRC20 auto-check error:', err.message);
  } finally {
    isCheckingTRC20 = false;
  }
}, TRC20_CHECK_INTERVAL);

// BEP20 Background Check
setInterval(async () => {
  if (isCheckingBEP20) return;
  
  try {
    isCheckingBEP20 = true;
    await handleCheckBEP20Deposits();
  } catch (err) {
    console.error('❌ BEP20 auto-check error:', err.message);
  } finally {
    isCheckingBEP20 = false;
  }
}, BEP20_CHECK_INTERVAL);

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

module.exports = app;
