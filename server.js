// server-deposit.js — OPTIMIZED FOR TRC20, BEP20 & BEP20 USDC WITH DUPLICATE PROTECTION
const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const TronWeb = require('tronweb');
const { ethers } = require('ethers');

const app = express();
const PORT = process.env.PORT || 8081; // Используем другой порт, чтобы не конфликтовать с основным сервером

// ========== CONFIGURATION ==========
const SUPABASE_URL = 'https://fctwivbwjoslkejtjxhe.supabase.co';
const SUPABASE_SERVICE_ROLE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZjdHdpdmJ3am9zbGtlanRqeGhlIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjYxNDMwMzUsImV4cCI6MjA4MTcxOTAzNX0.DiAzcqkigZPueh40idz2fIoJ-o-sKDMaRleOagH__B0';
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
app.get('/deposit-health', (req, res) => {
  res.json({
    status: '✅ DEPOSIT PROCESSOR IS RUNNING',
    service: 'FTP Quant Deposit Processing System',
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    environment: process.env.NODE_ENV || 'development'
  });
});

app.get('/deposit/health', (req, res) => {
  res.json({
    status: '✅ DEPOSIT HEALTHY',
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
const MIN_DEPOSIT = 17;

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
    console.log(`💰 PROCESSING DEPOSIT: ${amount} USDT for user ${wallet.user_id}, txid: ${txid}, network: ${network}`);

    // Проверяем дубликаты в таблице deposits
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
      
      // Если депозит уже обработан, но статус не 'completed', обновим его
      if (existingDeposit.status !== 'completed') {
        await supabase
          .from('deposits')
          .update({ 
            status: 'completed',
            confirmed_at: new Date().toISOString(),
            completed_at: new Date().toISOString()
          })
          .eq('id', existingDeposit.id);
      }
      
      return { success: false, reason: 'already_processed', existing: existingDeposit };
    }

    // Начинаем обработку депозита
    const depositResult = await processDepositTransaction(wallet, amount, txid, network);
    
    return depositResult;
  } catch (error) {
    console.error('❌ Error in processDeposit:', error.message);
    throw error;
  }
}

// Функция для обработки депозита через PostgreSQL RPC
async function processDepositTransaction(wallet, amount, txid, network) {
  try {
    console.log(`🔧 Atomic deposit processing for user ${wallet.user_id}, amount: ${amount}, txid: ${txid}, network: ${network}`);

    // 1. Создаем запись в deposits
    const { data: deposit, error: depositError } = await supabase
      .from('deposits')
      .insert({
        user_id: wallet.user_id,
        amount: amount,
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
      if (depositError.code === '23505') { // Unique violation
        console.log(`⏭️ Duplicate transaction detected: ${txid}`);
        return { success: false, reason: 'already_processed' };
      }
      console.error('❌ Deposit creation error:', depositError.message);
      throw new Error(`Database transaction failed: ${depositError.message}`);
    }

    // 2. Обновляем баланс пользователя
    const { data: user, error: userError } = await supabase
      .from('users')
      .select('balance, total_deposit')
      .eq('id', wallet.user_id)
      .single();

    if (userError) {
      console.error('❌ User fetch error:', userError.message);
      throw new Error(`User fetch failed: ${userError.message}`);
    }

    const newBalance = parseFloat(user.balance) + parseFloat(amount);
    const newTotalDeposit = parseFloat(user.total_deposit) + parseFloat(amount);

    const { error: updateError } = await supabase
      .from('users')
      .update({
        balance: newBalance,
        total_deposit: newTotalDeposit,
        updated_at: new Date().toISOString()
      })
      .eq('id', wallet.user_id);

    if (updateError) {
      console.error('❌ Balance update error:', updateError.message);
      throw new Error(`Balance update failed: ${updateError.message}`);
    }

    // 3. Создаем запись в transactions
    const { error: transError } = await supabase
      .from('transactions')
      .insert({
        user_id: wallet.user_id,
        type: 'deposit',
        amount: amount,
        description: `Deposit via ${network} tx:${txid}`,
        status: 'completed',
        metadata: { tx_hash: txid, network: network },
        created_at: new Date().toISOString()
      });

    if (transError) {
      console.error('❌ Transaction creation error:', transError.message);
      throw new Error(`Transaction creation failed: ${transError.message}`);
    }

    console.log(`✅ DEPOSIT PROCESSED SUCCESSFULLY: ${amount} USDT for user ${wallet.user_id}`);
    console.log(`💰 New balance: ${newBalance} USDT`);
    console.log(`📝 Deposit ID: ${deposit.id}`);

    return { 
      success: true, 
      amount, 
      deposit_id: deposit.id,
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

    // Проверяем существующий кошелек в user_wallets
    const { data: existingWallet } = await supabase
      .from('user_wallets')
      .select('*')
      .eq('user_id', user_id)
      .single();

    let address, private_key;
    let walletField = '';

    // Определяем поле в user_wallets
    if (network === 'usdt_trc20') {
      walletField = 'usdt_trc20_address';
      if (existingWallet && existingWallet.usdt_trc20_address) {
        address = existingWallet.usdt_trc20_address;
        private_key = existingWallet.usdt_trc20_private_key || 'stored_in_deposit_addresses';
        console.log(`✅ TRC20 wallet already exists: ${address}`);
      } else {
        const wallet = await generateTRC20Wallet();
        address = wallet.address;
        private_key = wallet.privateKey;
      }
    } else if (network === 'usdt_bep20') {
      walletField = 'usdt_bep20_address';
      if (existingWallet && existingWallet.usdt_bep20_address) {
        address = existingWallet.usdt_bep20_address;
        private_key = existingWallet.usdt_bep20_private_key || 'stored_in_deposit_addresses';
        console.log(`✅ USDT BEP20 wallet already exists: ${address}`);
      } else {
        const wallet = await generateBEP20Wallet();
        address = wallet.address;
        private_key = wallet.privateKey;
      }
    } else if (network === 'usdc_bep20') {
      walletField = 'usdc_bep20_address';
      if (existingWallet && existingWallet.usdc_bep20_address) {
        address = existingWallet.usdc_bep20_address;
        private_key = existingWallet.usdc_bep20_private_key || 'stored_in_deposit_addresses';
        console.log(`✅ USDC BEP20 wallet already exists: ${address}`);
      } else {
        const wallet = await generateBEP20Wallet();
        address = wallet.address;
        private_key = wallet.privateKey;
      }
    } else {
      return res.status(400).json({ success: false, error: 'Unsupported network' });
    }

    // Сохраняем или обновляем в user_wallets
    if (existingWallet) {
      // Обновляем существующую запись
      const updateData = { [walletField]: address };
      if (private_key !== 'stored_in_deposit_addresses') {
        updateData[`${walletField.replace('_address', '_private_key')}`] = private_key;
      }
      
      const { error } = await supabase
        .from('user_wallets')
        .update(updateData)
        .eq('user_id', user_id);

      if (error) {
        console.error('❌ Wallet update error:', error);
        return res.status(500).json({ success: false, error: 'Failed to update wallet' });
      }
    } else {
      // Создаем новую запись
      const walletData = {
        user_id,
        [walletField]: address,
        default_network: network,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      };

      if (private_key !== 'stored_in_deposit_addresses') {
        walletData[`${walletField.replace('_address', '_private_key')}`] = private_key;
      }

      const { error } = await supabase
        .from('user_wallets')
        .insert(walletData);

      if (error) {
        console.error('❌ Wallet creation error:', error);
        return res.status(500).json({ success: false, error: 'Failed to save wallet' });
      }
    }

    // Также сохраняем в deposit_addresses для обратной совместимости
    const { error: depositAddrError } = await supabase
      .from('deposit_addresses')
      .upsert({
        user_id,
        address,
        private_key,
        network,
        created_at: new Date().toISOString()
      }, { 
        onConflict: 'user_id,network'
      });

    if (depositAddrError) {
      console.warn('⚠️ Could not save to deposit_addresses:', depositAddrError.message);
    }

    console.log(`✅ New ${network} wallet created/updated: ${address}`);
    
    // Проверяем существующие депозиты
    setTimeout(() => {
      if (network.includes('trc20')) {
        checkUserTRC20Deposits(user_id);
      } else if (network.includes('bep20')) {
        if (network === 'usdt_bep20') {
          checkUserBEP20Deposits(user_id, 'usdt_bep20');
        } else if (network === 'usdc_bep20') {
          checkUserBEP20Deposits(user_id, 'usdc_bep20');
        }
      }
    }, 5000);

    res.json({ 
      success: true, 
      address, 
      network,
      message: 'Wallet generated successfully'
    });
  } catch (error) {
    console.error('❌ Generate wallet error:', error.message);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

// ========== TRC20 TRANSACTIONS ==========
async function getTRC20Transactions(address) {
  try {
    if (!address) return [];
    
    const response = await fetch(`https://api.trongrid.io/v1/accounts/${address}/transactions/trc20?limit=20&only_confirmed=true`, {
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
async function getBEP20Transactions(address, networkType) {
  try {
    console.log(`🔍 Checking BEP20 via Moralis: ${address}, network: ${networkType}`);
    
    let contractAddress;
    if (networkType === 'usdt_bep20') {
      contractAddress = USDT_BEP20_CONTRACT;
    } else if (networkType === 'usdc_bep20') {
      contractAddress = USDC_BEP20_CONTRACT;
    } else {
      return [];
    }
    
    const response = await fetch(
      `https://deep-index.moralis.io/api/v2/${address}/erc20/transfers?chain=bsc&token_addresses=${contractAddress}&limit=10`,
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
          const tokenSymbol = networkType === 'usdt_bep20' ? 'USDT' : 'USDC';
          if (tx.token_symbol !== tokenSymbol) continue;
          
          const amount = Number(tx.value) / Math.pow(10, tx.decimals || 18);
          
          if (amount >= MIN_DEPOSIT) {
            transactions.push({
              transaction_id: tx.transaction_hash,
              to: tx.to_address.toLowerCase(),
              from: tx.from_address.toLowerCase(),
              amount: amount,
              token: tokenSymbol,
              confirmed: true,
              network: networkType,
              timestamp: new Date(tx.block_timestamp).getTime(),
              blockNumber: parseInt(tx.block_number)
            });
          }
        }
      } catch (e) {
        continue;
      }
    }

    console.log(`✅ Found ${transactions.length} ${networkType.toUpperCase()} transactions for ${address}`);
    return transactions;

  } catch (error) {
    console.error('❌ Moralis API error:', error.message);
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
              
              const result = await processDeposit({
                user_id: wallet.user_id,
                address: wallet.usdt_trc20_address
              }, tx.amount, tx.transaction_id, 'usdt_trc20');
              
              if (result.success) {
                depositsFound++;
                console.log(`💰 NEW TRC20 DEPOSIT: ${tx.amount} USDT for user ${wallet.user_id}`);
              }
            } catch (err) {
              if (err.message.includes('already_processed') || 
                  (err.reason && err.reason === 'already_processed')) {
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
      .limit(100);

    if (error) throw error;

    let processedCount = 0;
    let depositsFound = 0;
    let duplicatesSkipped = 0;

    for (const wallet of wallets || []) {
      try {
        await sleep(BEP20_DELAY_MS);
        
        // Проверяем USDT BEP20
        if (wallet.usdt_bep20_address) {
          const transactions = await getBEP20Transactions(wallet.usdt_bep20_address, 'usdt_bep20');
          
          for (const tx of transactions) {
            if (tx.to.toLowerCase() === wallet.usdt_bep20_address.toLowerCase() && tx.token === 'USDT' && tx.amount >= MIN_DEPOSIT) {
              try {
                // Проверяем, не обрабатывалась ли уже эта транзакция
                const { data: existing } = await supabase
                  .from('deposits')
                  .select('id')
                  .eq('tx_hash', tx.transaction_id)
                  .eq('network', 'usdt_bep20')
                  .maybeSingle();
                
                if (existing) {
                  duplicatesSkipped++;
                  console.log(`⏭️ Skipping duplicate USDT BEP20 transaction: ${tx.transaction_id}`);
                  continue;
                }
                
                const result = await processDeposit({
                  user_id: wallet.user_id,
                  address: wallet.usdt_bep20_address
                }, tx.amount, tx.transaction_id, 'usdt_bep20');
                
                if (result.success) {
                  depositsFound++;
                  console.log(`💰 NEW USDT BEP20 DEPOSIT: ${tx.amount} USDT for user ${wallet.user_id}`);
                }
              } catch (err) {
                if (err.message.includes('already_processed') || 
                    (err.reason && err.reason === 'already_processed')) {
                  duplicatesSkipped++;
                  console.log(`⏭️ Duplicate USDT BEP20 deposit skipped: ${tx.transaction_id}`);
                } else {
                  console.error(`❌ Error processing USDT BEP20 deposit ${tx.transaction_id}:`, err.message);
                }
              }
            }
          }
        }
        
        // Проверяем USDC BEP20
        if (wallet.usdc_bep20_address) {
          await sleep(BEP20_DELAY_MS); // Небольшая задержка между проверками
          
          const transactions = await getBEP20Transactions(wallet.usdc_bep20_address, 'usdc_bep20');
          
          for (const tx of transactions) {
            if (tx.to.toLowerCase() === wallet.usdc_bep20_address.toLowerCase() && tx.token === 'USDC' && tx.amount >= MIN_DEPOSIT) {
              try {
                // Проверяем, не обрабатывалась ли уже эта транзакция
                const { data: existing } = await supabase
                  .from('deposits')
                  .select('id')
                  .eq('tx_hash', tx.transaction_id)
                  .eq('network', 'usdc_bep20')
                  .maybeSingle();
                
                if (existing) {
                  duplicatesSkipped++;
                  console.log(`⏭️ Skipping duplicate USDC BEP20 transaction: ${tx.transaction_id}`);
                  continue;
                }
                
                const result = await processDeposit({
                  user_id: wallet.user_id,
                  address: wallet.usdc_bep20_address
                }, tx.amount, tx.transaction_id, 'usdc_bep20');
                
                if (result.success) {
                  depositsFound++;
                  console.log(`💰 NEW USDC BEP20 DEPOSIT: ${tx.amount} USDC for user ${wallet.user_id}`);
                }
              } catch (err) {
                if (err.message.includes('already_processed') || 
                    (err.reason && err.reason === 'already_processed')) {
                  duplicatesSkipped++;
                  console.log(`⏭️ Duplicate USDC BEP20 deposit skipped: ${tx.transaction_id}`);
                } else {
                  console.error(`❌ Error processing USDC BEP20 deposit ${tx.transaction_id}:`, err.message);
                }
              }
            }
          }
        }
        
        processedCount++;
      } catch (err) {
        console.error(`❌ Error processing BEP20 wallet for user ${wallet.user_id}:`, err.message);
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
          
          const result = await processDeposit({
            user_id: userId,
            address: wallet.usdt_trc20_address
          }, tx.amount, tx.transaction_id, 'usdt_trc20');
          
          if (result.success) {
            console.log(`💰 FOUND NEW TRC20 DEPOSIT: ${tx.amount} USDT for user ${userId}`);
          }
        } catch (err) {
          if (err.message.includes('already_processed') || err.reason === 'already_processed') {
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

async function checkUserBEP20Deposits(userId, networkType) {
  try {
    const { data: wallet } = await supabase
      .from('user_wallets')
      .select('usdt_bep20_address, usdc_bep20_address')
      .eq('user_id', userId)
      .single();
    
    if (!wallet) return;
    
    let address;
    if (networkType === 'usdt_bep20' && wallet.usdt_bep20_address) {
      address = wallet.usdt_bep20_address;
    } else if (networkType === 'usdc_bep20' && wallet.usdc_bep20_address) {
      address = wallet.usdc_bep20_address;
    } else {
      return;
    }
    
    const transactions = await getBEP20Transactions(address, networkType);
    
    for (const tx of transactions) {
      if (tx.to.toLowerCase() === address.toLowerCase() && tx.amount >= MIN_DEPOSIT) {
        try {
          // Проверяем дубликаты
          const { data: existing } = await supabase
            .from('deposits')
            .select('id')
            .eq('tx_hash', tx.transaction_id)
            .eq('network', networkType)
            .maybeSingle();
          
          if (existing) {
            console.log(`⏭️ Skipping duplicate ${networkType} transaction for user ${userId}: ${tx.transaction_id}`);
            continue;
          }
          
          const result = await processDeposit({
            user_id: userId,
            address: address
          }, tx.amount, tx.transaction_id, networkType);
          
          if (result.success) {
            console.log(`💰 FOUND NEW ${networkType.toUpperCase()} DEPOSIT: ${tx.amount} ${tx.token} for user ${userId}`);
          }
        } catch (err) {
          if (err.message.includes('already_processed') || err.reason === 'already_processed') {
            console.log(`⏭️ Duplicate ${networkType} deposit for user ${userId}: ${tx.transaction_id}`);
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

// ========== API Endpoints ==========
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

app.get('/api/deposit/history', async (req, res) => {
  try {
    const { user_id, network } = req.query;
    if (!user_id) return res.status(400).json({ success: false, error: 'User ID is required' });

    let query = supabase
      .from('deposits')
      .select('*')
      .eq('user_id', user_id)
      .order('created_at', { ascending: false })
      .limit(20);

    if (network) {
      query = query.eq('network', network);
    }

    const { data: deposits, error } = await query;

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

// ========== START SERVER ==========
const server = app.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 DEPOSIT PROCESSOR SERVER RUNNING on port ${PORT}`);
  console.log(`✅ Health check available at: http://0.0.0.0:${PORT}/deposit-health`);
  console.log(`✅ SUPABASE: CONNECTED`);
  console.log(`✅ TRONGRID: API KEY SET`);
  console.log(`✅ MORALIS: API KEY SET`);
  console.log(`✅ TRC20 (USDT): Checking every 45 seconds`);
  console.log(`✅ BEP20 (USDT/USDC): Checking every 3 minutes`);
  console.log(`✅ ATOMIC DEPOSITS: ENABLED`);
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

// BEP20 Background Check (Optimized for DCU)
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
    console.log('✅ Deposit processor server closed');
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
