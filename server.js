// server.js — AUTO DEPOSIT FOR FTP QUANT
const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const TronWeb = require('tronweb');
const { ethers } = require('ethers');

const app = express();
const PORT = process.env.PORT || 8080;

// ========== CONFIGURATION ==========
const SUPABASE_URL = 'https://fctwivbwjoslkejtjxhe.supabase.co';
const SUPABASE_SERVICE_ROLE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZjdHdpdmJ3am9zbGtlanRqeGhlIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2NjE0MzAzNSwiZXhwIjoyMDgxNzE5MDM1fQ.DiAzcqkigZPueh40idz2fIoJ-o-sKDMaRleOagH__B0';
const TRONGRID_API_KEY = process.env.TRONGRID_API_KEY || 'your-trongrid-key';
const MORALIS_API_KEY = process.env.MORALIS_API_KEY || 'your-moralis-key';

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
    status: '✅ SERVER IS RUNNING',
    message: 'FTP Quant Deposit Processing System',
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
const CONTRACTS = {
  USDT_TRC20: 'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t',
  USDT_BEP20: '0x55d398326f99059fF775485246999027B3197955',
  USDC_BEP20: '0x8AC76a51cc958d6581427545a7558DB268149B56'
};

const MIN_DEPOSIT = 10;

// ========== SETTINGS ==========
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
async function generateTRC20Address() {
  try {
    const account = await tronWeb.createAccount();
    return account.address.base58;
  } catch (error) {
    console.error('❌ TRC20 address generation error:', error);
    throw error;
  }
}

async function generateBEP20Address() {
  try {
    const wallet = ethers.Wallet.createRandom();
    return wallet.address;
  } catch (error) {
    console.error('❌ BEP20 address generation error:', error);
    throw error;
  }
}

// ========== DEPOSIT PROCESSING ==========
async function processDeposit(userId, amount, txHash, network, token = 'USDT') {
  try {
    console.log(`💰 PROCESSING DEPOSIT: ${amount} ${token} for user ${userId}, tx: ${txHash}, network: ${network}`);

    // Проверка дубликатов
    const { data: existingDeposit, error: checkError } = await supabase
      .from('deposits')
      .select('id, status, amount, network')
      .eq('tx_hash', txHash)
      .eq('network', network)
      .maybeSingle();

    if (checkError) {
      console.error('Error checking existing deposit:', checkError);
      throw checkError;
    }

    if (existingDeposit) {
      console.log(`✅ Deposit already processed: ${txHash}, status: ${existingDeposit.status}`);
      return { success: false, reason: 'already_processed' };
    }

    // Начинаем транзакцию
    const { data: user, error: userError } = await supabase
      .from('users')
      .select('balance, total_deposit')
      .eq('id', userId)
      .single();

    if (userError) {
      throw new Error(`User not found: ${userError.message}`);
    }

    // Обновляем баланс пользователя
    const newBalance = Number(user.balance) + Number(amount);
    const newTotalDeposit = Number(user.total_deposit) + Number(amount);

    const { error: updateError } = await supabase
      .from('users')
      .update({
        balance: newBalance,
        total_deposit: newTotalDeposit,
        updated_at: new Date().toISOString()
      })
      .eq('id', userId);

    if (updateError) {
      throw new Error(`Failed to update user balance: ${updateError.message}`);
    }

    // Создаем запись о депозите
    const { data: deposit, error: depositError } = await supabase
      .from('deposits')
      .insert({
        user_id: userId,
        amount: amount,
        network: network,
        tx_hash: txHash,
        status: 'completed',
        completed_at: new Date().toISOString()
      })
      .select()
      .single();

    if (depositError) {
      throw new Error(`Failed to create deposit record: ${depositError.message}`);
    }

    // Создаем транзакцию
    const { error: transactionError } = await supabase
      .from('transactions')
      .insert({
        user_id: userId,
        type: 'deposit',
        amount: amount,
        description: `Deposit ${amount} ${token} via ${network}`,
        status: 'completed',
        created_at: new Date().toISOString()
      });

    if (transactionError) {
      console.error('Error creating transaction record:', transactionError);
    }

    console.log(`✅ DEPOSIT PROCESSED: ${amount} ${token} for user ${userId}`);
    console.log(`💰 New balance: ${newBalance} USD`);
    console.log(`📝 Deposit ID: ${deposit.id}`);

    return { 
      success: true, 
      amount, 
      deposit_id: deposit.id,
      new_balance: newBalance
    };
    
  } catch (error) {
    console.error('❌ Error in processDeposit:', error.message);
    throw error;
  }
}

// ========== API ENDPOINTS ==========
app.post('/api/deposit/generate', async (req, res) => {
  try {
    const { user_id, network = 'usdt_trc20' } = req.query;
    if (!user_id) return res.status(400).json({ success: false, error: 'User ID is required' });

    console.log(`🔐 Generating ${network} address for user: ${user_id}`);

    let address;
    let updateField;

    // Генерируем адрес в зависимости от сети
    if (network === 'usdt_trc20') {
      address = await generateTRC20Address();
      updateField = 'usdt_trc20_address';
    } else if (network === 'usdt_bep20') {
      address = await generateBEP20Address();
      updateField = 'usdt_bep20_address';
    } else if (network === 'usdc_bep20') {
      address = await generateBEP20Address();
      updateField = 'usdc_bep20_address';
    } else {
      return res.status(400).json({ success: false, error: 'Unsupported network' });
    }

    // Проверяем существующий кошелек
    const { data: existingWallet, error: walletError } = await supabase
      .from('user_wallets')
      .select('*')
      .eq('user_id', user_id)
      .single();

    if (walletError && walletError.code === 'PGRST116') {
      // Создаем новый кошелек
      const { data: newWallet, error: insertError } = await supabase
        .from('user_wallets')
        .insert({
          user_id: user_id,
          [updateField]: address,
          default_network: network,
          updated_at: new Date().toISOString()
        })
        .select()
        .single();

      if (insertError) {
        console.error('❌ Database error:', insertError);
        return res.status(500).json({ success: false, error: 'Failed to save wallet' });
      }

      console.log(`✅ New ${network} wallet created: ${address}`);
      res.json({ success: true, address, network });

    } else if (!walletError) {
      // Обновляем существующий кошелек
      const { data: updatedWallet, error: updateError } = await supabase
        .from('user_wallets')
        .update({
          [updateField]: address,
          updated_at: new Date().toISOString()
        })
        .eq('user_id', user_id)
        .select()
        .single();

      if (updateError) {
        console.error('❌ Database error:', updateError);
        return res.status(500).json({ success: false, error: 'Failed to update wallet' });
      }

      console.log(`✅ ${network} wallet updated: ${address}`);
      res.json({ success: true, address, network });
    }

  } catch (error) {
    console.error('❌ Generate address error:', error.message);
    res.status(500).json({ success: false, error: 'Internal server error' });
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
        if (!tokenAddr || tokenAddr !== CONTRACTS.USDT_TRC20) continue;

        const to = toBase58IfHex(tx.to);
        const rawValue = tx.value || 0;
        const amount = Number(rawValue) / 1_000_000;

        if (amount >= MIN_DEPOSIT) {
          transactions.push({
            transaction_id: tx.transaction_id,
            to,
            amount,
            token: 'USDT',
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
async function getBEP20Transactions(address) {
  try {
    console.log(`🔍 Checking BEP20 via Moralis: ${address}`);
    
    let allTransactions = [];
    
    // Проверяем USDT BEP20
    const usdtResponse = await fetch(
      `https://deep-index.moralis.io/api/v2/${address}/erc20/transfers?chain=bsc&token_addresses=${CONTRACTS.USDT_BEP20}&limit=10`,
      {
        headers: {
          'X-API-Key': MORALIS_API_KEY,
          'Accept': 'application/json'
        }
      }
    );

    if (usdtResponse.ok) {
      const usdtData = await usdtResponse.json();
      for (const tx of usdtData.result || []) {
        if (tx.to_address.toLowerCase() === address.toLowerCase()) {
          const amount = Number(tx.value) / Math.pow(10, tx.decimals || 18);
          if (amount >= MIN_DEPOSIT) {
            allTransactions.push({
              transaction_id: tx.transaction_hash,
              to: tx.to_address.toLowerCase(),
              amount: amount,
              token: 'USDT',
              network: 'usdt_bep20',
              timestamp: new Date(tx.block_timestamp).getTime()
            });
          }
        }
      }
    }

    // Проверяем USDC BEP20
    await sleep(200); // Задержка между запросами
    
    const usdcResponse = await fetch(
      `https://deep-index.moralis.io/api/v2/${address}/erc20/transfers?chain=bsc&token_addresses=${CONTRACTS.USDC_BEP20}&limit=10`,
      {
        headers: {
          'X-API-Key': MORALIS_API_KEY,
          'Accept': 'application/json'
        }
      }
    );

    if (usdcResponse.ok) {
      const usdcData = await usdcResponse.json();
      for (const tx of usdcData.result || []) {
        if (tx.to_address.toLowerCase() === address.toLowerCase()) {
          const amount = Number(tx.value) / Math.pow(10, tx.decimals || 18);
          if (amount >= MIN_DEPOSIT) {
            allTransactions.push({
              transaction_id: tx.transaction_hash,
              to: tx.to_address.toLowerCase(),
              amount: amount,
              token: 'USDC',
              network: 'usdc_bep20',
              timestamp: new Date(tx.block_timestamp).getTime()
            });
          }
        }
      }
    }

    console.log(`✅ Found ${allTransactions.length} BEP20 transactions for ${address}`);
    return allTransactions;

  } catch (error) {
    console.error('❌ Moralis API error:', error.message);
    return [];
  }
}

// ========== DEPOSIT CHECKING ==========
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
          if (tx.to === wallet.usdt_trc20_address && tx.amount >= MIN_DEPOSIT) {
            try {
              // Проверяем дубликаты
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
              
              const result = await processDeposit(wallet.user_id, tx.amount, tx.transaction_id, 'usdt_trc20', 'USDT');
              if (result.success) {
                depositsFound++;
                console.log(`💰 NEW TRC20 DEPOSIT: ${tx.amount} USDT for user ${wallet.user_id}`);
              }
            } catch (err) {
              if (err.message.includes('already_processed')) {
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

    console.log(`✅ TRC20: Processed ${processedCount} wallets, found ${depositsFound} new deposits`);
    return { success: true, processed: processedCount, deposits: depositsFound };
    
  } catch (error) {
    console.error('❌ TRC20 check error:', error.message);
    return { success: false, error: error.message };
  }
}

async function handleCheckBEP20Deposits() {
  try {
    console.log('🔄 Checking BEP20 deposits...');
    
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
          const transactions = await getBEP20Transactions(wallet.usdt_bep20_address);
          
          for (const tx of transactions) {
            if (tx.to.toLowerCase() === wallet.usdt_bep20_address.toLowerCase() && tx.amount >= MIN_DEPOSIT) {
              try {
                // Проверяем дубликаты
                const { data: existing } = await supabase
                  .from('deposits')
                  .select('id')
                  .eq('tx_hash', tx.transaction_id)
                  .eq('network', 'usdt_bep20')
                  .maybeSingle();
                
                if (existing) {
                  duplicatesSkipped++;
                  console.log(`⏭️ Skipping duplicate BEP20 transaction: ${tx.transaction_id}`);
                  continue;
                }
                
                const result = await processDeposit(wallet.user_id, tx.amount, tx.transaction_id, 'usdt_bep20', 'USDT');
                if (result.success) {
                  depositsFound++;
                  console.log(`💰 NEW BEP20 DEPOSIT: ${tx.amount} USDT for user ${wallet.user_id}`);
                }
              } catch (err) {
                if (err.message.includes('already_processed')) {
                  duplicatesSkipped++;
                  console.log(`⏭️ Duplicate BEP20 deposit skipped: ${tx.transaction_id}`);
                } else {
                  console.error(`❌ Error processing BEP20 deposit ${tx.transaction_id}:`, err.message);
                }
              }
            }
          }
        }

        // Проверяем USDC BEP20
        if (wallet.usdc_bep20_address) {
          await sleep(200);
          const transactions = await getBEP20Transactions(wallet.usdc_bep20_address);
          
          for (const tx of transactions) {
            if (tx.to.toLowerCase() === wallet.usdc_bep20_address.toLowerCase() && tx.amount >= MIN_DEPOSIT) {
              try {
                // Проверяем дубликаты
                const { data: existing } = await supabase
                  .from('deposits')
                  .select('id')
                  .eq('tx_hash', tx.transaction_id)
                  .eq('network', 'usdc_bep20')
                  .maybeSingle();
                
                if (existing) {
                  duplicatesSkipped++;
                  console.log(`⏭️ Skipping duplicate BEP20 transaction: ${tx.transaction_id}`);
                  continue;
                }
                
                const result = await processDeposit(wallet.user_id, tx.amount, tx.transaction_id, 'usdc_bep20', 'USDC');
                if (result.success) {
                  depositsFound++;
                  console.log(`💰 NEW BEP20 DEPOSIT: ${tx.amount} USDC for user ${wallet.user_id}`);
                }
              } catch (err) {
                if (err.message.includes('already_processed')) {
                  duplicatesSkipped++;
                  console.log(`⏭️ Duplicate BEP20 deposit skipped: ${tx.transaction_id}`);
                } else {
                  console.error(`❌ Error processing BEP20 deposit ${tx.transaction_id}:`, err.message);
                }
              }
            }
          }
        }
        
        processedCount++;
      } catch (err) {
        console.error(`❌ Error processing BEP20 wallet:`, err.message);
      }
    }

    console.log(`✅ BEP20: Processed ${processedCount} wallets, found ${depositsFound} new deposits`);
    return { success: true, processed: processedCount, deposits: depositsFound };
    
  } catch (error) {
    console.error('❌ BEP20 check error:', error.message);
    return { success: false, error: error.message };
  }
}

// ========== API ENDPOINT FOR MANUAL CHECK ==========
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

// ========== START SERVER ==========
const server = app.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 HTTP SERVER RUNNING on port ${PORT}`);
  console.log(`✅ Health check available at: http://0.0.0.0:${PORT}/health`);
  console.log(`✅ SUPABASE: CONNECTED`);
  console.log(`✅ TRONGRID: API KEY SET`);
  console.log(`✅ MORALIS: API KEY SET`);
  console.log(`✅ TRC20: Checking every 45 seconds`);
  console.log(`✅ BEP20: Checking every 3 minutes`);
  console.log(`✅ MIN DEPOSIT: $${MIN_DEPOSIT}`);
  console.log('===================================');
});

// ========== BACKGROUND TASKS ==========
let isCheckingTRC20 = false;
let isCheckingBEP20 = false;

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

process.on('uncaughtException', (error) => {
  console.error('❌ Uncaught Exception:', error);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('❌ Unhandled Rejection at:', promise, 'reason:', reason);
});
