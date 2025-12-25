// server-deposit.js — OPTIMIZED FOR USDT/TRC20, USDT/BEP20, USDC/BEP20
const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const TronWeb = require('tronweb');
const QRCode = require('qrcode');

const app = express();
const PORT = process.env.PORT || 8080;

// ========== CONFIGURATION ==========
const SUPABASE_URL = 'https://fctwivbwjoslkejtjxhe.supabase.co';
const SUPABASE_SERVICE_ROLE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZjdHdpdmJ3am9zbGtlanRqeGhlIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2NjE0MzAzNSwiZXhwIjoyMDgxNzE5MDM1fQ.ri54X2tfq8sh_fCHlGHuQ82u03O3oajHpZ-JX1fBQ_Q';
const TRONGRID_API_KEY = '8fa63ef4-f010-4ad2-a556-a7124563bafd';
const MORALIS_API_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJub25jZSI6IjcxODVlYzdiLTQ3NzctNDFhNS05ZDI4LTE0YjFlZmJkZTA5NSIsIm9yZ0lkIjoiNDg1NjY3IiwidXNlcklkIjoiNDk5NjYxIiwidHlwZUlkIjoiNjdjYjQzMzgtMmY2OC00MmE3LThmMzItYmJiMDljMDkyM2NlIiwidHlwZSI6IlBST0pFQ1QiLCJpYXQiOjE3NjU0OTA0MDMsImV4cCI6NDkyMTI1MDQwM30.0Z_G2u-E8EdfZQzyUZFY4CVbgUqR2H5e4TjQzi9MnzU';

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
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    endpoints: {
      health: '/health',
      generateWallet: '/api/wallet/generate',
      depositHistory: '/api/deposit/history',
      checkDeposits: '/api/check-deposits'
    }
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
  'usdt_trc20': 'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t',
  'usdt_bep20': '0x55d398326f99059fF775485246999027B3197955',
  'usdc_bep20': '0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d'
};

const NETWORKS = {
  'usdt_trc20': { name: 'USDT (TRC20)', decimals: 6, scan: 'https://tronscan.org/#/transaction/' },
  'usdt_bep20': { name: 'USDT (BEP20)', decimals: 18, scan: 'https://bscscan.com/tx/' },
  'usdc_bep20': { name: 'USDC (BEP20)', decimals: 18, scan: 'https://bscscan.com/tx/' }
};

const MIN_DEPOSIT = 10;
const CHECK_INTERVALS = {
  'usdt_trc20': 45000, // 45 секунд
  'usdt_bep20': 90000, // 1.5 минуты
  'usdc_bep20': 90000  // 1.5 минуты
};

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

// ========== WALLET MANAGEMENT ==========
async function getOrCreateWallet(userId, network) {
  try {
    console.log(`🔍 Checking wallet for user ${userId}, network: ${network}`);
    
    // Проверяем существующий кошелек
    const { data: existingWallet, error: fetchError } = await supabase
      .from('user_wallets')
      .select('*')
      .eq('user_id', userId)
      .single();

    if (fetchError && fetchError.code !== 'PGRST116') {
      console.error('❌ Error fetching wallet:', fetchError);
      throw fetchError;
    }

    let address, qrCode;
    
    if (existingWallet) {
      // Проверяем есть ли адрес для этой сети
      const addressField = `${network.replace('_', '_')}_address`;
      address = existingWallet[addressField];
      
      if (!address) {
        // Генерируем новый адрес для сети
        address = await generateAddress(network);
        
        // Обновляем кошелек
        const updateData = {};
        updateData[addressField] = address;
        updateData.updated_at = new Date().toISOString();
        
        const { error: updateError } = await supabase
          .from('user_wallets')
          .update(updateData)
          .eq('user_id', userId);
          
        if (updateError) {
          console.error('❌ Error updating wallet:', updateError);
          throw updateError;
        }
      }
    } else {
      // Создаем новый кошелек
      address = await generateAddress(network);
      
      const walletData = {
        user_id: userId,
        default_network: network,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      };
      
      walletData[`${network.replace('_', '_')}_address`] = address;
      
      const { error: insertError } = await supabase
        .from('user_wallets')
        .insert(walletData);
        
      if (insertError) {
        console.error('❌ Error creating wallet:', insertError);
        throw insertError;
      }
    }

    // Генерируем QR-код
    qrCode = await QRCode.toDataURL(address);
    
    console.log(`✅ Wallet ready for user ${userId}: ${address.substring(0, 10)}...`);
    
    return { address, qrCode };
    
  } catch (error) {
    console.error('❌ Error in getOrCreateWallet:', error.message);
    throw error;
  }
}

async function generateAddress(network) {
  try {
    if (network === 'usdt_trc20') {
      const account = await tronWeb.createAccount();
      return account.address.base58;
    } else {
      // Для BEP20 используем статические адреса (можно заменить на генерацию)
      const staticAddresses = {
        'usdt_bep20': '0x742d35Cc6634C0532925a3b844Bc454e4438f44e',
        'usdc_bep20': '0x742d35Cc6634C0532925a3b844Bc454e4438f44e'
      };
      return staticAddresses[network] || '0x0000000000000000000000000000000000000000';
    }
  } catch (error) {
    console.error('❌ Error generating address:', error);
    throw error;
  }
}

// ========== DEPOSIT PROCESSING ==========
async function processDeposit(userId, amount, txHash, network) {
  try {
    console.log(`💰 PROCESSING DEPOSIT: ${amount} ${network} for user ${userId}, tx: ${txHash.substring(0, 20)}...`);

    // 1. Проверяем дубликат в deposits
    const { data: existingDeposit } = await supabase
      .from('deposits')
      .select('id, status')
      .eq('tx_hash', txHash)
      .eq('network', network)
      .maybeSingle();

    if (existingDeposit) {
      console.log(`⏭️ Deposit already processed: ${txHash}`);
      return { success: false, reason: 'already_processed', deposit_id: existingDeposit.id };
    }

    // 2. Получаем текущий баланс пользователя
    const { data: userData } = await supabase
      .from('users')
      .select('balance, total_deposit')
      .eq('id', userId)
      .single();

    if (!userData) {
      throw new Error('User not found');
    }

    // 3. Обновляем баланс пользователя
    const newBalance = parseFloat(userData.balance) + parseFloat(amount);
    const newTotalDeposit = parseFloat(userData.total_deposit) + parseFloat(amount);

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

    // 4. Создаем запись о депозите
    const { data: deposit, error: depositError } = await supabase
      .from('deposits')
      .insert({
        user_id: userId,
        amount: amount,
        network: network,
        tx_hash: txHash,
        status: 'completed',
        confirmed_at: new Date().toISOString(),
        completed_at: new Date().toISOString(),
        created_at: new Date().toISOString()
      })
      .select()
      .single();

    if (depositError) {
      console.error('❌ Error creating deposit record:', depositError);
      throw depositError;
    }

    // 5. Создаем транзакцию в истории
    const { error: transactionError } = await supabase
      .from('transactions')
      .insert({
        user_id: userId,
        type: 'deposit',
        amount: amount,
        description: `Deposit ${amount} ${NETWORKS[network].name}`,
        status: 'completed',
        metadata: {
          tx_hash: txHash,
          network: network,
          deposit_id: deposit.id
        },
        created_at: new Date().toISOString()
      });

    if (transactionError) {
      console.error('❌ Error creating transaction:', transactionError);
    }

    console.log(`✅ DEPOSIT COMPLETED: ${amount} ${network} for user ${userId}`);
    console.log(`💰 New balance: ${newBalance}`);
    console.log(`📝 Deposit ID: ${deposit.id}`);

    return {
      success: true,
      amount: amount,
      deposit_id: deposit.id,
      new_balance: newBalance,
      new_total_deposit: newTotalDeposit
    };

  } catch (error) {
    console.error('❌ Error in processDeposit:', error.message);
    throw error;
  }
}

// ========== TRANSACTION CHECKING ==========
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
        if (!tokenAddr || tokenAddr !== CONTRACTS.usdt_trc20) continue;

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
            contract: tokenAddr,
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

async function getBEP20Transactions(address, network) {
  try {
    console.log(`🔍 Checking ${network} via Moralis: ${address}`);
    
    const contract = CONTRACTS[network];
    const tokenSymbol = network.includes('usdc') ? 'USDC' : 'USDT';
    
    const response = await fetch(
      `https://deep-index.moralis.io/api/v2/${address}/erc20/transfers?chain=bsc&token_addresses=${contract}&limit=10`,
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
        if (tx.to_address.toLowerCase() === address.toLowerCase() && 
            tx.token_symbol === tokenSymbol) {
          const amount = Number(tx.value) / Math.pow(10, tx.decimals || 18);
          
          if (amount >= MIN_DEPOSIT) {
            transactions.push({
              transaction_id: tx.transaction_hash,
              to: tx.to_address.toLowerCase(),
              from: tx.from_address.toLowerCase(),
              amount: amount,
              token: tokenSymbol,
              contract: tx.token_address,
              confirmed: true,
              network: network,
              timestamp: new Date(tx.block_timestamp).getTime(),
              blockNumber: parseInt(tx.block_number)
            });
          }
        }
      } catch (e) {
        continue;
      }
    }

    console.log(`✅ Found ${transactions.length} ${network} transactions for ${address}`);
    return transactions;

  } catch (error) {
    console.error(`❌ Moralis API error for ${network}:`, error.message);
    return [];
  }
}

// ========== API ENDPOINTS ==========
app.post('/api/wallet/generate', async (req, res) => {
  try {
    const { user_id, network = 'usdt_trc20' } = req.query;
    
    if (!user_id) {
      return res.status(400).json({ success: false, error: 'User ID is required' });
    }

    if (!['usdt_trc20', 'usdt_bep20', 'usdc_bep20'].includes(network)) {
      return res.status(400).json({ success: false, error: 'Unsupported network' });
    }

    console.log(`🔐 Generating ${network} wallet for user: ${user_id}`);

    const { address, qrCode } = await getOrCreateWallet(user_id, network);

    res.json({
      success: true,
      network: network,
      network_name: NETWORKS[network].name,
      address: address,
      qr_code: qrCode,
      min_deposit: MIN_DEPOSIT,
      contract_address: CONTRACTS[network],
      scan_url: NETWORKS[network].scan
    });

  } catch (error) {
    console.error('❌ Generate wallet error:', error.message);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

app.get('/api/deposit/history', async (req, res) => {
  try {
    const { user_id, limit = 20 } = req.query;
    
    if (!user_id) {
      return res.status(400).json({ success: false, error: 'User ID is required' });
    }

    const { data: deposits, error } = await supabase
      .from('deposits')
      .select('*')
      .eq('user_id', user_id)
      .order('created_at', { ascending: false })
      .limit(parseInt(limit));

    if (error) {
      console.error('❌ Database error:', error);
      return res.status(500).json({ success: false, error: 'Failed to fetch deposit history' });
    }

    // Также получаем транзакции для полной истории
    const { data: transactions } = await supabase
      .from('transactions')
      .select('*')
      .eq('user_id', user_id)
      .eq('type', 'deposit')
      .order('created_at', { ascending: false })
      .limit(parseInt(limit));

    res.json({
      success: true,
      deposits: deposits || [],
      transactions: transactions || [],
      count: deposits?.length || 0
    });

  } catch (error) {
    console.error('❌ Deposit history error:', error.message);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

app.get('/api/check-deposits', async (req, res) => {
  try {
    console.log('🔄 Manual deposit check triggered via API');
    
    const results = {};
    
    // Проверяем все сети
    for (const network of ['usdt_trc20', 'usdt_bep20', 'usdc_bep20']) {
      try {
        results[network] = await checkNetworkDeposits(network);
      } catch (error) {
        results[network] = { success: false, error: error.message };
      }
    }

    res.json({
      success: true,
      timestamp: new Date().toISOString(),
      results: results
    });

  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// ========== BACKGROUND CHECKING ==========
async function checkNetworkDeposits(network) {
  try {
    console.log(`🔄 Checking ${network} deposits...`);
    
    // Получаем все кошельки для этой сети
    const { data: wallets, error } = await supabase
      .from('user_wallets')
      .select('user_id, usdt_trc20_address, usdt_bep20_address, usdc_bep20_address')
      .not(`${network.replace('_', '_')}_address`, 'is', null)
      .limit(100);

    if (error) throw error;

    let processedCount = 0;
    let depositsFound = 0;
    let duplicatesSkipped = 0;

    for (const wallet of wallets || []) {
      try {
        const addressField = `${network.replace('_', '_')}_address`;
        const address = wallet[addressField];
        
        if (!address) continue;

        await sleep(100); // Задержка между запросами

        // Получаем транзакции в зависимости от сети
        let transactions = [];
        if (network === 'usdt_trc20') {
          transactions = await getTRC20Transactions(address);
        } else {
          transactions = await getBEP20Transactions(address, network);
        }

        for (const tx of transactions) {
          // Проверяем что транзакция на наш адрес
          const checkAddress = network === 'usdt_trc20' ? address : address.toLowerCase();
          const txTo = network === 'usdt_trc20' ? tx.to : tx.to.toLowerCase();
          
          if (txTo === checkAddress && tx.amount >= MIN_DEPOSIT) {
            try {
              // Проверяем дубликат
              const { data: existing } = await supabase
                .from('deposits')
                .select('id')
                .eq('tx_hash', tx.transaction_id)
                .eq('network', network)
                .maybeSingle();
              
              if (existing) {
                duplicatesSkipped++;
                console.log(`⏭️ Skipping duplicate ${network} transaction: ${tx.transaction_id.substring(0, 20)}...`);
                continue;
              }
              
              // Обрабатываем депозит
              const result = await processDeposit(wallet.user_id, tx.amount, tx.transaction_id, network);
              if (result.success) {
                depositsFound++;
                console.log(`💰 NEW ${network} DEPOSIT: ${tx.amount} for user ${wallet.user_id}`);
              }
            } catch (err) {
              if (err.message.includes('already_processed')) {
                duplicatesSkipped++;
                console.log(`⏭️ Duplicate ${network} deposit skipped: ${tx.transaction_id.substring(0, 20)}...`);
              } else {
                console.error(`❌ Error processing ${network} deposit ${tx.transaction_id}:`, err.message);
              }
            }
          }
        }
        processedCount++;
      } catch (err) {
        console.error(`❌ Error processing ${network} wallet:`, err.message);
      }
    }

    console.log(`✅ ${network}: Processed ${processedCount} wallets, found ${depositsFound} new deposits, skipped ${duplicatesSkipped} duplicates`);
    
    return { 
      success: true, 
      processed: processedCount, 
      deposits: depositsFound, 
      duplicates: duplicatesSkipped 
    };
    
  } catch (error) {
    console.error(`❌ ${network} check error:`, error.message);
    return { success: false, error: error.message };
  }
}

// ========== START SERVER ==========
const server = app.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 FTP QUANT DEPOSIT SERVER RUNNING on port ${PORT}`);
  console.log(`✅ Health check: http://0.0.0.0:${PORT}/health`);
  console.log(`✅ SUPABASE: CONNECTED`);
  console.log(`✅ Networks: USDT/TRC20, USDT/BEP20, USDC/BEP20`);
  console.log(`✅ Min deposit: $${MIN_DEPOSIT}`);
  console.log('===================================');

  // Запускаем фоновую проверку при старте
  setTimeout(() => {
    checkAllDeposits();
  }, 10000);
});

// ========== BACKGROUND TASKS ==========
let isChecking = {
  'usdt_trc20': false,
  'usdt_bep20': false,
  'usdc_bep20': false
};

async function checkAllDeposits() {
  for (const network of ['usdt_trc20', 'usdt_bep20', 'usdc_bep20']) {
    if (isChecking[network]) continue;
    
    try {
      isChecking[network] = true;
      await checkNetworkDeposits(network);
    } catch (err) {
      console.error(`❌ ${network} auto-check error:`, err.message);
    } finally {
      isChecking[network] = false;
    }
  }
}

// Интервальная проверка
setInterval(checkAllDeposits, CHECK_INTERVALS.usdt_trc20);

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
