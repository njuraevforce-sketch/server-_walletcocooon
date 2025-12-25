// server.js — FTP QUANT DEPOSIT PROCESSOR
const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const TronWeb = require('tronweb');
const QRCode = require('qrcode');
const { ethers } = require('ethers');

const app = express();
const PORT = process.env.PORT || 8080;

// ========== CONFIGURATION ==========
const SUPABASE_URL = process.env.SUPABASE_URL || 'https://fctwivbwjoslkejtjxhe.supabase.co';
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZjdHdpdmJ3am9zbGtlanRqeGhlIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjYxNDMwMzUsImV4cCI6MjA4MTcxOTAzNX0.DiAzcqkigZPueh40idz2fIoJ-o-sKDMaRleOagH__B0';
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

const NETWORK_TYPES = {
  'usdt_trc20': 'trc20',
  'usdt_bep20': 'bep20',
  'usdc_bep20': 'bep20'
};

const MIN_DEPOSIT = 10; // Минимальный депозит 10 USD

// ========== OPTIMIZED SETTINGS ==========
const TRC20_CHECK_INTERVAL = 30000; // 30 секунд
const BEP20_CHECK_INTERVAL = 60000; // 1 минута
const BEP20_DELAY_MS = 1000; // 1 запрос/секунду
const TRC20_DELAY_MS = 500; // 2 запроса/секунду

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

// ========== DATABASE FUNCTIONS ==========
async function getUserWallet(user_id, network) {
  try {
    const { data, error } = await supabase
      .from('user_wallets')
      .select('*')
      .eq('user_id', user_id)
      .single();

    if (error) {
      if (error.code === 'PGRST116') {
        // Кошелек не найден, создаем новый
        return await createUserWallet(user_id);
      }
      throw error;
    }

    return data;
  } catch (error) {
    console.error('Error getting user wallet:', error);
    throw error;
  }
}

async function createUserWallet(user_id) {
  try {
    const { data, error } = await supabase
      .from('user_wallets')
      .insert({
        user_id: user_id,
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString()
      })
      .select()
      .single();

    if (error) throw error;
    return data;
  } catch (error) {
    console.error('Error creating user wallet:', error);
    throw error;
  }
}

async function updateWalletAddress(user_id, network, address, privateKey) {
  try {
    const fieldMap = {
      'usdt_trc20': { address: 'usdt_trc20_address', privateKey: 'usdt_trc20_private_key' },
      'usdt_bep20': { address: 'usdt_bep20_address', privateKey: 'usdt_bep20_private_key' },
      'usdc_bep20': { address: 'usdc_bep20_address', privateKey: 'usdc_bep20_private_key' }
    };

    const fields = fieldMap[network];
    if (!fields) throw new Error('Invalid network');

    const updateData = {
      [fields.address]: address,
      updated_at: new Date().toISOString()
    };

    if (privateKey) {
      updateData[fields.privateKey] = privateKey;
    }

    const { data, error } = await supabase
      .from('user_wallets')
      .update(updateData)
      .eq('user_id', user_id)
      .select()
      .single();

    if (error) throw error;
    return data;
  } catch (error) {
    console.error('Error updating wallet address:', error);
    throw error;
  }
}

// ========== DEPOSIT PROCESSING ==========
async function processDeposit(user_id, amount, txid, network) {
  try {
    console.log(`💰 PROCESSING DEPOSIT: ${amount} USD for user ${user_id}, txid: ${txid}, network: ${network}`);

    // Проверка дубликатов
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
      console.log(`✅ Deposit already processed: ${txid}, status: ${existingDeposit.status}`);
      return { success: false, reason: 'already_processed', existing: existingDeposit };
    }

    // Создаем запись о депозите
    const { data: deposit, error: depositError } = await supabase
      .from('deposits')
      .insert({
        user_id: user_id,
        amount: amount,
        network: network,
        tx_hash: txid,
        status: 'completed',
        completed_at: new Date().toISOString(),
        created_at: new Date().toISOString()
      })
      .select()
      .single();

    if (depositError) {
      console.error('Error creating deposit:', depositError);
      throw depositError;
    }

    // Обновляем баланс пользователя
    const { error: balanceError } = await supabase.rpc('increment_balance', {
      user_id: user_id,
      amount: amount
    });

    if (balanceError) {
      console.error('Error updating balance:', balanceError);
      throw balanceError;
    }

    // Создаем транзакцию в истории
    const { error: transactionError } = await supabase
      .from('transactions')
      .insert({
        user_id: user_id,
        type: 'deposit',
        amount: amount,
        description: `Deposit ${amount} USD via ${network}`,
        status: 'completed',
        created_at: new Date().toISOString(),
        metadata: { deposit_id: deposit.id, tx_hash: txid, network: network }
      });

    if (transactionError) {
      console.error('Error creating transaction:', transactionError);
      throw transactionError;
    }

    // Обновляем total_deposit пользователя
    const { error: updateError } = await supabase
      .from('users')
      .update({ 
        total_deposit: supabase.sql`total_deposit + ${amount}`,
        updated_at: new Date().toISOString()
      })
      .eq('id', user_id);

    if (updateError) {
      console.error('Error updating total deposit:', updateError);
    }

    console.log(`✅ DEPOSIT PROCESSED: ${amount} USD for user ${user_id}`);
    return { 
      success: true, 
      amount, 
      deposit_id: deposit.id,
      user_id: user_id
    };
    
  } catch (error) {
    console.error('❌ Error in processDeposit:', error.message);
    throw error;
  }
}

// ========== API Endpoints ==========
app.post('/api/deposit/generate', async (req, res) => {
  try {
    const { user_id, network = 'usdt_trc20' } = req.body;
    if (!user_id) return res.status(400).json({ success: false, error: 'User ID is required' });

    console.log(`🔐 Generating ${network} address for user: ${user_id}`);

    // Получаем или создаем кошелек пользователя
    let wallet = await getUserWallet(user_id, network);
    
    // Проверяем существующий адрес
    const fieldMap = {
      'usdt_trc20': 'usdt_trc20_address',
      'usdt_bep20': 'usdt_bep20_address',
      'usdc_bep20': 'usdc_bep20_address'
    };

    const addressField = fieldMap[network];
    if (!addressField) {
      return res.status(400).json({ success: false, error: 'Unsupported network' });
    }

    let address = wallet[addressField];
    let private_key = null;
    let isNew = false;

    if (!address) {
      // Генерируем новый адрес
      isNew = true;
      let walletData;
      
      if (network === 'usdt_trc20') {
        walletData = await generateTRC20Wallet();
      } else if (network === 'usdt_bep20' || network === 'usdc_bep20') {
        walletData = await generateBEP20Wallet();
      } else {
        return res.status(400).json({ success: false, error: 'Unsupported network' });
      }

      address = walletData.address;
      private_key = walletData.privateKey;

      // Сохраняем в базу данных
      await updateWalletAddress(user_id, network, address, private_key);
      
      console.log(`✅ New ${network} address created: ${address}`);
    } else {
      console.log(`✅ Using existing ${network} address: ${address}`);
    }

    // Генерируем QR-код
    let qrCode = '';
    try {
      qrCode = await QRCode.toDataURL(address, {
        width: 300,
        height: 300,
        margin: 1,
        color: {
          dark: '#000000',
          light: '#FFFFFF'
        }
      });
    } catch (qrError) {
      console.error('QR code generation error:', qrError);
    }

    res.json({ 
      success: true, 
      address, 
      private_key,
      qr_code: qrCode,
      is_new: isNew, 
      network 
    });
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

app.get('/api/qrcode', async (req, res) => {
  try {
    const { text } = req.query;
    if (!text) return res.status(400).json({ success: false, error: 'Text is required' });

    const qrCode = await QRCode.toDataURL(text, {
      width: 300,
      height: 300,
      margin: 1,
      color: {
        dark: '#000000',
        light: '#FFFFFF'
      }
    });

    const base64Data = qrCode.replace(/^data:image\/png;base64,/, '');
    const imgBuffer = Buffer.from(base64Data, 'base64');

    res.writeHead(200, {
      'Content-Type': 'image/png',
      'Content-Length': imgBuffer.length
    });
    res.end(imgBuffer);
  } catch (error) {
    console.error('❌ QR code error:', error.message);
    res.status(500).json({ success: false, error: 'Failed to generate QR code' });
  }
});

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
    const contract = CONTRACTS[network];
    if (!contract) return [];
    
    const response = await fetch(
      `https://deep-index.moralis.io/api/v2/${address}/erc20/transfers?chain=bsc&token_addresses=${contract}&limit=20`,
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
            const token = network === 'usdc_bep20' ? 'USDC' : 'USDT';
            transactions.push({
              transaction_id: tx.transaction_hash,
              to: tx.to_address.toLowerCase(),
              from: tx.from_address.toLowerCase(),
              amount: amount,
              token: token,
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

    return transactions;

  } catch (error) {
    console.error('❌ Moralis API error:', error.message);
    return [];
  }
}

// ========== BACKGROUND DEPOSIT CHECKING ==========
async function checkAllDeposits() {
  try {
    console.log('🔄 Checking all deposits...');
    
    // Получаем всех пользователей с кошельками
    const { data: wallets, error } = await supabase
      .from('user_wallets')
      .select('*')
      .limit(500);

    if (error) throw error;

    let processed = { trc20: 0, bep20: 0 };
    let newDeposits = { trc20: 0, bep20: 0 };

    for (const wallet of wallets || []) {
      const user_id = wallet.user_id;
      
      // Проверяем TRC20
      if (wallet.usdt_trc20_address) {
        await sleep(TRC20_DELAY_MS);
        const transactions = await getTRC20Transactions(wallet.usdt_trc20_address);
        
        for (const tx of transactions) {
          if (tx.to === wallet.usdt_trc20_address) {
            try {
              const result = await processDeposit(user_id, tx.amount, tx.transaction_id, 'usdt_trc20');
              if (result.success) {
                newDeposits.trc20++;
                console.log(`💰 NEW TRC20 DEPOSIT: ${tx.amount} USDT for user ${user_id}`);
              }
            } catch (err) {
              if (err.message.includes('already_processed')) {
                continue;
              }
              console.error(`❌ Error processing TRC20 deposit:`, err.message);
            }
          }
        }
        processed.trc20++;
      }

      // Проверяем BEP20 (USDT)
      if (wallet.usdt_bep20_address) {
        await sleep(BEP20_DELAY_MS);
        const transactions = await getBEP20Transactions(wallet.usdt_bep20_address, 'usdt_bep20');
        
        for (const tx of transactions) {
          if (tx.to.toLowerCase() === wallet.usdt_bep20_address.toLowerCase()) {
            try {
              const result = await processDeposit(user_id, tx.amount, tx.transaction_id, 'usdt_bep20');
              if (result.success) {
                newDeposits.bep20++;
                console.log(`💰 NEW BEP20 DEPOSIT: ${tx.amount} USDT for user ${user_id}`);
              }
            } catch (err) {
              if (err.message.includes('already_processed')) {
                continue;
              }
              console.error(`❌ Error processing BEP20 deposit:`, err.message);
            }
          }
        }
        processed.bep20++;
      }

      // Проверяем BEP20 (USDC)
      if (wallet.usdc_bep20_address) {
        await sleep(BEP20_DELAY_MS);
        const transactions = await getBEP20Transactions(wallet.usdc_bep20_address, 'usdc_bep20');
        
        for (const tx of transactions) {
          if (tx.to.toLowerCase() === wallet.usdc_bep20_address.toLowerCase()) {
            try {
              const result = await processDeposit(user_id, tx.amount, tx.transaction_id, 'usdc_bep20');
              if (result.success) {
                newDeposits.bep20++;
                console.log(`💰 NEW BEP20 DEPOSIT: ${tx.amount} USDC for user ${user_id}`);
              }
            } catch (err) {
              if (err.message.includes('already_processed')) {
                continue;
              }
              console.error(`❌ Error processing BEP20 deposit:`, err.message);
            }
          }
        }
        processed.bep20++;
      }
    }

    console.log(`✅ Deposit check completed: TRC20(${processed.trc20}), BEP20(${processed.bep20}), New deposits: TRC20(${newDeposits.trc20}), BEP20(${newDeposits.bep20})`);
    
  } catch (error) {
    console.error('❌ Error checking deposits:', error.message);
  }
}

// ========== START SERVER ==========
const server = app.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 FTP QUANT DEPOSIT SERVER RUNNING on port ${PORT}`);
  console.log(`✅ Health check: http://0.0.0.0:${PORT}/health`);
  console.log(`✅ SUPABASE: CONNECTED`);
  console.log(`✅ MIN DEPOSIT: $${MIN_DEPOSIT}`);
  console.log('===================================');
});

// ========== BACKGROUND TASKS ==========
let isChecking = false;

// Запускаем проверку депозитов каждые 30 секунд
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
}, 30000);

// Запускаем первую проверку через 10 секунд после старта
setTimeout(() => {
  checkAllDeposits();
}, 10000);

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
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('❌ Unhandled Rejection at:', promise, 'reason:', reason);
});
