// server.js — FTP QUANT DEPOSIT SYSTEM (TRC20, BEP20 USDT & USDC)
const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const TronWeb = require('tronweb');
const ethers = require('ethers');
const crypto = require('crypto');

const app = express();
const PORT = process.env.PORT || 8080;

// ========== CONFIGURATION ==========
const SUPABASE_URL = 'https://fctwivbwjoslkejtjxhe.supabase.co';
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const TRONGRID_API_KEY = '8fa63ef4-f010-4ad2-a556-a7124563bafd';
const MORALIS_API_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJub25jZSI6IjI4NWZhOGE1LTBmYWItNDkxOC1hMmE3LTk0ZjFlNmJiMjI2ZSIsIm9yZ0lkIjoiNDgxOTA3IiwidXNlcklkIjoiNDk1NzgwIiwidHlwZUlkIjoiZmQzZGMwMjItM2VjOC00MjM5LWEzZjUtNTUxMTVjNGRhMjVjIiwidHlwZSI6IlBST0pFQ1QiLCJpYXQiOjE3NjM0MjIzNzksImV4cCI6NDkxOTE4MjM3OX0.BdOpy-5ApiiwAMM7TDRtY-knkLRGWUsHLf6B7y_nb5k';
const ENCRYPTION_KEY = process.env.ENCRYPTION_KEY;

// ========== БЕЗОПАСНОСТЬ: API KEY ПРОВЕРКА ==========
const API_SECRET_KEY = process.env.API_SECRET_KEY || "default-secret-key-change-me-123";

// ========== ПРОСТОЙ RATE LIMIT (без пакета) ==========
const rateLimitStore = new Map();

function simpleRateLimit(req, res, next) {
  const ip = req.ip || req.connection.remoteAddress;
  const now = Date.now();
  const windowMs = 15 * 60 * 1000; // 15 минут
  const max = 50; // максимум 50 запросов
  
  // Пропускаем health checks
  if (req.path === '/health' || req.path === '/api/health' || req.path === '/') {
    return next();
  }
  
  if (!rateLimitStore.has(ip)) {
    rateLimitStore.set(ip, []);
  }
  
  const requests = rateLimitStore.get(ip);
  
  // Удаляем старые запросы
  const validRequests = requests.filter(time => now - time < windowMs);
  rateLimitStore.set(ip, validRequests);
  
  if (validRequests.length >= max) {
    console.log(`🚫 Rate limit exceeded for IP: ${ip}`);
    return res.status(429).json({
      success: false,
      error: 'Too many requests, please try again later'
    });
  }
  
  validRequests.push(now);
  next();
}

// Применяем rate limit ко всем публичным endpoint
app.use(simpleRateLimit);

// Функция проверки API ключа
function requireApiKey(req, res, next) {
    const clientKey = req.headers['x-api-key'] || req.query.api_key;
    
    if (!clientKey) {
        console.error('🚨 BLOCKED: No API key provided', {
            ip: req.ip,
            path: req.path,
            user_id: req.query.user_id,
            timestamp: new Date().toISOString()
        });
        return res.status(401).json({
            success: false,
            error: 'API key required. Use x-api-key header or api_key query parameter.'
        });
    }
    
    if (clientKey !== API_SECRET_KEY) {
        console.error('🚨 BLOCKED: Invalid API key', {
            ip: req.ip,
            path: req.path,
            timestamp: new Date().toISOString()
        });
        return res.status(403).json({
            success: false,
            error: 'Invalid API key'
        });
    }
    
    console.log('✅ Authorized API access:', {
        ip: req.ip,
        path: req.path,
        user_id: req.query.user_id
    });
    next();
}

// ========== ФУНКЦИИ ШИФРОВАНИЯ/ДЕШИФРОВАНИЯ ==========
function encryptPrivateKey(text) {
    try {
        if (!text || !ENCRYPTION_KEY) return text;
        const iv = crypto.randomBytes(16);
        const cipher = crypto.createCipheriv('aes-256-gcm', 
            crypto.createHash('sha256').update(ENCRYPTION_KEY).digest(), 
            iv
        );
        let encrypted = cipher.update(text, 'utf8', 'hex');
        encrypted += cipher.final('hex');
        const authTag = cipher.getAuthTag();
        return iv.toString('hex') + ':' + encrypted + ':' + authTag.toString('hex');
    } catch (error) {
        console.error('❌ Encryption error:', error.message);
        return text;
    }
}

function decryptPrivateKey(encryptedText) {
    try {
        if (!encryptedText || !ENCRYPTION_KEY) return encryptedText;
        if (!encryptedText.includes(':')) return encryptedText;
        
        const parts = encryptedText.split(':');
        if (parts.length !== 3) return encryptedText;
        
        const iv = Buffer.from(parts[0], 'hex');
        const encrypted = parts[1];
        const authTag = Buffer.from(parts[2], 'hex');
        
        const decipher = crypto.createDecipheriv('aes-256-gcm',
            crypto.createHash('sha256').update(ENCRYPTION_KEY).digest(),
            iv
        );
        decipher.setAuthTag(authTag);
        
        let decrypted = decipher.update(encrypted, 'hex', 'utf8');
        decrypted += decipher.final('utf8');
        return decrypted;
    } catch (error) {
        console.error('❌ Decryption error:', error.message);
        return encryptedText;
    }
}

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
    message: 'FTP QUANT Deposit Processing System',
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    environment: process.env.NODE_ENV || 'development',
    encryption: ENCRYPTION_KEY ? 'ENABLED' : 'DISABLED'
  });
});

app.get('/health', (req, res) => {
  res.json({
    status: '✅ HEALTHY',
    service: 'FTP QUANT Deposit Processor',
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    encryption: ENCRYPTION_KEY ? 'AES-256-GCM' : 'NONE'
  });
});

app.get('/api/health', (req, res) => {
  res.json({
    status: '✅ API HEALTHY',
    timestamp: new Date().toISOString(),
    encryption: ENCRYPTION_KEY ? 'ACTIVE' : 'INACTIVE'
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
const USDT_BSC_CONTRACT = '0x55d398326f99059fF775485246999027B3197955';
const USDC_BSC_CONTRACT = '0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d';
const MIN_DEPOSIT = 1;

// ========== NETWORK FIELD MAPPING ==========
const networkFields = {
  usdt_trc20: { 
    addressField: 'usdt_trc20_address', 
    privateKeyField: 'usdt_trc20_private_key',
    contractAddress: USDT_TRC20_CONTRACT
  },
  usdt_bep20: { 
    addressField: 'usdt_bep20_address', 
    privateKeyField: 'usdt_bep20_private_key',
    contractAddress: USDT_BSC_CONTRACT
  },
  usdc_bep20: { 
    addressField: 'usdc_bep20_address', 
    privateKeyField: 'usdc_bep20_private_key',
    contractAddress: USDC_BSC_CONTRACT
  }
};

// ========== OPTIMIZED SETTINGS ==========
const TRC20_CHECK_INTERVAL = 45000;
const BEP20_CHECK_INTERVAL = 120000;
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

// ========== ОБЩАЯ ФУНКЦИЯ ДЛЯ ГЕНЕРАЦИИ КОШЕЛЬКА ==========
async function generateWallet(user_id, network) {
  try {
    console.log(`🔐 Generating ${network} wallet for user: ${user_id}`);

    const fields = networkFields[network];
    if (!fields) {
      throw new Error('Unsupported network');
    }

    const { addressField, privateKeyField } = fields;

    // Проверяем существующий кошелек
    const { data: existingWallet, error: walletError } = await supabase
      .from('user_wallets')
      .select('*')
      .eq('user_id', user_id)
      .single();

    let address, privateKey;
    
    if (existingWallet && existingWallet[addressField]) {
      console.log(`✅ Wallet already exists: ${existingWallet[addressField]}`);
      
      const { data: pkData } = await supabase
        .from('private_keys')
        .select('private_key')
        .eq('user_id', user_id)
        .eq('network', network)
        .maybeSingle();

      let decryptedPrivateKey = null;
      if (pkData?.private_key) {
        decryptedPrivateKey = decryptPrivateKey(pkData.private_key);
      }

      return { 
        success: true, 
        address: existingWallet[addressField], 
        private_key: decryptedPrivateKey || null,
        exists: true, 
        network 
      };
    }

    // Генерируем новый кошелек
    if (network === 'usdt_trc20') {
      const wallet = await generateTRC20Wallet();
      address = wallet.address;
      privateKey = wallet.privateKey;
    } else {
      const wallet = await generateBEP20Wallet();
      address = wallet.address;
      privateKey = wallet.privateKey;
    }

    console.log(`✅ Generated ${network} wallet: ${address}`);

    // ШИФРУЕМ ПРИВАТНЫЙ КЛЮЧ
    const encryptedPrivateKey = encryptPrivateKey(privateKey);

    // Сохраняем в базу данных
    const walletData = {
      [addressField]: address,
      updated_at: new Date().toISOString()
    };

    if (existingWallet) {
      const { error } = await supabase
        .from('user_wallets')
        .update(walletData)
        .eq('user_id', user_id);

      if (error) {
        console.error('❌ Database update error:', error);
        throw new Error('Failed to update wallet');
      }
    } else {
      walletData.user_id = user_id;
      walletData.default_network = network;
      walletData.created_at = new Date().toISOString();
      
      const { error } = await supabase
        .from('user_wallets')
        .insert(walletData);

      if (error) {
        console.error('❌ Database insert error:', error);
        throw new Error('Failed to save wallet');
      }
    }

    // СОХРАНЕНИЕ ЗАШИФРОВАННОГО ПРИВАТНОГО КЛЮЧА
    const { error: pkError } = await supabase
      .from('private_keys')
      .upsert({
        user_id: user_id,
        network: network,
        address: address,
        private_key: encryptedPrivateKey,
        updated_at: new Date().toISOString()
      }, {
        onConflict: 'user_id,network'
      });

    if (pkError) {
      console.error('❌ Error saving encrypted private key:', pkError);
      throw new Error('Failed to save private key');
    }

    console.log(`✅ ${network} wallet saved to database`);

    // Запускаем проверку депозитов через 10 секунд
    setTimeout(() => {
      if (network === 'usdt_trc20') {
        checkUserTRC20Deposits(user_id);
      } else {
        checkUserBEP20Deposits(user_id);
      }
    }, 10000);

    return { 
      success: true, 
      address: address, 
      // ⚠️ ВАЖНО: Возвращаем приватный ключ только для API, не для публичного!
      private_key: privateKey,
      encrypted_stored: !!ENCRYPTION_KEY,
      exists: false, 
      network: network 
    };
    
  } catch (error) {
    console.error('❌ Generate wallet error:', error.message);
    throw error;
  }
}

// ========== АТОМАРНАЯ ФУНКЦИЯ ДЛЯ ДЕПОЗИТОВ ==========
async function processDeposit(userId, amount, txid, network) {
  try {
    console.log(`💰 ATOMIC DEPOSIT PROCESSING: $${amount} for user ${userId}, tx: ${txid}, network: ${network}`);

    if (amount < MIN_DEPOSIT) {
      console.log(`⏭️ Deposit too small: $${amount}, minimum: $${MIN_DEPOSIT}`);
      return { success: false, error: `Minimum deposit is $${MIN_DEPOSIT}` };
    }

    const { data: existingDeposit, error: checkError } = await supabase
      .from('deposits')
      .select('id, status, amount, user_id')
      .eq('tx_hash', txid)
      .eq('network', network)
      .maybeSingle();

    if (checkError) {
      console.error('❌ Error checking existing deposit:', checkError);
      throw checkError;
    }

    if (existingDeposit) {
      console.log(`⏭️ Deposit already exists: #${existingDeposit.id}, status: ${existingDeposit.status}`);
      
      if (existingDeposit.status === 'completed') {
        return { 
          success: true, 
          already_processed: true,
          deposit_id: existingDeposit.id,
          message: 'Deposit already processed'
        };
      }
      
      if (existingDeposit.status === 'pending') {
        console.log(`🔄 Processing existing pending deposit #${existingDeposit.id}`);
        const result = await processDepositAtomic(userId, amount, txid, network);
        if (result.success) {
          const { error: updateError } = await supabase
            .from('deposits')
            .update({
              status: 'completed',
              confirmed_at: new Date().toISOString(),
              completed_at: new Date().toISOString()
            })
            .eq('id', existingDeposit.id);
          
          if (updateError) {
            console.error('❌ Error updating deposit status:', updateError);
          }
        }
        return result;
      }
    }

    return await processDepositAtomic(userId, amount, txid, network);
    
  } catch (error) {
    console.error('❌ Error in processDeposit:', error.message);
    
    try {
      await supabase
        .from('system_logs')
        .insert({
          log_type: 'deposit_processing_error',
          message: `Deposit processing error: ${error.message}`,
          metadata: {
            user_id: userId,
            amount: amount,
            tx_hash: txid,
            network: network,
            error: error.message
          }
        });
    } catch (logErr) {
      console.error('❌ Error logging error:', logErr);
    }
    
    return { success: false, error: error.message };
  }
}

async function processDepositAtomic(userId, amount, txid, network) {
  try {
    console.log(`🚀 Processing deposit atomically for user ${userId}, $${amount}`);
    
    const { data: result, error } = await supabase.rpc('create_deposit_with_balance', {
      p_user_id: userId,
      p_amount: amount,
      p_network: network,
      p_tx_hash: txid
    });

    if (error) {
      console.error('❌ Atomic deposit RPC error:', error);
      
      if (error.message && error.message.includes('duplicate')) {
        console.log(`⏭️ Duplicate detected by RPC: ${txid}`);
        
        const { data: existingDeposit } = await supabase
          .from('deposits')
          .select('*')
          .eq('tx_hash', txid)
          .eq('network', network)
          .maybeSingle();
        
        if (existingDeposit && existingDeposit.status === 'completed') {
          return { 
            success: true, 
            already_processed: true,
            deposit_id: existingDeposit.id
          };
        }
      }
      
      throw error;
    }

    if (!result || !result.success) {
      console.error('❌ Atomic deposit failed:', result?.error);
      throw new Error(result?.error || 'Deposit processing failed');
    }

    console.log(`✅ ATOMIC DEPOSIT SUCCESS: #${result.deposit_id}, new balance: $${result.new_balance}`);
    
    await supabase
      .from('system_logs')
      .insert({
        log_type: 'deposit_atomic_success',
        message: `Atomic deposit successful for user ${userId}`,
        metadata: {
          deposit_id: result.deposit_id,
          user_id: userId,
          amount: amount,
          old_balance: result.old_balance,
          new_balance: result.new_balance,
          tx_hash: txid
        }
      });

    return {
      success: true,
      deposit_id: result.deposit_id,
      old_balance: result.old_balance,
      new_balance: result.new_balance,
      amount: amount
    };
    
  } catch (error) {
    console.error('❌ Atomic deposit error:', error.message);
    throw error;
  }
}

// ========== API Endpoints ==========

// 1. Защищенный endpoint (требует API ключа)
app.post('/api/deposit/generate', requireApiKey, async (req, res) => {
  try {
    const { user_id, network = 'usdt_trc20' } = req.query;
    if (!user_id) {
      return res.status(400).json({ 
        success: false, 
        error: 'User ID is required' 
      });
    }

    console.log(`🔐 [SECURE] Generating ${network} wallet for user: ${user_id}, IP: ${req.ip}`);

    const result = await generateWallet(user_id, network);
    res.json(result);
    
  } catch (error) {
    console.error('❌ API Generate wallet error:', error.message);
    res.status(500).json({ 
      success: false, 
      error: 'Internal server error' 
    });
  }
});

// 2. Публичный endpoint (без API ключа)
app.post('/public/deposit/generate', async (req, res) => {
  try {
    console.log('🔓 [PUBLIC] Deposit generation request:', {
      user_id: req.body.user_id,
      network: req.body.network,
      ip: req.ip,
      timestamp: new Date().toISOString()
    });

    const { user_id, network = 'usdt_trc20' } = req.body;
    
    if (!user_id) {
      console.log('❌ [PUBLIC] Missing user_id');
      return res.status(400).json({ 
        success: false, 
        error: 'User ID is required' 
      });
    }

    const allowedNetworks = ['usdt_trc20', 'usdt_bep20', 'usdc_bep20'];
    if (!allowedNetworks.includes(network)) {
      console.log('❌ [PUBLIC] Unsupported network:', network);
      return res.status(400).json({ 
        success: false, 
        error: 'Unsupported network' 
      });
    }

    // Проверяем существование пользователя в БД
    const { data: user } = await supabase
      .from('users')
      .select('id')
      .eq('id', user_id)
      .single();
      
    if (!user) {
      console.log('❌ [PUBLIC] User not found:', user_id);
      return res.status(404).json({ 
        success: false, 
        error: 'User not found' 
      });
    }

    console.log(`✅ [PUBLIC] User verified: ${user_id}`);

    // Используем ту же функцию generateWallet
    const result = await generateWallet(user_id, network);
    
    // ⚠️ ВАЖНО: Убираем приватный ключ из ответа для публичного endpoint
    const publicResult = {
      success: true,
      address: result.address,
      network: result.network,
      exists: result.exists
      // НЕ включаем private_key!
    };
    
    // Логируем
    await supabase
      .from('system_logs')
      .insert({
        log_type: 'public_deposit_generated',
        message: `Public deposit address generated for user ${user_id}`,
        metadata: {
          user_id: user_id,
          network: network,
          address: result.address,
          ip: req.ip
        }
      });

    res.json(publicResult);
    
  } catch (error) {
    console.error('❌ [PUBLIC] Error:', error.message);
    
    await supabase
      .from('system_logs')
      .insert({
        log_type: 'public_deposit_error',
        message: `Public deposit error: ${error.message}`,
        metadata: {
          error: error.message,
          ip: req.ip,
          body: req.body
        }
      });
    
    res.status(500).json({ 
      success: false, 
      error: 'Internal server error' 
    });
  }
});

app.get('/api/deposit/history', requireApiKey, async (req, res) => {
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
app.get('/api/check-deposits', requireApiKey, async (req, res) => {
  try {
    console.log('🔄 [SECURE] Manual deposit check triggered via API');
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

// ========== TRC20 TRANSACTIONS ==========
async function getTRC20Transactions(address) {
  try {
    if (!address) return [];
    
    const response = await fetch(`https://api.trongrid.io/v1/accounts/${address}/transactions/trc20?limit=10&only_confirmed=true`, {
      headers: { 'TRON-PRO-API-KEY': TRONGRID_API_KEY }
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
async function getBEP20Transactions(address) {
  try {
    console.log(`🔍 Checking BEP20 via Moralis: ${address}`);
    
    const response = await fetch(
      `https://deep-index.moralis.io/api/v2/${address}/erc20/transfers?chain=bsc&token_addresses=${USDT_BSC_CONTRACT},${USDC_BSC_CONTRACT}&limit=10`,
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
        if (tx.to_address.toLowerCase() === address.toLowerCase() && (tx.token_symbol === 'USDT' || tx.token_symbol === 'USDC')) {
          const amount = Number(tx.value) / Math.pow(10, tx.decimals || 18);
          
          if (amount >= MIN_DEPOSIT) {
            const network = tx.token_symbol === 'USDT' ? 'usdt_bep20' : 'usdc_bep20';
            
            transactions.push({
              transaction_id: tx.transaction_hash,
              to: tx.to_address.toLowerCase(),
              from: tx.from_address.toLowerCase(),
              amount: amount,
              token: tx.token_symbol,
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

    console.log(`✅ Found ${transactions.length} BEP20 transactions for ${address}`);
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
      .select('*')
      .not('usdt_trc20_address', 'is', null)
      .limit(100);

    if (error) throw error;

    let processedCount = 0;
    let depositsFound = 0;
    let duplicatesSkipped = 0;
    let errors = 0;

    for (const wallet of wallets || []) {
      try {
        await sleep(TRC20_DELAY_MS);
        
        const transactions = await getTRC20Transactions(wallet.usdt_trc20_address);
        
        for (const tx of transactions) {
          if (tx.to === wallet.usdt_trc20_address && tx.token === 'USDT' && tx.amount >= MIN_DEPOSIT) {
            try {
              const { data: existing } = await supabase
                .from('deposits')
                .select('id, status')
                .eq('tx_hash', tx.transaction_id)
                .eq('network', 'usdt_trc20')
                .maybeSingle();
              
              if (existing && existing.status === 'completed') {
                duplicatesSkipped++;
                console.log(`⏭️ Skipping duplicate TRC20 transaction: ${tx.transaction_id}`);
                continue;
              }
              
              const result = await processDeposit(wallet.user_id, tx.amount, tx.transaction_id, 'usdt_trc20');
              if (result.success) {
                depositsFound++;
                console.log(`💰 NEW TRC20 DEPOSIT: $${tx.amount} for user ${wallet.user_id}`);
              }
            } catch (err) {
              if (err.message && (err.message.includes('already_processed') || err.message.includes('duplicate'))) {
                duplicatesSkipped++;
                console.log(`⏭️ Duplicate TRC20 deposit skipped: ${tx.transaction_id}`);
              } else {
                console.error(`❌ Error processing TRC20 deposit ${tx.transaction_id}:`, err.message);
                errors++;
              }
            }
          }
        }
        processedCount++;
      } catch (err) {
        console.error(`❌ Error processing TRC20 wallet ${wallet.usdt_trc20_address}:`, err.message);
        errors++;
      }
    }

    console.log(`✅ TRC20: Processed ${processedCount} wallets, found ${depositsFound} new deposits, skipped ${duplicatesSkipped} duplicates, errors: ${errors}`);
    return { 
      success: true, 
      processed: processedCount, 
      deposits: depositsFound, 
      duplicates: duplicatesSkipped,
      errors: errors
    };
    
  } catch (error) {
    console.error('❌ TRC20 check error:', error.message);
    return { success: false, error: error.message };
  }
}

// ========== OPTIMIZED BEP20 CHECKING ==========
async function handleCheckBEP20Deposits() {
  try {
    console.log('🔄 Checking BEP20 deposits...');
    
    const { data: wallets, error } = await supabase
      .from('user_wallets')
      .select('*')
      .or('usdt_bep20_address.not.is.null,usdc_bep20_address.not.is.null')
      .limit(100);

    if (error) throw error;

    let processedCount = 0;
    let depositsFound = 0;
    let duplicatesSkipped = 0;
    let errors = 0;

    for (const wallet of wallets || []) {
      try {
        const addresses = [];
        if (wallet.usdt_bep20_address) addresses.push({ address: wallet.usdt_bep20_address, network: 'usdt_bep20' });
        if (wallet.usdc_bep20_address) addresses.push({ address: wallet.usdc_bep20_address, network: 'usdc_bep20' });

        for (const addr of addresses) {
          await sleep(BEP20_DELAY_MS);
          
          const transactions = await getBEP20Transactions(addr.address);
          
          for (const tx of transactions) {
            if (tx.to.toLowerCase() === addr.address.toLowerCase() && tx.network === addr.network && tx.amount >= MIN_DEPOSIT) {
              try {
                const { data: existing } = await supabase
                  .from('deposits')
                  .select('id, status')
                  .eq('tx_hash', tx.transaction_id)
                  .eq('network', addr.network)
                  .maybeSingle();
                
                if (existing && existing.status === 'completed') {
                  duplicatesSkipped++;
                  console.log(`⏭️ Skipping duplicate ${addr.network} transaction: ${tx.transaction_id}`);
                  continue;
                }
                
                const result = await processDeposit(wallet.user_id, tx.amount, tx.transaction_id, addr.network);
                if (result.success) {
                  depositsFound++;
                  console.log(`💰 NEW ${addr.network} DEPOSIT: $${tx.amount} ${tx.token} for user ${wallet.user_id}`);
                }
              } catch (err) {
                if (err.message && (err.message.includes('already_processed') || err.message.includes('duplicate'))) {
                  duplicatesSkipped++;
                  console.log(`⏭️ Duplicate ${addr.network} deposit skipped: ${tx.transaction_id}`);
                } else {
                  console.error(`❌ Error processing ${addr.network} deposit ${tx.transaction_id}:`, err.message);
                  errors++;
                }
              }
            }
          }
        }
        processedCount++;
      } catch (err) {
        console.error(`❌ Error processing BEP20 wallet ${wallet.user_id}:`, err.message);
        errors++;
      }
    }

    console.log(`✅ BEP20: Processed ${processedCount} wallets, found ${depositsFound} new deposits, skipped ${duplicatesSkipped} duplicates, errors: ${errors}`);
    return { 
      success: true, 
      processed: processedCount, 
      deposits: depositsFound, 
      duplicates: duplicatesSkipped,
      errors: errors
    };
    
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
      .select('*')
      .eq('user_id', userId)
      .single();
    
    if (!wallet || !wallet.usdt_trc20_address) return;
    
    const transactions = await getTRC20Transactions(wallet.usdt_trc20_address);
    
    for (const tx of transactions) {
      if (tx.to === wallet.usdt_trc20_address && tx.token === 'USDT' && tx.amount >= MIN_DEPOSIT) {
        try {
          await processDeposit(userId, tx.amount, tx.transaction_id, 'usdt_trc20');
        } catch (err) {
          console.error(`❌ Error processing transaction ${tx.transaction_id}:`, err.message);
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
      .select('*')
      .eq('user_id', userId)
      .single();
    
    if (!wallet) return;
    
    const addresses = [];
    if (wallet.usdt_bep20_address) addresses.push({ address: wallet.usdt_bep20_address, network: 'usdt_bep20' });
    if (wallet.usdc_bep20_address) addresses.push({ address: wallet.usdc_bep20_address, network: 'usdc_bep20' });

    for (const addr of addresses) {
      const transactions = await getBEP20Transactions(addr.address);
      
      for (const tx of transactions) {
        if (tx.to.toLowerCase() === addr.address.toLowerCase() && tx.network === addr.network && tx.amount >= MIN_DEPOSIT) {
          try {
            await processDeposit(userId, tx.amount, tx.transaction_id, addr.network);
          } catch (err) {
            console.error(`❌ Error processing transaction ${tx.transaction_id}:`, err.message);
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
  console.log(`🚀 HTTP SERVER RUNNING on port ${PORT}`);
  console.log(`✅ Health check available at: http://0.0.0.0:${PORT}/health`);
  console.log(`✅ API Health check: http://0.0.0.0:${PORT}/api/health`);
  console.log(`✅ PUBLIC Endpoint: POST http://0.0.0.0:${PORT}/public/deposit/generate`);
  console.log(`✅ SECURE Endpoint: POST http://0.0.0.0:${PORT}/api/deposit/generate (requires API key)`);
  console.log(`✅ RATE LIMIT: 50 requests per 15 minutes per IP`);
  console.log(`✅ SUPABASE: CONNECTED`);
  console.log(`✅ TRONGRID: API KEY SET`);
  console.log(`✅ MORALIS: API KEY SET`);
  console.log(`✅ TRC20 (USDT): Checking every 45 seconds`);
  console.log(`✅ BEP20 (USDT & USDC): Checking every 3 minutes`);
  console.log(`✅ MINIMUM DEPOSIT: $${MIN_DEPOSIT} USDT`);
  console.log(`✅ PRIVATE KEY ENCRYPTION: ${ENCRYPTION_KEY ? 'AES-256-GCM ENABLED' : 'DISABLED'}`);
  console.log(`✅ ATOMIC DEPOSITS: ENABLED`);
  console.log(`✅ SECURITY: Public endpoints DO NOT return private keys`);
  
  if (!ENCRYPTION_KEY) {
    console.warn(`⚠️  WARNING: ENCRYPTION_KEY not set! Private keys stored in plain text!`);
  }
  
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

process.on('SIGTERM', () => {
  console.log('🛑 Received SIGTERM, shutting down gracefully');
  server.close(() => {
    console.log('✅ Server closed');
    process.exit(0);
  });
});

process.on('uncaughtException', (error) => {
  console.error('❌ Uncaught Exception:', error);
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('❌ Unhandled Rejection at:', promise, 'reason:', reason);
});
