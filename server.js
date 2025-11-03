const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const TronWeb = require('tronweb');
const { ethers } = require('ethers');

const app = express();
const PORT = process.env.PORT || 3000;

// ========== ENVIRONMENT VARIABLES ==========
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const TRONGRID_API_KEY = process.env.TRONGRID_API_KEY;
const MORALIS_API_KEY = process.env.MORALIS_API_KEY;

// ========== BSC RPC CONFIGURATION ==========
const BSC_RPC_URLS = [
  'https://bsc-dataseed.binance.org/',
  'https://bsc-dataseed1.defibit.io/',
  'https://bsc-dataseed1.ninicoin.io/',
];

let currentRpcIndex = 0;
function getNextBscRpc() {
  const rpc = BSC_RPC_URLS[currentRpcIndex];
  currentRpcIndex = (currentRpcIndex + 1) % BSC_RPC_URLS.length;
  return rpc;
}

let bscProvider = new ethers.providers.JsonRpcProvider(getNextBscRpc());

// COMPANY wallets
const COMPANY = {
  MASTER: {
    address: 'TKn5J3ZnTxE9fmgMhVjXognH4VUjx4Tid2',
    privateKey: process.env.MASTER_PRIVATE_KEY
  },
  MAIN: {
    address: 'TNVpDk1JZSxmC9XniB1tSPaRdAvvKMMavC',
    privateKey: process.env.MAIN_PRIVATE_KEY
  }
};

const COMPANY_BSC = {
  MASTER: {
    address: '0x60F3159e6b935759d6b4994473eeeD1e3ad27408',
    privateKey: process.env.MASTER_BSC_PRIVATE_KEY
  },
  MAIN: {
    address: '0x01F28A131bdda7255EcBE800C3ebACBa2c7076c7',
    privateKey: process.env.MAIN_BSC_PRIVATE_KEY
  }
};

// Проверяем обязательные переменные
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.error('❌ MISSING REQUIRED ENV VARIABLES: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY are required');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

const tronWeb = new TronWeb({
  fullHost: 'https://api.trongrid.io',
  headers: { 'TRON-PRO-API-KEY': TRONGRID_API_KEY }
});

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
const CHECK_INTERVAL_MS = 2 * 60 * 1000; // 2 минуты

// ========== HELPERS ==========
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ========== MORALIS API FUNCTIONS ==========
async function moralisRequest(endpoint, retries = 3) {
  if (!MORALIS_API_KEY) {
    console.error('❌ MORALIS_API_KEY not configured');
    return { result: [] };
  }

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const response = await fetch(`https://deep-index.moralis.io/api/v2${endpoint}`, {
        headers: {
          'X-API-Key': MORALIS_API_KEY,
          'Accept': 'application/json'
        }
      });

      if (response.status === 429) {
        if (attempt < retries) {
          await sleep(2000 * Math.pow(2, attempt));
          continue;
        }
      }

      const data = await response.json();
      return response.ok ? data : { result: [] };
    } catch (error) {
      if (attempt === retries) throw error;
      await sleep(1000 * (attempt + 1));
    }
  }
  return { result: [] };
}

// ========== BSC FUNCTIONS ==========
async function getBSCTransactions(address) {
  try {
    if (!address) return [];

    const data = await moralisRequest(`/${address}/erc20/transfers?chain=bsc&limit=50`);
    
    if (data.result && Array.isArray(data.result)) {
      const transactions = [];
      for (const tx of data.result) {
        try {
          if (tx.address && tx.address.toLowerCase() === USDT_BSC_CONTRACT.toLowerCase() &&
              tx.to_address && tx.to_address.toLowerCase() === address.toLowerCase()) {
            
            const amount = Number(tx.value) / Math.pow(10, tx.decimals || 18);
            
            transactions.push({
              transaction_id: tx.transaction_hash,
              to: tx.to_address,
              from: tx.from_address,
              amount: amount,
              token: 'USDT',
              confirmed: true,
              network: 'BEP20',
              timestamp: new Date(tx.block_timestamp).getTime()
            });
          }
        } catch (e) { continue; }
      }
      
      transactions.sort((a, b) => b.timestamp - a.timestamp);
      return transactions;
    }
    return [];
  } catch (error) {
    console.error('❌ BSC transactions error:', error.message);
    return [];
  }
}

// ========== TRON FUNCTIONS ==========
async function getUSDTTransactions(address) {
  try {
    if (!address) return [];
    
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

        const to = tx.to;
        const from = tx.from;
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
      } catch (innerErr) {
        continue;
      }
    }

    transactions.sort((a, b) => b.timestamp - a.timestamp);
    return transactions;
  } catch (error) {
    console.error('❌ getUSDTTransactions error:', error.message);
    return [];
  }
}

// ========== UNIVERSAL DEPOSIT PROCESSING ==========
async function processDeposit(wallet, amount, txid, network) {
  try {
    console.log(`💰 PROCESSING DEPOSIT: ${amount} USDT for user ${wallet.user_id}, txid: ${txid}, network: ${network}`);

    // Проверяем существующий депозит
    const { data: existingDeposit } = await supabase
      .from('deposits')
      .select('id, status, amount')
      .eq('txid', txid)
      .eq('network', network)
      .maybeSingle();

    if (existingDeposit) {
      console.log(`✅ Deposit already processed: ${txid}`);
      return { success: false, reason: 'already_processed' };
    }

    await ensureUserExists(wallet.user_id);

    // Создаем запись о депозите
    const { data: newDeposit, error: depositError } = await supabase
      .from('deposits')
      .insert({
        user_id: wallet.user_id,
        amount,
        txid,
        network,
        status: 'processing',
        created_at: new Date().toISOString()
      })
      .select()
      .single();

    if (depositError) {
      if (depositError.code === '23505') {
        return { success: false, reason: 'concurrent_processing' };
      }
      throw new Error(`Deposit insert failed: ${depositError.message}`);
    }

    // Получаем данные пользователя
    const { data: user } = await supabase
      .from('users')
      .select('balance, total_profit, vip_level')
      .eq('id', wallet.user_id)
      .single();

    const currentBalance = Number(user.balance) || 0;
    const newBalance = currentBalance + amount;
    const newTotalProfit = (Number(user.total_profit) || 0) + amount;

    // Обновляем баланс пользователя
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

    // Подтверждаем депозит
    await supabase
      .from('deposits')
      .update({ status: 'confirmed' })
      .eq('id', newDeposit.id);

    // Создаем запись в транзакциях
    await supabase.from('transactions').insert({
      user_id: wallet.user_id,
      type: 'deposit',
      amount,
      description: `Депозит USDT (${network}) - ${txid.substring(0, 10)}...`,
      status: 'completed',
      created_at: new Date().toISOString()
    });

    console.log(`✅ DEPOSIT PROCESSED: ${amount} USDT for user ${wallet.user_id}`);
    console.log(`💰 New balance: ${newBalance} USDT`);

    return { success: true, amount, deposit_id: newDeposit.id };

  } catch (error) {
    console.error('❌ Error processing deposit:', error.message);
    
    // Откатываем в случае ошибки
    try {
      await supabase
        .from('deposits')
        .delete()
        .eq('txid', txid)
        .eq('network', network)
        .eq('status', 'processing');
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

    // Проверяем существующий кошелек
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

    // Генерируем кошелек в зависимости от сети
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

    // Сохраняем в базу
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
    
    // Запускаем проверку депозитов
    setTimeout(() => checkUserDeposits(user_id, network), 5000);

    res.json({ success: true, address, exists: false, network });
  } catch (error) {
    console.error('❌ Generate wallet error:', error.message);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

// Endpoint для получения адреса пополнения
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

    res.json({ 
      success: true, 
      address: wallet.address
    });
  } catch (error) {
    console.error('❌ Get deposit address error:', error.message);
    res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

// ========== DEPOSIT CHECKING ==========
async function handleCheckDeposits() {
  try {
    console.log('🔄 Checking deposits for all users and networks...');
    const { data: wallets, error } = await supabase.from('user_wallets').select('*').limit(200);
    if (error) throw error;

    console.log(`🔍 Checking ${wallets?.length || 0} wallets across all networks`);
    
    let processedCount = 0;
    let depositsFound = 0;

    for (const wallet of wallets || []) {
      try {
        // Задержка для избежания лимитов
        await sleep(wallet.network === 'BEP20' ? 500 : 1000);
        
        let transactions = [];
        if (wallet.network === 'TRC20') {
          transactions = await getUSDTTransactions(wallet.address);
        } else if (wallet.network === 'BEP20') {
          transactions = await getBSCTransactions(wallet.address);
        }

        // Обрабатываем транзакции
        for (const tx of transactions) {
          const recipient = wallet.network === 'TRC20' ? tx.to : tx.to.toLowerCase();
          const walletAddress = wallet.network === 'TRC20' ? wallet.address : wallet.address.toLowerCase();
          
          if (recipient === walletAddress && tx.token === 'USDT' && tx.amount >= MIN_DEPOSIT) {
            try {
              const result = await processDeposit(wallet, tx.amount, tx.transaction_id, wallet.network);
              if (result.success) {
                depositsFound++;
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

    const message = `✅ Processed ${processedCount} wallets, found ${depositsFound} new deposits`;
    console.log(message);
    return { success: true, message };
  } catch (error) {
    console.error('❌ Deposit check error:', error.message);
    return { success: false, error: error.message };
  }
}

// Ручной запуск проверки депозитов
app.post('/check-deposits', async (req, res) => {
  try {
    const result = await handleCheckDeposits();
    res.json(result);
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// ========== helper DB functions ==========
async function ensureUserExists(userId) {
  try {
    const { data } = await supabase.from('users').select('id').eq('id', userId).single();
    if (!data) {
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
    }
  } catch (error) {
    console.error('❌ ensureUserExists error:', error.message);
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
    
    if (!wallet) return;
    
    console.log(`🔍 Checking ${network} deposits for user ${userId}, wallet: ${wallet.address}`);
    
    await sleep(network === 'BEP20' ? 500 : 0);
    
    let transactions = [];
    if (network === 'TRC20') {
      transactions = await getUSDTTransactions(wallet.address);
    } else if (network === 'BEP20') {
      transactions = await getBSCTransactions(wallet.address);
    }
    
    for (const tx of transactions) {
      const recipient = network === 'TRC20' ? tx.to : tx.to.toLowerCase();
      const walletAddress = network === 'TRC20' ? wallet.address : wallet.address.toLowerCase();
      
      if (recipient === walletAddress && tx.token === 'USDT' && tx.amount >= MIN_DEPOSIT) {
        try {
          const result = await processDeposit(wallet, tx.amount, tx.transaction_id, network);
          if (result.success) {
            console.log(`💰 FOUND NEW DEPOSIT: ${tx.amount} USDT for user ${userId} (${network})`);
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

// ========== HEALTH CHECK ==========
app.get('/', (req, res) => {
  res.json({
    status: '✅ WORKING',
    message: 'Cocoon AI - Deposit System',
    timestamp: new Date().toISOString(),
    networks: ['TRC20', 'BEP20'],
    environment: process.env.RAILWAY_ENVIRONMENT_NAME || 'development'
  });
});

// ========== Scheduler ==========
setInterval(async () => {
  try {
    console.log('🕒 AUTO-CHECK: Scanning for deposits...');
    await handleCheckDeposits();
  } catch (err) {
    console.error('❌ Auto-check internal error:', err.message);
  }
}, CHECK_INTERVAL_MS);

// ========== START SERVER ==========
app.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 SERVER RUNNING on port ${PORT}`);
  console.log(`✅ SUPABASE: CONNECTED`);
  console.log(`✅ TRONGRID: ${TRONGRID_API_KEY ? 'SET' : 'MISSING'}`);
  console.log(`✅ MORALIS API: ${MORALIS_API_KEY ? 'AVAILABLE' : 'MISSING'}`);
  console.log(`⏰ AUTO-CHECK: EVERY ${Math.round(CHECK_INTERVAL_MS / 1000)}s`);
  console.log('===================================');
});
