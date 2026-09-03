// server.js — Oracle Deposit System (BEP20 + ERC20 + TRC20 USDT/USDC)
// Built from the user's original full server flow.
// Changes vs original:
// - UNIFIED EVM WALLETS: One 0x... address generated and shared across USDT/USDC and BEP20/ERC20.
// - TRC20 restored for USDT
// - ERC20 explicitly fixed (contract_addresses, limit 100, strict 6 decimals)
// - BEP20 explicitly fixed (contract_addresses, limit 100, strict 18 decimals)
// - Old vulnerable getChainTokenTransfers removed completely
// - Compatible with Supabase RPC public.credit_chain_deposit
// - V2: AUTO-SWEEP ADDED FOR BEP20 ONLY (Non-blocking)
// - V3: WEBSOCKET FIX FOR NODE.JS 20 SUPABASE COMPATIBILITY
// - V4: MORALIS FIX - Removed URL contract filters, strictly local filtering
// - PixelNFT compatibility: atomic database RPCs, credited/reversed statuses,
//   encrypted key envelopes and existing PixelNFT app/admin API contracts.
//   Provider queries, confirmation flags, scan timers and BEP20 transfer flow
//   deliberately remain the same as the supplied server (6)(3).js.

const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const ethers = require('ethers');
const crypto = require('crypto');
const WebSocket = require('ws'); // ИСПРАВЛЕНИЕ: Добавлен пакет ws для Node.js < 22

const app = express();
app.set('trust proxy', true);
const PORT = Number(process.env.PORT || 8080);

// ========== CONFIGURATION ==========
const SUPABASE_URL = process.env.SUPABASE_URL || 'https://fkjwueogfmdolcjtvvme.supabase.co';
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const MORALIS_API_KEY = process.env.MORALIS_API_KEY || '';
const ENCRYPTION_KEY = process.env.ENCRYPTION_KEY;
const API_SECRET_KEY = process.env.API_SECRET_KEY;

// НАСТРОЙКИ АВТОСБОРА (только для BEP20)
const HOT_WALLET_PRIVATE_KEY = process.env.HOT_WALLET_PRIVATE_KEY; 
const ADMIN_SWEEP_ADDRESS = process.env.ADMIN_SWEEP_ADDRESS;       

if (!SUPABASE_SERVICE_ROLE_KEY) {
  console.error('❌ Missing SUPABASE_SERVICE_ROLE_KEY env');
  process.exit(1);
}
if (!ENCRYPTION_KEY || String(ENCRYPTION_KEY).length < 32) {
  console.error('❌ Missing/invalid ENCRYPTION_KEY env (must be 32+ chars)');
  process.exit(1);
}
if (!MORALIS_API_KEY) {
  console.warn('⚠️ MORALIS_API_KEY is empty (BEP20/ERC20 checks may fail).');
}
if (!API_SECRET_KEY || String(API_SECRET_KEY).length < 32) {
  console.error('❌ Missing/invalid API_SECRET_KEY env (must be 32+ chars)');
  process.exit(1);
}
if (!HOT_WALLET_PRIVATE_KEY || !ADMIN_SWEEP_ADDRESS) {
  console.warn('⚠️ BEP20 Auto-sweep is disabled: HOT_WALLET_PRIVATE_KEY or ADMIN_SWEEP_ADDRESS missing.');
}

// ========== INITIALIZE SERVICES ==========
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  auth: {
    autoRefreshToken: false,
    persistSession: false
  },
  realtime: {
    transport: WebSocket
  }
});

// ========== MIDDLEWARE ==========
app.use(express.json({ limit: '1mb' }));
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', '*');
  res.header('Access-Control-Allow-Methods', '*');
  res.header('Access-Control-Expose-Headers', 'Server-Timing, X-Response-Time');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

// ========== SIMPLE RATE LIMIT ==========
const rateLimitStore = new Map();
function simpleRateLimit(req, res, next) {
  const ip = req.ip || req.connection?.remoteAddress || 'unknown';
  const now = Date.now();
  const windowMs = 15 * 60 * 1000;
  const max = 60;

  if (req.path === '/health' || req.path === '/api/health' || req.path === '/') {
    return next();
  }

  if (!rateLimitStore.has(ip)) {
    rateLimitStore.set(ip, []);
  }

  const requests = rateLimitStore.get(ip) || [];
  const validRequests = requests.filter((time) => now - time < windowMs);
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

app.use(simpleRateLimit);

// ========== DEPOSIT CHECK COOLDOWNS ==========
const userDepositCheckCooldown = new Map();
const adminDepositCheckCooldown = new Map();

function cleanupCooldownStore(store, olderThanMs) {
  const now = Date.now();
  for (const [key, timestamp] of store.entries()) {
    if (now - timestamp > olderThanMs) store.delete(key);
  }
}

function createCooldownMiddleware(store, cooldownMs, errorMessage) {
  return (req, res, next) => {
    const authHeader = req.headers['authorization'] || '';
    const key = authHeader || req.ip || req.connection?.remoteAddress || 'unknown';
    const now = Date.now();
    const last = store.get(key) || 0;

    cleanupCooldownStore(store, cooldownMs * 10);

    if (now - last < cooldownMs) {
      const waitSeconds = Math.ceil((cooldownMs - (now - last)) / 1000);
      return res.status(429).json({
        success: false,
        error: errorMessage,
        wait_seconds: waitSeconds
      });
    }

    store.set(key, now);
    next();
  };
}

const userDepositCheckCooldownMiddleware = createCooldownMiddleware(
  userDepositCheckCooldown,
  Number(process.env.USER_DEPOSIT_CHECK_COOLDOWN_MS || 60000),
  'Please wait before checking your deposit again'
);

const adminDepositCheckCooldownMiddleware = createCooldownMiddleware(
  adminDepositCheckCooldown,
  Number(process.env.ADMIN_DEPOSIT_CHECK_COOLDOWN_MS || 60000),
  'Please wait before checking all deposits again'
);


// ========== CONSTANTS ==========
const MIN_DEPOSIT = 1;

// BSC
const USDT_BSC_CONTRACT = '0x55d398326f99059fF775485246999027B3197955';
const USDC_BSC_CONTRACT = '0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d';

// Ethereum
const USDT_ETH_CONTRACT = '0xdAC17F958D2ee523a2206206994597C13D831ec7';
const USDC_ETH_CONTRACT = '0xA0b86991c6218b36c1d19d4a2e9eb0ce3606eb48';

// TRON
const USDT_TRON_CONTRACT = 'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t';
const TRONGRID_API_BASE = process.env.TRONGRID_API_BASE || 'https://api.trongrid.io';
const TRONGRID_API_KEY = process.env.TRONGRID_API_KEY || '';
if (!TRONGRID_API_KEY) {
  console.warn('⚠️ TRONGRID_API_KEY is empty (TRC20 checks may hit strict public limits).');
}

const networkFields = {
  usdt_bep20: { addressField: 'usdt_bep20_address' },
  usdc_bep20: { addressField: 'usdc_bep20_address' },
  usdt_erc20: { addressField: 'usdt_erc20_address' },
  usdc_erc20: { addressField: 'usdc_erc20_address' },
  usdt_trc20: { addressField: 'usdt_trc20_address' }
};

const allowedNetworks = Object.keys(networkFields);

// ========== CHECK SETTINGS ==========
const BEP20_CHECK_INTERVAL = Number(process.env.BEP20_CHECK_INTERVAL || 120000);
const ERC20_CHECK_INTERVAL = Number(process.env.ERC20_CHECK_INTERVAL || 150000);
const TRC20_CHECK_INTERVAL = Number(process.env.TRC20_CHECK_INTERVAL || 60000);
const API_DELAY_MS = Number(process.env.API_DELAY_MS || 400);

// ========== HELPERS ==========
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const BASE58_ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';

function base58Encode(buffer) {
  if (!buffer || !buffer.length) return '';

  let value = BigInt('0x' + buffer.toString('hex'));
  let encoded = '';

  while (value > 0n) {
    const mod = Number(value % 58n);
    encoded = BASE58_ALPHABET[mod] + encoded;
    value /= 58n;
  }

  for (const byte of buffer) {
    if (byte === 0) encoded = '1' + encoded;
    else break;
  }

  return encoded || '1';
}

function tronBase58CheckFromHex(hexAddress) {
  const payload = Buffer.from(String(hexAddress || '').replace(/^0x/, ''), 'hex');
  const hash1 = crypto.createHash('sha256').update(payload).digest();
  const hash2 = crypto.createHash('sha256').update(hash1).digest();
  const checksum = hash2.subarray(0, 4);
  return base58Encode(Buffer.concat([payload, checksum]));
}

function generateTRONWallet() {
  const wallet = ethers.Wallet.createRandom();
  const ethHex = String(wallet.address || '').replace(/^0x/, '');
  const tronHexAddress = '41' + ethHex;
  const tronAddress = tronBase58CheckFromHex(tronHexAddress);

  return {
    address: tronAddress,
    privateKey: wallet.privateKey,
    hexAddress: tronHexAddress
  };
}

function readParam(req, key, fallback = undefined) {
  if (req.body && req.body[key] !== undefined) return req.body[key];
  if (req.query && req.query[key] !== undefined) return req.query[key];
  return fallback;
}

async function getUserFromBearerToken(req) {
  try {
    if (req.pixelUser) return req.pixelUser;
    const authHeader = req.headers['authorization'] || '';
    const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : null;
    if (!token) return null;

    const { data, error } = await supabase.auth.getUser(token);
    if (error || !data?.user) {
      console.warn('⚠️ Bearer token validation failed:', error?.message || 'No user');
      return null;
    }

    // getUser verifies the signature; the DB additionally rejects revoked
    // sessions and blocked profiles. Never authorize from user_metadata.
    const claims = JSON.parse(Buffer.from(token.split('.')[1], 'base64url').toString('utf8'));
    if (claims.sub !== data.user.id || !/^[0-9a-f-]{36}$/i.test(claims.session_id || '')) return null;
    const { data: state, error: stateError } = await supabase.rpc('pixel_app_api', {
      p_user: data.user.id, p_session: claims.session_id,
      p_action: 'user.state', p_payload: {}
    });
    if (stateError || !state || state.error) return null;
    req.pixelSession = claims.session_id;
    req.pixelState = state;
    req.pixelUser = data.user;
    return req.pixelUser;
  } catch (error) {
    console.warn('⚠️ Bearer token validation error:', error.message);
    return null;
  }
}

async function safeSystemLog(logType, message, metadata = {}) {
  try {
    const payload = {
      log_type: String(logType || 'server_log').slice(0, 100),
      message: String(message || '').slice(0, 1000),
      metadata
    };

    const { error } = await supabase.from('system_logs').insert(payload);
    if (error) {
      if (!String(error.message || '').toLowerCase().includes('relation') && !String(error.message || '').toLowerCase().includes('does not exist')) {
        console.warn('⚠️ system_logs insert skipped:', error.message);
      }
    }
  } catch (error) {
    // intentionally swallow
  }
}

// Logging and reconciliation must never delay a user-facing address response.
// Errors remain visible in Railway logs, while the critical wallet writes are
// still awaited inside generateWallet().
function runInBackground(label, task) {
  Promise.resolve()
    .then(task)
    .catch((error) => console.warn(`⚠️ Background task failed (${label}):`, error.message));
}


// ========== DEPOSIT ADDRESS SYNC ==========
// register_deposit_wallets writes addresses and encrypted keys atomically.
// This compatibility helper verifies the saved address; it must not bypass
// the database's write restrictions with a separate upsert.
function getDepositAsset(network) {
  return String(network || '').toLowerCase().startsWith('usdc_') ? 'USDC' : 'USDT';
}

async function syncDepositAddress(userId, network, address) {
  if (!userId || !network || !address) return;

  const normalizedNetwork = String(network).trim().toLowerCase();
  const asset = getDepositAsset(normalizedNetwork);

  const { data, error } = await supabase
    .from('deposit_addresses')
    .select('address')
    .eq('user_id', userId)
    .eq('asset', asset)
    .eq('network', normalizedNetwork)
    .eq('is_active', true)
    .maybeSingle();

  if (error) {
    throw new Error(`Failed to sync deposit address: ${error.message}`);
  }
  const matches = normalizedNetwork.endsWith('_trc20')
    ? data?.address === address
    : data?.address?.toLowerCase() === String(address).toLowerCase();
  if (!matches) throw new Error('DEPOSIT_ADDRESS_MISMATCH');
}

// ========== API KEY CHECK ==========
function requireApiKey(req, res, next) {
  const clientKey = req.headers['x-api-key'];

  if (!clientKey) {
    console.error('🚨 BLOCKED: No API key provided', {
      ip: req.ip,
      path: req.path,
      timestamp: new Date().toISOString()
    });
    return res.status(401).json({
      success: false,
      error: 'API key required. Use x-api-key header.'
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

  next();
}

// ========== ENCRYPTION ==========
function encryptPrivateKey(text) {
  try {
    if (!/^(0x)?[0-9a-fA-F]{64}$/.test(String(text || ''))) throw new Error('INVALID_PRIVATE_KEY');
    const iv = crypto.randomBytes(12);
    const cipher = crypto.createCipheriv(
      'aes-256-gcm',
      crypto.createHash('sha256').update(ENCRYPTION_KEY).digest(),
      iv
    );
    let encrypted = cipher.update(text, 'utf8', 'hex');
    encrypted += cipher.final('hex');
    const authTag = cipher.getAuthTag();
    return `v1:${iv.toString('hex')}:${authTag.toString('hex')}:${encrypted}`;
  } catch (error) {
    console.error('❌ Encryption error:', error.message);
    throw new Error('PRIVATE_KEY_ENCRYPTION_FAILED');
  }
}

function decryptPrivateKey(encryptedText) {
  try {
    const parts = String(encryptedText || '').split(':');
    const modern = parts.length === 4 && parts[0] === 'v1';
    const legacy = parts.length === 3;
    if (!modern && !legacy) throw new Error('KEY_ENVELOPE_INVALID');
    const ivHex = modern ? parts[1] : parts[0];
    const encrypted = modern ? parts[3] : parts[1];
    const tagHex = parts[2];
    if (!(modern ? /^[a-f0-9]{24}$/ : /^[a-f0-9]{32}$/).test(ivHex) ||
        !/^[a-f0-9]{32}$/.test(tagHex) || !/^(?:[a-f0-9]{2})+$/.test(encrypted)) {
      throw new Error('KEY_ENVELOPE_INVALID');
    }
    const iv = Buffer.from(ivHex, 'hex');
    const authTag = Buffer.from(tagHex, 'hex');

    const decipher = crypto.createDecipheriv(
      'aes-256-gcm',
      crypto.createHash('sha256').update(ENCRYPTION_KEY).digest(),
      iv
    );
    decipher.setAuthTag(authTag);

    let decrypted = decipher.update(encrypted, 'hex', 'utf8');
    decrypted += decipher.final('utf8');
    if (!/^(0x)?[0-9a-fA-F]{64}$/.test(decrypted)) throw new Error('INVALID_PRIVATE_KEY');
    return decrypted;
  } catch (error) {
    console.error('❌ Decryption error:', error.message);
    throw new Error('PRIVATE_KEY_DECRYPTION_FAILED');
  }
}

// ========== AUTO-SWEEP LOGIC (BEP20) ==========
async function sweepDepositBEP20(userId, token, network) {
  if (!HOT_WALLET_PRIVATE_KEY || !ADMIN_SWEEP_ADDRESS) return;

  console.log(`🧹 Starting Auto-Sweep for user ${userId} (${token} on ${network})`);

  try {
    const provider = new ethers.JsonRpcProvider('https://bsc-dataseed.binance.org/');
    const hotWallet = new ethers.Wallet(HOT_WALLET_PRIVATE_KEY, provider);

    // The DB removes only its outer PGP envelope. AES-GCM decryption stays
    // here on the deposit server, using the original ENCRYPTION_KEY.
    const { data: keyData, error } = await supabase.rpc('deposit_key_envelope', {
      p_user_id: userId, p_network: network
    });

    if (error || !keyData) throw new Error('Cannot find private key for sweep');

    const userPrivateKey = decryptPrivateKey(keyData.server_cipher);
    const userWallet = new ethers.Wallet(userPrivateKey, provider);
    if (userWallet.address.toLowerCase() !== String(keyData.address).toLowerCase()) {
      throw new Error('SWEEP_KEY_ADDRESS_MISMATCH');
    }

    const contractAddress = token === 'USDT' ? USDT_BSC_CONTRACT : USDC_BSC_CONTRACT;
    const tokenContract = new ethers.Contract(
      contractAddress, 
      ['function transfer(address to, uint256 value) returns (bool)', 'function balanceOf(address owner) view returns (uint256)'], 
      userWallet
    );

    const balanceWei = await tokenContract.balanceOf(userWallet.address);
    if (balanceWei === 0n) {
      console.log(`⏭️ Sweep skipped: 0 ${token} balance for ${userId}`);
      return;
    }

    // 1. Estimate gas
    const gasLimit = await tokenContract.transfer.estimateGas(ADMIN_SWEEP_ADDRESS, balanceWei);
    const feeData = await provider.getFeeData();
    const gasCost = gasLimit * feeData.gasPrice;

    // 2. Fund gas if needed
    const userBnbBalance = await provider.getBalance(userWallet.address);
    
    if (userBnbBalance < gasCost) {
      const neededBnb = gasCost - userBnbBalance;
      const buffer = feeData.gasPrice * 20000n; // Safety buffer
      const safeFundAmount = neededBnb + buffer;
      
      console.log(`⛽ Funding ${ethers.formatEther(safeFundAmount)} BNB for gas to ${userWallet.address}`);
      
      const fundTx = await hotWallet.sendTransaction({
        to: userWallet.address,
        value: safeFundAmount
      });
      await fundTx.wait();
      console.log(`✅ Gas funded. TxHash: ${fundTx.hash}`);
    }

    // 3. Perform sweep
    console.log(`💸 Sweeping ${ethers.formatUnits(balanceWei, 18)} ${token} to Admin Wallet...`);
    const sweepTx = await tokenContract.transfer(ADMIN_SWEEP_ADDRESS, balanceWei);
    await sweepTx.wait();

    console.log(`✅ Sweep successful! TxHash: ${sweepTx.hash}`);

    await safeSystemLog('sweep_success', `Auto-sweep successful for user ${userId}`, {
      user_id: userId, token, network, tx_hash: sweepTx.hash
    });

  } catch (error) {
    console.error(`❌ Sweep failed for user ${userId}:`, error.message);
    await safeSystemLog('sweep_error', `Auto-sweep failed: ${error.message}`, { user_id: userId, token, network });
  }
}

// ========== WALLET GENERATION ==========
async function generateEVMWallet() {
  try {
    const wallet = ethers.Wallet.createRandom();
    return {
      address: wallet.address,
      privateKey: wallet.privateKey
    };
  } catch (error) {
    console.error('❌ EVM wallet generation error:', error.message);
    throw error;
  }
}

async function generateWallet(user_id, network) {
  const fields = networkFields[network];
  if (!fields) throw new Error('Unsupported network');
  const { data: existingWallet, error: walletError } = await supabase
    .from('deposit_wallets').select('*').eq('user_id', user_id).maybeSingle();
  if (walletError) throw walletError;
  // Never replace an existing address/key pair, including partially migrated
  // wallets. Incomplete old records need explicit recovery, not new keys.
  if (existingWallet && allowedNetworks.some(net => !existingWallet[networkFields[net].addressField])) {
    throw new Error('WALLET_KEYS_INCOMPLETE');
  }

  const evm = existingWallet ? null : await generateEVMWallet();
  const tron = existingWallet ? null : generateTRONWallet();
  const { data: wallet, error } = await supabase.rpc('register_deposit_wallets', {
    p_user_id: user_id,
    p_evm_address: evm?.address || null,
    p_evm_key: evm ? encryptPrivateKey(evm.privateKey) : null,
    p_tron_address: tron?.address || null,
    p_tron_key: tron ? encryptPrivateKey(tron.privateKey) : null
  });
  if (error) throw error;
  if (!wallet || wallet.user_id !== user_id || allowedNetworks.some(net => !wallet[networkFields[net].addressField])) {
    throw new Error('WALLET_SAVE_FAILED');
  }
  const address = wallet[fields.addressField];
  // Another server may have won the DB lock; always return the saved address.
  const exists = !!existingWallet || (evm && wallet.usdt_bep20_address !== evm.address);
  if (!exists) {
    runInBackground('deposit_wallet_generated', () => safeSystemLog(
      'deposit_wallet_generated', 'Deposit wallets generated for user ' + user_id,
      { user_id, network, address }
    ));
    setTimeout(() => {
      if (network.includes('bep20')) checkUserBEP20Deposits(user_id).catch(console.error);
      if (network.includes('erc20')) checkUserERC20Deposits(user_id).catch(console.error);
      if (network.includes('trc20')) checkUserTRC20Deposits(user_id).catch(console.error);
    }, 10000);
  }
  return { success: true, address, exists: !!exists, network, wallet };
}

// One DB transaction registers the unified EVM wallet, TRON wallet and all
// five encrypted key/address records. Concurrent calls reuse that transaction.
const walletGenerationInFlight = new Map();
async function generateWalletSingleFlight(userId, network) {
  if (!networkFields[network]) throw new Error('Unsupported network');
  let operation = walletGenerationInFlight.get(userId);
  if (!operation) {
    operation = generateWallet(userId, network).finally(() => {
      if (walletGenerationInFlight.get(userId) === operation) walletGenerationInFlight.delete(userId);
    });
    walletGenerationInFlight.set(userId, operation);
  }
  const result = await operation;
  return { ...result, network, address: result.wallet[networkFields[network].addressField] };
}

// ========== DEPOSIT PROCESSING ==========
async function processDeposit(userId, amount, txid, network, address = null, confirmations = 1) {
  try {
    const normalizedNetwork = String(network || '').trim().toLowerCase();
    const hash = String(txid || '').trim().toLowerCase().replace(/^0x/, '');
    const normalizedTxid = normalizedNetwork.endsWith('_trc20') ? hash : '0x' + hash;
    if (!Number.isFinite(Number(amount)) || Number(amount) < MIN_DEPOSIT) {
      return { success: false, error: 'Minimum deposit is $' + MIN_DEPOSIT };
    }
    // The RPC owns the duplicate check AND all financial writes. A separate
    // JS read cannot protect against concurrent manual/background checks.
    return await processDepositAtomic(userId, Number(amount), normalizedTxid,
      normalizedNetwork, address, confirmations);
  } catch (error) {
    console.error('❌ Error in processDeposit:', error.message);
    await safeSystemLog('deposit_processing_error', 'Deposit processing error: ' + error.message, {
      user_id: userId, amount, tx_hash: txid, network, address, confirmations
    });
    return { success: false, error: error.message };
  }
}

async function processDepositAtomic(userId, amount, txid, network, address = null, confirmations = 1) {
  const { data: result, error } = await supabase.rpc('credit_chain_deposit', {
    p_user_id: userId,
    p_amount: amount,
    p_network: network,
    p_tx_hash: txid,
    p_address: address || null,
    // Original server protocol: confirmed provider transfer -> 1, else -> 0.
    // This is not a fabricated count of blockchain confirmations.
    p_confirmations: Math.max(Number(confirmations || 0), 0)
  });
  if (error) throw error;
  if (!result || result.success !== true) throw new Error(result?.error || 'Deposit processing failed');
  const alreadyProcessed = result.already_processed === true || result.duplicate === true;
  if (!alreadyProcessed) {
    await safeSystemLog('deposit_atomic_success', 'Atomic deposit successful for user ' + userId, {
      deposit_id: result.deposit_id, user_id: userId, amount: result.amount,
      old_balance: result.old_balance, new_balance: result.new_balance,
      asset: result.asset, tx_hash: txid, network
    });
  }
  return {
    success: true,
    already_processed: alreadyProcessed,
    deposit_id: result.deposit_id,
    status: result.status || 'credited',
    old_balance: result.old_balance,
    new_balance: result.new_balance,
    amount: result.amount,
    asset: result.asset,
    network: result.network,
    personal_bonus: result.personal_bonus,
    inviter_bonus: result.inviter_bonus
  };
}

// ========== CHAIN TRANSFERS ==========

async function getBEP20Transactions(address) {
  try {
    if (!address) return [];

    const params = new URLSearchParams({
      chain: 'bsc',
      limit: '100'
    });
    // ⚠️ ИСПРАВЛЕНИЕ: Фильтры contract_addresses убраны из URL, чтобы не ломать Moralis

    const url = `https://deep-index.moralis.io/api/v2/${address}/erc20/transfers?${params.toString()}`;

    const response = await fetch(url, {
      headers: {
        'X-API-Key': MORALIS_API_KEY,
        Accept: 'application/json'
      }
    });

    if (!response.ok) {
      throw new Error(`Moralis API error: ${response.status}`);
    }

    const data = await response.json();
    const transactions = [];

    const validContracts = [USDT_BSC_CONTRACT.toLowerCase(), USDC_BSC_CONTRACT.toLowerCase()];

    for (const tx of data.result || []) {
      try {
        const toAddress = String(tx.to_address || '').toLowerCase();
        if (toAddress !== String(address).toLowerCase()) continue;

        const tokenContract = String(tx.address || '').toLowerCase();
        
        // ⚠️ ИСПРАВЛЕНИЕ: Локальная фильтрация USDT/USDC контрактов
        if (!validContracts.includes(tokenContract)) continue;

        const isUSDT = tokenContract === USDT_BSC_CONTRACT.toLowerCase();
        
        const decimals = Number(tx.decimals || 18);
        const amount = Number(tx.value) / Math.pow(10, decimals);
        
        if (!Number.isFinite(amount) || amount < MIN_DEPOSIT) continue;

        transactions.push({
          transaction_id: tx.transaction_hash,
          to: toAddress,
          from: String(tx.from_address || '').toLowerCase(),
          amount,
          token: isUSDT ? 'USDT' : 'USDC',
          confirmed: true,
          network: isUSDT ? 'usdt_bep20' : 'usdc_bep20',
          timestamp: new Date(tx.block_timestamp).getTime(),
          blockNumber: Number(tx.block_number || 0)
        });
      } catch (innerErr) {
        continue;
      }
    }

    transactions.sort((a, b) => b.timestamp - a.timestamp);
    return transactions;
  } catch (error) {
    console.error(`❌ BEP20 transfer fetch error:`, error.message);
    return [];
  }
}

async function getERC20Transactions(address) {
  try {
    if (!address) return [];

    const params = new URLSearchParams({
      chain: 'eth',
      limit: '100'
    });
    // ⚠️ ИСПРАВЛЕНИЕ: Фильтры contract_addresses убраны из URL, чтобы не ломать Moralis

    const url = `https://deep-index.moralis.io/api/v2/${address}/erc20/transfers?${params.toString()}`;

    const response = await fetch(url, {
      headers: {
        'X-API-Key': MORALIS_API_KEY,
        Accept: 'application/json'
      }
    });

    if (!response.ok) {
      throw new Error(`Moralis API error: ${response.status}`);
    }

    const data = await response.json();
    const transactions = [];

    const validContracts = [USDT_ETH_CONTRACT.toLowerCase(), USDC_ETH_CONTRACT.toLowerCase()];

    for (const tx of data.result || []) {
      try {
        const toAddress = String(tx.to_address || '').toLowerCase();
        if (toAddress !== String(address).toLowerCase()) continue;

        const tokenContract = String(tx.address || '').toLowerCase();
        
        // ⚠️ ИСПРАВЛЕНИЕ: Локальная фильтрация USDT/USDC контрактов
        if (!validContracts.includes(tokenContract)) continue;

        const isUSDT = tokenContract === USDT_ETH_CONTRACT.toLowerCase();
        
        const decimals = Number(tx.decimals || 6);
        const amount = Number(tx.value) / Math.pow(10, decimals);
        
        if (!Number.isFinite(amount) || amount < MIN_DEPOSIT) continue;

        transactions.push({
          transaction_id: tx.transaction_hash,
          to: toAddress,
          from: String(tx.from_address || '').toLowerCase(),
          amount,
          token: isUSDT ? 'USDT' : 'USDC',
          confirmed: true,
          network: isUSDT ? 'usdt_erc20' : 'usdc_erc20',
          timestamp: new Date(tx.block_timestamp).getTime(),
          blockNumber: Number(tx.block_number || 0)
        });
      } catch (innerErr) {
        continue;
      }
    }

    transactions.sort((a, b) => b.timestamp - a.timestamp);
    return transactions;
  } catch (error) {
    console.error(`❌ ERC20 transfer fetch error:`, error.message);
    return [];
  }
}

async function getTRC20Transactions(address) {
  try {
    if (!address) return [];

    const params = new URLSearchParams({
      only_confirmed: 'true',
      only_to: 'true',
      limit: '200',
      contract_address: USDT_TRON_CONTRACT
    });

    const headers = {
      Accept: 'application/json'
    };

    if (TRONGRID_API_KEY) {
      headers['TRON-PRO-API-KEY'] = TRONGRID_API_KEY;
    }

    const response = await fetch(`${TRONGRID_API_BASE}/v1/accounts/${encodeURIComponent(address)}/transactions/trc20?${params.toString()}`, {
      headers
    });

    if (!response.ok) {
      throw new Error(`TronGrid API error: ${response.status}`);
    }

    const data = await response.json();
    const transactions = [];

    for (const tx of data.data || []) {
      try {
        const toAddress = String(tx.to || '').trim();
        if (toAddress !== String(address).trim()) continue;

        const decimals = Number(tx.token_info?.decimals ?? tx.tokenInfo?.tokenDecimal ?? tx.decimals ?? 6);
        const rawValue = tx.value ?? tx.amount ?? tx.quant ?? '0';
        const amount = Number(rawValue) / Math.pow(10, decimals);
        const tokenSymbol = String(tx.token_info?.symbol || tx.token_info?.name || tx.tokenName || 'USDT').toUpperCase();
        const confirmed = tx.confirmed !== false;

        if (tokenSymbol !== 'USDT') continue;
        if (!confirmed) continue;
        if (!Number.isFinite(amount) || amount < MIN_DEPOSIT) continue;

        transactions.push({
          transaction_id: String(tx.transaction_id || tx.hash || ''),
          to: toAddress,
          from: String(tx.from || '').trim(),
          amount,
          token: 'USDT',
          confirmed: true,
          network: 'usdt_trc20',
          timestamp: Number(tx.block_timestamp || tx.blockTimeStamp || 0),
          blockNumber: Number(tx.block_number || tx.block || 0)
        });
      } catch (innerErr) {
        continue;
      }
    }

    transactions.sort((a, b) => b.timestamp - a.timestamp);
    return transactions;
  } catch (error) {
    console.error('❌ TRC20 transfer fetch error:', error.message);
    return [];
  }
}

// ========== CHAIN CHECKERS ==========
async function handleCheckBEP20Deposits() {
  try {
    console.log('🔄 Checking BEP20 deposits...');

    const { data: wallets, error } = await supabase
      .from('deposit_wallets')
      .select('*')
      .or('usdt_bep20_address.not.is.null,usdc_bep20_address.not.is.null')
      .limit(200);

    if (error) throw error;

    let processedCount = 0;
    let depositsFound = 0;
    let duplicatesSkipped = 0;
    let errors = 0;

    for (const wallet of wallets || []) {
      try {
        const addresses = Array.from(
          new Set([wallet.usdt_bep20_address, wallet.usdc_bep20_address].filter(Boolean))
        );

        for (const address of addresses) {
          await sleep(API_DELAY_MS);

          const transactions = await getBEP20Transactions(address);
          for (const tx of transactions) {
            try {
              const { data: existing } = await supabase
                .from('deposits')
                .select('id, status')
                .eq('tx_hash', tx.transaction_id)
                .eq('network', tx.network)
                .maybeSingle();

              if (existing && ['credited', 'reversed'].includes(existing.status)) {
                duplicatesSkipped++;
                console.log(`⏭️ Skipping duplicate ${tx.network} transaction: ${tx.transaction_id}`);
                continue;
              }

              const result = await processDeposit(wallet.user_id, tx.amount, tx.transaction_id, tx.network, tx.to || address, tx.confirmed ? 1 : 0);
              if (!result.success) throw new Error(result.error || 'Deposit processing failed');
              if (result.already_processed) { duplicatesSkipped++; continue; }
              if (result.success) {
                depositsFound++;
                console.log(`💰 NEW ${tx.network} DEPOSIT: $${tx.amount} ${tx.token} for user ${wallet.user_id}`);
                
                // ВЫЗОВ АВТОСБОРА (Без await, чтобы не блокировать выполнение других юзеров)
                sweepDepositBEP20(wallet.user_id, tx.token, tx.network).catch(err => 
                  console.error(`Sweep background error:`, err.message)
                );
              }
            } catch (err) {
              if (String(err.message || '').includes('already_processed') || String(err.message || '').includes('duplicate')) {
                duplicatesSkipped++;
                console.log(`⏭️ Duplicate ${tx.network} deposit skipped: ${tx.transaction_id}`);
              } else {
                console.error(`❌ Error processing ${tx.network} deposit ${tx.transaction_id}:`, err.message);
                errors++;
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
      errors
    };
  } catch (error) {
    console.error('❌ BEP20 check error:', error.message);
    return { success: false, error: error.message };
  }
}

async function handleCheckERC20Deposits() {
  try {
    console.log('🔄 Checking ERC20 deposits...');

    const { data: wallets, error } = await supabase
      .from('deposit_wallets')
      .select('*')
      .or('usdt_erc20_address.not.is.null,usdc_erc20_address.not.is.null')
      .limit(200);

    if (error) throw error;

    let processedCount = 0;
    let depositsFound = 0;
    let duplicatesSkipped = 0;
    let errors = 0;

    for (const wallet of wallets || []) {
      try {
        const addresses = Array.from(
          new Set([wallet.usdt_erc20_address, wallet.usdc_erc20_address].filter(Boolean))
        );

        for (const address of addresses) {
          await sleep(API_DELAY_MS);

          const transactions = await getERC20Transactions(address);
          for (const tx of transactions) {
            try {
              const { data: existing } = await supabase
                .from('deposits')
                .select('id, status')
                .eq('tx_hash', tx.transaction_id)
                .eq('network', tx.network)
                .maybeSingle();

              if (existing && ['credited', 'reversed'].includes(existing.status)) {
                duplicatesSkipped++;
                console.log(`⏭️ Skipping duplicate ${tx.network} transaction: ${tx.transaction_id}`);
                continue;
              }

              const result = await processDeposit(wallet.user_id, tx.amount, tx.transaction_id, tx.network, tx.to || address, tx.confirmed ? 1 : 0);
              if (!result.success) throw new Error(result.error || 'Deposit processing failed');
              if (result.already_processed) { duplicatesSkipped++; continue; }
              if (result.success) {
                depositsFound++;
                console.log(`💰 NEW ${tx.network} DEPOSIT: $${tx.amount} ${tx.token} for user ${wallet.user_id}`);
              }
            } catch (err) {
              if (String(err.message || '').includes('already_processed') || String(err.message || '').includes('duplicate')) {
                duplicatesSkipped++;
                console.log(`⏭️ Duplicate ${tx.network} deposit skipped: ${tx.transaction_id}`);
              } else {
                console.error(`❌ Error processing ${tx.network} deposit ${tx.transaction_id}:`, err.message);
                errors++;
              }
            }
          }
        }

        processedCount++;
      } catch (err) {
        console.error(`❌ Error processing ERC20 wallet ${wallet.user_id}:`, err.message);
        errors++;
      }
    }

    console.log(`✅ ERC20: Processed ${processedCount} wallets, found ${depositsFound} new deposits, skipped ${duplicatesSkipped} duplicates, errors: ${errors}`);
    return {
      success: true,
      processed: processedCount,
      deposits: depositsFound,
      duplicates: duplicatesSkipped,
      errors
    };
  } catch (error) {
    console.error('❌ ERC20 check error:', error.message);
    return { success: false, error: error.message };
  }
}

async function handleCheckTRC20Deposits() {
  try {
    console.log('🔄 Checking TRC20 deposits...');

    const { data: wallets, error } = await supabase
      .from('deposit_wallets')
      .select('*')
      .not('usdt_trc20_address', 'is', null)
      .limit(200);

    if (error) throw error;

    let processedCount = 0;
    let depositsFound = 0;
    let duplicatesSkipped = 0;
    let errors = 0;

    for (const wallet of wallets || []) {
      try {
        const addresses = Array.from(new Set([wallet.usdt_trc20_address].filter(Boolean)));

        for (const address of addresses) {
          await sleep(API_DELAY_MS);

          const transactions = await getTRC20Transactions(address);
          for (const tx of transactions) {
            try {
              const { data: existing } = await supabase
                .from('deposits')
                .select('id, status')
                .eq('tx_hash', tx.transaction_id)
                .eq('network', tx.network)
                .maybeSingle();

              if (existing && ['credited', 'reversed'].includes(existing.status)) {
                duplicatesSkipped++;
                console.log(`⏭️ Skipping duplicate ${tx.network} transaction: ${tx.transaction_id}`);
                continue;
              }

              const result = await processDeposit(wallet.user_id, tx.amount, tx.transaction_id, tx.network, tx.to || address, tx.confirmed ? 1 : 0);
              if (!result.success) throw new Error(result.error || 'Deposit processing failed');
              if (result.already_processed) { duplicatesSkipped++; continue; }
              if (result.success) {
                depositsFound++;
                console.log(`💰 NEW ${tx.network} DEPOSIT: $${tx.amount} ${tx.token} for user ${wallet.user_id}`);
              }
            } catch (err) {
              if (String(err.message || '').includes('already_processed') || String(err.message || '').includes('duplicate')) {
                duplicatesSkipped++;
                console.log(`⏭️ Duplicate ${tx.network} deposit skipped: ${tx.transaction_id}`);
              } else {
                console.error(`❌ Error processing ${tx.network} deposit ${tx.transaction_id}:`, err.message);
                errors++;
              }
            }
          }
        }

        processedCount++;
      } catch (err) {
        console.error(`❌ Error processing TRC20 wallet ${wallet.user_id}:`, err.message);
        errors++;
      }
    }

    console.log(`✅ TRC20: Processed ${processedCount} wallets, found ${depositsFound} new deposits, skipped ${duplicatesSkipped} duplicates, errors: ${errors}`);
    return {
      success: true,
      processed: processedCount,
      deposits: depositsFound,
      duplicates: duplicatesSkipped,
      errors
    };
  } catch (error) {
    console.error('❌ TRC20 check error:', error.message);
    return { success: false, error: error.message };
  }
}

async function checkUserTRC20Deposits(userId) {
  const summary = { success: true, network_group: 'trc20', checked: 0, deposits: 0, duplicates: 0, errors: 0 };

  try {
    const { data: wallet, error } = await supabase
      .from('deposit_wallets')
      .select('*')
      .eq('user_id', userId)
      .maybeSingle();

    if (error) throw error;
    if (!wallet) return summary;

    const addresses = Array.from(new Set([wallet.usdt_trc20_address].filter(Boolean)));

    for (const address of addresses) {
      const transactions = await getTRC20Transactions(address);
      summary.checked += transactions.length;

      for (const tx of transactions) {
        try {
          const result = await processDeposit(userId, tx.amount, tx.transaction_id, tx.network, tx.to || address, tx.confirmed ? 1 : 0);
          if (!result?.success) throw new Error(result?.error || 'Deposit processing failed');
          if (result?.success) {
            if (result.already_processed) summary.duplicates++;
            else summary.deposits++;
          }
        } catch (err) {
          summary.errors++;
          console.error(`❌ Error processing transaction ${tx.transaction_id}:`, err.message);
        }
      }
    }

    return summary;
  } catch (error) {
    summary.success = false;
    summary.error = error.message;
    console.error('❌ checkUserTRC20Deposits error:', error.message);
    return summary;
  }
}

async function checkUserBEP20Deposits(userId) {
  const summary = { success: true, network_group: 'bep20', checked: 0, deposits: 0, duplicates: 0, errors: 0 };

  try {
    const { data: wallet, error } = await supabase
      .from('deposit_wallets')
      .select('*')
      .eq('user_id', userId)
      .maybeSingle();

    if (error) throw error;
    if (!wallet) return summary;

    const addresses = Array.from(
      new Set([wallet.usdt_bep20_address, wallet.usdc_bep20_address].filter(Boolean))
    );

    for (const address of addresses) {
      const transactions = await getBEP20Transactions(address);
      summary.checked += transactions.length;

      for (const tx of transactions) {
        try {
          const result = await processDeposit(userId, tx.amount, tx.transaction_id, tx.network, tx.to || address, tx.confirmed ? 1 : 0);
          if (!result?.success) throw new Error(result?.error || 'Deposit processing failed');
          if (result?.success) {
            if (result.already_processed) {
              summary.duplicates++;
            } else {
              summary.deposits++;
              // ВЫЗОВ АВТОСБОРА при "быстрой" проверке сразу после создания кошелька
              sweepDepositBEP20(userId, tx.token, tx.network).catch(console.error);
            }
          }
        } catch (err) {
          summary.errors++;
          console.error(`❌ Error processing transaction ${tx.transaction_id}:`, err.message);
        }
      }
    }

    return summary;
  } catch (error) {
    summary.success = false;
    summary.error = error.message;
    console.error('❌ checkUserBEP20Deposits error:', error.message);
    return summary;
  }
}

async function checkUserERC20Deposits(userId) {
  const summary = { success: true, network_group: 'erc20', checked: 0, deposits: 0, duplicates: 0, errors: 0 };

  try {
    const { data: wallet, error } = await supabase
      .from('deposit_wallets')
      .select('*')
      .eq('user_id', userId)
      .maybeSingle();

    if (error) throw error;
    if (!wallet) return summary;

    const addresses = Array.from(
      new Set([wallet.usdt_erc20_address, wallet.usdc_erc20_address].filter(Boolean))
    );

    for (const address of addresses) {
      const transactions = await getERC20Transactions(address);
      summary.checked += transactions.length;

      for (const tx of transactions) {
        try {
          const result = await processDeposit(userId, tx.amount, tx.transaction_id, tx.network, tx.to || address, tx.confirmed ? 1 : 0);
          if (!result?.success) throw new Error(result?.error || 'Deposit processing failed');
          if (result?.success) {
            if (result.already_processed) summary.duplicates++;
            else summary.deposits++;
          }
        } catch (err) {
          summary.errors++;
          console.error(`❌ Error processing transaction ${tx.transaction_id}:`, err.message);
        }
      }
    }

    return summary;
  } catch (error) {
    summary.success = false;
    summary.error = error.message;
    console.error('❌ checkUserERC20Deposits error:', error.message);
    return summary;
  }
}

// Compatibility with pixelnft-api: an empty body checks this user's three
// chain groups; an explicit network preserves the original single-group API.
async function checkUserRequestedNetworks(userId, network = '') {
  const jobs = [];
  if (!network || network.includes('bep20')) jobs.push(checkUserBEP20Deposits(userId));
  if (!network || network.includes('erc20')) jobs.push(checkUserERC20Deposits(userId));
  if (!network || network.includes('trc20')) jobs.push(checkUserTRC20Deposits(userId));
  const groups = await Promise.all(jobs);
  const failures = groups.filter(g => !g.success || g.errors > 0).map(g => ({
    network: g.network_group, error: g.error || 'DEPOSIT_PROCESSING_FAILED', count: g.errors
  }));
  return {
    success: failures.length === 0,
    checked: groups.reduce((sum, g) => sum + Number(g.checked || 0), 0),
    deposits: groups.reduce((sum, g) => sum + Number(g.deposits || 0), 0),
    duplicates: groups.reduce((sum, g) => sum + Number(g.duplicates || 0), 0),
    errors: groups.reduce((sum, g) => sum + Number(g.errors || 0), 0),
    failures, groups
  };
}

async function verifyAdminUnlock(req, user) {
  const { data, error } = await supabase.rpc('pixel_app_api', {
    p_user: user.id, p_session: req.pixelSession,
    p_action: 'admin.overview', p_payload: { admin_token: readParam(req, 'admin_token', '') }
  });
  return !error && !!data && !data.error;
}

// ========== HTTP ROUTES ==========
app.get('/', (req, res) => {
  res.json({
    status: '✅ SERVER IS RUNNING',
    message: 'Oracle Deposit Processing System',
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    environment: process.env.NODE_ENV || 'development',
    encryption: ENCRYPTION_KEY ? 'ENABLED' : 'DISABLED'
  });
});

app.get('/health', (req, res) => {
  res.json({
    status: '✅ HEALTHY',
    service: 'Oracle Deposit Processor',
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

// 1. Protected endpoint (API key required)
app.post('/api/deposit/generate', requireApiKey, async (req, res) => {
  const requestStarted = process.hrtime.bigint();
  try {
    const user_id = readParam(req, 'user_id');
    const network = String(readParam(req, 'network', 'usdt_bep20')).trim().toLowerCase();

    if (!user_id) {
      return res.status(400).json({ success: false, error: 'User ID is required' });
    }

    if (!allowedNetworks.includes(network)) {
      return res.status(400).json({ success: false, error: 'Unsupported network' });
    }

    console.log(`🔐 [SECURE] Generating ${network} wallet for user: ${user_id}, IP: ${req.ip}`);

    const walletStarted = process.hrtime.bigint();
    const result = await generateWalletSingleFlight(user_id, network);
    const walletMs = Number(process.hrtime.bigint() - walletStarted) / 1e6;

    if (result.exists) {
      runInBackground('deposit_address_sync', () => syncDepositAddress(user_id, result.network, result.address));
    } else {
      await syncDepositAddress(user_id, result.network, result.address);
    }

    const totalMs = Number(process.hrtime.bigint() - requestStarted) / 1e6;
    res.set('Server-Timing', `wallet;dur=${walletMs.toFixed(1)}, total;dur=${totalMs.toFixed(1)}`);
    res.set('X-Response-Time', `${totalMs.toFixed(1)}ms`);
    return res.json({ ...result, min_deposit: MIN_DEPOSIT, timing_ms: Math.round(totalMs) });
  } catch (error) {
    console.error('❌ API Generate wallet error:', error.message);
    return res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

// 2. Public/app endpoint
// Requires Bearer auth and resolves user only from the token.
app.post('/public/deposit/generate', async (req, res) => {
  const requestStarted = process.hrtime.bigint();
  try {
    const network = String(readParam(req, 'network', 'usdt_bep20')).trim().toLowerCase();
    const authStarted = process.hrtime.bigint();
    const bearerUser = await getUserFromBearerToken(req);
    const authMs = Number(process.hrtime.bigint() - authStarted) / 1e6;

    console.log('🔓 [PUBLIC] Deposit generation request:', {
      resolved_user_id: bearerUser?.id || null,
      network,
      ip: req.ip,
      timestamp: new Date().toISOString(),
      bearer_auth: !!bearerUser
    });

    if (!bearerUser?.id) {
      return res.status(401).json({ success: false, error: 'Auth required' });
    }

    const user_id = bearerUser.id;

    if (!allowedNetworks.includes(network)) {
      return res.status(400).json({ success: false, error: 'Unsupported network' });
    }

    if (req.pixelState?.config?.deposits_enabled === false) {
      return res.status(403).json({ success: false, error: 'DEPOSITS_PAUSED' });
    }
    const walletStarted = process.hrtime.bigint();
    const result = await generateWalletSingleFlight(user_id, network);
    const walletMs = Number(process.hrtime.bigint() - walletStarted) / 1e6;

    const syncStarted = process.hrtime.bigint();
    if (result.exists) {
      runInBackground('public_deposit_address_sync', () => syncDepositAddress(user_id, result.network, result.address));
    } else {
      await syncDepositAddress(user_id, result.network, result.address);
    }
    const syncMs = result.exists ? 0 : Number(process.hrtime.bigint() - syncStarted) / 1e6;

    runInBackground('public_deposit_generated', () => safeSystemLog('public_deposit_generated', `Public deposit address generated for user ${user_id}`, {
      user_id,
      network,
      address: result.address,
      ip: req.ip,
      bearer_auth: true
    }));

    const totalMs = Number(process.hrtime.bigint() - requestStarted) / 1e6;
    res.set('Cache-Control', 'private, no-store');
    res.set('Server-Timing', `auth;dur=${authMs.toFixed(1)}, wallet;dur=${walletMs.toFixed(1)}, sync;dur=${syncMs.toFixed(1)}, total;dur=${totalMs.toFixed(1)}`);
    res.set('X-Response-Time', `${totalMs.toFixed(1)}ms`);

    return res.json({
      success: true,
      address: result.address,
      wallet: result.wallet,
      network: result.network,
      exists: result.exists,
      min_deposit: MIN_DEPOSIT,
      timing_ms: Math.round(totalMs)
    });
  } catch (error) {
    console.error('❌ [PUBLIC] Error:', error.message);

    runInBackground('public_deposit_error', () => safeSystemLog('public_deposit_error', `Public deposit error: ${error.message}`, {
      error: error.message,
      ip: req.ip,
      body: req.body || null,
      query: req.query || null
    }));

    return res.status(500).json({ success: false, error: 'Internal server error' });
  }
});


// 3. Public/app endpoint: user manually checks only their own deposit address.
// Requires Bearer auth and resolves user only from the token.
app.post('/public/deposit/check', userDepositCheckCooldownMiddleware, async (req, res) => {
  try {
    const network = String(readParam(req, 'network', '') || '').trim().toLowerCase();
    const bearerUser = await getUserFromBearerToken(req);

    console.log('🔎 [PUBLIC] User deposit check request:', {
      resolved_user_id: bearerUser?.id || null,
      network,
      ip: req.ip,
      timestamp: new Date().toISOString(),
      bearer_auth: !!bearerUser
    });

    if (!bearerUser?.id) {
      return res.status(401).json({ success: false, error: 'Auth required' });
    }

    if (network && !allowedNetworks.includes(network)) {
      return res.status(400).json({ success: false, error: 'Unsupported network' });
    }

    const user_id = bearerUser.id;
    if (req.pixelState?.config?.deposits_enabled === false) {
      return res.status(403).json({ success: false, error: 'DEPOSITS_PAUSED' });
    }
    const result = await checkUserRequestedNetworks(user_id, network);

    await safeSystemLog('public_deposit_check', `User triggered deposit check for ${user_id}`, {
      user_id,
      network,
      result,
      ip: req.ip
    });

    return res.json({
      success: result.success,
      found: Number(result?.deposits || 0) > 0,
      network: network || 'all',
      checked: result.checked,
      deposits: result.deposits,
      errors: result.failures,
      result: result || null,
      message: Number(result?.deposits || 0) > 0
        ? 'Deposit found and credited'
        : 'Deposit not found yet'
    });
  } catch (error) {
    console.error('❌ [PUBLIC] Deposit check error:', error.message);

    await safeSystemLog('public_deposit_check_error', `Public deposit check error: ${error.message}`, {
      error: error.message,
      ip: req.ip,
      body: req.body || null,
      query: req.query || null
    });

    return res.status(500).json({ success: false, error: 'Internal server error' });
  }
});


// 4. Public/app endpoint: returns only the authenticated user's deposits.
app.get('/public/deposit/history', async (req, res) => {
  try {
    const bearerUser = await getUserFromBearerToken(req);
    if (!bearerUser?.id) {
      return res.status(401).json({ success: false, error: 'Auth required' });
    }

    const network = String(readParam(req, 'network', '') || '').trim().toLowerCase();
    let query = supabase
      .from('deposits')
      .select('id, asset, network, tx_hash, address, amount, status, confirmations, created_at, completed_at')
      .eq('user_id', bearerUser.id)
      .order('created_at', { ascending: false })
      .limit(50);

    if (network) query = query.eq('network', network);

    const { data, error } = await query;
    if (error) throw error;

    return res.json({ success: true, deposits: data || [] });
  } catch (error) {
    console.error('❌ Public deposit history error:', error.message);
    return res.status(500).json({ success: false, error: 'Failed to fetch deposit history' });
  }
});

// 4. Public/admin endpoint: existing all-wallet check with PixelNFT admin unlock.
// This keeps API_SECRET_KEY on the server and never exposes it to frontend.
app.post('/public/admin/check-deposits', adminDepositCheckCooldownMiddleware, async (req, res) => {
  try {
    const bearerUser = await getUserFromBearerToken(req);

    if (!bearerUser?.id) {
      return res.status(401).json({ success: false, error: 'Auth required' });
    }

    const { data: profile, error: profileError } = await supabase
      .from('profiles')
      .select('*')
      .eq('id', bearerUser.id)
      .maybeSingle();

    if (profileError) {
      console.error('❌ [ADMIN] Profile lookup error:', profileError.message);
      return res.status(500).json({ success: false, error: 'Profile lookup failed' });
    }

    const isAdmin = profile?.status === 'active' && ['admin', 'owner'].includes(profile?.role);

    if (!isAdmin) {
      console.warn('🚫 [ADMIN] Deposit check blocked for non-admin:', bearerUser.id);
      return res.status(403).json({ success: false, error: 'Admin only' });
    }

    if (!await verifyAdminUnlock(req, bearerUser)) {
      return res.status(403).json({ success: false, error: 'ADMIN_UNLOCK_REQUIRED' });
    }

    console.log('🔄 [ADMIN] Manual all-deposit check triggered:', {
      admin_id: bearerUser.id,
      ip: req.ip,
      timestamp: new Date().toISOString()
    });

    const bep20Result = await handleCheckBEP20Deposits();
    const erc20Result = await handleCheckERC20Deposits();
    const trc20Result = await handleCheckTRC20Deposits();

    await safeSystemLog('admin_deposit_check', `Admin triggered all-deposit check`, {
      admin_id: bearerUser.id,
      bep20: bep20Result,
      erc20: erc20Result,
      trc20: trc20Result,
      ip: req.ip
    });

    return res.json({
      success: true,
      bep20: bep20Result,
      erc20: erc20Result,
      trc20: trc20Result
    });
  } catch (error) {
    console.error('❌ [ADMIN] Deposit check error:', error.message);
    return res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

// The existing PixelNFT admin panel calls this path for one selected user.
app.post('/admin/deposit/check', adminDepositCheckCooldownMiddleware, async (req, res) => {
  try {
    const user = await getUserFromBearerToken(req);
    if (!user) return res.status(401).json({ success: false, error: 'Auth required' });
    if (!await verifyAdminUnlock(req, user)) {
      return res.status(403).json({ success: false, error: 'ADMIN_UNLOCK_REQUIRED' });
    }
    const userId = String(readParam(req, 'user_id', ''));
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(userId)) {
      return res.status(400).json({ success: false, error: 'USER_NOT_FOUND' });
    }
    const result = await checkUserRequestedNetworks(userId);
    await safeSystemLog('admin_deposit_check', 'Admin checked selected user deposits', {
      admin_id: user.id, user_id: userId, result
    });
    return res.json({ ...result, user_id: userId, errors: result.failures, found: result.deposits > 0 });
  } catch (error) {
    console.error('❌ [ADMIN] Selected user deposit check error:', error.message);
    return res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

app.get('/api/deposit/history', requireApiKey, async (req, res) => {
  try {
    const user_id = readParam(req, 'user_id');
    const network = readParam(req, 'network');

    if (!user_id) {
      return res.status(400).json({ success: false, error: 'User ID is required' });
    }

    let query = supabase
      .from('deposits')
      .select('*')
      .eq('user_id', user_id)
      .order('created_at', { ascending: false })
      .limit(50);

    if (network) {
      query = query.eq('network', network);
    }

    const { data: deposits, error } = await query;

    if (error) {
      console.error('❌ Database error:', error.message);
      return res.status(500).json({ success: false, error: 'Failed to fetch deposit history' });
    }

    return res.json({ success: true, deposits: deposits || [] });
  } catch (error) {
    console.error('❌ Deposit history error:', error.message);
    return res.status(500).json({ success: false, error: 'Internal server error' });
  }
});

app.get('/api/check-deposits', requireApiKey, async (req, res) => {
  try {
    console.log('🔄 [SECURE] Manual deposit check triggered via API');
    const bep20Result = await handleCheckBEP20Deposits();
    const erc20Result = await handleCheckERC20Deposits();
    const trc20Result = await handleCheckTRC20Deposits();

    return res.json({
      success: true,
      bep20: bep20Result,
      erc20: erc20Result,
      trc20: trc20Result
    });
  } catch (error) {
    return res.status(500).json({ success: false, error: error.message });
  }
});

// ========== START SERVER ==========
const server = app.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 HTTP SERVER RUNNING on port ${PORT}`);
  console.log(`✅ Health check available at: http://0.0.0.0:${PORT}/health`);
  console.log(`✅ API Health check: http://0.0.0.0:${PORT}/api/health`);
  console.log(`✅ PUBLIC Endpoint: POST http://0.0.0.0:${PORT}/public/deposit/generate`);
  console.log(`✅ PUBLIC Endpoint: POST http://0.0.0.0:${PORT}/public/deposit/check`);
  console.log(`✅ ADMIN Endpoint:  POST http://0.0.0.0:${PORT}/public/admin/check-deposits`);
  console.log(`✅ SECURE Endpoint: POST http://0.0.0.0:${PORT}/api/deposit/generate (requires API key)`);
  console.log(`✅ SECURE Endpoint: GET  http://0.0.0.0:${PORT}/api/deposit/history (requires API key)`);
  console.log(`✅ SECURE Endpoint: GET  http://0.0.0.0:${PORT}/api/check-deposits (requires API key)`);
  console.log(`✅ RATE LIMIT: 60 requests per 15 minutes per IP`);
  console.log(`✅ SUPABASE DEPOSIT STORAGE: CONFIGURED (connection verified on first request)`);
  console.log(`✅ MORALIS: ${MORALIS_API_KEY ? 'API KEY SET' : 'API KEY MISSING'}`);
  console.log(`✅ BEP20 (USDT & USDC): Checking every ${BEP20_CHECK_INTERVAL} ms`);
  console.log(`✅ ERC20 (USDT & USDC): Checking every ${ERC20_CHECK_INTERVAL} ms`);
  console.log(`✅ TRC20 (USDT): Checking every ${TRC20_CHECK_INTERVAL} ms`);
  console.log(`✅ MINIMUM DEPOSIT: $${MIN_DEPOSIT}`);
  console.log(`✅ PRIVATE KEY ENCRYPTION: ${ENCRYPTION_KEY ? 'AES-256-GCM ENABLED' : 'DISABLED'}`);
  console.log(`✅ ATOMIC DEPOSITS: ENABLED`);
  console.log(`✅ AUTO-SWEEP BEP20: ${HOT_WALLET_PRIVATE_KEY && ADMIN_SWEEP_ADDRESS ? 'ENABLED' : 'DISABLED'}`);
  console.log(`✅ SECURITY: Public endpoints DO NOT return private keys`);
  console.log('===================================');
});

// ========== BACKGROUND TASKS ==========
let isCheckingBEP20 = false;
let isCheckingERC20 = false;
let isCheckingTRC20 = false;

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

setInterval(async () => {
  if (isCheckingERC20) return;

  try {
    isCheckingERC20 = true;
    await handleCheckERC20Deposits();
  } catch (err) {
    console.error('❌ ERC20 auto-check error:', err.message);
  } finally {
    isCheckingERC20 = false;
  }
}, ERC20_CHECK_INTERVAL);

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
