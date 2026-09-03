'use strict';

// PixelNFT Deposit Service v20 — standalone CommonJS build.
// This is the user's original server file adapted to the v20 database contract.
// It never writes financial or wallet tables directly: all critical mutations go
// through the SECURITY DEFINER RPC functions installed by PixelNFT-mobile-v20.

const http = require('node:http');
const crypto = require('node:crypto');
const { createClient } = require('@supabase/supabase-js');
const ethers = require('ethers');

const VERSION = '20.1.0';
const MIN_DEPOSIT = 30;
const MAX_DEPOSIT = 100000;
const TRANSFER_TOPIC = ethers.id('Transfer(address,address,uint256)').toLowerCase();
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const NETWORK_FIELDS = Object.freeze({
  usdt_bep20: 'usdt_bep20_address',
  usdc_bep20: 'usdc_bep20_address',
  usdt_erc20: 'usdt_erc20_address',
  usdc_erc20: 'usdc_erc20_address',
  usdt_trc20: 'usdt_trc20_address'
});

const CANONICAL_NETWORKS = Object.freeze([
  Object.freeze({ key: 'usdt_trc20', asset: 'USDT', chain: 'TRC20' }),
  Object.freeze({ key: 'usdt_bep20', asset: 'USDT', chain: 'BEP20' }),
  Object.freeze({ key: 'usdc_bep20', asset: 'USDC', chain: 'BEP20' }),
  Object.freeze({ key: 'usdt_erc20', asset: 'USDT', chain: 'ERC20' }),
  Object.freeze({ key: 'usdc_erc20', asset: 'USDC', chain: 'ERC20' })
]);

const RPC = Object.freeze({
  app: 'pixel_app_api',
  rateLimit: 'pixel_rate_limit',
  registerWallets: 'register_deposit_wallets',
  claimScan: 'deposit_scan_claim',
  checkpointScan: 'deposit_scan_checkpoint',
  creditDeposit: 'credit_chain_deposit',
  keyEnvelope: 'deposit_key_envelope'
});

class DepositError extends Error {
  constructor(code, status = 503) {
    super(code);
    this.code = code;
    this.status = status;
  }
}

function fail(code, status) {
  throw new DepositError(code, status);
}

function statusForCode(code) {
  if (code === 'SESSION_INVALID') return 401;
  if (['ADMIN_REQUIRED', 'ADMIN_2FA_REQUIRED', 'ORIGIN_DENIED'].includes(code)) return 403;
  if (code === 'RATE_LIMITED') return 429;
  if (['DATABASE_ERROR', 'DATABASE_UNAVAILABLE', 'CHAIN_UNAVAILABLE', 'CHAIN_RPC_ERROR', 'SERVICE_UNAVAILABLE'].includes(code)) return 503;
  return 400;
}

function databaseCode(error) {
  const text = [error?.message, error?.details, error?.hint].filter(Boolean).join(' ');
  const exact = text.trim();
  if (/^[A-Z][A-Z0-9_]{2,80}$/.test(exact)) return exact;
  const match = text.match(/\b[A-Z][A-Z0-9_]{2,80}\b/);
  return match ? match[0] : 'DATABASE_ERROR';
}

function integer(value) {
  if (value === null || value === undefined || value === '' || typeof value === 'boolean') fail('CHAIN_DATA_INVALID');
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number < 0) fail('CHAIN_DATA_INVALID');
  return number;
}

function hex(number) {
  return `0x${BigInt(number).toString(16)}`;
}

function amount6(raw, decimals) {
  if (!Number.isInteger(decimals) || decimals < 6 || decimals > 18) fail('TOKEN_DECIMALS_INVALID');
  let value;
  try {
    value = BigInt(raw);
  } catch {
    fail('CHAIN_DATA_INVALID');
  }
  if (value < 0n) fail('CHAIN_DATA_INVALID');
  const micro = value / (10n ** BigInt(decimals - 6));
  if (micro > 1000000000000000n) fail('DEPOSIT_TOO_LARGE');
  return `${micro / 1000000n}.${String(micro % 1000000n).padStart(6, '0')}`;
}

function depositAmountAllowed(amount) {
  if (!/^\d+\.\d{6}$/.test(amount || '')) fail('CHAIN_DATA_INVALID');
  const [whole, fraction] = amount.split('.');
  const micro = BigInt(whole) * 1000000n + BigInt(fraction);
  return micro >= BigInt(MIN_DEPOSIT) * 1000000n && micro <= BigInt(MAX_DEPOSIT) * 1000000n;
}

const BASE58_ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
const sha256 = (value) => crypto.createHash('sha256').update(value).digest();

function base58Encode(buffer) {
  let value = BigInt(`0x${buffer.toString('hex')}`);
  let result = '';
  while (value > 0n) {
    result = BASE58_ALPHABET[Number(value % 58n)] + result;
    value /= 58n;
  }
  for (const byte of buffer) {
    if (byte !== 0) break;
    result = `1${result}`;
  }
  return result || '1';
}

function tronAddress(evmAddress) {
  if (!/^0x[0-9a-f]{40}$/i.test(evmAddress || '')) fail('ADDRESS_INVALID');
  const bytes = Buffer.from(`41${evmAddress.slice(2)}`, 'hex');
  return base58Encode(Buffer.concat([bytes, sha256(sha256(bytes)).subarray(0, 4)]));
}

function tronHex(address) {
  if (!/^T[1-9A-HJ-NP-Za-km-z]{33}$/.test(address || '')) fail('ADDRESS_INVALID');
  let value = 0n;
  for (const character of address) {
    const index = BASE58_ALPHABET.indexOf(character);
    if (index < 0) fail('ADDRESS_INVALID');
    value = value * 58n + BigInt(index);
  }
  const raw = Buffer.from(value.toString(16).padStart(50, '0'), 'hex');
  if (
    raw.length !== 25 ||
    raw[0] !== 0x41 ||
    !crypto.timingSafeEqual(sha256(sha256(raw.subarray(0, 21))).subarray(0, 4), raw.subarray(21))
  ) fail('ADDRESS_INVALID');
  return raw.subarray(1, 21).toString('hex');
}

function encryptionKey(value) {
  if (/^[0-9a-f]{64}$/i.test(value || '')) return Buffer.from(value, 'hex');
  // Backward-compatible support for the old ENCRYPTION_KEY variable. New
  // deployments should always use a stable 64-hex WALLET_ENCRYPTION_KEY.
  if (typeof value === 'string' && Buffer.byteLength(value, 'utf8') >= 32) return sha256(value);
  fail('WALLET_ENCRYPTION_KEY_REQUIRED');
}

function encryptPrivateKey(privateKey, key) {
  const text = String(privateKey || '').replace(/^0x/, '').toLowerCase();
  if (!/^[0-9a-f]{64}$/.test(text)) fail('KEY_INVALID');
  const nonce = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv('aes-256-gcm', key, nonce);
  const encrypted = Buffer.concat([cipher.update(text, 'utf8'), cipher.final()]);
  return ['v1', nonce.toString('hex'), cipher.getAuthTag().toString('hex'), encrypted.toString('hex')].join(':');
}

function decryptPrivateKey(envelope, key) {
  if (!/^v1:[a-f0-9]{24}:[a-f0-9]{32}:[a-f0-9]{128,}$/.test(envelope || '')) fail('KEY_ENVELOPE_INVALID');
  const [, iv, tag, data] = envelope.split(':');
  const decipher = crypto.createDecipheriv('aes-256-gcm', key, Buffer.from(iv, 'hex'));
  decipher.setAuthTag(Buffer.from(tag, 'hex'));
  const plain = Buffer.concat([decipher.update(Buffer.from(data, 'hex')), decipher.final()]).toString('utf8');
  if (!/^[0-9a-f]{64}$/.test(plain)) fail('KEY_INVALID');
  return `0x${plain}`;
}

function generateWalletInputs(userId, key) {
  const evm = ethers.Wallet.createRandom();
  const tron = ethers.Wallet.createRandom();
  return {
    p_user_id: userId,
    p_evm_address: evm.address,
    p_evm_key: encryptPrivateKey(evm.privateKey, key),
    p_tron_address: tronAddress(tron.address),
    p_tron_key: encryptPrivateKey(tron.privateKey, key)
  };
}

function transferLog(log, network, address) {
  if (log?.removed === true) return null;
  const contract = network.chain === 'TRC20' ? tronHex(network.contract) : network.contract.slice(2).toLowerCase();
  const recipient = network.chain === 'TRC20' ? tronHex(address) : address.slice(2).toLowerCase();
  const actual = String(log?.address || '').replace(/^0x/, '').toLowerCase().replace(/^41(?=[a-f0-9]{40}$)/, '');
  if (actual !== contract || !Array.isArray(log?.topics) || log.topics.length !== 3) return null;
  const topics = log.topics.map((topic) => `0x${String(topic).replace(/^0x/, '').toLowerCase()}`);
  if (topics[0] !== TRANSFER_TOPIC || topics[2] !== `0x${recipient.padStart(64, '0')}`) return null;
  const data = String(log.data || '').replace(/^0x/, '');
  if (!/^[0-9a-f]{64}$/i.test(data)) fail('CHAIN_DATA_INVALID');
  return amount6(`0x${data}`, network.decimals);
}

function sessionId(token) {
  try {
    const payload = JSON.parse(Buffer.from(token.split('.')[1], 'base64url').toString('utf8'));
    if (UUID_RE.test(payload.session_id || '')) return payload.session_id;
  } catch {
    // handled below
  }
  fail('SESSION_INVALID', 401);
}

function booleanEnv(value, fallback) {
  if (value === undefined || value === null || value === '') return fallback;
  return /^(1|true|yes|on)$/i.test(String(value));
}

function httpsUrl(value, required = true) {
  if (!value && !required) return null;
  let parsed;
  try {
    parsed = new URL(value);
  } catch {
    fail('URL_INVALID');
  }
  if (parsed.protocol !== 'https:') fail('HTTPS_REQUIRED');
  return parsed.toString();
}

function settingsFromEnv(env) {
  const walletKeySource = env.WALLET_ENCRYPTION_KEY || env.ENCRYPTION_KEY;
  encryptionKey(walletKeySource);
  const config = {
    SUPABASE_URL: env.SUPABASE_URL || 'https://fkjwueogfmdolcjtvvme.supabase.co',
    SUPABASE_SERVICE_ROLE_KEY: env.SUPABASE_SERVICE_ROLE_KEY,
    WALLET_ENCRYPTION_KEY: walletKeySource,
    BSC_RPC_URL: env.BSC_RPC_URL,
    ETH_RPC_URL: env.ETH_RPC_URL,
    TRON_RPC_URL: env.TRON_RPC_URL || 'https://api.trongrid.io',
    TRONGRID_API_KEY: env.TRONGRID_API_KEY || '',
    API_SECRET_KEY: env.API_SECRET_KEY || '',
    HOT_WALLET_PRIVATE_KEY: env.HOT_WALLET_PRIVATE_KEY || '',
    ADMIN_SWEEP_ADDRESS: env.ADMIN_SWEEP_ADDRESS || '',
    PORT: Number(env.PORT || 8080),
    HOST: env.HOST || '0.0.0.0',
    EVM_BLOCK_RANGE: Number(env.EVM_BLOCK_RANGE || 1000),
    BACKGROUND_SCAN_ENABLED: booleanEnv(env.BACKGROUND_SCAN_ENABLED, true),
    BACKGROUND_SCAN_INTERVAL_MS: Number(env.BACKGROUND_SCAN_INTERVAL_MS || 120000),
    BACKGROUND_BATCH_SIZE: Number(env.BACKGROUND_BATCH_SIZE || 50)
  };

  if (!config.SUPABASE_SERVICE_ROLE_KEY) fail('SERVICE_ROLE_KEY_REQUIRED');
  if (!config.BSC_RPC_URL || !config.ETH_RPC_URL) fail('CHAIN_NOT_CONFIGURED');
  config.SUPABASE_URL = httpsUrl(config.SUPABASE_URL);
  config.BSC_RPC_URL = httpsUrl(config.BSC_RPC_URL);
  config.ETH_RPC_URL = httpsUrl(config.ETH_RPC_URL);
  config.TRON_RPC_URL = httpsUrl(config.TRON_RPC_URL);

  if (!Number.isInteger(config.PORT) || config.PORT < 1 || config.PORT > 65535) fail('CONFIG_INVALID');
  if (!Number.isInteger(config.EVM_BLOCK_RANGE) || config.EVM_BLOCK_RANGE < 1 || config.EVM_BLOCK_RANGE > 5000) fail('CONFIG_INVALID');
  if (!Number.isInteger(config.BACKGROUND_SCAN_INTERVAL_MS) || config.BACKGROUND_SCAN_INTERVAL_MS < 60000) fail('CONFIG_INVALID');
  if (!Number.isInteger(config.BACKGROUND_BATCH_SIZE) || config.BACKGROUND_BATCH_SIZE < 1 || config.BACKGROUND_BATCH_SIZE > 200) fail('CONFIG_INVALID');
  if ((config.HOT_WALLET_PRIVATE_KEY && !config.ADMIN_SWEEP_ADDRESS) || (!config.HOT_WALLET_PRIVATE_KEY && config.ADMIN_SWEEP_ADDRESS)) fail('SWEEP_CONFIG_INCOMPLETE');
  if (config.HOT_WALLET_PRIVATE_KEY) {
    try {
      const hot = new ethers.Wallet(config.HOT_WALLET_PRIVATE_KEY);
      if (!ethers.isAddress(config.ADMIN_SWEEP_ADDRESS) || hot.address.toLowerCase() === config.ADMIN_SWEEP_ADDRESS.toLowerCase()) fail('SWEEP_CONFIG_INVALID');
    } catch (error) {
      if (error instanceof DepositError) throw error;
      fail('SWEEP_CONFIG_INVALID');
    }
  }
  return config;
}

function chainAccess(settings, fetchImpl = fetch) {
  let requestId = 0;

  async function json(url, options, deadline) {
    const remaining = Math.min(9000, deadline - Date.now());
    if (remaining < 100) fail('SCAN_INCOMPLETE');
    let response;
    try {
      response = await fetchImpl(url, { ...options, redirect: 'error', signal: AbortSignal.timeout(remaining) });
    } catch {
      fail('CHAIN_UNAVAILABLE');
    }
    if (response.status === 429) fail('CHAIN_RATE_LIMITED');
    if (!response.ok) fail('CHAIN_UNAVAILABLE');
    try {
      return await response.json();
    } catch {
      fail('CHAIN_DATA_INVALID');
    }
  }

  return {
    async evm(chain, method, params, deadline) {
      const url = chain === 'ERC20' ? settings.ETH_RPC_URL : settings.BSC_RPC_URL;
      const data = await json(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ jsonrpc: '2.0', id: ++requestId, method, params })
      }, deadline);
      if (data.error) {
        if (method === 'eth_getLogs' && /range|too many|limit exceeded|response size/i.test(data.error.message || '')) fail('LOG_RANGE_TOO_LARGE');
        fail('CHAIN_RPC_ERROR');
      }
      if (data.result === undefined) fail('CHAIN_DATA_INVALID');
      return data.result;
    },
    async tron(path, body, deadline) {
      const headers = { 'Content-Type': 'application/json' };
      if (settings.TRONGRID_API_KEY) headers['TRON-PRO-API-KEY'] = settings.TRONGRID_API_KEY;
      const options = body === null ? { method: 'GET', headers } : { method: 'POST', headers, body: JSON.stringify(body || {}) };
      return json(new URL(path, settings.TRON_RPC_URL), options, deadline);
    }
  };
}

function createScanners({ rpc, chain, maxBlocks = 1000, onCredit = null }) {
  const checkpoint = (user, network, lease, fields) => rpc(RPC.checkpointScan, {
    p_user_id: user,
    p_network: network.key,
    p_lease: lease,
    p_block: null,
    p_timestamp: null,
    p_release: false,
    p_error: null,
    p_cursor: null,
    ...fields
  });
  const credit = (user, network, address, amount, txHash, eventIndex, confirmations) => rpc(RPC.creditDeposit, {
    p_user_id: user,
    p_amount: amount,
    p_network: network.key,
    p_tx_hash: txHash,
    p_address: address,
    p_confirmations: confirmations,
    p_event_index: eventIndex
  });

  function notifyCredit(user, network, address, result) {
    if (!onCredit || result?.duplicate) return;
    Promise.resolve(onCredit({ user, network, address, result })).catch((error) => {
      console.error('BEP20 sweep scheduling failed:', error.message);
    });
  }

  async function firstBlock(network, createdAt, head, deadline) {
    const time = Math.floor(Date.parse(createdAt) / 1000) - 120;
    if (!Number.isFinite(time)) fail('WALLET_TIMESTAMP_INVALID');
    let low = 0;
    let high = head;
    while (low < high) {
      const middle = Math.floor((low + high) / 2);
      const block = await chain.evm(network.chain, 'eth_getBlockByNumber', [hex(middle), false], deadline);
      if (!block) fail('CHAIN_HISTORY_UNAVAILABLE');
      if (integer(block.timestamp) < time) low = middle + 1;
      else high = middle;
    }
    return Math.max(0, low - 1);
  }

  async function evmScan(user, network, address, createdAt, scanState, deadline) {
    const chainId = integer(await chain.evm(network.chain, 'eth_chainId', [], deadline));
    if (chainId !== (network.chain === 'ERC20' ? 1 : 56)) fail('WRONG_CHAIN');
    const decimals = integer(await chain.evm(network.chain, 'eth_call', [{ to: network.contract, data: '0x313ce567' }, 'latest'], deadline));
    if (decimals !== network.decimals) fail('TOKEN_DECIMALS_INVALID');

    const head = integer(await chain.evm(network.chain, 'eth_blockNumber', [], deadline));
    const lastConfirmed = Math.max(0, head - network.min_confirmations + 1);
    let from = scanState.last_block === null || scanState.last_block === undefined
      ? await firstBlock(network, createdAt, lastConfirmed, deadline)
      : integer(scanState.last_block) + 1;
    let size = maxBlocks;
    let credited = 0;
    let duplicates = 0;
    let through = from - 1;
    const rejected = [];
    const recipientTopic = `0x${address.slice(2).toLowerCase().padStart(64, '0')}`;
    const receipts = new Map();
    const blocks = new Map();

    while (from <= lastConfirmed && Date.now() < deadline - 2500) {
      const to = Math.min(lastConfirmed, from + size - 1);
      let logs;
      try {
        logs = await chain.evm(network.chain, 'eth_getLogs', [{
          address: network.contract,
          topics: [TRANSFER_TOPIC, null, recipientTopic],
          fromBlock: hex(from),
          toBlock: hex(to)
        }], deadline);
      } catch (error) {
        if (error.code === 'LOG_RANGE_TOO_LARGE' && size > 1) {
          size = Math.max(1, Math.floor(size / 2));
          continue;
        }
        throw error;
      }
      if (!Array.isArray(logs)) fail('CHAIN_DATA_INVALID');

      for (const log of logs) {
        const amount = transferLog(log, network, address);
        if (amount === null) continue;
        const blockNumber = integer(log.blockNumber);
        const eventIndex = integer(log.logIndex);
        const txHash = String(log.transactionHash || '').toLowerCase();
        if (blockNumber < from || blockNumber > to || eventIndex > 2147483647 || !/^0x[0-9a-f]{64}$/.test(txHash)) fail('CHAIN_DATA_INVALID');

        if (!receipts.has(txHash)) receipts.set(txHash, await chain.evm(network.chain, 'eth_getTransactionReceipt', [txHash], deadline));
        const receipt = receipts.get(txHash);
        if (!receipt || integer(receipt.status) !== 1 || integer(receipt.blockNumber) !== blockNumber || String(receipt.transactionHash).toLowerCase() !== txHash) fail('RECEIPT_UNCONFIRMED');

        if (!blocks.has(blockNumber)) blocks.set(blockNumber, await chain.evm(network.chain, 'eth_getBlockByNumber', [hex(blockNumber), false], deadline));
        const block = blocks.get(blockNumber);
        if (!block?.hash || String(block.hash).toLowerCase() !== String(receipt.blockHash).toLowerCase() || String(log.blockHash).toLowerCase() !== String(block.hash).toLowerCase()) fail('CHAIN_REORG_RETRY');
        const proven = receipt.logs?.find((item) => integer(item.logIndex) === eventIndex && transferLog(item, network, address) === amount);
        if (!proven) fail('RECEIPT_LOG_MISMATCH');

        if (!depositAmountAllowed(amount)) {
          rejected.push({ tx_hash: txHash, event_index: eventIndex, amount, code: 'DEPOSIT_AMOUNT_OUT_OF_RANGE' });
          continue;
        }
        const result = await credit(user, network, address, amount, txHash, eventIndex, head - blockNumber + 1);
        if (result.duplicate) duplicates += 1;
        else {
          credited += 1;
          notifyCredit(user, network, address, result);
        }
      }
      through = to;
      await checkpoint(user, network, scanState.lease_token, { p_block: through });
      from = to + 1;
    }

    await checkpoint(user, network, scanState.lease_token, { p_release: true });
    return { network: network.key, credited, duplicates, rejected, caught_up: from > lastConfirmed, through_block: through };
  }

  async function tronScan(user, network, address, createdAt, scanState, deadline) {
    tronHex(address);
    tronHex(network.contract);
    const tip = await chain.tron('/walletsolidity/getnowblock', {}, deadline);
    const header = tip?.block_header?.raw_data;
    if (!header) fail('CHAIN_DATA_INVALID');
    const head = integer(header.number);
    const timestamp = integer(header.timestamp);
    let window;
    try {
      window = scanState.cursor ? JSON.parse(scanState.cursor) : null;
    } catch {
      fail('SCAN_CURSOR_INVALID');
    }
    if (window && (
      !Number.isSafeInteger(window.min) || !Number.isSafeInteger(window.max) ||
      window.min < 0 || window.max < window.min || typeof window.fingerprint !== 'string' ||
      window.fingerprint.length > 3500
    )) fail('SCAN_CURSOR_INVALID');
    if (!window) {
      const start = scanState.last_timestamp === null || scanState.last_timestamp === undefined
        ? Math.max(0, Date.parse(createdAt) - 120000)
        : Math.max(0, integer(scanState.last_timestamp) - 300000);
      if (!Number.isSafeInteger(start)) fail('WALLET_TIMESTAMP_INVALID');
      window = { min: start, max: timestamp, fingerprint: '' };
    }

    let credited = 0;
    let duplicates = 0;
    let caughtUp = false;
    const rejected = [];
    while (Date.now() < deadline - 2500) {
      const params = new URLSearchParams({
        only_confirmed: 'true',
        only_to: 'true',
        limit: '200',
        order_by: 'block_timestamp,asc',
        contract_address: network.contract,
        min_timestamp: String(window.min),
        max_timestamp: String(window.max)
      });
      if (window.fingerprint) params.set('fingerprint', window.fingerprint);
      const page = await chain.tron(`/v1/accounts/${encodeURIComponent(address)}/transactions/trc20?${params}`, null, deadline);
      if (page.success === false || !Array.isArray(page.data)) fail('CHAIN_DATA_INVALID');
      const transactionIds = [...new Set(page.data
        .filter((item) => item.type === 'Transfer' && item.to === address && item.token_info?.address === network.contract)
        .map((item) => item.transaction_id))];

      for (const txHash of transactionIds) {
        if (!/^[0-9a-f]{64}$/i.test(txHash || '')) fail('CHAIN_DATA_INVALID');
        const receipt = await chain.tron('/walletsolidity/gettransactioninfobyid', { value: txHash }, deadline);
        if (!receipt?.id || receipt.id.toLowerCase() !== txHash.toLowerCase() || receipt.receipt?.result !== 'SUCCESS' || !Array.isArray(receipt.log)) fail('RECEIPT_UNCONFIRMED');
        const blockNumber = integer(receipt.blockNumber);
        const confirmations = head - blockNumber + 1;
        if (confirmations < network.min_confirmations) fail('RECEIPT_UNCONFIRMED');
        for (let eventIndex = 0; eventIndex < receipt.log.length; eventIndex += 1) {
          const amount = transferLog(receipt.log[eventIndex], network, address);
          if (amount === null) continue;
          if (!depositAmountAllowed(amount)) {
            rejected.push({ tx_hash: txHash.toLowerCase(), event_index: eventIndex, amount, code: 'DEPOSIT_AMOUNT_OUT_OF_RANGE' });
            continue;
          }
          const result = await credit(user, network, address, amount, txHash, eventIndex, confirmations);
          if (result.duplicate) duplicates += 1;
          else credited += 1;
        }
      }

      if (page.meta?.links?.next) {
        const next = page.meta?.fingerprint;
        if (typeof next !== 'string' || !next || next.length > 3500 || next === window.fingerprint) fail('SCAN_CURSOR_INVALID');
        window.fingerprint = next;
        await checkpoint(user, network, scanState.lease_token, { p_cursor: JSON.stringify(window) });
      } else {
        await checkpoint(user, network, scanState.lease_token, { p_timestamp: window.max, p_cursor: '', p_release: true });
        caughtUp = true;
        break;
      }
    }
    if (!caughtUp) await checkpoint(user, network, scanState.lease_token, { p_release: true });
    return { network: network.key, credited, duplicates, rejected, caught_up: caughtUp };
  }

  return async function scanAddress(user, network, address, createdAt, deadline) {
    const scanState = await rpc(RPC.claimScan, { p_user_id: user, p_network: network.key, p_seconds: 90 });
    if (!scanState.acquired) return { network: network.key, credited: 0, duplicates: 0, rejected: [], caught_up: false, busy: true };
    try {
      return await (network.chain === 'TRC20' ? tronScan : evmScan)(user, network, address, createdAt, scanState, deadline);
    } catch (error) {
      const code = error instanceof DepositError ? error.code : 'SCAN_FAILED';
      try {
        await checkpoint(user, network, scanState.lease_token, { p_release: true, p_error: code });
      } catch {
        // A stale lease expires without fabricating scan progress.
      }
      throw new DepositError(code);
    }
  };
}

function createService(config, dependencies = {}) {
  const db = dependencies.db || createClient(config.SUPABASE_URL, config.SUPABASE_SERVICE_ROLE_KEY, {
    auth: { persistSession: false, autoRefreshToken: false, detectSessionInUrl: false }
  });
  const walletKey = encryptionKey(config.WALLET_ENCRYPTION_KEY);
  const chain = dependencies.chain || chainAccess(config);
  const generationInFlight = new Map();
  const sweepInFlight = new Map();
  let backgroundTimer = null;
  let backgroundBusy = false;
  let backgroundOffset = 0;

  async function rpc(name, args) {
    const { data, error } = await db.rpc(name, args);
    if (error) {
      const code = databaseCode(error);
      fail(code, statusForCode(code));
    }
    if (data?.error) {
      const code = typeof data.error === 'string' ? data.error : data.error.code || 'DATABASE_ERROR';
      fail(code, statusForCode(code));
    }
    return data;
  }

  async function safeSystemLog(logType, message, metadata = {}) {
    try {
      await db.from('system_logs').insert({
        log_type: String(logType || 'deposit_server').slice(0, 100),
        message: String(message || '').slice(0, 1000),
        metadata
      });
    } catch {
      // Diagnostics never replace a completed financial operation.
    }
  }

  async function sweepDepositBEP20(userId, network, depositAddress) {
    if (!config.HOT_WALLET_PRIVATE_KEY || network.chain !== 'BEP20') return;
    const key = `${userId}:${network.key}`;
    if (sweepInFlight.has(key)) return sweepInFlight.get(key);
    const operation = (async () => {
      try {
        const envelope = await rpc(RPC.keyEnvelope, { p_user_id: userId, p_network: network.key });
        if (!envelope?.server_cipher || String(envelope.address).toLowerCase() !== String(depositAddress).toLowerCase()) fail('KEY_ADDRESS_MISMATCH');
        const provider = new ethers.JsonRpcProvider(config.BSC_RPC_URL);
        const hotWallet = new ethers.Wallet(config.HOT_WALLET_PRIVATE_KEY, provider);
        const userWallet = new ethers.Wallet(decryptPrivateKey(envelope.server_cipher, walletKey), provider);
        if (userWallet.address.toLowerCase() !== String(depositAddress).toLowerCase()) fail('KEY_ADDRESS_MISMATCH');
        const token = new ethers.Contract(network.contract, [
          'function transfer(address to, uint256 value) returns (bool)',
          'function balanceOf(address owner) view returns (uint256)'
        ], userWallet);
        const balance = await token.balanceOf(userWallet.address);
        if (balance === 0n) return;
        const gasLimit = await token.transfer.estimateGas(config.ADMIN_SWEEP_ADDRESS, balance);
        const feeData = await provider.getFeeData();
        const gasPrice = feeData.gasPrice || feeData.maxFeePerGas;
        if (!gasPrice) fail('GAS_PRICE_UNAVAILABLE');
        const requiredGas = gasLimit * gasPrice;
        const currentGas = await provider.getBalance(userWallet.address);
        if (currentGas < requiredGas) {
          const funding = requiredGas - currentGas + gasPrice * 20000n;
          const fundTx = await hotWallet.sendTransaction({ to: userWallet.address, value: funding });
          await fundTx.wait();
        }
        const sweepTx = await token.transfer(config.ADMIN_SWEEP_ADDRESS, balance);
        await sweepTx.wait();
        await safeSystemLog('sweep_success', 'BEP20 auto-sweep completed', {
          user_id: userId,
          network: network.key,
          tx_hash: sweepTx.hash
        });
      } catch (error) {
        console.error(`BEP20 auto-sweep failed for ${userId}/${network.key}:`, error.message);
        await safeSystemLog('sweep_error', 'BEP20 auto-sweep failed', {
          user_id: userId,
          network: network.key,
          code: error instanceof DepositError ? error.code : 'SWEEP_FAILED'
        });
      }
    })().finally(() => sweepInFlight.delete(key));
    sweepInFlight.set(key, operation);
    return operation;
  }

  const scan = createScanners({
    rpc,
    chain,
    maxBlocks: config.EVM_BLOCK_RANGE,
    onCredit: ({ user, network, address }) => sweepDepositBEP20(user, network, address)
  });

  const appApi = (actor, action, payload = {}) => rpc(RPC.app, {
    p_user: actor.id,
    p_session: actor.session,
    p_action: action,
    p_payload: payload
  });

  async function authenticate(request) {
    const match = /^Bearer ([A-Za-z0-9._-]+)$/.exec(request.headers.authorization || '');
    if (!match) fail('SESSION_INVALID', 401);
    const { data, error } = await db.auth.getUser(match[1]);
    if (error || !data?.user) fail('SESSION_INVALID', 401);
    const actor = { id: data.user.id, session: sessionId(match[1]) };
    await appApi(actor, 'user.state');
    return actor;
  }

  async function readBody(request) {
    let size = 0;
    let text = '';
    for await (const chunk of request) {
      size += chunk.length;
      if (size > 8192) fail('PAYLOAD_TOO_LARGE', 413);
      text += chunk.toString('utf8');
    }
    try {
      const body = JSON.parse(text || '{}');
      if (!body || Array.isArray(body) || typeof body !== 'object') fail('INPUT_INVALID', 400);
      return body;
    } catch (error) {
      if (error instanceof DepositError) throw error;
      fail('INPUT_INVALID', 400);
    }
  }

  function apiKeyAllowed(request) {
    if (!config.API_SECRET_KEY || config.API_SECRET_KEY.length < 32) return false;
    const candidate = String(request.headers['x-api-key'] || '');
    const expected = Buffer.from(config.API_SECRET_KEY);
    const actual = Buffer.from(candidate);
    return expected.length === actual.length && crypto.timingSafeEqual(expected, actual);
  }

  async function depositsEnabled() {
    const { data, error } = await db.from('app_config').select('deposits_enabled').eq('id', true).single();
    if (error || !data) fail('DATABASE_ERROR', 503);
    if (!data.deposits_enabled) fail('DEPOSITS_PAUSED', 400);
  }

  async function generateWallet(userId, requestedNetwork = 'usdt_trc20') {
    const network = String(requestedNetwork || 'usdt_trc20').trim().toLowerCase();
    if (!NETWORK_FIELDS[network]) fail('NETWORK_INVALID', 400);
    if (!UUID_RE.test(userId || '')) fail('USER_NOT_FOUND', 400);
    let operation = generationInFlight.get(userId);
    if (!operation) {
      operation = rpc(RPC.registerWallets, generateWalletInputs(userId, walletKey))
        .finally(() => generationInFlight.delete(userId));
      generationInFlight.set(userId, operation);
    }
    const wallet = await operation;
    const address = wallet?.[NETWORK_FIELDS[network]];
    if (!address) fail('ADDRESS_UNAVAILABLE', 503);
    return { success: true, wallet, address, network, min_deposit: MIN_DEPOSIT, max_deposit: MAX_DEPOSIT };
  }

  async function loadNetworksAndAddresses(userId, selectedNetwork = '') {
    const [networkResult, addressResult] = await Promise.all([
      db.from('networks').select('key,asset,chain,contract,decimals,min_confirmations,enabled').eq('enabled', true),
      db.from('deposit_addresses').select('network,address,created_at').eq('user_id', userId).eq('is_active', true)
    ]);
    if (networkResult.error || addressResult.error) fail('DATABASE_ERROR', 503);
    let networks = networkResult.data || [];
    const filter = String(selectedNetwork || '').trim().toLowerCase();
    if (filter) {
      if (!NETWORK_FIELDS[filter]) fail('NETWORK_INVALID', 400);
      networks = networks.filter((network) => network.key === filter);
    }
    if (!(addressResult.data || []).length) fail('DEPOSIT_ADDRESS_REQUIRED', 400);
    return { networks, addresses: addressResult.data || [] };
  }

  async function scanUser(userId, selectedNetwork = '') {
    const { networks, addresses } = await loadNetworksAndAddresses(userId, selectedNetwork);
    const deadline = Date.now() + 46000;
    const results = await Promise.all(networks.map(async (network) => {
      const row = addresses.find((address) => address.network === network.key);
      if (!row) return { network: network.key, error: 'DEPOSIT_ADDRESS_REQUIRED' };
      try {
        return await scan(userId, network, row.address, row.created_at, deadline);
      } catch (error) {
        return { network: network.key, error: error instanceof DepositError ? error.code : 'SCAN_FAILED' };
      }
    }));
    const errors = results.filter((result) => result.error).map((result) => ({ network: result.network, code: result.error }));
    const checked = results.filter((result) => !result.error);
    const rejected = checked.flatMap((result) => (result.rejected || []).map((event) => ({ network: result.network, ...event })));
    if (errors.length || rejected.length) {
      await safeSystemLog('deposit_check_attention', 'Deposit check needs operator attention', {
        user_id: userId,
        errors,
        rejected
      });
    }
    const creditedCount = checked.reduce((total, result) => total + result.credited, 0);
    return {
      success: true,
      checked,
      errors,
      rejected_count: rejected.length,
      credited_count: creditedCount,
      found: creditedCount > 0,
      caught_up: errors.length === 0 && checked.every((result) => result.caught_up)
    };
  }

  async function history(userId, network = '') {
    let query = db.from('deposits')
      .select('id,user_id,asset,network,tx_hash,event_index,address,amount,status,confirmations,personal_bonus,inviter_bonus,created_at,completed_at,reversed_at')
      .eq('user_id', userId)
      .order('created_at', { ascending: false })
      .limit(50);
    const normalized = String(network || '').trim().toLowerCase();
    if (normalized) query = query.eq('network', normalized);
    const { data, error } = await query;
    if (error) fail('DATABASE_ERROR', 503);
    return { success: true, deposits: data || [] };
  }

  async function readiness() {
    const [configResult, networkResult, ...schemaChecks] = await Promise.all([
      db.from('app_config').select('id,deposits_enabled,personal_bonus_bps,inviter_bonus_bps').eq('id', true).single(),
      db.from('networks').select('key,asset,chain,contract,decimals,min_confirmations,enabled'),
      db.from('deposit_wallets').select('user_id,usdt_bep20_address,usdc_bep20_address,usdt_erc20_address,usdc_erc20_address,usdt_trc20_address').limit(1),
      db.from('deposit_addresses').select('user_id,asset,network,address,is_active,created_at').limit(1),
      db.from('deposits').select('id,user_id,asset,network,tx_hash,event_index,address,amount,status,confirmations').limit(1)
    ]);
    if (configResult.error || networkResult.error || schemaChecks.some((item) => item.error) || !configResult.data) {
      return { ready: false, code: 'DATABASE_UNAVAILABLE' };
    }
    const expected = new Map(CANONICAL_NETWORKS.map((item) => [item.key, `${item.asset}:${item.chain}`]));
    const actual = new Map((networkResult.data || []).map((item) => [item.key, `${item.asset}:${item.chain}`]));
    if ([...expected].some(([key, value]) => actual.get(key) !== value)) return { ready: false, code: 'DATABASE_CONTRACT_MISMATCH' };
    return {
      ready: true,
      deposits_enabled: configResult.data.deposits_enabled,
      networks: expected.size,
      schema_tables_checked: 5,
      sweep_enabled: Boolean(config.HOT_WALLET_PRIVATE_KEY)
    };
  }

  async function scanBatch(limit = config.BACKGROUND_BATCH_SIZE) {
    const [networkResult, addressResult] = await Promise.all([
      db.from('networks').select('key,asset,chain,contract,decimals,min_confirmations,enabled').eq('enabled', true),
      db.from('deposit_addresses').select('user_id,network,address,created_at').eq('is_active', true)
        .order('created_at', { ascending: true }).range(backgroundOffset, backgroundOffset + limit - 1)
    ]);
    if (networkResult.error || addressResult.error) fail('DATABASE_ERROR', 503);
    const rows = addressResult.data || [];
    const networks = new Map((networkResult.data || []).map((network) => [network.key, network]));
    const results = [];
    for (let index = 0; index < rows.length; index += 4) {
      const group = rows.slice(index, index + 4);
      const groupResults = await Promise.all(group.map(async (row) => {
        const network = networks.get(row.network);
        if (!network) return { user_id: row.user_id, network: row.network, error: 'NETWORK_DISABLED' };
        try {
          const result = await scan(row.user_id, network, row.address, row.created_at, Date.now() + 20000);
          return { user_id: row.user_id, ...result };
        } catch (error) {
          return { user_id: row.user_id, network: row.network, error: error instanceof DepositError ? error.code : 'SCAN_FAILED' };
        }
      }));
      results.push(...groupResults);
    }
    backgroundOffset = rows.length < limit ? 0 : backgroundOffset + rows.length;
    return { success: true, checked: results, next_offset: backgroundOffset };
  }

  function startBackground() {
    if (!config.BACKGROUND_SCAN_ENABLED || backgroundTimer) return;
    const run = async () => {
      if (backgroundBusy) return;
      backgroundBusy = true;
      try {
        await scanBatch();
      } catch (error) {
        console.error('Background deposit scan failed:', error.message);
        await safeSystemLog('deposit_background_error', 'Background deposit scan failed', {
          code: error instanceof DepositError ? error.code : 'SCAN_FAILED'
        });
      } finally {
        backgroundBusy = false;
      }
    };
    backgroundTimer = setInterval(run, config.BACKGROUND_SCAN_INTERVAL_MS);
    backgroundTimer.unref?.();
    setTimeout(run, 10000).unref?.();
  }

  function stopBackground() {
    if (backgroundTimer) clearInterval(backgroundTimer);
    backgroundTimer = null;
  }

  function reply(response, status, data) {
    response.writeHead(status, {
      'Content-Type': 'application/json; charset=utf-8',
      'Cache-Control': 'no-store',
      'X-Content-Type-Options': 'nosniff',
      'Referrer-Policy': 'no-referrer'
    });
    response.end(JSON.stringify(data));
  }

  async function handler(request, response) {
    try {
      const url = new URL(request.url, 'http://localhost');
      const path = url.pathname;

      if (request.method === 'GET' && path === '/') {
        return reply(response, 200, { service: 'pixelnft-deposit', version: VERSION, status: 'running' });
      }
      if (request.method === 'GET' && path === '/health') {
        return reply(response, 200, { service: 'pixelnft-deposit', version: VERSION, status: 'healthy', uptime: process.uptime() });
      }
      if (request.method === 'GET' && path === '/api/health') {
        return reply(response, 200, { service: 'pixelnft-deposit', version: VERSION, status: 'healthy' });
      }
      if (request.method === 'GET' && path === '/ready') {
        const state = await readiness();
        return reply(response, state.ready ? 200 : 503, { service: 'pixelnft-deposit', version: VERSION, status: state.ready ? 'ready' : 'blocked', ...state });
      }

      // This service is private behind the Supabase Edge Function. It never
      // accepts direct browser-origin requests or enables wildcard CORS.
      if (request.headers.origin) fail('ORIGIN_DENIED', 403);

      if (request.method === 'GET' && path === '/public/deposit/history') {
        const actor = await authenticate(request);
        return reply(response, 200, await history(actor.id, url.searchParams.get('network') || ''));
      }
      if (request.method === 'GET' && path === '/api/deposit/history') {
        if (!apiKeyAllowed(request)) fail('API_KEY_INVALID', 403);
        const userId = url.searchParams.get('user_id') || '';
        if (!UUID_RE.test(userId)) fail('USER_NOT_FOUND', 400);
        return reply(response, 200, await history(userId, url.searchParams.get('network') || ''));
      }
      if (request.method === 'GET' && path === '/api/check-deposits') {
        if (!apiKeyAllowed(request)) fail('API_KEY_INVALID', 403);
        return reply(response, 200, await scanBatch(200));
      }

      const allowedPosts = new Set([
        '/public/deposit/generate',
        '/public/deposit/check',
        '/admin/deposit/check',
        '/public/admin/check-deposits',
        '/api/deposit/generate'
      ]);
      if (request.method !== 'POST' || !allowedPosts.has(path)) fail('NOT_FOUND', 404);
      const input = await readBody(request);

      if (path === '/api/deposit/generate') {
        if (!apiKeyAllowed(request)) fail('API_KEY_INVALID', 403);
        if (!UUID_RE.test(input.user_id || '')) fail('USER_NOT_FOUND', 400);
        await depositsEnabled();
        return reply(response, 200, await generateWallet(input.user_id, input.network));
      }

      const actor = await authenticate(request);
      const allowed = await rpc(RPC.rateLimit, {
        p_key: `daemon:${path}:${actor.id}`,
        p_limit: 1,
        p_seconds: path.endsWith('/check') || path.endsWith('check-deposits') ? 60 : 5
      });
      if (!allowed) fail('RATE_LIMITED', 429);

      if (path === '/public/deposit/generate') {
        await depositsEnabled();
        return reply(response, 200, await generateWallet(actor.id, input.network));
      }

      let target = actor.id;
      if (path === '/admin/deposit/check' || path === '/public/admin/check-deposits') {
        await appApi(actor, 'admin.overview', { admin_token: input.admin_token });
        if (!UUID_RE.test(input.user_id || '')) fail('USER_NOT_FOUND', 400);
        target = input.user_id;
      }
      await depositsEnabled();
      const result = await scanUser(target, input.network);
      await safeSystemLog(path.startsWith('/admin') || path.includes('/admin/') ? 'admin_deposit_check' : 'public_deposit_check', 'Deposit check completed', {
        actor_id: actor.id,
        user_id: target,
        credited_count: result.credited_count,
        error_count: result.errors.length
      });
      return reply(response, 200, result);
    } catch (error) {
      const code = error instanceof DepositError ? error.code : 'SERVICE_UNAVAILABLE';
      if (!(error instanceof DepositError)) console.error('Unhandled request error:', error);
      return reply(response, error instanceof DepositError ? error.status : 503, { error: code, code });
    }
  }

  return { handler, readiness, scanUser, scanBatch, startBackground, stopBackground };
}

function main() {
  try {
    const config = settingsFromEnv(process.env);
    if (!/^[0-9a-f]{64}$/i.test(process.env.WALLET_ENCRYPTION_KEY || '')) {
      console.warn('WALLET_ENCRYPTION_KEY should be migrated to a stable 64-hex value; legacy ENCRYPTION_KEY compatibility is active.');
    }
    const service = createService(config);
    const server = http.createServer(service.handler);
    server.requestTimeout = 65000;
    server.headersTimeout = 10000;
    server.listen(config.PORT, config.HOST, () => {
      console.log(`PixelNFT deposit service v${VERSION} listening on ${config.HOST}:${config.PORT}; HTTPS reverse proxy required`);
      console.log(`Database: ${config.SUPABASE_URL}`);
      console.log(`Deposit range: $${MIN_DEPOSIT}-$${MAX_DEPOSIT}`);
      console.log(`BEP20 auto-sweep: ${config.HOT_WALLET_PRIVATE_KEY ? 'enabled' : 'disabled'}`);
      console.log(`Background scanner: ${config.BACKGROUND_SCAN_ENABLED ? 'enabled' : 'disabled'}`);
      service.startBackground();
    });
    for (const signal of ['SIGINT', 'SIGTERM']) {
      process.on(signal, () => {
        service.stopBackground();
        server.close(() => process.exit(0));
      });
    }
  } catch (error) {
    console.error(error instanceof DepositError ? error.code : 'STARTUP_FAILED');
    process.exitCode = 1;
  }
}

if (require.main === module) main();

module.exports = {
  VERSION,
  MIN_DEPOSIT,
  MAX_DEPOSIT,
  TRANSFER_TOPIC,
  DepositError,
  integer,
  amount6,
  depositAmountAllowed,
  settingsFromEnv,
  encryptionKey,
  encryptPrivateKey,
  decryptPrivateKey,
  tronAddress,
  tronHex,
  transferLog,
  generateWalletInputs,
  chainAccess,
  createScanners,
  createService
};
