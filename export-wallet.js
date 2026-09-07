#!/usr/bin/env node
const { createClient } = require('@supabase/supabase-js');
const crypto = require('crypto');
const WebSocket = require('ws'); // ИСПРАВЛЕНИЕ: Добавлен пакет ws

const SUPABASE_URL = process.env.SUPABASE_URL || 'https://fkjwueogfmdolcjtvvme.supabase.co';
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const ENCRYPTION_KEY = process.env.ENCRYPTION_KEY;

const ALLOWED_NETWORKS = new Set([
  'usdt_bep20',
  'usdc_bep20',
  'usdt_erc20',
  'usdc_erc20',
  'trx',
  'usdt_trc20',
  'trc_20',
]);
function fail(message, code = 1) {
  console.error(message);
  process.exit(code);
}

function maskPrivateKey(key) {
  if (!key || key.length < 16) return key || '';
  return `${key.slice(0, 8)}...${key.slice(-8)}`;
}

function decryptPrivateKey(encryptedText) {
  if (!encryptedText) {
    throw new Error('Empty server key envelope');
  }

  if (!ENCRYPTION_KEY || String(ENCRYPTION_KEY).length < 32) {
    throw new Error('Missing or invalid ENCRYPTION_KEY');
  }

  const parts = String(encryptedText).split(':');
  const modern = parts.length === 4 && parts[0] === 'v1';
  const legacy = parts.length === 3;
  if (!modern && !legacy) {
    throw new Error('Invalid server key envelope');
  }

  const ivHex = modern ? parts[1] : parts[0];
  const authTagHex = parts[2];
  const encryptedHex = modern ? parts[3] : parts[1];

  if (
    !(modern ? /^[a-f0-9]{24}$/ : /^[a-f0-9]{32}$/).test(ivHex) ||
    !/^[a-f0-9]{32}$/.test(authTagHex) ||
    !/^(?:[a-f0-9]{2})+$/.test(encryptedHex)
  ) {
    throw new Error('Invalid server key envelope');
  }

  const iv = Buffer.from(ivHex, 'hex');
  const authTag = Buffer.from(authTagHex, 'hex');
  const key = crypto.createHash('sha256').update(ENCRYPTION_KEY).digest();

  const decipher = crypto.createDecipheriv('aes-256-gcm', key, iv);
  decipher.setAuthTag(authTag);

  let decrypted = decipher.update(encryptedHex, 'hex', 'utf8');
  decrypted += decipher.final('utf8');
  if (!/^(0x)?[0-9a-fA-F]{64}$/.test(decrypted)) {
    throw new Error('Decrypted private key is invalid');
  }
  return decrypted;
}

function addressesMatch(network, first, second) {
  const left = String(first || '').trim();
  const right = String(second || '').trim();
  if (!left || !right) return false;
  return network.endsWith('_trc20') ? left === right : left.toLowerCase() === right.toLowerCase();
}

function parseArgs(argv) {
  const args = argv.slice(2);
  const options = {
    userId: '',
    network: '',
    json: false,
    masked: false,
  };

  for (const arg of args) {
    if (arg === '--json') {
      options.json = true;
      continue;
    }
    if (arg === '--masked') {
      options.masked = true;
      continue;
    }
    if (!options.userId) {
      options.userId = arg;
      continue;
    }
    if (!options.network) {
      options.network = arg;
      continue;
    }
  }

  return options;
}

function printUsage() {
  console.log(`Usage:
  node export-wallet.js <user_id> <network> [--json] [--masked]

Examples:
  node export-wallet.js 483251ac-6d97-40c2-ad11-6c74b01ba7af usdc_bep20
  node export-wallet.js 483251ac-6d97-40c2-ad11-6c74b01ba7af usdt_bep20 --json
  node export-wallet.js 483251ac-6d97-40c2-ad11-6c74b01ba7af usdc_bep20 --masked

Notes:
  --json    Print machine-readable JSON
  --masked  Mask private_key output for safer viewing
`);
}

async function main() {
  const { userId, network, json, masked } = parseArgs(process.argv);

  if (!userId || !network) {
    printUsage();
    process.exit(1);
  }

  if (!SUPABASE_SERVICE_ROLE_KEY) {
    fail('Missing SUPABASE_SERVICE_ROLE_KEY');
  }

  if (!ALLOWED_NETWORKS.has(network)) {
    fail(`Unsupported network: ${network}`);
  }

  // ИСПРАВЛЕНИЕ: Добавлен транспорт realtime
  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
    auth: {
      autoRefreshToken: false,
      persistSession: false,
    },
    realtime: {
      transport: WebSocket
    }
  });

  const { data, error } = await supabase
    .from('deposit_private_keys')
    .select('user_id, network, address, created_at')
    .eq('user_id', userId)
    .eq('network', network)
    .maybeSingle();

  if (error) {
    fail(`Supabase error: ${error.message}`);
  }

  if (!data) {
    fail('Wallet not found');
  }

  const { data: keyEnvelope, error: keyEnvelopeError } = await supabase.rpc(
    'deposit_key_envelope',
    {
      p_user_id: userId,
      p_network: network,
    }
  );

  if (keyEnvelopeError) {
    fail(`Supabase key envelope error: ${keyEnvelopeError.message}`);
  }
  if (!keyEnvelope?.server_cipher || !addressesMatch(network, data.address, keyEnvelope.address)) {
    fail('Wallet key/address mismatch');
  }

  const privateKey = decryptPrivateKey(keyEnvelope.server_cipher);
  const output = {
    user_id: data.user_id,
    network: data.network,
    address: data.address,
    private_key: masked ? maskPrivateKey(privateKey) : privateKey,
    created_at: data.created_at,
  };

  if (json) {
    console.log(JSON.stringify(output, null, 2));
    return;
  }

  console.log('Wallet export successful');
  console.log('------------------------');
  console.log(`User ID:     ${output.user_id}`);
  console.log(`Network:     ${output.network}`);
  console.log(`Address:     ${output.address}`);
  console.log(`Private Key: ${output.private_key}`);
  console.log(`Created At:  ${output.created_at || 'n/a'}`);
}

main().catch((err) => {
  fail(`Export failed: ${err.message}`);
});
