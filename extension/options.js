const accountInput = document.getElementById('account-id');
const wsInput = document.getElementById('ws-url');
const statusEl = document.getElementById('status');

function validAccountId(accountId) {
  return /^FLOW-\d{3,}$/.test(accountId);
}

function validLocalWsUrl(wsUrl) {
  try {
    const url = new URL(wsUrl);
    const port = Number(url.port);
    return url.protocol === 'ws:' && url.hostname === '127.0.0.1' && Number.isInteger(port) && port >= 1 && port <= 65535;
  } catch (_) {
    return false;
  }
}

function validNonce(nonce) {
  return typeof nonce === 'string' && /^[A-Za-z0-9_-]{16,}$/.test(nonce);
}

async function loadOptions() {
  const data = await chrome.storage.local.get(['account_id', 'ws_url']);
  accountInput.value = data.account_id || '';
  wsInput.value = data.ws_url || '';
}

async function bootstrapFromUrl() {
  const params = new URLSearchParams(window.location.search);
  if (params.get('bootstrap') !== '1') return false;
  const account_id = (params.get('account_id') || '').trim();
  const ws_url = (params.get('ws_url') || '').trim();
  const nonce = (params.get('nonce') || '').trim();
  window.history.replaceState({}, document.title, window.location.pathname);
  if (!validAccountId(account_id) || !validLocalWsUrl(ws_url) || !validNonce(nonce)) {
    statusEl.textContent = 'Bootstrap rejected.';
    return true;
  }
  const data = await chrome.storage.local.get(['bootstrap_nonces']);
  const used = Array.isArray(data.bootstrap_nonces) ? data.bootstrap_nonces : [];
  if (used.includes(nonce)) {
    statusEl.textContent = 'Bootstrap nonce already used.';
    return true;
  }
  await chrome.storage.local.set({
    account_id,
    ws_url,
    bootstrap_nonces: [...used.slice(-20), nonce],
  });
  accountInput.value = account_id;
  wsInput.value = ws_url;
  chrome.runtime.sendMessage({ type: 'RECONNECT', account_id, ws_url }).catch(() => {});
  statusEl.textContent = 'Bootstrap saved. Reconnecting...';
  return true;
}

document.getElementById('save').addEventListener('click', async () => {
  const account_id = accountInput.value.trim();
  const ws_url = wsInput.value.trim();
  if (!validAccountId(account_id) || !validLocalWsUrl(ws_url)) {
    statusEl.textContent = 'Invalid account or WebSocket URL.';
    return;
  }
  await chrome.storage.local.set({ account_id, ws_url });
  chrome.runtime.sendMessage({ type: 'RECONNECT', account_id, ws_url }).catch(() => {});
  statusEl.textContent = 'Saved. Reconnecting...';
});

bootstrapFromUrl().then((handled) => {
  if (!handled) loadOptions();
});
