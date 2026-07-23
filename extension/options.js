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

function validLocalHttpUrl(apiUrl) {
  try {
    const url = new URL(apiUrl);
    const port = Number(url.port);
    return url.protocol === 'http:' && url.hostname === '127.0.0.1' && Number.isInteger(port) && port >= 1 && port <= 65535;
  } catch (_) {
    return false;
  }
}

function validNonce(nonce) {
  return typeof nonce === 'string' && /^[A-Za-z0-9_-]{16,}$/.test(nonce);
}

function emptyMetrics() {
  return {
    tokenCapturedAt: null,
    requestCount: 0,
    successCount: 0,
    failedCount: 0,
    lastError: null,
  };
}

function safeWs(wsUrl) {
  try {
    const url = new URL(wsUrl);
    return { host: url.hostname, port: Number(url.port) };
  } catch (_) {
    return {};
  }
}

function safeErrorCode(error) {
  const name = error?.name || '';
  const message = error?.message || '';
  if (message.includes('Could not establish connection') || message.includes('Receiving end does not exist')) return 'background_message_failed';
  if (name === 'TypeError') return 'runtime_error';
  return 'unexpected_exception';
}

async function recordBootstrapDiagnostic(api_url, account_id, ws_url, event, error_code = null) {
  if (!validAccountId(account_id) || !validLocalWsUrl(ws_url) || !validLocalHttpUrl(api_url)) return;
  const payload = {
    type: 'bootstrap_diagnostic',
    source: 'options',
    event,
    account_id,
    ws: safeWs(ws_url),
    at: new Date().toISOString(),
  };
  if (error_code) payload.error_code = error_code;
  try {
    await fetch(`${api_url}/api/ext/bootstrap-diagnostic`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
  } catch (_) {}
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
  const api_url = (params.get('api_url') || '').trim();
  const nonce = (params.get('nonce') || '').trim();
  window.history.replaceState({}, document.title, window.location.pathname);
  await recordBootstrapDiagnostic(api_url, account_id, ws_url, 'bootstrap_detected');
  if (!validAccountId(account_id) || !validLocalWsUrl(ws_url) || !validLocalHttpUrl(api_url) || !validNonce(nonce)) {
    statusEl.textContent = 'Bootstrap rejected.';
    await recordBootstrapDiagnostic(api_url, account_id, ws_url, 'bootstrap_failed', 'invalid_parameters');
    return true;
  }
  await recordBootstrapDiagnostic(api_url, account_id, ws_url, 'parameters_validated');
  const data = await chrome.storage.local.get(['bootstrap_nonces']);
  const used = Array.isArray(data.bootstrap_nonces) ? data.bootstrap_nonces : [];
  if (used.includes(nonce)) {
    statusEl.textContent = 'Bootstrap nonce already used.';
    await recordBootstrapDiagnostic(api_url, account_id, ws_url, 'bootstrap_failed', 'nonce_already_used');
    return true;
  }
  try {
    await recordBootstrapDiagnostic(api_url, account_id, ws_url, 'bootstrap_reset_requested');
    await chrome.runtime.sendMessage({ type: 'BOOTSTRAP_RESET' });
    await recordBootstrapDiagnostic(api_url, account_id, ws_url, 'bootstrap_reset_succeeded');
  } catch (error) {
    statusEl.textContent = 'Bootstrap reset failed.';
    await recordBootstrapDiagnostic(api_url, account_id, ws_url, 'bootstrap_failed', safeErrorCode(error));
    return true;
  }
  try {
    await recordBootstrapDiagnostic(api_url, account_id, ws_url, 'storage_cleanup_started');
    await chrome.storage.local.remove(['flowKey', 'callbackSecret', 'metrics']);
    await recordBootstrapDiagnostic(api_url, account_id, ws_url, 'storage_cleanup_succeeded');
    await recordBootstrapDiagnostic(api_url, account_id, ws_url, 'account_config_write_started');
    await chrome.storage.local.set({
      account_id,
      ws_url,
      api_url,
      bootstrap_nonces: [...used.slice(-20), nonce],
      metrics: emptyMetrics(),
    });
    await recordBootstrapDiagnostic(api_url, account_id, ws_url, 'account_config_write_succeeded');
    await recordBootstrapDiagnostic(api_url, account_id, ws_url, 'nonce_recorded');
  } catch (error) {
    statusEl.textContent = 'Bootstrap storage failed.';
    await recordBootstrapDiagnostic(api_url, account_id, ws_url, 'bootstrap_failed', safeErrorCode(error) === 'runtime_error' ? 'storage_write_failed' : safeErrorCode(error));
    return true;
  }
  accountInput.value = account_id;
  wsInput.value = ws_url;
  await recordBootstrapDiagnostic(api_url, account_id, ws_url, 'reconnect_requested');
  chrome.runtime.sendMessage({ type: 'RECONNECT', account_id, ws_url, api_url })
    .then(() => recordBootstrapDiagnostic(api_url, account_id, ws_url, 'reconnect_acknowledged'))
    .catch((error) => recordBootstrapDiagnostic(api_url, account_id, ws_url, 'bootstrap_failed', safeErrorCode(error)));
  statusEl.textContent = 'Bootstrap saved. Reconnecting...';
  await recordBootstrapDiagnostic(api_url, account_id, ws_url, 'bootstrap_completed');
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
