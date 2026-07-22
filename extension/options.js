const accountInput = document.getElementById('account-id');
const wsInput = document.getElementById('ws-url');
const statusEl = document.getElementById('status');

async function loadOptions() {
  const data = await chrome.storage.local.get(['account_id', 'ws_url']);
  accountInput.value = data.account_id || 'FLOW-001';
  wsInput.value = data.ws_url || 'ws://127.0.0.1:9222';
}

document.getElementById('save').addEventListener('click', async () => {
  const account_id = accountInput.value.trim() || 'FLOW-001';
  const ws_url = wsInput.value.trim() || 'ws://127.0.0.1:9222';
  await chrome.storage.local.set({ account_id, ws_url });
  chrome.runtime.sendMessage({ type: 'RECONNECT', account_id, ws_url }).catch(() => {});
  statusEl.textContent = 'Saved. Reconnecting...';
});

loadOptions();
