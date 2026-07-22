/**
 * Long-running API executor for MV3.
 * It performs fetch/response parsing outside the background service worker.
 */

chrome.runtime.onMessage.addListener((message) => {
  if (message?.type !== 'OFFSCREEN_API_REQUEST') return false;
  handleApiRequest(message.msg, message.flowKey);
  return true;
});

async function handleApiRequest(msg, flowKey) {
  const startedAt = Date.now();
  const { id, params } = msg;
  const { url, method, headers, body } = params || {};

  try {
    const fetchHeaders = { ...(headers || {}) };
    if (!flowKey) {
      sendResponse({ id, status: 503, error: 'NO_FLOW_KEY' });
      return;
    }
    fetchHeaders.authorization = `Bearer ${flowKey}`;

    console.log(`[FlowAgentOffscreen] get_media/request start id=${String(id).slice(0, 8)} at=${startedAt}`);
    const response = await fetch(url, {
      method: method || 'POST',
      headers: fetchHeaders,
      credentials: 'include',
      body: method === 'GET' ? undefined : JSON.stringify(body),
    });
    const fetchDoneAt = Date.now();
    const responseText = await response.text();
    const textDoneAt = Date.now();

    let responseData;
    try {
      responseData = JSON.parse(responseText);
    } catch {
      responseData = responseText;
    }
    const jsonDoneAt = Date.now();
    console.log(
      `[FlowAgentOffscreen] api_response timings id=${String(id).slice(0, 8)} ` +
      `fetch_ms=${fetchDoneAt - startedAt} text_ms=${textDoneAt - fetchDoneAt} json_ms=${jsonDoneAt - textDoneAt} chars=${responseText.length}`,
    );

    sendResponse({ id, status: response.status, data: responseData });
  } catch (e) {
    sendResponse({ id, status: 500, error: e.message || 'API_REQUEST_FAILED' });
  }
}

function sendResponse(payload) {
  chrome.runtime.sendMessage({ type: 'OFFSCREEN_API_RESPONSE', payload }).catch(() => {});
}
