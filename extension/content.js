/**
 * Content script — bridge between background.js and injected.js
 * Injects injected.js into MAIN world to access window.grecaptcha
 */
(function () {
  const s = document.createElement('script');
  s.src = chrome.runtime.getURL('injected.js');
  s.onload = () => s.remove();
  (document.head || document.documentElement).appendChild(s);
})();

chrome.runtime.onMessage.addListener((msg, _, reply) => {
  if (msg.type === 'GET_PAGE_CREDITS') {
    readPageCredits()
      .then((credits) => reply({ credits }))
      .catch((e) => reply({ error: e.message || 'PAGE_CREDITS_UNAVAILABLE' }));
    return true;
  }
  if (msg.type !== 'GET_CAPTCHA') return;

  const { requestId, pageAction } = msg;

  const handler = (e) => {
    if (e.detail?.requestId === requestId) {
      window.removeEventListener('CAPTCHA_RESULT', handler);
      clearTimeout(timer);
      reply({ token: e.detail.token, error: e.detail.error });
    }
  };

  const timer = setTimeout(() => {
    window.removeEventListener('CAPTCHA_RESULT', handler);
    reply({ error: 'CONTENT_TIMEOUT' });
  }, 25000);

  window.addEventListener('CAPTCHA_RESULT', handler);

  window.dispatchEvent(new CustomEvent('GET_CAPTCHA', {
    detail: { requestId, pageAction },
  }));

  return true; // keep channel open for async reply
});

// ─── TRPC Media URL Monitor ─────────────────────────────────
// Forward intercepted TRPC responses with media URLs to background.js
window.addEventListener('TRPC_MEDIA_URLS', (e) => {
  const { url, body } = e.detail || {};
  if (!body) return;
  chrome.runtime.sendMessage({
    type: 'TRPC_MEDIA_URLS',
    trpcUrl: url,
    body,
  }).catch(() => {});
});

async function readPageCredits() {
  const parse = () => {
    const text = document.body?.innerText || '';
    const match = text.match(/([\d,]+)\s*(?:个\s*)?Google Flow\s*(?:点数|points|credits)/i);
    if (!match) return null;
    const credits = Number(match[1].replace(/,/g, ''));
    return Number.isInteger(credits) && credits >= 0 ? credits : null;
  };

  const existing = parse();
  if (existing !== null) return existing;

  const accountButton = [...document.querySelectorAll('[aria-label]')].find((element) => {
    const label = element.getAttribute('aria-label') || '';
    return /Google\s*(?:账号|Account)/i.test(label);
  });
  if (!accountButton) throw new Error('GOOGLE_ACCOUNT_BUTTON_NOT_FOUND');
  accountButton.click();
  try {
    const deadline = Date.now() + 5000;
    while (Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 100));
      const credits = parse();
      if (credits !== null) return credits;
    }
    throw new Error('PAGE_CREDITS_NOT_FOUND');
  } finally {
    accountButton.click();
  }
}
