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
  if (msg.type === 'CLICK_NEW_PROJECT') {
    clickNewProject()
      .then((target) => {
        // Reply before navigation destroys this content-script context.
        reply({ clicked: true });
        setTimeout(() => {
          target.scrollIntoView({ block: 'center', inline: 'center' });
          target.focus();
          target.click();
        }, 0);
      })
      .catch((e) => reply({ error: e.message || 'NEW_PROJECT_CLICK_FAILED' }));
    return true;
  }
  if (msg.type === 'GET_PAGE_CREDITS') {
    readPageCredits()
      .then((credits) => reply({ credits }))
      .catch((e) => reply({ error: e.message || 'PAGE_CREDITS_UNAVAILABLE' }));
    return true;
  }
  if (msg.type === 'SUBMIT_VIDEO_UI') {
    bridgePageEvent('SUBMIT_VIDEO_UI', 'SUBMIT_VIDEO_UI_RESULT', msg.payload, 150000)
      .then(reply)
      .catch((e) => reply({ error: e.message || 'PAGE_VIDEO_SUBMIT_FAILED' }));
    return true;
  }
  if (msg.type === 'GET_PAGE_VIDEO_STATUS') {
    const mediaId = String(msg.mediaId || '');
    const tiles = [...document.querySelectorAll('flow-video-tile')];
    const image = [...document.querySelectorAll('img')].find((item) => item.src.includes(`/image/${mediaId}`));
    const tile = image?.closest('flow-video-tile');
    reply({ completed: !!image && !!tile, mediaId, tileIndex: tile ? tiles.indexOf(tile) : -1 });
    return;
  }
  if (msg.type === 'GET_PAGE_VIDEO_JOB_IDS') {
    const ids = [...document.querySelectorAll('flow-video-tile img')]
      .map((image) => image.src.match(/\/image\/([0-9a-f-]{36})/i)?.[1])
      .filter(Boolean);
    reply({ mediaIds: [...new Set(ids)] });
    return;
  }
  if (msg.type === 'DOWNLOAD_PAGE_VIDEO') {
    clickPageVideoDownload(String(msg.mediaId || ''))
      .then(reply)
      .catch((e) => reply({ error: e.message || 'PAGE_VIDEO_DOWNLOAD_CLICK_FAILED' }));
    return true;
  }
  if (msg.type === 'GET_PAGE_VIDEO_DOWNLOAD_COORDS') {
    getPageVideoDownloadCoords(String(msg.mediaId || ''), String(msg.target || 'more'), Number(msg.tileIndex))
      .then(reply)
      .catch((e) => reply({ error: e.message || 'PAGE_VIDEO_DOWNLOAD_COORDS_FAILED' }));
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

async function clickPageVideoDownload(mediaId) {
  const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const press = (element) => {
    element.focus?.();
    for (const type of ['pointerdown', 'mousedown', 'mouseup', 'click']) {
      element.dispatchEvent(new MouseEvent(type, { bubbles: true, view: window }));
    }
  };
  const deadline = Date.now() + 15000;
  let tile = null;
  while (Date.now() < deadline && !tile) {
    const image = [...document.querySelectorAll('img')]
      .find((item) => item.src.includes(`/image/${mediaId}`));
    tile = image?.closest('flow-video-tile') || null;
    if (!tile) await delay(250);
  }
  if (!tile) throw new Error('PAGE_VIDEO_TILE_NOT_FOUND');
  const container = tile.closest('flow-grid-tile-container') || tile.parentElement || tile;
  for (const type of ['mouseenter', 'mouseover', 'mousemove']) {
    container.dispatchEvent(new MouseEvent(type, { bubbles: true, view: window }));
  }
  await delay(300);
  const more = [...container.querySelectorAll('button')]
    .find((button) => /更多选项|More options/i.test(button.getAttribute('aria-label') || ''));
  if (!more) throw new Error('PAGE_VIDEO_MORE_BUTTON_NOT_FOUND');
  const visibleDownload = () => [...document.querySelectorAll('button,[role=menuitem],[role=option]')]
    .find((element) => {
      const text = (element.innerText || element.textContent || '').trim();
      return element.getClientRects().length > 0 && /(^|\n)(下载|Download)(\n|$)/i.test(text);
    });
  if (!visibleDownload()) {
    if (more.getAttribute('aria-expanded') === 'true') {
      press(more);
      await delay(150);
    }
    press(more);
  }
  const menuDeadline = Date.now() + 5000;
  let download = null;
  while (Date.now() < menuDeadline && !download) {
    download = visibleDownload();
    if (!download) await delay(100);
  }
  if (!download) throw new Error('PAGE_VIDEO_DOWNLOAD_MENU_NOT_FOUND');
  press(download);
  return { clicked: true, mediaId };
}

async function getPageVideoDownloadCoords(mediaId, target, tileIndex = -1) {
  const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const requestedIndex = Number.isInteger(tileIndex) ? tileIndex : -1;
  const findTile = () => {
    const tiles = [...document.querySelectorAll('flow-video-tile')];
    const image = [...document.querySelectorAll('img')]
      .find((item) => item.src.includes(`/image/${mediaId}`));
    return image?.closest('flow-video-tile') || tiles[requestedIndex] || (tiles.length === 1 ? tiles[0] : null);
  };
  if (target === 'hover') {
    const tile = findTile();
    const container = tile?.closest('flow-grid-tile-container') || tile;
    if (!container) throw new Error('PAGE_VIDEO_TILE_NOT_FOUND');
    const rect = container.getBoundingClientRect();
    return { x: rect.left + rect.width / 2, y: rect.top + rect.height / 2 };
  }
  if (target === 'download') {
    const deadline = Date.now() + 5000;
    while (Date.now() < deadline) {
      const item = [...document.querySelectorAll('button,[role=menuitem],[role=option]')]
        .find((element) => {
          const text = (element.innerText || element.textContent || '').trim();
          return element.getClientRects().length > 0 && /(^|\n)(下载|Download)(\n|$)/i.test(text);
        });
      if (item) {
        const rect = item.getBoundingClientRect();
        return { x: rect.left + rect.width / 2, y: rect.top + rect.height / 2 };
      }
      await delay(100);
    }
    throw new Error('PAGE_VIDEO_DOWNLOAD_MENU_NOT_FOUND');
  }
  if (target === 'resolution') {
    const deadline = Date.now() + 5000;
    while (Date.now() < deadline) {
      const item = [...document.querySelectorAll('[role=menuitem],button')]
        .find((element) => {
          const text = (element.innerText || element.textContent || '').trim();
          return element.getClientRects().length > 0 && /原始尺寸|Original/i.test(text);
        });
      if (item) {
        const rect = item.getBoundingClientRect();
        return { x: rect.left + rect.width / 2, y: rect.top + rect.height / 2 };
      }
      await delay(100);
    }
    throw new Error('PAGE_VIDEO_DOWNLOAD_RESOLUTION_NOT_FOUND');
  }
  const deadline = Date.now() + 15000;
  while (Date.now() < deadline) {
    const tile = findTile();
    const container = tile?.closest('flow-grid-tile-container') || tile?.parentElement || tile;
    if (container) {
      for (const type of ['mouseenter', 'mouseover', 'mousemove']) {
        container.dispatchEvent(new MouseEvent(type, { bubbles: true, view: window }));
      }
      await delay(300);
      const more = [...container.querySelectorAll('button')]
        .find((button) => /更多选项|More options/i.test(button.getAttribute('aria-label') || ''));
      if (more) {
        const rect = more.getBoundingClientRect();
        return { x: rect.left + rect.width / 2, y: rect.top + rect.height / 2 };
      }
    }
    await delay(200);
  }
  throw new Error('PAGE_VIDEO_MORE_BUTTON_NOT_FOUND');
}

function bridgePageEvent(requestType, responseType, payload, timeoutMs) {
  const requestId = crypto.randomUUID();
  return new Promise((resolve, reject) => {
    const handler = (event) => {
      if (event.detail?.requestId !== requestId) return;
      window.removeEventListener(responseType, handler);
      clearTimeout(timer);
      if (event.detail.error) reject(new Error(event.detail.error));
      else resolve(event.detail);
    };
    const timer = setTimeout(() => {
      window.removeEventListener(responseType, handler);
      reject(new Error(`${requestType}_TIMEOUT`));
    }, timeoutMs);
    window.addEventListener(responseType, handler);
    window.dispatchEvent(new CustomEvent(requestType, { detail: { requestId, payload } }));
  });
}

async function clickNewProject() {
  const deadline = Date.now() + 15000;
  let target = null;
  while (Date.now() < deadline) {
    target = [...document.querySelectorAll('button, a, [role="button"]')].find((element) => {
      const text = `${element.textContent || ''} ${element.getAttribute('aria-label') || ''}`.trim();
      return element.getClientRects().length > 0 && !element.disabled && /新建项目|创建项目|New project|Create project/i.test(text);
    });
    if (target) break;
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  if (!target) throw new Error('NEW_PROJECT_BUTTON_NOT_FOUND');
  // The Angular shell can report document complete before the button handler is hydrated.
  await new Promise((resolve) => setTimeout(resolve, 1000));
  return target;
}

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
