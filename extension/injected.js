/**
 * Injected into MAIN world on labs.google — has access to window.grecaptcha
 * Also intercepts TRPC fetch responses to capture fresh signed media URLs.
 */
const SITE_KEY = '6LdsFiUsAAAAAIjVDZcuLhaHiDn5nnHVXVRQGeMV';

// ─── TRPC Response Monitor ─────────────────────────────────
// Monkey-patch fetch to intercept TRPC responses containing media URLs.
// Fresh signed GCS URLs are extracted and forwarded to the agent.

const _originalFetch = window.fetch;
window.fetch = async function (...args) {
  const response = await _originalFetch.apply(this, args);
  try {
    const url = typeof args[0] === 'string' ? args[0] : args[0]?.url || '';
    // Only intercept TRPC calls on labs.google that return project/flow data
    if (url.includes('/fx/api/trpc/') && response.ok) {
      const clone = response.clone();
      clone.text().then(text => {
        if (text.includes('storage.googleapis.com/ai-sandbox-videofx/')) {
          window.dispatchEvent(new CustomEvent('TRPC_MEDIA_URLS', {
            detail: { url, body: text },
          }));
        }
      }).catch(() => {});
    }
  } catch {}
  return response;
};


window.addEventListener('GET_CAPTCHA', async ({ detail }) => {
  const { requestId, pageAction } = detail;
  try {
    await waitForGrecaptcha();
    const token = await window.grecaptcha.enterprise.execute(SITE_KEY, {
      action: pageAction,
    });
    window.dispatchEvent(new CustomEvent('CAPTCHA_RESULT', {
      detail: { requestId, token },
    }));
  } catch (e) {
    window.dispatchEvent(new CustomEvent('CAPTCHA_RESULT', {
      detail: { requestId, error: e.message },
    }));
  }
});

window.addEventListener('SUBMIT_VIDEO_UI', async ({ detail }) => {
  const { requestId, payload } = detail || {};
  try {
    await submitVideoThroughPage(payload || {});
    window.dispatchEvent(new CustomEvent('SUBMIT_VIDEO_UI_RESULT', { detail: { requestId, clicked: true } }));
  } catch (error) {
    window.dispatchEvent(new CustomEvent('SUBMIT_VIDEO_UI_RESULT', {
      detail: { requestId, error: error?.message || 'PAGE_VIDEO_SUBMIT_FAILED' },
    }));
  }
});

async function submitVideoThroughPage(payload) {
  const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
  const findButton = (predicate) => [...document.querySelectorAll('button')].find(predicate);
  const promptIngredientCount = () => document.querySelectorAll('flow-ingredient-chip').length;
  const press = (element) => {
    if (!element) return;
    for (const type of ['pointerdown', 'mousedown', 'mouseup', 'click']) {
      element.dispatchEvent(new MouseEvent(type, { bubbles: true, view: window }));
    }
  };
  const waitFor = async (finder, timeout = 30000, label = 'unknown') => {
    const deadline = Date.now() + timeout;
    while (Date.now() < deadline) {
      const value = finder();
      if (value) return value;
      await delay(200);
    }
    throw new Error(`PAGE_ELEMENT_TIMEOUT:${label}`);
  };

  const clear = findButton((button) => button.getAttribute('aria-label') === '清除提示');
  clear?.click();
  await waitFor(() => promptIngredientCount() === 0, 10000, 'clear_prompt_media');

  const images = payload.images || [];
  if (!images.length) throw new Error('PAGE_VIDEO_IMAGES_REQUIRED');

  let settings = findButton((button) => button.getAttribute('aria-label') === '设置触发器');
  if (!settings) {
    let agentMode = document.querySelector('flow-agent-mode-toggle-chip button');
    if (!agentMode) {
      const closeAgentPanel = findButton((button) => button.getAttribute('aria-label') === '关闭');
      if (closeAgentPanel) press(closeAgentPanel);
      await delay(500);
    }
    agentMode = await waitFor(() => document.querySelector('flow-agent-mode-toggle-chip button'), 30000, 'agent_mode');
    if (agentMode.getAttribute('aria-pressed') !== 'false') press(agentMode);
    await delay(400);
    const dismiss = findButton((button) => /知道了|Got it/i.test(button.innerText || ''));
    if (dismiss) press(dismiss);
    settings = await waitFor(() => findButton((button) => button.getAttribute('aria-label') === '设置触发器'), 30000, 'settings');
  }
  press(settings);
  await delay(300);
  for (const wanted of ['视频', payload.aspectRatio || '9:16', '360p', `${payload.duration || 10} 秒`, 'x1']) {
    const option = await waitFor(() => [...document.querySelectorAll('.cdk-overlay-container [role=radio],.cdk-overlay-container button')]
      .find((element) => {
        const lines = (element.innerText || '').split('\n').map((line) => line.trim()).filter(Boolean);
        return element.getClientRects().length > 0 && (lines.includes(wanted) || lines.at(-1) === wanted);
      }), 10000, `option:${wanted}`);
    if (!option) throw new Error(`PAGE_VIDEO_OPTION_NOT_FOUND:${wanted}`);
    option.click();
    await delay(350);
  }
  press(settings);

  for (const image of images) {
    const add = await waitFor(() => findButton((button) => button.getAttribute('aria-label') === '在提示框中添加素材'), 30000, 'add_media');
    if (!findButton((button) => button.innerText.includes('上传媒体内容'))) {
      add.click();
      await delay(300);
    }
    const upload = await waitFor(() => findButton((button) => button.innerText.includes('上传媒体内容')), 30000, 'upload_media');
    let fileInput = null;
    const originalClick = HTMLInputElement.prototype.click;
    HTMLInputElement.prototype.click = function () {
      if (this.type === 'file') {
        fileInput = this;
        return;
      }
      return originalClick.call(this);
    };
    try {
      upload.click();
    } finally {
      HTMLInputElement.prototype.click = originalClick;
    }
    if (!fileInput) throw new Error('PAGE_FILE_INPUT_NOT_FOUND');
    const binary = atob(image.imageBase64 || '');
    const bytes = new Uint8Array(binary.length);
    for (let index = 0; index < binary.length; index += 1) bytes[index] = binary.charCodeAt(index);
    const transfer = new DataTransfer();
    transfer.items.add(new File([bytes], image.fileName || 'image.png', { type: image.mimeType || 'image/png' }));
    fileInput.files = transfer.files;
    fileInput.dispatchEvent(new Event('change', { bubbles: true }));

    const asset = await waitFor(() => [...document.querySelectorAll('button[role=option]')]
      .find((button) => {
        const lines = (button.innerText || '').split('\n').map((line) => line.trim()).filter(Boolean);
        return button.getAttribute('role') === 'option' && lines.includes(image.fileName);
      }), 90000, `uploaded_asset:${image.fileName}`);
    asset.click();
    await waitFor(() => asset.classList.contains('asset-item-active'), 10000, `active_asset:${image.fileName}`);

    const previousIngredientCount = promptIngredientCount();
    const attach = await waitFor(
      () => findButton((button) => button.innerText.includes('添加到提示')),
      10000,
      'attach_media',
    );
    await delay(750);
    let attached = false;
    for (let attempt = 0; attempt < 5; attempt += 1) {
      press(attach);
      try {
        await waitFor(
          () => promptIngredientCount() === previousIngredientCount + 1,
          3000,
          `prompt_media:${image.fileName}`,
        );
        attached = true;
        break;
      } catch (error) {
        if (!String(error?.message || '').startsWith('PAGE_ELEMENT_TIMEOUT:prompt_media:')) throw error;
      }
      await delay(500);
    }
    if (!attached) throw new Error(`PAGE_ELEMENT_TIMEOUT:prompt_media:${image.fileName}`);
  }

  if (promptIngredientCount() !== images.length) throw new Error('PAGE_VIDEO_MEDIA_ATTACH_MISMATCH');

  const editor = await waitFor(() => document.querySelector('[contenteditable=true]'), 30000, 'prompt_editor');
  editor.focus();
  const selection = window.getSelection();
  const range = document.createRange();
  range.selectNodeContents(editor);
  selection.removeAllRanges();
  selection.addRange(range);
  document.execCommand('insertText', false, String(payload.prompt || ''));
  editor.dispatchEvent(new InputEvent('input', { bubbles: true, inputType: 'insertText', data: String(payload.prompt || '') }));

  const generate = await waitFor(() => {
    const button = findButton((item) => item.getAttribute('aria-label') === '开始生成');
    return button && !button.disabled ? button : null;
  }, 90000, 'generate_button');
  generate.click();
}

function waitForGrecaptcha(timeout = 10000) {
  return new Promise((resolve, reject) => {
    const start = Date.now();
    const check = () => {
      if (window.grecaptcha?.enterprise?.execute) return resolve();
      if (Date.now() - start > timeout) return reject(new Error('grecaptcha not available'));
      setTimeout(check, 200);
    };
    check();
  });
}
