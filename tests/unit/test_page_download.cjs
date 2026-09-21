const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync(require('node:path').join(__dirname, '../../extension/background.js'), 'utf8');
const code = source.slice(source.indexOf('async function handlePageVideoMedia('), source.indexOf('async function waitForNewProjectButton('));

async function runDownload({ cached = false, sourceReady = false } = {}) {
  let listener, clicks = 0;
  const replies = [], stored = {};
  const item = { id: 42, state: 'complete', filename: 'C:/Downloads/output.mp4', mime: 'video/mp4',
    url: 'https://labs.google/video/output', referrer: 'https://labs.google/project/project' };
  const job = { tabId: 1, projectId: 'project', outputId: 'output', ...(cached ? { downloadId: 42 } : {}) };
  let activated = false;
  const context = {
    pageVideoJobs: new Map([['job', job]]), hydratePageVideoJobs: async () => {},
    getPageVideoJob: async () => job, persistPageVideoJobs: async () => {},
    chrome: {
      downloads: { download: async () => 42, onCreated: { addListener(fn) { listener = fn; }, removeListener() {} }, search: async () => [item] },
      storage: { local: { set: async value => Object.assign(stored, value) } },
      tabs: { update: async () => { activated = true; return {}; }, sendMessage: async (_, msg) => {
        if (msg.type === 'GET_PAGE_VIDEO_SOURCE') return sourceReady ? {source:'https://labs.google/video/output',width:720,height:1280} : {};
        if (msg.type === 'GET_PAGE_VIDEO_DOWNLOAD_STATE') return { menuItems: [], hidden: false };
        if (msg.target === 'resolution') return { error: 'PAGE_VIDEO_DOWNLOAD_RESOLUTION_NOT_FOUND' };
        return { x: msg.target === 'download' ? 20 : 10, y: 10 };
      } },
    },
    trustedMove: async () => {},
    trustedPressEnter: async () => { clicks++; await listener(item); },
    trustedClick: async (_, x) => { clicks++; if (x === 20) {
      await listener({ id: 99, mime: 'video/mp4', filename: 'unrelated.mp4', url: 'https://unrelated.example/video.mp4' });
      await listener(item);
    } },
    setTimeout: (fn, ms) => { if (ms < 30000) fn(); return 1; }, clearTimeout() {},
    fallbackToWebSocket() {}, sendToAgent: async value => replies.push(value),
  };
  vm.createContext(context);
  vm.runInContext(code, context);
  await context.handlePageVideoMedia({ id: 'request', params: { mediaId: 'output' } });
  return { response: replies.at(-1), clicks, job, activated };
}

test('direct download success does not require a resolution submenu', async () => {
  const { response, job, activated } = await runDownload();
  assert.equal(response.status, 200);
  assert.equal(response.data.localFilePath, 'C:/Downloads/output.mp4');
  assert.equal(job.downloadId, 42);
  assert.equal(activated, true);
});

test('retry after completed browser download reuses persisted download identity', async () => {
  const { response, clicks } = await runDownload({ cached: true });
  assert.equal(response.status, 200);
  assert.equal(clicks, 0);
});

test('resolved full-size source downloads without interacting with any menu', async () => {
  const { response, clicks, job } = await runDownload({ sourceReady: true });
  assert.equal(response.status, 200);
  assert.equal(clicks, 0);
  assert.equal(job.downloadId, 42);
});

test('resolution lookup cannot click the composer settings button containing 720p', async () => {
  const content = fs.readFileSync(require('node:path').join(__dirname, '../../extension/content.js'), 'utf8');
  const coords = content.slice(content.indexOf('async function getPageVideoDownloadCoords('), content.indexOf('function bridgePageEvent('));
  const element = (text, role, x) => ({ innerText: text, getClientRects: () => [1],
    getAttribute: () => role, closest: () => role ? {} : null,
    getBoundingClientRect: () => ({ left: x, top: 0, width: 10, height: 10 }) });
  const settings = element('视频 · 720p · 10 秒\ncrop_9_16\nx1', null, 1100);
  const resolution = element('720p 原始尺寸', 'menuitem', 80);
  const context = { document: { querySelectorAll: () => [settings, resolution] }, setTimeout };
  vm.createContext(context);
  vm.runInContext(coords, context);
  const result = await context.getPageVideoDownloadCoords('output', 'resolution');
  assert.equal(result.x, 85);
  assert.match(result.label, /原始尺寸/);
});

test('lazy video metadata is loaded before selecting the full-size source', async () => {
  const content = fs.readFileSync(require('node:path').join(__dirname, '../../extension/content.js'), 'utf8');
  let listener, loads = 0;
  const handlers = {};
  const video = { src: 'https://labs.google/video/output', readyState: 0, videoWidth: 0, videoHeight: 0,
    addEventListener: (name, fn) => { handlers[name] = fn; }, removeEventListener() {},
    load() { loads++; this.videoWidth = 720; this.videoHeight = 1280; handlers.loadedmetadata(); },
    closest: () => tile,
  };
  const tile = { querySelector: () => video };
  const context = { setTimeout, clearTimeout,
    document: { readyState: 'complete', head: null,
      querySelectorAll: selector => selector === 'flow-video-tile' ? [tile] : [video] },
    window: { addEventListener() {} },
    chrome: { runtime: { onMessage: { addListener(fn) { listener = fn; } } } },
  };
  vm.createContext(context);
  vm.runInContext(content, context);
  const result = await new Promise(resolve => listener({ type: 'GET_PAGE_VIDEO_SOURCE', mediaId: 'output' }, null, resolve));
  assert.equal(loads, 1);
  assert.equal(result.source, video.src);
  assert.equal(result.width, 720);
});


test('normalized download failure offers download-only recovery', () => {
  const gateway = fs.readFileSync(require('node:path').join(__dirname, '../../gateway/main.py'), 'utf8');
  const code = gateway.slice(gateway.indexOf('function taskAction('), gateway.indexOf('function taskAction(')+gateway.slice(gateway.indexOf('function taskAction(')).indexOf('\n'));
  const context = {esc: value => value, isDryRunTask: () => false};
  vm.createContext(context);vm.runInContext(code, context);
  assert.match(context.taskAction({task_id:'job',status:'failed',last_error_category:'download_failed'}), /retryDownload/);
  assert.doesNotMatch(context.taskAction({task_id:'other',status:'failed',last_error_category:'page_preparation_failed'}), /retryDownload/);
});
