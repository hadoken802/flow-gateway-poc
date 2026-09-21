const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { PageWorkflow } = require('../../extension/page-workflow.js');

async function runSubmission(uiResult) {
  const manifest = JSON.parse(fs.readFileSync(path.join(__dirname, '../../extension/manifest.json')));
  const source = fs.readFileSync(path.join(__dirname, '../../extension/background.js'), 'utf8');
  const code = source.slice(source.indexOf('async function handlePageSubmitVideo('), source.indexOf('async function handlePageVideoStatus('));
  let clicks = 0;
  const responses = [];
  const stored = {};
  const workflow = new PageWorkflow({ get: async key => ({ [key]: stored[key] }), set: async value => Object.assign(stored, structuredClone(value)) });
  const context = {
    pageWorkflow: workflow,
    chrome: {
      storage: { local: { set: async () => {} } },
      tabs: { query: async () => [{ id: 1, url: 'https://flow.google.com/project/project' }], reload: async () => {}, sendMessage: async () => ({ prepared: true, ...uiResult }) },
      webRequest: { onBeforeRequest: { addListener() {}, removeListener() {} } },
      scripting: { executeScript: async () => { clicks++; return [{ result: { x: 1, y: 1 } }]; } },
    },
    FLOW_TAB_PATTERNS: [], pageVideoJobs: new Map(),
    waitForTabComplete: async () => {},
    waitForPageVideoBridge: async () => {},
    waitForStablePageVideoIds: async () => ({ mediaIds: [] }),
    trustedClick: async () => { clicks++; },
    waitForPageVideoSubmission: async () => { throw new Error('REMOTE_SUBMISSION_NOT_CONFIRMED'); },
    fallbackToWebSocket() {}, sendToAgent: async (response) => responses.push(response),
  };
  vm.createContext(context);
  vm.runInContext(code, context);
  await context.handlePageSubmitVideo({ id: 'request', params: { projectId: 'project' } });
  return { clicks, response: responses.at(-1) };
}

test('preparation failure never clicks Generate', async () => {
  const result = await runSubmission({ error: 'PAGE_ELEMENT_TIMEOUT:option:视频' });
  assert.equal(result.clicks, 0);
  assert.match(result.response.error, /PAGE_ELEMENT_TIMEOUT:option:视频/);
});

test('unconfirmed submission is never clicked a second time', async () => {
  const result = await runSubmission({ generateX: 1, generateY: 1 });
  assert.equal(result.clicks, 1);
  assert.equal(result.response.error, 'REMOTE_SUBMISSION_NOT_CONFIRMED');
});

async function preparePage({ initiallyOpen = false, autoAttach = false, splitStages = false } = {}) {
  const source = fs.readFileSync(path.join(__dirname, '../../extension/injected.js'), 'utf8');
  const code = source.slice(source.indexOf('async function submitVideoThroughPage('), source.indexOf('function waitForGrecaptcha('));
  let clock = 0, open = initiallyOpen, count = 0, uploaded = false, selected = false, attached = false;
  const button = (label, action = () => {}) => ({
    innerText: label, disabled: false, getClientRects: () => [1],
    getAttribute: (name) => name === 'aria-label' ? label : null,
    click: action, dispatchEvent: (event) => { if (event.type === 'click') action(); },
    scrollIntoView() {}, getBoundingClientRect: () => ({ left: 0, top: 0, width: 10, height: 10 }),
    classList: { contains: () => selected },
  });
  const attach = () => { count = 1; attached = true; };
  class Input { constructor() { this.type = 'file'; } click() {} dispatchEvent() { uploaded = true; } }
  const buttons = [button('清除提示', () => { count = 0; }), button('设置触发器', () => { open = !open; }),
    button('在提示框中添加素材'), button('上传媒体内容', () => new Input().click()), button('开始生成')];
  const asset = button('image.png', () => { selected = true; if (autoAttach) attach(); });
  asset.getAttribute = (name) => name === 'role' ? 'option' : name === 'aria-selected' ? String(selected) : null;
  const options = ['视频', '9:16', '720p', '10 秒', 'x1'].map((value) => button(value));
  const editor = { focus() {}, dispatchEvent() {} };
  const document = {
    querySelectorAll(selector) {
      if (selector === 'button') return [...buttons, ...(selected && !attached ? [button('添加到提示', attach)] : [])];
      if (selector === 'flow-ingredient-chip') return Array(count).fill({});
      if (selector.includes('.cdk-overlay-container')) return open ? options : [];
      if (selector === 'button[role=option]') return uploaded && !attached ? [asset] : [];
      return [];
    },
    querySelector: () => editor,
    createRange: () => ({ selectNodeContents() {} }), execCommand() {},
  };
  const context = { document, window: { getSelection: () => ({ removeAllRanges() {}, addRange() {} }) },
    Date: { now: () => clock }, setTimeout: (fn, ms) => { clock += ms; fn(); },
    HTMLInputElement: Input, Uint8Array, atob: () => 'x', File: class {},
    DataTransfer: class { constructor() { this.items = { add() {} }; this.files = []; } },
    Event: class { constructor(type) { this.type = type; } },
  };
  context.MouseEvent = context.InputEvent = context.Event;
  vm.createContext(context); vm.runInContext(code, context);
  const payload = { images: [{ fileName: 'image.png' }], prompt: 'original' };
  if (splitStages) {
    for (const stage of ['settings', 'upload', 'attach']) {
      assert.equal((await context.submitVideoThroughPage({ ...payload, stage, imageIndex: 0 })).verified, true);
    }
    return context.submitVideoThroughPage({ ...payload, stage: 'prompt' });
  }
  return context.submitVideoThroughPage(payload);
}

test('already-open settings are not toggled closed', async () => {
  assert.equal((await preparePage({ initiallyOpen: true })).generateX, 5);
});

test('asset selection that attaches immediately is accepted', async () => {
  assert.equal((await preparePage({ autoAttach: true })).generateX, 5);
});

test('concurrent pointer actions on one tab serialize debugger ownership', async () => {
  const source = fs.readFileSync(path.join(__dirname, '../../extension/background.js'), 'utf8');
  const start = source.includes('const debuggerQueues') ? source.indexOf('const debuggerQueues') : source.indexOf('async function trustedClick(');
  const code = source.slice(start, source.indexOf('function waitForTabComplete('));
  let attached = false, events = 0;
  const context = { setTimeout, chrome: { debugger: {
    attach: async () => { if (attached) throw new Error('Another debugger is already attached'); attached = true; },
    sendCommand: async () => { events++; await new Promise(resolve => setTimeout(resolve, 1)); },
    detach: async () => { attached = false; },
  } } };
  vm.createContext(context); vm.runInContext(code, context);
  await Promise.all([context.trustedMove(1, 1, 1), context.trustedClick(1, 1, 1)]);
  assert.equal(events, 4);
  assert.equal(attached, false);
});

for (const ambiguous of [false, true]) {
  test(`page recovery ${ambiguous ? 'rejects multiple results' : 'binds the unique result without submitting'}`, async () => {
    const source = fs.readFileSync(path.join(__dirname, '../../extension/background.js'), 'utf8');
    const code = source.slice(source.indexOf('async function reconcilePageProject('), source.indexOf('async function handlePageVideoStatus('));
    const projectId = '11111111-1111-4111-8111-111111111111';
    const mediaId = '22222222-2222-4222-8222-222222222222';
    let response;
    const jobs = new Map();
    const context = { pageWorkflow: { transition: async () => {} }, chrome: { tabs: { query: async () => [{ id: 1, url: `https://flow.google.com/project/${projectId}` }] } },
      FLOW_TAB_PATTERNS: [], waitForTabComplete: async () => {}, waitForPageVideoBridge: async () => {},
      waitForStablePageVideoIds: async () => ({ mediaIds: ambiguous ? [mediaId, projectId] : [mediaId] }),
      hydratePageVideoJobs: async () => {}, persistPageVideoJobs: async () => {}, pageVideoJobs: jobs,
      fallbackToWebSocket() {}, sendToAgent: async (result) => { response = result; } };
    vm.createContext(context); vm.runInContext(code, context);
    await context.handlePageReconcileVideo({ id: 'recovery', params: { projectId } });
    assert.equal(response.status, ambiguous ? 409 : 200);
    assert.equal(jobs.size, ambiguous ? 0 : 1);
  });
}

test('separate settings, upload, attachment and prompt stages prepare one generation', async () => {
  assert.equal((await preparePage({ splitStages: true })).generateX, 5);
});

test('project creation never binds an unrelated existing project tab', async () => {
  const source = fs.readFileSync(path.join(__dirname, '../../extension/background.js'), 'utf8');
  const code = source.slice(source.indexOf('async function handlePageCreateProject('), source.indexOf('async function handlePageSubmitVideo('));
  const home = { id: 1, url: 'https://flow.google.com/' };
  const old = { id: 2, url: 'https://flow.google.com/project/11111111-1111-4111-8111-111111111111' };
  let response, now = 0;
  const context = { FLOW_TAB_PATTERNS: [], Date: { now: () => now += 1000 }, setTimeout: fn => fn(),
    chrome: { storage: { local: { set: async () => {} } }, tabs: {
      query: async () => [home, old], get: async () => home, update: async () => {}, reload: async () => {},
    } }, waitForNewProjectButton: async () => ({ x: 1, y: 1 }), trustedClick: async () => {},
    clickNewProjectButtonInPage: async () => {}, fallbackToWebSocket() {}, sendToAgent: async result => { response = result; } };
  vm.createContext(context); vm.runInContext(code, context);
  await context.handlePageCreateProject({ id: 'request' });
  assert.equal(response.status, 502);
  assert.equal(response.error, 'PROJECT_NAVIGATION_TIMEOUT');
});


test('project creation activates its target tab before clicking New project', async () => {
  const source = fs.readFileSync(path.join(__dirname, '../../extension/background.js'), 'utf8');
  const code = source.slice(source.indexOf('async function handlePageCreateProject('), source.indexOf('async function handlePageSubmitVideo('));
  let active = false, clicked = false;
  const replies = [];
  const project = '11111111-2222-4333-8444-555555555555';
  const context = {
    chrome: { storage: { local: { set: async () => {} } },
      tabs: { query: async () => [{id: 1, url: 'https://flow.google.com/'}],
        update: async (_, data) => { active = data.active === true || active; return {}; },
        get: async () => ({id:1, url:'https://flow.google.com/project/'+project}) } },
    FLOW_TAB_PATTERNS: [], waitForNewProjectButton: async () => ({x:1,y:1}),
    trustedClick: async () => { assert.equal(active, true); clicked = true; },
    setTimeout: fn => fn(), Date,
    fallbackToWebSocket() {}, sendToAgent: async value => replies.push(value),
  };
  vm.createContext(context); vm.runInContext(code, context);
  await context.handlePageCreateProject({id:'request'});
  assert.equal(clicked, true);
  assert.equal(replies.at(-1).status, 200);
});


test('Generate click rechecks its semantic target after debugger attachment', async () => {
  const source = fs.readFileSync(path.join(__dirname, '../../extension/background.js'), 'utf8');
  const code = source.slice(source.indexOf('async function trustedClick('), source.indexOf('async function trustedMove('));
  let attached = false;
  const events = [];
  const context = {
    withTabDebugger: async (_, fn) => { attached = true; return fn({tabId:1}); },
    chrome: {tabs: {sendMessage: async (_, msg) => {
      assert.equal(attached, true); assert.equal(msg.type, 'GET_PAGE_VIDEO_GENERATE_TARGET');
      return {x:20,y:40};
    }}, debugger: {sendCommand: async (_, __, event) => events.push(event)},
    storage: {local:{set:async () => {}}}},
  };
  vm.createContext(context);vm.runInContext(code, context);
  await context.trustedClick(1, 10, 10, 'generate');
  assert.equal(events.length, 3);
  assert.ok(events.every(event => event.x === 20 && event.y === 40));
});


test('an obscured Generate target sends no pointer event', async () => {
  const source = fs.readFileSync(path.join(__dirname, '../../extension/background.js'), 'utf8');
  const code = source.slice(source.indexOf('async function trustedClick('), source.indexOf('async function trustedMove('));
  let events = 0;
  const context = {withTabDebugger: async (_, fn) => fn({tabId:1}),
    chrome: {tabs:{sendMessage:async () => ({error:'GENERATE_TARGET_OBSCURED'})},
      debugger:{sendCommand:async () => {events++;}}}};
  vm.createContext(context);vm.runInContext(code, context);
  await assert.rejects(context.trustedClick(1,10,10,'generate'), /GENERATE_TARGET_OBSCURED/);
  assert.equal(events,0);
});
