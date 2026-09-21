const { test } = require('node:test');
const assert = require('node:assert/strict');
const { PageWorkflow } = require('../../extension/page-workflow.js');
function storage() {
  const data = {};
  return { get: async key => ({ [key]: structuredClone(data[key]) }),
    set: async values => Object.assign(data, structuredClone(values)) };
}
test('restarting after submission intent never claims a second click', async () => {
  const db = storage(), first = new PageWorkflow(db);
  await first.prepare('project', ['settings', 'upload', 'attach', 'prompt'], async () => ({}), async () => {});
  assert.equal(await first.claimSubmission('project'), true);
  const resumed = new PageWorkflow(db);
  assert.equal((await resumed.prepare('project', [], () => assert.fail(), () => assert.fail())).resumeOnly, true);
  assert.equal(await resumed.claimSubmission('project'), false);
});
test('concurrent submit claims allow exactly one click', async () => {
  const workflow = new PageWorkflow(storage());
  await workflow.prepare('project', ['prompt'], async () => ({}), async () => {});
  assert.deepEqual(await Promise.all([workflow.claimSubmission('project'), workflow.claimSubmission('project')]), [true, false]);
});
test('preparation reloads once, revalidates, and resumes without submitting', async () => {
  const workflow = new PageWorkflow(storage());
  let uploads = 0, reloads = 0;
  await workflow.prepare('project', ['settings', 'upload'], async stage => {
    if (stage === 'upload' && ++uploads === 1) throw new Error('PAGE_ELEMENT_TIMEOUT:upload');
  }, async () => { reloads++; });
  assert.equal(uploads, 2); assert.equal(reloads, 1);
  assert.equal((await workflow.read('project')).phase, 'prepared');
});
test('repeated stage failure stops and survives process restart', async () => {
  const db = storage(), workflow = new PageWorkflow(db);
  await assert.rejects(workflow.prepare('project', ['upload'], async () => { throw new Error('PAGE_ELEMENT_TIMEOUT:upload'); }, async () => {}));
  assert.equal((await workflow.read('project')).phase, 'failed_preparation');
  await assert.rejects(new PageWorkflow(db).prepare('project', ['upload'], () => assert.fail(), () => assert.fail()));
});
