/* Durable boundary between reversible page preparation and billable submission. */
(function (root) {
  class PageWorkflow {
    constructor(storage, now = () => Date.now()) {
      this.storage = storage;
      this.now = now;
      this.claims = new Map();
    }
    async read(projectId) {
      return (await this.storage.get(`workflow:${projectId}`))[`workflow:${projectId}`]
        || { projectId, phase: 'preparing', stage: 'settings', failures: {}, revision: 1 };
    }
    async save(state) {
      state.updatedAt = this.now();
      await this.storage.set({ [`workflow:${state.projectId}`]: state,
        lastWorkflow: { projectId: state.projectId, phase: state.phase, stage: state.stage,
          updatedAt: state.updatedAt, failures: state.failures, error: state.error || null } });
    }
    async prepare(projectId, stages, execute, reload) {
      const state = await this.read(projectId);
      if (['submit_intent', 'submitted', 'completed', 'needs_review'].includes(state.phase)) return { resumeOnly: true };
      if (state.phase === 'failed_preparation') throw new Error(state.error || 'WORKFLOW_PREPARATION_EXHAUSTED');
      const deadline = this.now() + 210000;
      let prepared;
      // Validate from the first stage after a reload. Uploaded assets are reused;
      // the persisted submission boundary is never reset by page recovery.
      for (let index = 0; index < stages.length;) {
        const stage = stages[index];
        state.phase = 'preparing'; state.stage = stage; state.error = null;
        await this.save(state);
        try {
          if (this.now() >= deadline) throw new Error('WORKFLOW_PREPARATION_DEADLINE');
          prepared = await execute(stage, deadline);
          index += 1;
        } catch (error) {
          state.failures[stage] = (state.failures[stage] || 0) + 1;
          state.error = `WORKFLOW_STAGE_FAILED:${stage}:${error.message}`;
          if (state.failures[stage] >= 2 || this.now() >= deadline
              || !/TIMEOUT|NOT_FOUND|MISMATCH|message port|message channel|Receiving end|context invalidated/i.test(error.message || '')) {
            state.phase = 'failed_preparation'; await this.save(state);
            throw new Error(state.error);
          }
          await this.save(state);
          await reload();
          index = 0;
        }
      }
      state.phase = 'prepared'; state.error = null; await this.save(state);
      return prepared;
    }
    async claimSubmission(projectId) {
      const previous = this.claims.get(projectId) || Promise.resolve();
      const claim = previous.catch(() => {}).then(async () => {
        const state = await this.read(projectId);
        if (state.phase !== 'prepared') return false;
        state.phase = 'submit_intent'; state.stage = 'submit';
        await this.save(state); // Persist BEFORE the first pointer event.
        return true;
      });
      this.claims.set(projectId, claim);
      try { return await claim; }
      finally { if (this.claims.get(projectId) === claim) this.claims.delete(projectId); }
    }
    async transition(projectId, phase, fields = {}) {
      const state = await this.read(projectId);
      Object.assign(state, fields, { phase, stage: phase });
      await this.save(state);
    }
  }
  root.PageWorkflow = PageWorkflow;
  if (typeof module !== 'undefined') module.exports = { PageWorkflow };
})(globalThis);
