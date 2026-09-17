// Release publication may reuse only trusted default-branch CI for the exact SHA.
// No artifacts or workflow code from a pull_request run authorize publication.
async function gate(github, context, core, options = {}) {
  const fallback = {checks: true, run_id: context.runId, reused: false};
  const repository = context.payload.repository;
  if (context.eventName !== 'release') return fallback;
  const sha = context.sha;
  const branch = repository.default_branch;
  const now = options.now || Date.now;
  const sleep = options.sleep || (ms => new Promise(resolve => setTimeout(resolve, ms)));
  const deadline = now() + (options.timeout ?? 35 * 60 * 1000);
  while (now() < deadline) {
    const runs = await github.paginate(github.rest.actions.listWorkflowRuns, {
      ...context.repo, workflow_id: 'ci.yaml', head_sha: sha, per_page: 100,
    });
    const run = runs.filter(run => run.id !== context.runId &&
      run.head_sha === sha && run.head_repository?.id === repository.id &&
      run.head_branch === branch &&
      ['push', 'workflow_dispatch'].includes(run.event))
      .sort((a, b) => b.id - a.id)[0];
    if (!run) {
      core.info(`No matching CI for ${branch} at ${sha}; run verification jobs here.`);
      return fallback;
    }
    if (run.status !== 'completed') {
      core.info(`Waiting for CI run ${run.id} on ${sha}`);
      await sleep(15000);
      continue;
    }
    if (run.conclusion !== 'success') throw new Error(`Matching CI run ${run.id} failed (${run.conclusion}). Rerun its failed jobs first.`);
    const jobs = await github.paginate(github.rest.actions.listJobsForWorkflowRun, {
      ...context.repo, run_id: run.id, filter: 'latest', per_page: 100,
    });
    for (const name of ['lint / lint', 'tests / tests', 'build / runtime']) {
      const matches = jobs.filter(job => job.name === name);
      if (matches.length !== 1 || matches[0].status !== 'completed' || matches[0].conclusion !== 'success') {
        throw new Error(`CI run ${run.id} has no successful ${name} job`);
      }
    }
    const artifacts = await github.paginate(github.rest.actions.listWorkflowRunArtifacts, {
      ...context.repo, run_id: run.id, per_page: 100,
    });
    if (!artifacts.some(artifact => artifact.name === 'runtime-binary' && !artifact.expired)) {
      core.info('Verified binary artifact is missing/expired; run full verification again.');
      return fallback;
    }
    core.info(`Reusing verified source and runtime binary from CI run ${run.id} on ${sha}.`);
    return {checks: false, run_id: run.id, reused: true};
  }
  throw new Error('Timed out waiting for matching CI; verification remains blocked.');
}
module.exports = {gate};
