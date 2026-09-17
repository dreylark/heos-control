# Releases

The repository prepares release pull requests and publishes versioned artifacts.
Deployment and environment configuration belong to the consumer.

## Prepare and review

Open **Actions → Prepare release → Run workflow** on the default branch. Choose
`patch`, `minor`, `major`, or `explicit` with a SemVer value without the `v` prefix.
The version must be greater than the current version and its tag must not exist.
Explicit versions may skip numbers; review that choice in the pull request.

[The preparation workflow](../.github/workflows/prepare-release.yaml) updates
`VERSION`, chart `version`/`appVersion`, OpenAPI `info.version` and generated API
bindings. It checks synchronized versions and the release helper before creating
or updating `release/v<VERSION>`. An advancing default branch fails preparation;
rerun from its new head. Avoid editing the generated branch by hand.

Enable **Settings → Actions → General → Workflow permissions → Allow GitHub
Actions to create and approve pull requests**. Preparation uses `GITHUB_TOKEN`
with contents and pull-request write permissions; no personal token is required.
It does not approve or merge its own PR.

GitHub creates a native `pull_request` CI run for bot-created PRs, initially
requiring approval. Open the PR and choose **Approve workflows to run** when
prompted. This is approval to execute CI, separate from reviewing or merging the
PR. See [GitHub's workflow trigger rules](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/trigger-a-workflow).

Wait for **CI**, then review and merge manually. A green **Prepare release** run
only confirms preparation. CI runs once for the PR and checks GitHub's merge
commit, including lint, race-enabled tests and coverage, runtime smoke tests and
full container integration/recovery qualification for same-repository `release/v*`
branches. Preparation does not dispatch a second branch run. If CI fails, rerun
the failed PR run after resolving the cause; a manually dispatched branch run
does not replace the PR's merge-commit checks. Explicit full CI dispatch remains
available for separate diagnostics.

## Publish

After merging, create and publish a GitHub Release with tag `v<VERSION>` at the
merged commit. Draft releases and tag pushes alone do not publish packages;
published prereleases are supported.

[The release workflow](../.github/workflows/release.yaml) reuses successful
main-branch CI only for the exact release commit and a retained runtime artifact.
It waits for an existing matching run or performs fresh verification when no
usable run exists. Full container integration and recovery/resource qualification
always run before publication, even when ordinary checks and compilation are
reused. Failed, cancelled or skipped required checks block publication. PR runs
cannot authorize publication or publish packages.

The release publishes an OCI image and Helm chart in GitHub Container Registry:

- Image: `ghcr.io/dreylark/heos-control:<VERSION>`.
- Chart: `oci://ghcr.io/dreylark/heos-control/charts/heos-control`, version `<VERSION>`.

The published chart pins the image digest produced by that release. The workflow
verifies the binary's embedded version and commit, retains published chart and
reference metadata as artifacts, and records their references in its summary.
Source chart defaults remain suitable for local builds; digest substitution
happens in the staged publication copy.

Consumers select the chart version and provide their own configuration, database
and Secrets. Package visibility is a GitHub setting: private packages require
registry credentials even when their source repository is public.
