# TypeScript package releases

The TypeScript SDK is one release set for now. `@zenml-io/kitaru`, `@zenml-io/kitaru-mastra`, and `@zenml-io/kitaru-vercel-ai` use the same version and are published from the same tested artifacts.

## Prepare a release

1. Update the version in all three package manifests and both adapters' exact `workspace:` dependency on `@zenml-io/kitaru`.
2. Use stable SemVer or an `-rc.N` prerelease. Release candidates are published under npm's `rc` tag; stable versions use `latest`.
3. Run `pnpm run pack:check` locally.
4. Run the **Release TypeScript packages** workflow manually with `typescript/kitaru/v<VERSION>`. A manual run builds and tests the artifacts but cannot publish them.
5. Download the `typescript-distributions` artifact from the rehearsal, verify `SHA256SUMS`, inspect the three tarballs, and smoke-test their public exports in a clean project.
6. After the change is on `develop`, create the immutable tag on the exact rehearsed commit and push it. The tag-triggered run publishes core first, then the two adapters, verifies a clean registry install, and creates a GitHub release.

Do not publish these packages from a local checkout. The workflow requires the tagged commit to be part of `develop` and publishes only the tarballs produced by its build job.

## Verify and recover a release

npm scans newly published packages before making them installable. The publish command can succeed while unauthenticated `npm view` and `npm install` still return 404. The workflow waits up to 20 minutes for all three packages to become visible before performing one clean installation and export check.

If publishing succeeds but verification times out, do not create a new version or move the immutable tag. Confirm all three exact versions with `npm view`, then rerun the failed workflow jobs. The publish preflight downloads any existing version and skips it only when its tarball exactly matches the release artifact. After the run succeeds, confirm that the GitHub release is a prerelease for an RC and contains all three tarballs plus `SHA256SUMS`.

The first published version of a new npm package may acquire a `latest` dist-tag even when it is published with `--tag rc`. The version remains a prerelease, and the `rc` tag remains the supported RC channel. Check all three dist-tags after the first release so the default-install behavior is an explicit choice.

## npm authentication

Configure npm trusted publishing separately for each package with organization `zenml-io`, repository `kitaru`, workflow filename `release-typescript.yml`, environment `npm-publish`, and the `npm publish` allowed action.

Trusted publishing cannot create a new npm package. For the first release only, add a short-lived granular access token with publish access as the `NPM_TOKEN` repository or `npm-publish` environment secret, run the tag release, configure trusted publishing on all three newly created packages, then remove the secret and revoke the token. Later releases use GitHub Actions OIDC and need no npm token.

After all three trusted publishers are configured, select npm's publishing-access option that disallows bypass-2FA tokens, remove `NPM_TOKEN` from GitHub, revoke the bootstrap token on npmjs.com, and use the next manual rehearsal plus RC tag to prove OIDC publishing end to end.
