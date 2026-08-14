# TypeScript package releases

The TypeScript SDK is one release set for now. `@zenml-io/kitaru`, `@zenml-io/kitaru-mastra`, and `@zenml-io/kitaru-vercel-ai` use the same version and are published from the same tested artifacts.

## Prepare a release

1. Update the version in all three package manifests and both adapters' exact `workspace:` dependency on `@zenml-io/kitaru`.
2. Use stable SemVer or an `-rc.N` prerelease. Release candidates are published under npm's `rc` tag; stable versions use `latest`.
3. Run `pnpm run pack:check` locally.
4. Run the **Release TypeScript packages** workflow manually with `typescript/kitaru/v<VERSION>`. A manual run builds and tests the artifacts but cannot publish them.
5. After the change is on `develop`, push the same namespaced tag. The tag-triggered run publishes core first, then the two adapters, verifies a clean registry install, and creates a GitHub release.

Do not publish these packages from a local checkout. The workflow requires the tagged commit to be part of `develop` and publishes only the tarballs produced by its build job.

## npm authentication

Configure npm trusted publishing for each package with this repository, `.github/workflows/release-typescript.yml`, and the `npm-publish` GitHub environment.

Trusted publishing cannot create a new npm package. For the first release only, add a short-lived granular access token with publish access as the `NPM_TOKEN` environment secret, run the tag release, configure trusted publishing on all three newly created packages, then remove the secret and revoke the token. Later releases use GitHub Actions OIDC and need no npm token.
