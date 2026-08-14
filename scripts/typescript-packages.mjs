import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const defaultRepositoryRoot = resolve(
  fileURLToPath(new URL("..", import.meta.url)),
);
const versionPattern = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(-rc\.(0|[1-9]\d*))?$/;
const repositoryUrl = "https://github.com/zenml-io/kitaru.git";
const homepage = "https://github.com/zenml-io/kitaru#readme";
const bugsUrl = "https://github.com/zenml-io/kitaru/issues";
const registry = "https://registry.npmjs.org/";

const packageDefinitions = [
  { name: "@zenml-io/kitaru", path: "packages/core" },
  {
    name: "@zenml-io/kitaru-mastra",
    path: "packages/mastra",
  },
  {
    name: "@zenml-io/kitaru-vercel-ai",
    path: "packages/vercel-ai",
  },
];

function assertEqual(actual, expected, message) {
  if (actual !== expected) {
    throw new Error(`${message}: expected ${expected}, found ${String(actual)}`);
  }
}

function tarballName(packageName, version) {
  return `${packageName.slice(1).replace("/", "-")}-${version}.tgz`;
}

export async function loadTypescriptPackageMetadata({
  repositoryRoot = defaultRepositoryRoot,
  tag,
} = {}) {
  const packages = await Promise.all(
    packageDefinitions.map(async (definition) => {
      const manifestPath = resolve(repositoryRoot, definition.path, "package.json");
      const manifest = JSON.parse(await readFile(manifestPath, "utf8"));
      return { ...definition, manifest };
    }),
  );

  const version = packages[0].manifest.version;
  if (typeof version !== "string" || !versionPattern.test(version)) {
    throw new Error(
      `TypeScript package version must be stable SemVer or an rc prerelease, found ${String(version)}`,
    );
  }

  for (const packageEntry of packages) {
    const { manifest, name, path } = packageEntry;
    assertEqual(manifest.name, name, `${path} package name`);
    if (manifest.version !== version) {
      throw new Error(`${name} must share version ${version}, found ${manifest.version}`);
    }
    assertEqual(manifest.license, "Apache-2.0", `${name} license`);
    assertEqual(manifest.repository?.type, "git", `${name} repository type`);
    assertEqual(manifest.repository?.url, repositoryUrl, `${name} repository URL`);
    assertEqual(manifest.repository?.directory, path, `${name} repository directory`);
    assertEqual(manifest.homepage, homepage, `${name} homepage`);
    assertEqual(manifest.bugs?.url, bugsUrl, `${name} bugs URL`);
    assertEqual(manifest.publishConfig?.access, "public", `${name} publish access`);
    assertEqual(manifest.publishConfig?.registry, registry, `${name} publish registry`);
  }

  for (const packageEntry of packages.slice(1)) {
    const expectedDependency = `workspace:${version}`;
    const actualDependency =
      packageEntry.manifest.dependencies?.["@zenml-io/kitaru"];
    if (actualDependency !== expectedDependency) {
      throw new Error(
        `${packageEntry.name} must depend on @zenml-io/kitaru as ${expectedDependency}, found ${String(actualDependency)}`,
      );
    }
  }

  const expectedTag = `typescript/kitaru/v${version}`;
  if (tag !== undefined && tag !== expectedTag) {
    throw new Error(`${tag} does not match package version ${version}`);
  }

  return {
    version,
    tag: expectedTag,
    npm_tag: version.includes("-rc.") ? "rc" : "latest",
    prerelease: version.includes("-rc."),
    packages: packages.map(({ name, path }) => ({
      name,
      path,
      tarball: tarballName(name, version),
    })),
  };
}

function parseArguments(args) {
  const options = {};
  for (let index = 0; index < args.length; index += 1) {
    const option = args[index];
    if (option !== "--repo-root" && option !== "--tag") {
      throw new Error(`Unknown option: ${option}`);
    }
    const value = args[index + 1];
    if (value === undefined) {
      throw new Error(`${option} requires a value`);
    }
    if (option === "--repo-root") {
      options.repositoryRoot = resolve(value);
    } else {
      options.tag = value;
    }
    index += 1;
  }
  return options;
}

async function main() {
  const metadata = await loadTypescriptPackageMetadata(
    parseArguments(process.argv.slice(2)),
  );
  process.stdout.write(`${JSON.stringify(metadata)}\n`);
}

const invokedPath = process.argv[1] && pathToFileURL(resolve(process.argv[1])).href;
if (invokedPath === import.meta.url) {
  main().catch((error) => {
    process.stderr.write(`${error.message}\n`);
    process.exitCode = 1;
  });
}
