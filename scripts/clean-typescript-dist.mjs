import { existsSync, readFileSync, rmSync } from "node:fs";
import { basename, dirname, join, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const packageRoot = resolve(process.cwd());
const relativeRoot = packageRoot.slice(repositoryRoot.length + 1);
const allowedRoot =
  packageRoot.startsWith(`${repositoryRoot}${sep}packages${sep}`) ||
  packageRoot.startsWith(`${repositoryRoot}${sep}examples${sep}typescript${sep}`);

if (!allowedRoot || !existsSync(join(packageRoot, "package.json"))) {
  throw new Error(
    `Refusing to clean dist outside a TypeScript package root: ${relativeRoot || basename(packageRoot)}`,
  );
}

const packageJson = JSON.parse(
  readFileSync(join(packageRoot, "package.json"), "utf8"),
);
if (typeof packageJson.name !== "string") {
  throw new Error(`Package at ${relativeRoot} has no name`);
}

rmSync(join(packageRoot, "dist"), { force: true, recursive: true });
