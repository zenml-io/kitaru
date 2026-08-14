import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import openapiTS, { astToString } from "openapi-typescript";

const packageRoot = fileURLToPath(new URL("..", import.meta.url));
const schemaPath = fileURLToPath(
  new URL("../../../openapi/openapi.json", import.meta.url),
);
const outputPath = fileURLToPath(
  new URL("../src/generated/openapi.ts", import.meta.url),
);

const schema = JSON.parse(await readFile(schemaPath, "utf8"));
const generated = `${astToString(await openapiTS(schema)).trimEnd()}\n`;

if (process.argv.includes("--check")) {
  const existing = await readFile(outputPath, "utf8").catch(() => "");
  if (existing !== generated) {
    console.error(
      `Generated OpenAPI types are out of date. Run: pnpm --dir ${packageRoot} generate`,
    );
    process.exitCode = 1;
  }
} else {
  await mkdir(dirname(outputPath), { recursive: true });
  await writeFile(outputPath, generated, "utf8");
  console.log(`Wrote ${outputPath}`);
}
