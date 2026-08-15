import { assertSupportedNodeVersion } from "./preflight.js";

assertSupportedNodeVersion();

const { main } = await import("./workflow-runner.js");
await main();
