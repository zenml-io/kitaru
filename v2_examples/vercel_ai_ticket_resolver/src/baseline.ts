import { randomUUID } from "node:crypto";
import { access, mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

import {
  createTicketRun,
  type ModelProvider,
  type PolicyMode,
  validateModelProviderEnvironment,
} from "./agent.js";
import { ticketCases } from "./fixtures.js";
import type { TicketInput } from "./models.js";
import { renderTicketPrompt } from "./models.js";

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const MANIFEST_NAME = "baseline-sessions.json";

export interface ManifestSession {
  session_id: string;
  status: "completed";
}

export interface BaselineManifest {
  evidence_set_id: string;
  mode: PolicyMode;
  provider: ModelProvider;
  schema_version: 1;
  sessions: Record<string, ManifestSession>;
  status: "recording" | "completed";
}

export interface RecordTicketContext {
  sessionIdFile: string;
  ticket: TicketInput;
}

export interface RecordBaselineOptions {
  adoptions?: Readonly<Record<string, string>>;
  environment?: Readonly<Record<string, string | undefined>>;
  fresh?: boolean;
  provider?: ModelProvider;
  recordTicket?: (context: RecordTicketContext) => Promise<void>;
  stateDir?: string;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseManifest(value: unknown): BaselineManifest {
  if (!isRecord(value)) {
    throw new Error("baseline manifest must be an object");
  }
  if (
    value.schema_version !== 1 ||
    typeof value.evidence_set_id !== "string" ||
    !UUID_PATTERN.test(value.evidence_set_id) ||
    !["baseline", "strict"].includes(String(value.mode)) ||
    !["deterministic", "openai"].includes(String(value.provider)) ||
    !["recording", "completed"].includes(String(value.status)) ||
    !isRecord(value.sessions)
  ) {
    throw new Error("baseline manifest has invalid metadata");
  }
  const sessions: Record<string, ManifestSession> = {};
  const sessionIds = new Set<string>();
  for (const [ticketId, rawSession] of Object.entries(value.sessions)) {
    if (
      !ticketCases.some(({ ticket }) => ticket.ticket_id === ticketId) ||
      !isRecord(rawSession) ||
      rawSession.status !== "completed" ||
      typeof rawSession.session_id !== "string" ||
      !UUID_PATTERN.test(rawSession.session_id)
    ) {
      throw new Error(
        `baseline manifest has an invalid session for ${ticketId}`,
      );
    }
    if (sessionIds.has(rawSession.session_id)) {
      throw new Error(
        `baseline manifest reuses session ${rawSession.session_id}`,
      );
    }
    sessionIds.add(rawSession.session_id);
    sessions[ticketId] = {
      session_id: rawSession.session_id,
      status: "completed",
    };
  }
  if (value.status === "completed" && Object.keys(sessions).length !== 10) {
    throw new Error("baseline manifest is completed without ten sessions");
  }
  return {
    evidence_set_id: value.evidence_set_id,
    mode: value.mode as PolicyMode,
    provider: value.provider as ModelProvider,
    schema_version: 1,
    sessions,
    status: value.status as BaselineManifest["status"],
  };
}

async function exists(path: string): Promise<boolean> {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

async function writeJsonAtomically(
  path: string,
  value: unknown,
): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  const temporary = `${path}.${randomUUID()}.tmp`;
  await writeFile(temporary, `${JSON.stringify(value, null, 2)}\n`, "utf8");
  await rename(temporary, path);
}

export async function loadBaselineManifest(
  stateDir = resolve(".state"),
): Promise<BaselineManifest | undefined> {
  const path = join(stateDir, MANIFEST_NAME);
  if (!(await exists(path))) {
    return undefined;
  }
  let raw: unknown;
  try {
    raw = JSON.parse(await readFile(path, "utf8"));
  } catch (error) {
    throw new Error("baseline manifest is not valid JSON", { cause: error });
  }
  return parseManifest(raw);
}

async function createManifest(
  stateDir: string,
  provider: ModelProvider,
): Promise<BaselineManifest> {
  const manifest: BaselineManifest = {
    evidence_set_id: randomUUID(),
    mode: "baseline",
    provider,
    schema_version: 1,
    sessions: {},
    status: "recording",
  };
  await writeJsonAtomically(join(stateDir, MANIFEST_NAME), manifest);
  return manifest;
}

async function archiveManifest(
  stateDir: string,
  manifest: BaselineManifest,
): Promise<void> {
  await writeJsonAtomically(
    join(stateDir, "evidence-sets", `${manifest.evidence_set_id}.json`),
    manifest,
  );
}

function validateAdoptions(adoptions: Readonly<Record<string, string>>): void {
  for (const [ticketId, sessionId] of Object.entries(adoptions)) {
    if (
      !ticketCases.some(({ ticket }) => ticket.ticket_id === ticketId) ||
      !UUID_PATTERN.test(sessionId)
    ) {
      throw new Error(`Invalid adoption ${ticketId}=${sessionId}`);
    }
  }
}

export async function recordBaseline(
  options: RecordBaselineOptions = {},
): Promise<BaselineManifest> {
  const stateDir = resolve(options.stateDir ?? ".state");
  const provider = options.provider ?? "deterministic";
  const baseEnvironment = options.environment ?? process.env;
  validateModelProviderEnvironment(provider, baseEnvironment);
  const adoptions = options.adoptions ?? {};
  validateAdoptions(adoptions);
  await mkdir(stateDir, { recursive: true });

  let manifest = await loadBaselineManifest(stateDir);
  if (options.fresh && manifest) {
    await archiveManifest(stateDir, manifest);
    manifest = undefined;
  }
  manifest ??= await createManifest(stateDir, provider);
  if (manifest.provider !== provider) {
    throw new Error(
      `Existing evidence set uses ${manifest.provider}; pass --fresh to record ${provider}`,
    );
  }
  if (manifest.status === "completed") {
    return manifest;
  }

  const recordTicket =
    options.recordTicket ??
    (async ({ sessionIdFile, ticket }: RecordTicketContext) => {
      const runEnvironment = {
        ...baseEnvironment,
        KITARU_SESSION_ID_FILE: sessionIdFile,
      };
      await createTicketRun({
        environment: runEnvironment,
        mode: "baseline",
        prompt: renderTicketPrompt(ticket),
        provider,
      }).generate();
    });

  for (const { ticket } of ticketCases) {
    if (manifest.sessions[ticket.ticket_id]) {
      continue;
    }
    const sessionIdFile = join(
      stateDir,
      "attempts",
      manifest.evidence_set_id,
      `${ticket.ticket_id}.session-id`,
    );
    await mkdir(dirname(sessionIdFile), { recursive: true });
    if (await exists(sessionIdFile)) {
      const attemptedSessionId = (await readFile(sessionIdFile, "utf8")).trim();
      const adoptedSessionId = adoptions[ticket.ticket_id];
      if (adoptedSessionId !== attemptedSessionId) {
        throw new Error(
          `Ambiguous attempt for ${ticket.ticket_id}: session ${attemptedSessionId || "<empty>"} is not committed. Inspect its remote status, then pass --adopt ${ticket.ticket_id}=${attemptedSessionId} only if it completed.`,
        );
      }
      if (
        Object.values(manifest.sessions).some(
          ({ session_id }) => session_id === adoptedSessionId,
        )
      ) {
        throw new Error(
          `Session ${adoptedSessionId} is already in the manifest`,
        );
      }
      manifest.sessions[ticket.ticket_id] = {
        session_id: adoptedSessionId,
        status: "completed",
      };
      await writeJsonAtomically(join(stateDir, MANIFEST_NAME), manifest);
      continue;
    }
    if (adoptions[ticket.ticket_id] !== undefined) {
      throw new Error(
        `Cannot adopt ${ticket.ticket_id}: no ambiguous session-ID file exists`,
      );
    }

    await recordTicket({ sessionIdFile, ticket });
    if (!(await exists(sessionIdFile))) {
      throw new Error(
        `Recording ${ticket.ticket_id} completed without a Kitaru session ID file`,
      );
    }
    const completedSessionId = (await readFile(sessionIdFile, "utf8")).trim();
    if (!UUID_PATTERN.test(completedSessionId)) {
      throw new Error(
        `Recording ${ticket.ticket_id} wrote an invalid Kitaru session ID`,
      );
    }
    if (
      Object.values(manifest.sessions).some(
        ({ session_id }) => session_id === completedSessionId,
      )
    ) {
      throw new Error(
        `Session ${completedSessionId} is already in the manifest`,
      );
    }
    manifest.sessions[ticket.ticket_id] = {
      session_id: completedSessionId,
      status: "completed",
    };
    await writeJsonAtomically(join(stateDir, MANIFEST_NAME), manifest);
  }

  manifest.status = "completed";
  await writeJsonAtomically(join(stateDir, MANIFEST_NAME), manifest);
  return manifest;
}

interface BaselineArguments {
  adoptions: Record<string, string>;
  fresh: boolean;
  provider: ModelProvider;
}

export function parseBaselineArguments(args: string[]): BaselineArguments {
  const parsed: BaselineArguments = {
    adoptions: {},
    fresh: false,
    provider: "deterministic",
  };
  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index];
    if (argument === "--fresh") {
      parsed.fresh = true;
      continue;
    }
    if (argument === "--provider") {
      const provider = args[index + 1];
      if (provider !== "deterministic" && provider !== "openai") {
        throw new Error("--provider must be deterministic or openai");
      }
      parsed.provider = provider;
      index += 1;
      continue;
    }
    if (argument === "--adopt") {
      const adoption = args[index + 1];
      const separator = adoption?.indexOf("=") ?? -1;
      if (!adoption || separator < 1) {
        throw new Error("--adopt must be followed by ticket-id=session-id");
      }
      parsed.adoptions[adoption.slice(0, separator)] = adoption.slice(
        separator + 1,
      );
      index += 1;
      continue;
    }
    throw new Error(`Unknown baseline argument: ${argument}`);
  }
  return parsed;
}

async function main(): Promise<void> {
  const options = parseBaselineArguments(process.argv.slice(2));
  const manifest = await recordBaseline(options);
  console.log(JSON.stringify(manifest, null, 2));
}

const entrypoint = process.argv[1]
  ? pathToFileURL(resolve(process.argv[1])).href
  : undefined;
if (entrypoint === import.meta.url) {
  await main();
}
