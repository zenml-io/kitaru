import type { CSSProperties, ReactNode } from "react";

// Shared primitives ---------------------------------------------------------

type Tone =
  | "default"
  | "accent"
  | "muted"
  | "warn"
  | "success"
  | "danger"
  | "info";

const toneBorder = (tone: Tone) => {
  switch (tone) {
    case "accent":
      return "color-mix(in oklab, var(--color-fd-primary) 55%, transparent)";
    case "warn":
      return "color-mix(in oklab, #f59e0b 55%, transparent)";
    case "success":
      return "color-mix(in oklab, #10b981 55%, transparent)";
    case "danger":
      return "color-mix(in oklab, #ef4444 55%, transparent)";
    case "info":
      return "color-mix(in oklab, #6366f1 55%, transparent)";
    case "muted":
      return "var(--color-fd-border)";
    default:
      return "var(--color-fd-border)";
  }
};
const toneFill = (tone: Tone) => {
  switch (tone) {
    case "accent":
      return "color-mix(in oklab, var(--color-fd-primary) 10%, transparent)";
    case "warn":
      return "color-mix(in oklab, #f59e0b 10%, transparent)";
    case "success":
      return "color-mix(in oklab, #10b981 10%, transparent)";
    case "danger":
      return "color-mix(in oklab, #ef4444 10%, transparent)";
    case "info":
      return "color-mix(in oklab, #6366f1 10%, transparent)";
    case "muted":
      return "color-mix(in oklab, var(--color-fd-muted-foreground) 5%, transparent)";
    default:
      return "var(--color-fd-card)";
  }
};
const toneText = (tone: Tone) =>
  tone === "muted"
    ? "var(--color-fd-muted-foreground)"
    : "var(--color-fd-foreground)";

const mono = "var(--font-mono, ui-monospace, SFMono-Regular, Menlo, monospace)";

function DiagramFrame({
  children,
  compact,
}: {
  children: ReactNode;
  compact?: boolean;
}) {
  return (
    <figure
      style={{
        margin: "28px 0",
        padding: compact ? "20px 16px" : "28px 20px",
        border: "1px solid var(--color-fd-border)",
        borderRadius: 14,
        background:
          "color-mix(in oklab, var(--color-fd-muted-foreground) 3%, transparent)",
        overflowX: "auto",
      }}
    >
      {children}
    </figure>
  );
}

function Node({
  title,
  subtitle,
  bullets,
  tone = "default",
  isMono,
  minWidth = 180,
  fullWidth,
  conceptual,
}: {
  title: ReactNode;
  subtitle?: ReactNode;
  bullets?: string[];
  tone?: Tone;
  isMono?: boolean;
  minWidth?: number;
  fullWidth?: boolean;
  conceptual?: boolean;
}) {
  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "flex-start",
        gap: 4,
        padding: "10px 14px",
        minWidth: fullWidth ? undefined : minWidth,
        width: fullWidth ? "100%" : undefined,
        border: `1px ${conceptual ? "dashed" : "solid"} ${toneBorder(tone)}`,
        background: conceptual ? "transparent" : toneFill(tone),
        color: toneText(tone),
        borderRadius: 10,
        fontSize: 13,
        lineHeight: 1.35,
        opacity: conceptual ? 0.75 : 1,
      }}
    >
      <span
        style={{
          fontFamily: isMono ? mono : "inherit",
          fontWeight: 600,
          color: "var(--color-fd-foreground)",
          fontSize: 13.5,
        }}
      >
        {title}
      </span>
      {subtitle ? (
        <span
          style={{
            fontSize: 11.5,
            color: "var(--color-fd-muted-foreground)",
            fontWeight: 400,
          }}
        >
          {subtitle}
        </span>
      ) : null}
      {bullets && bullets.length > 0 ? (
        <ul
          style={{
            margin: "6px 0 0",
            padding: 0,
            listStyle: "none",
            display: "flex",
            flexDirection: "column",
            gap: 2,
            fontSize: 11.5,
            color: "var(--color-fd-muted-foreground)",
          }}
        >
          {bullets.map((b) => (
            <li
              key={b}
              style={{
                display: "flex",
                gap: 6,
                alignItems: "flex-start",
              }}
            >
              <span
                aria-hidden
                style={{
                  color: "var(--color-fd-muted-foreground)",
                  fontFamily: mono,
                  fontSize: 10,
                  lineHeight: 1.6,
                }}
              >
                ·
              </span>
              <span style={{ fontFamily: mono, fontSize: 11 }}>{b}</span>
            </li>
          ))}
        </ul>
      ) : null}
    </div>
  );
}

function Subgraph({
  label,
  tone = "default",
  children,
  annotation,
}: {
  label: string;
  tone?: Tone;
  children: ReactNode;
  annotation?: string;
}) {
  return (
    <div
      style={{
        position: "relative",
        border: `1.5px dashed ${toneBorder(tone)}`,
        borderRadius: 14,
        padding: "22px 16px 16px",
        background: "transparent",
      }}
    >
      <span
        style={{
          position: "absolute",
          top: -10,
          left: 18,
          padding: "2px 10px",
          background: "var(--color-fd-background)",
          border: `1px solid ${toneBorder(tone)}`,
          borderRadius: 6,
          fontSize: 11,
          fontFamily: mono,
          fontWeight: 600,
          letterSpacing: "0.04em",
          textTransform: "uppercase",
          color: "var(--color-fd-foreground)",
        }}
      >
        {label}
      </span>
      {annotation ? (
        <span
          style={{
            position: "absolute",
            top: -10,
            right: 18,
            padding: "2px 10px",
            background: "var(--color-fd-background)",
            fontSize: 11,
            fontStyle: "italic",
            color: "var(--color-fd-muted-foreground)",
          }}
        >
          {annotation}
        </span>
      ) : null}
      {children}
    </div>
  );
}

function VArrow({ label }: { label?: string }) {
  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        color: "var(--color-fd-muted-foreground)",
        fontSize: 11,
        lineHeight: 1,
        padding: "4px 0",
      }}
    >
      <svg width="14" height="24" viewBox="0 0 14 24" aria-hidden>
        <path
          d="M7 0 V20 M2 16 L7 20 L12 16"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.4"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </svg>
      {label ? (
        <span
          style={{
            marginTop: 4,
            fontFamily: mono,
            color: "var(--color-fd-muted-foreground)",
          }}
        >
          {label}
        </span>
      ) : null}
    </div>
  );
}

function HArrow({ label }: { label?: string }) {
  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        color: "var(--color-fd-muted-foreground)",
        fontSize: 11,
        lineHeight: 1,
        padding: "0 4px",
        minWidth: 40,
      }}
    >
      <svg width="44" height="12" viewBox="0 0 44 12" aria-hidden>
        <path
          d="M0 6 H38 M34 2 L38 6 L34 10"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.4"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </svg>
      {label ? (
        <span
          style={{
            marginTop: 4,
            fontFamily: mono,
            color: "var(--color-fd-muted-foreground)",
          }}
        >
          {label}
        </span>
      ) : null}
    </div>
  );
}

function Caption({ children }: { children: ReactNode }) {
  return (
    <figcaption
      style={{
        marginTop: 14,
        textAlign: "center",
        fontSize: 12.5,
        color: "var(--color-fd-muted-foreground)",
        fontStyle: "italic",
      }}
    >
      {children}
    </figcaption>
  );
}

function Legend({
  items,
}: {
  items: { tone: Tone; label: string; conceptual?: boolean }[];
}) {
  return (
    <div
      style={{
        display: "flex",
        gap: 14,
        flexWrap: "wrap",
        justifyContent: "center",
        marginTop: 18,
        fontSize: 11.5,
        color: "var(--color-fd-muted-foreground)",
      }}
    >
      {items.map((i) => (
        <span
          key={i.label}
          style={{ display: "inline-flex", alignItems: "center", gap: 6 }}
        >
          <span
            aria-hidden
            style={{
              width: 12,
              height: 12,
              borderRadius: 3,
              background: i.conceptual ? "transparent" : toneFill(i.tone),
              border: `1px ${i.conceptual ? "dashed" : "solid"} ${toneBorder(i.tone)}`,
              opacity: i.conceptual ? 0.75 : 1,
            }}
          />
          {i.label}
        </span>
      ))}
    </div>
  );
}

const row: CSSProperties = {
  display: "flex",
  gap: 12,
  flexWrap: "wrap",
};
const col: CSSProperties = {
  display: "flex",
  flexDirection: "column",
  gap: 10,
};

// ============================================================
// Diagram 1: Full Execution Architecture
// ============================================================

export function ExecutionArchitectureDiagram() {
  return (
    <DiagramFrame>
      <div
        style={{
          display: "flex",
          flexDirection: "column",
          gap: 14,
          minWidth: 680,
        }}
      >
        {/* Consumers row */}
        <div style={{ ...row, justifyContent: "center" }}>
          <Node
            title="Consumer"
            subtitle="user · service · upstream agent"
            tone="muted"
            minWidth={240}
          />
        </div>
        <div style={{ display: "flex", justifyContent: "center" }}>
          <VArrow label="invoke" />
        </div>

        {/* Invocation API */}
        <div style={{ display: "flex", justifyContent: "center" }}>
          <Node
            title="Kitaru invocation API"
            subtitle="CLI · SDK · MCP · HTTP"
            tone="default"
            isMono
            minWidth={320}
          />
        </div>
        <div style={{ display: "flex", justifyContent: "center" }}>
          <VArrow />
        </div>

        {/* Control plane subgraph */}
        <Subgraph
          label="Control plane"
          tone="accent"
          annotation="long-lived, shared"
        >
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(170px, 1fr))",
              gap: 10,
            }}
          >
            <Node title="Auth & session" tone="accent" />
            <Node title="Flow / deployment registry" tone="accent" />
            <Node title="Execution metadata" tone="accent" />
            <Node title="Checkpoint state" tone="accent" />
            <Node title="Log metadata" tone="accent" />
            <Node title="Credential brokering" tone="accent" />
          </div>
        </Subgraph>
        <div style={{ display: "flex", justifyContent: "center" }}>
          <VArrow label="schedules run on your stack" />
        </div>

        {/* Orchestration plane subgraph */}
        <Subgraph
          label="Orchestration plane"
          tone="warn"
          annotation="per-run, durable"
        >
          <Node
            title="Runner"
            subtitle="the durable brain of one execution"
            bullets={[
              "loads the selected flow snapshot",
              "controls checkpoint order",
              "persists state after every checkpoint",
              "retry · replay · resume · wait",
              "can wait for days without burning compute",
            ]}
            tone="warn"
            fullWidth
            isMono
          />
        </Subgraph>
        <div style={{ display: "flex", justifyContent: "center" }}>
          <VArrow label="delegates each checkpoint" />
        </div>

        {/* Execution plane subgraph */}
        <Subgraph
          label="Execution plane"
          tone="success"
          annotation="where your code actually runs"
        >
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(170px, 1fr))",
              gap: 10,
            }}
          >
            <Node
              title="Inline"
              subtitle="same process as runner"
              tone="success"
              isMono
            />
            <Node
              title="Isolated job"
              subtitle="separate container / pod"
              tone="success"
              isMono
            />
            <Node
              title="Sandbox"
              subtitle="restricted egress / capabilities"
              tone="success"
              isMono
              conceptual
            />
            <Node
              title="External / MCP tool"
              subtitle="remote capability or API"
              tone="success"
              isMono
              conceptual
            />
          </div>
        </Subgraph>
        <div style={{ display: "flex", justifyContent: "center" }}>
          <VArrow label="persists outputs" />
        </div>

        {/* Persistence */}
        <Subgraph label="Persistence" tone="info" annotation="your cloud">
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "1fr 1fr",
              gap: 10,
            }}
          >
            <Node
              title="Artifact / state store"
              subtitle="your S3 / GCS / Azure Blob"
              bullets={[
                "checkpoint outputs",
                "files · errors · logs",
                "replay lineage",
              ]}
              tone="info"
            />
            <Node
              title="Metadata store"
              subtitle="runs · versions · statuses"
              tone="info"
            />
          </div>
        </Subgraph>

        {/* Ops rail */}
        <div style={{ display: "flex", justifyContent: "center" }}>
          <VArrow label="read by" />
        </div>
        <Subgraph label="Operations">
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(130px, 1fr))",
              gap: 10,
            }}
          >
            <Node title="Kitaru UI" subtitle="browse · replay" />
            <Node title="CLI" subtitle="kitaru executions" isMono />
            <Node title="Python SDK" subtitle="KitaruClient" isMono />
            <Node title="MCP tools" subtitle="for AI assistants" />
          </div>
        </Subgraph>
      </div>
      <Legend
        items={[
          { tone: "accent", label: "Control plane" },
          { tone: "warn", label: "Orchestration plane" },
          { tone: "success", label: "Execution plane" },
          { tone: "info", label: "Persistence" },
          {
            tone: "success",
            label: "Conceptual — via adapters or your platform",
            conceptual: true,
          },
        ]}
      />
      <Caption>
        Kitaru separates durable control flow (orchestration plane) from code
        execution (execution plane). Checkpoints are the contract between them.
      </Caption>
    </DiagramFrame>
  );
}

// ============================================================
// Diagram 2: Three Planes — horizontal banded view
// ============================================================

export function ThreePlanesDiagram() {
  return (
    <DiagramFrame>
      <div style={{ ...col, gap: 12 }}>
        <Subgraph
          label="Control plane"
          tone="accent"
          annotation="long-lived · shared · the Kitaru server"
        >
          <div style={{ ...row }}>
            <Node title="Auth" tone="accent" minWidth={110} />
            <Node title="Deployment registry" tone="accent" minWidth={170} />
            <Node title="Execution metadata" tone="accent" minWidth={170} />
            <Node title="Checkpoint state" tone="accent" minWidth={150} />
            <Node title="Log metadata" tone="accent" minWidth={140} />
          </div>
        </Subgraph>

        <Subgraph
          label="Orchestration plane"
          tone="warn"
          annotation="per-run · durable · the runner"
        >
          <div style={{ ...row }}>
            <Node title="Checkpoint order" tone="warn" minWidth={160} />
            <Node title="Replay" tone="warn" minWidth={100} />
            <Node title="Resume" tone="warn" minWidth={110} />
            <Node title="Wait / suspend" tone="warn" minWidth={150} />
            <Node title="Retry policy" tone="warn" minWidth={140} />
            <Node title="State durability" tone="warn" minWidth={160} />
          </div>
        </Subgraph>

        <Subgraph
          label="Execution plane"
          tone="success"
          annotation="where code runs · what @checkpoint targets"
        >
          <div style={{ ...row }}>
            <Node title="Inline" tone="success" minWidth={100} isMono />
            <Node title="Isolated job" tone="success" minWidth={130} isMono />
            <Node
              title="Sandbox"
              tone="success"
              minWidth={110}
              isMono
              conceptual
            />
            <Node
              title="External / MCP tool"
              tone="success"
              minWidth={180}
              isMono
              conceptual
            />
            <Node
              title="Custom backend"
              tone="success"
              minWidth={160}
              isMono
              conceptual
            />
          </div>
        </Subgraph>
      </div>
      <Legend
        items={[
          { tone: "success", label: "Shipped execution target" },
          {
            tone: "success",
            label: "Conceptual — same contract, via adapters or your platform",
            conceptual: true,
          },
        ]}
      />
      <Caption>
        The three planes run independently. The control plane survives if a
        runner dies. A runner survives if an execution target dies.
      </Caption>
    </DiagramFrame>
  );
}

// ============================================================
// Diagram 3: @flow body shape
// ============================================================

export function FlowShapeDiagram() {
  return (
    <DiagramFrame>
      <Subgraph label="@flow">
        <div
          style={{
            ...col,
            alignItems: "center",
            gap: 6,
            maxWidth: 560,
            margin: "0 auto",
          }}
        >
          <Node title="input" tone="muted" isMono minWidth={130} />
          <VArrow />
          <Node
            title="@checkpoint"
            subtitle="persisted output · replay boundary"
            tone="accent"
            isMono
          />
          <VArrow />
          <Node
            title="@checkpoint"
            subtitle="persisted output · replay boundary"
            tone="accent"
            isMono
          />
          <VArrow />
          <Node
            title="kitaru.wait()"
            subtitle="suspends · compute released · resumes on input"
            tone="warn"
            isMono
          />
          <VArrow label="input arrives" />
          <Node
            title="@checkpoint"
            subtitle="persisted output · replay boundary"
            tone="accent"
            isMono
          />
          <VArrow />
          <Node title="result" tone="success" isMono minWidth={130} />
        </div>
      </Subgraph>
      <Caption>
        The flow body orchestrates; checkpoints are durable replay boundaries.
      </Caption>
    </DiagramFrame>
  );
}

// ============================================================
// Diagram 4: Checkpoint replay (richer — artifacts + cache hits)
// ============================================================

function ReplayCell({
  label,
  state,
}: {
  label: string;
  state: "ok" | "fail" | "cached" | "rerun";
}) {
  const tone: Tone =
    state === "ok"
      ? "success"
      : state === "fail"
        ? "danger"
        : state === "cached"
          ? "muted"
          : "accent";
  const badge = {
    ok: "✓ ran",
    fail: "✗ failed",
    cached: "↺ cache hit",
    rerun: "● re-runs",
  }[state];
  return (
    <div style={{ ...col, alignItems: "center", gap: 6, minWidth: 110 }}>
      <Node title={label} subtitle={badge} tone={tone} isMono minWidth={110} />
      {state !== "fail" ? (
        <>
          <svg
            width="10"
            height="16"
            viewBox="0 0 10 16"
            aria-hidden
            style={{ color: "var(--color-fd-muted-foreground)" }}
          >
            <path
              d="M5 0 V12 M2 9 L5 12 L8 9"
              fill="none"
              stroke="currentColor"
              strokeWidth="1.3"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          </svg>
          <div
            style={{
              fontSize: 10.5,
              fontFamily: mono,
              padding: "3px 8px",
              borderRadius: 6,
              border: "1px dashed var(--color-fd-border)",
              color: "var(--color-fd-muted-foreground)",
              background: "var(--color-fd-background)",
            }}
          >
            artifact
          </div>
        </>
      ) : (
        <div
          style={{
            fontSize: 10.5,
            fontFamily: mono,
            padding: "3px 8px",
            borderRadius: 6,
            border: `1px dashed ${toneBorder("danger")}`,
            color: toneBorder("danger"),
            background: toneFill("danger"),
          }}
        >
          no artifact
        </div>
      )}
    </div>
  );
}

export function CheckpointReplayDiagram() {
  return (
    <DiagramFrame>
      <div style={{ ...col, gap: 18 }}>
        <div>
          <div
            style={{
              fontSize: 11,
              fontFamily: mono,
              color: "var(--color-fd-muted-foreground)",
              letterSpacing: "0.08em",
              textTransform: "uppercase",
              marginBottom: 10,
            }}
          >
            First run · fails at c4
          </div>
          <div style={{ ...row, alignItems: "flex-start" }}>
            <ReplayCell label="c1" state="ok" />
            <HArrow />
            <ReplayCell label="c2" state="ok" />
            <HArrow />
            <ReplayCell label="c3" state="ok" />
            <HArrow />
            <ReplayCell label="c4" state="fail" />
          </div>
        </div>

        <div
          style={{
            padding: "8px 14px",
            border: "1px dashed var(--color-fd-border)",
            borderRadius: 8,
            fontSize: 12,
            color: "var(--color-fd-muted-foreground)",
            textAlign: "center",
          }}
        >
          fix code or inputs ·{" "}
          <code>kitaru executions replay &lt;exec-id&gt;</code>
        </div>

        <div>
          <div
            style={{
              fontSize: 11,
              fontFamily: mono,
              color: "var(--color-fd-muted-foreground)",
              letterSpacing: "0.08em",
              textTransform: "uppercase",
              marginBottom: 10,
            }}
          >
            Replay · c1–c3 return cached outputs, c4 re-executes
          </div>
          <div style={{ ...row, alignItems: "flex-start" }}>
            <ReplayCell label="c1" state="cached" />
            <HArrow />
            <ReplayCell label="c2" state="cached" />
            <HArrow />
            <ReplayCell label="c3" state="cached" />
            <HArrow />
            <ReplayCell label="c4" state="rerun" />
          </div>
        </div>
      </div>
      <Legend
        items={[
          { tone: "success", label: "Ran this time" },
          { tone: "muted", label: "Cache hit (skipped)" },
          { tone: "accent", label: "Re-executed" },
          { tone: "danger", label: "Failed" },
        ]}
      />
      <Caption>
        Every successful checkpoint writes an artifact. Replay reads those
        artifacts back instead of rerunning — cost and time amortize over every
        debug cycle.
      </Caption>
    </DiagramFrame>
  );
}

// ============================================================
// Diagram 5: Failed checkpoint as durable context
// ============================================================

export function FailedCheckpointDiagram() {
  return (
    <DiagramFrame>
      <div style={{ ...col, gap: 14 }}>
        <div
          style={{ ...row, alignItems: "flex-start", justifyContent: "center" }}
        >
          <Node
            title="query_expansion"
            subtitle="success · artifact: expanded query set"
            tone="success"
            isMono
          />
          <HArrow />
          <Node
            title="retrieval"
            subtitle="soft failure · artifact: 'document missing / entitlement denied'"
            tone="danger"
            isMono
            minWidth={260}
          />
          <HArrow />
          <Node title="synthesis" subtitle="not yet run" tone="muted" isMono />
        </div>

        <div style={{ display: "flex", justifyContent: "center" }}>
          <VArrow label="failure becomes durable context" />
        </div>

        <Subgraph label="Recovery paths" tone="accent">
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))",
              gap: 10,
            }}
          >
            <Node
              title="Retry"
              subtitle="same input, same code"
              tone="accent"
            />
            <Node
              title="Replay with new input"
              subtitle="e.g. corrected document id"
              tone="accent"
            />
            <Node
              title="Replay with new code"
              subtitle="e.g. new retrieval strategy"
              tone="accent"
            />
            <Node
              title="Feed error into the agent loop"
              subtitle="let the agent self-correct"
              tone="accent"
            />
            <Node
              title="Wait for human correction"
              subtitle="kitaru.wait(), then resume"
              tone="accent"
            />
          </div>
        </Subgraph>
      </div>
      <Caption>
        In classical pipelines a failed step is a crash. In Kitaru it&rsquo;s a
        typed artifact that every recovery path can read.
      </Caption>
    </DiagramFrame>
  );
}

// ============================================================
// Diagram 6: Wait / resume timeline
// ============================================================

export function WaitResumeDiagram() {
  return (
    <DiagramFrame>
      <div style={{ ...col, gap: 14 }}>
        <div
          style={{
            fontSize: 11,
            fontFamily: mono,
            color: "var(--color-fd-muted-foreground)",
            letterSpacing: "0.08em",
            textTransform: "uppercase",
          }}
        >
          Running · compute active
        </div>
        <div style={{ ...row, alignItems: "center" }}>
          <Node title="c1" tone="success" isMono minWidth={70} />
          <HArrow />
          <Node title="c2" tone="success" isMono minWidth={70} />
          <HArrow />
          <Node
            title="kitaru.wait()"
            subtitle="question · schema · name"
            tone="warn"
            isMono
          />
        </div>

        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 14,
            padding: "12px 16px",
            border: "1px dashed var(--color-fd-border)",
            borderRadius: 10,
            background:
              "color-mix(in oklab, var(--color-fd-muted-foreground) 5%, transparent)",
            color: "var(--color-fd-muted-foreground)",
            fontSize: 12.5,
            flexWrap: "wrap",
          }}
        >
          <span
            style={{
              fontFamily: mono,
              fontSize: 11,
              letterSpacing: "0.06em",
              textTransform: "uppercase",
            }}
          >
            Waiting
          </span>
          <span>compute released · server holds durable state</span>
          <span
            style={{
              marginLeft: "auto",
              fontFamily: mono,
              fontSize: 11,
              color: "var(--color-fd-muted-foreground)",
            }}
          >
            seconds → days
          </span>
        </div>

        <div style={{ ...row, alignItems: "center" }}>
          <Node
            title="input arrives"
            subtitle="human · agent · webhook · CLI · MCP · UI"
            tone="accent"
            isMono
          />
          <HArrow label="resume" />
          <Node title="c3" tone="success" isMono minWidth={70} />
          <HArrow />
          <Node title="c4" tone="success" isMono minWidth={70} />
          <HArrow />
          <Node title="result" tone="success" isMono minWidth={90} />
        </div>
      </div>
      <Caption>
        A wait is not a timeout. The run stays durable forever — until input
        lands, from whichever surface provides it.
      </Caption>
    </DiagramFrame>
  );
}

// ============================================================
// Diagram 7: How-it-works components (for how-it-works.mdx)
// ============================================================

export function ComponentsDiagram() {
  return (
    <DiagramFrame>
      <div style={{ ...col, gap: 14 }}>
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "1fr auto 1fr",
            gap: 18,
            alignItems: "stretch",
          }}
        >
          <Node
            title="Client"
            subtitle="your laptop, CI, or service"
            bullets={["SDK", "CLI", "UI", "MCP"]}
          />
          <div style={{ alignSelf: "center" }}>
            <HArrow label="submit · input · replay" />
          </div>
          <Node
            title="Server"
            subtitle="central coordination"
            tone="accent"
            bullets={[
              "execution metadata",
              "checkpoint state",
              "log metadata",
              "auth + credentials",
            ]}
          />
        </div>
        <div style={{ display: "flex", justifyContent: "center" }}>
          <VArrow label="state · logs · results" />
        </div>
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "1fr 1fr",
            gap: 18,
          }}
        >
          <Node
            title="Runner"
            subtitle="your Python process or pod"
            tone="warn"
            bullets={[
              "runs checkpoints",
              "calls kitaru.llm()",
              "writes artifacts",
              "can wait + resume",
            ]}
          />
          <Node
            title="Cloud storage"
            subtitle="your bucket"
            tone="info"
            bullets={["S3", "GCS", "Azure Blob", "local fs (dev)"]}
          />
        </div>
      </div>
      <Caption>
        Locally: all three collapse into one Python process. In production:
        separate tiers across your stack.
      </Caption>
    </DiagramFrame>
  );
}

// ============================================================
// Diagram 8: Gateway stack — fits behind your platform
// ============================================================

export function GatewayStackDiagram() {
  return (
    <DiagramFrame>
      <div
        style={{
          ...col,
          alignItems: "stretch",
          gap: 10,
          maxWidth: 620,
          margin: "0 auto",
        }}
      >
        <Node
          title="Consumer"
          subtitle="internal user · product · upstream agent"
          tone="default"
        />
        <VArrow />
        <Node
          title="Your gateway / product API"
          subtitle="what your org owns"
          bullets={[
            "auth · entitlements",
            "rate limits · policy",
            "interceptors · guardrails",
            "product-specific endpoints",
          ]}
          tone="default"
        />
        <VArrow />
        <Node
          title="Kitaru invocation API"
          subtitle="the runtime primitive"
          bullets={[
            "version / tag resolution",
            "schema validation",
            "run record + FlowHandle",
            "credential brokering",
          ]}
          tone="accent"
          isMono
        />
        <VArrow />
        <Node
          title="Runner on your stack"
          subtitle="durable execution"
          bullets={[
            "checkpoint order",
            "replay · resume · wait",
            "artifacts + state",
            "retry + isolation",
          ]}
          tone="warn"
          isMono
        />
        <VArrow />
        <Node
          title="Artifacts + state"
          subtitle="your S3 / GCS / Azure Blob bucket"
          tone="info"
        />
      </div>
      <Caption>
        Kitaru drops in underneath your existing platform. Your auth, UI, and
        governance stay yours.
      </Caption>
    </DiagramFrame>
  );
}

// ============================================================
// Diagram 9: Model / Harness / Runtime / Platform — the four-layer split
// ============================================================

export function HarnessRuntimePlatformDiagram() {
  return (
    <DiagramFrame>
      <div style={{ ...col, gap: 12 }}>
        <Subgraph
          label="Model layer"
          tone="muted"
          annotation="the LLM itself · your pick"
        >
          <div style={{ ...row }}>
            <Node title="OpenAI" tone="muted" minWidth={110} isMono />
            <Node title="Anthropic" tone="muted" minWidth={120} isMono />
            <Node title="Google" tone="muted" minWidth={100} isMono />
            <Node title="Open-weights" tone="muted" minWidth={140} isMono />
            <Node
              title="Fine-tuned in-house"
              tone="muted"
              minWidth={180}
              isMono
            />
          </div>
        </Subgraph>

        <Subgraph
          label="Harness layer"
          tone="muted"
          annotation="the loop around the model"
        >
          <div style={{ ...row }}>
            <Node
              title="Pydantic AI / Harness"
              tone="muted"
              minWidth={180}
              isMono
            />
            <Node title="LangGraph" tone="muted" minWidth={120} isMono />
            <Node title="Claude Agent SDK" tone="muted" minWidth={170} isMono />
            <Node
              title="OpenAI Agents SDK"
              tone="muted"
              minWidth={170}
              isMono
            />
            <Node title="Raw Python" tone="muted" minWidth={130} isMono />
          </div>
        </Subgraph>

        <Subgraph
          label="Runtime layer"
          tone="accent"
          annotation="how the agent survives · Kitaru lives here"
        >
          <div style={{ ...row }}>
            <Node title="Checkpoints" tone="accent" minWidth={130} isMono />
            <Node title="Replay" tone="accent" minWidth={100} isMono />
            <Node title="Resume" tone="accent" minWidth={110} isMono />
            <Node title="Wait" tone="accent" minWidth={90} isMono />
            <Node
              title="Versions + tag routing"
              tone="accent"
              minWidth={200}
              isMono
            />
            <Node title="Invocation" tone="accent" minWidth={130} isMono />
            <Node
              title="Artifacts + state"
              tone="accent"
              minWidth={160}
              isMono
            />
            <Node
              title="Isolated execution"
              tone="accent"
              minWidth={180}
              isMono
            />
          </div>
        </Subgraph>

        <Subgraph
          label="Platform layer"
          tone="muted"
          annotation="how the org governs · stays yours"
        >
          <div style={{ ...row }}>
            <Node title="Auth + entitlements" tone="muted" minWidth={180} />
            <Node
              title="Interceptors + guardrails"
              tone="muted"
              minWidth={210}
            />
            <Node title="Observability" tone="muted" minWidth={140} />
            <Node title="Product UI" tone="muted" minWidth={120} />
            <Node title="Policy" tone="muted" minWidth={90} />
          </div>
        </Subgraph>
      </div>
      <Legend
        items={[
          { tone: "muted", label: "Composable (your choice)" },
          { tone: "accent", label: "Where Kitaru lives" },
        ]}
      />
      <Caption>
        Harnesses define behavior. Kitaru defines durable execution. Platforms
        define governance.
      </Caption>
    </DiagramFrame>
  );
}

// ============================================================
// Diagram 10: Buyer matrix — harness-first vs runtime-first
// ============================================================

export function BuyerMatrixDiagram() {
  return (
    <DiagramFrame>
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1fr 1fr",
          gap: 18,
        }}
      >
        <Node
          title="Individual or small team building one agent"
          subtitle="optimize for velocity"
          bullets={[
            "pick a harness (Pydantic AI / Harness, LangGraph, Claude SDK…)",
            "adopt its runtime if it has one",
            "Kitaru is probably overkill",
          ]}
          tone="muted"
        />
        <Node
          title="Platform team supporting many agent teams"
          subtitle="optimize for durability + portability"
          bullets={[
            "teams pick their own harness",
            "durable execution must be harness-independent",
            "infra must be self-hosted",
            "Kitaru is the right size primitive",
          ]}
          tone="accent"
        />
      </div>
      <Caption>
        Harness-first tools optimize for how a single agent is built. Kitaru
        optimizes for how many agents are run.
      </Caption>
    </DiagramFrame>
  );
}

// ============================================================
// Agent Harness Platform diagrams
// ============================================================

// ------------------------------------------------------------
// Diagram 11: Agent Harness Platform overview (landing page hero)
// ------------------------------------------------------------

export function AgentHarnessPlatformOverviewDiagram() {
  return (
    <DiagramFrame>
      <div style={{ ...col, gap: 14, minWidth: 640 }}>
        <Node
          title="Agent Profile"
          subtitle="one per agent · everything below is mostly configuration"
          bullets={[
            "model + system prompt",
            "allowed_tools — which capabilities are on",
            "allowed_services — which typed calls are reachable",
            "skill files — the editable procedure",
            "proxy rules — which hosts get which credential",
            "approval points — where the agent pauses for a human",
          ]}
          tone="default"
          fullWidth
        />
        <div style={{ display: "flex", justifyContent: "center" }}>
          <VArrow label="configures" />
        </div>

        <Node
          title="Agent Harness Platform library"
          subtitle="reusable rails — built once, shared by every team"
          bullets={[
            "builds a PydanticAI agent from the profile",
            "wraps it in a Kitaru durable flow",
          ]}
          tone="default"
          fullWidth
          isMono
        />
        <div style={{ display: "flex", justifyContent: "center" }}>
          <VArrow label="produces one durable agent with four capabilities" />
        </div>

        <Subgraph
          label="Kitaru durable flow"
          tone="accent"
          annotation="Stage 1 · completed work survives a crash"
        >
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(210px, 1fr))",
              gap: 10,
            }}
          >
            <Node
              title="exec"
              subtitle="Stage 2 · shell in a Docker sandbox"
              bullets={["egress credentials added by the proxy — Stage 4"]}
              tone="success"
              isMono
            />
            <Node
              title="skill"
              subtitle="Stage 3 · operator-editable markdown on the host"
              tone="muted"
              isMono
            />
            <Node
              title="exec_service"
              subtitle="Stage 5 · typed host-side handlers"
              tone="info"
              isMono
            />
            <Node
              title="ask_question"
              subtitle="Stage 6 · kitaru.wait() durable human pause"
              tone="warn"
              isMono
            />
          </div>
        </Subgraph>
      </div>
      <Legend
        items={[
          { tone: "accent", label: "Kitaru durability (Stage 1)" },
          { tone: "success", label: "Sandboxed shell execution" },
          { tone: "info", label: "Typed host-side service calls" },
          { tone: "warn", label: "Human-in-the-loop pause" },
          { tone: "muted", label: "Host-side files / config" },
        ]}
      />
      <Caption>
        A platform team defines the rails once. Product teams mostly change the
        Profile — which tools, services, skills, and approval points an agent
        gets. Each capability is one stage of this tour.
      </Caption>
    </DiagramFrame>
  );
}

// ------------------------------------------------------------
// Diagram 12: Durability across a crash (Stage 1)
// ------------------------------------------------------------

export function DurableAgentReplayDiagram() {
  return (
    <DiagramFrame>
      <div style={{ ...col, gap: 18 }}>
        <div>
          <div
            style={{
              fontSize: 11,
              fontFamily: mono,
              color: "var(--color-fd-muted-foreground)",
              letterSpacing: "0.08em",
              textTransform: "uppercase",
              marginBottom: 10,
            }}
          >
            First run · FORCE_FAILURE=1
          </div>
          <div style={{ ...row, alignItems: "center" }}>
            <Node
              title="default"
              subtitle="turn 1 · real LLM + tool work"
              tone="success"
              isMono
              minWidth={180}
            />
            <HArrow label="checkpoint saved" />
            <Node
              title="✗ crash"
              subtitle="process dies before turn 2"
              tone="danger"
              isMono
              minWidth={170}
            />
            <Node
              title="default_2"
              subtitle="turn 2 · never reached"
              tone="muted"
              isMono
              minWidth={150}
            />
          </div>
        </div>

        <div
          style={{
            padding: "8px 14px",
            border: "1px dashed var(--color-fd-border)",
            borderRadius: 8,
            fontSize: 12,
            color: "var(--color-fd-muted-foreground)",
            textAlign: "center",
          }}
        >
          re-run · <code>python stage_1_basic_agent.py</code>
        </div>

        <div>
          <div
            style={{
              fontSize: 11,
              fontFamily: mono,
              color: "var(--color-fd-muted-foreground)",
              letterSpacing: "0.08em",
              textTransform: "uppercase",
              marginBottom: 10,
            }}
          >
            Second run · no flag
          </div>
          <div style={{ ...row, alignItems: "center" }}>
            <Node
              title="default"
              subtitle="↺ cache hit · $0 · zero LLM calls"
              tone="muted"
              isMono
              minWidth={180}
            />
            <HArrow />
            <Node
              title="default_2"
              subtitle="● runs fresh · one LLM call"
              tone="accent"
              isMono
              minWidth={170}
            />
            <HArrow />
            <Node
              title="result"
              subtitle="flow finishes"
              tone="success"
              isMono
              minWidth={130}
            />
          </div>
        </div>
      </div>
      <Legend
        items={[
          { tone: "success", label: "Ran · output checkpointed" },
          { tone: "muted", label: "Cache hit (skipped, $0)" },
          { tone: "accent", label: "Re-executed" },
          { tone: "danger", label: "Crash" },
        ]}
      />
      <Caption>
        Turn 1&rsquo;s model work was saved before the crash. The re-run serves
        it from cache for free and only re-pays for turn 2 — the part that never
        finished.
      </Caption>
    </DiagramFrame>
  );
}

// ------------------------------------------------------------
// Diagram 13: Sandbox boundary (Stage 2)
// ------------------------------------------------------------

export function SandboxBoundaryDiagram() {
  return (
    <DiagramFrame>
      <Subgraph
        label="Host process"
        tone="muted"
        annotation="your laptop / runner — no agent shell here"
      >
        <div style={{ ...col, alignItems: "center", gap: 8 }}>
          <Node
            title="Kitaru flow + PydanticAI agent"
            subtitle="decides the commands · calls exec(command)"
            tone="accent"
            isMono
            minWidth={320}
          />
          <VArrow label="exec(command) → run(command) → ExecResult" />
          <div style={{ width: "100%" }}>
            <Subgraph
              label="DockerSandbox container"
              tone="success"
              annotation="own filesystem + network namespace"
            >
              <div style={{ ...col, alignItems: "center", gap: 8 }}>
                <Node
                  title="long-lived bash --noprofile --norc"
                  subtitle="one shell per run · state persists across exec calls"
                  bullets={[
                    "exec 1:  cd /tmp",
                    "exec 2:  ls  (still in /tmp)",
                    "rm -rf / destroys the container, not the host",
                  ]}
                  tone="success"
                  isMono
                  fullWidth
                />
                <Node
                  title="/workspace"
                  subtitle="named volume · survives container teardown"
                  tone="info"
                  isMono
                  minWidth={280}
                />
              </div>
            </Subgraph>
          </div>
        </div>
      </Subgraph>
      <Caption>
        Host filesystem damage is contained to the container and its mounted{" "}
        <code>/workspace</code> volume; outbound network calls still have real
        effects. Docker is the tutorial sandbox — a filesystem and namespace
        boundary for accidental misbehavior, not a full hostile-code boundary.
        Because <code>exec</code> only needs{" "}
        <code>run(command) → ExecResult</code>, a fork can swap in gVisor,
        Firecracker, or an external sandbox.
      </Caption>
    </DiagramFrame>
  );
}

// ------------------------------------------------------------
// Diagram 14: Credential proxy (Stage 4)
// ------------------------------------------------------------

export function CredentialProxyDiagram() {
  return (
    <DiagramFrame>
      <div style={{ ...col, gap: 14, minWidth: 640 }}>
        <Node
          title="Host · resolves wiki-token once at flow start"
          subtitle="kitaru.get_secret('wiki-token') → handed to the proxy's env, never to the worker"
          tone="accent"
          isMono
          fullWidth
        />
        <div style={{ display: "flex", justifyContent: "center" }}>
          <VArrow label="token to the proxy only" />
        </div>

        <div
          style={{ ...row, alignItems: "stretch", justifyContent: "center" }}
        >
          <Node
            title="Worker container"
            subtitle="no WIKI_TOKEN here"
            bullets={[
              "curl http://wiki.local/...",
              "cat $WIKI_TOKEN → nothing to read",
            ]}
            tone="success"
            isMono
            minWidth={210}
          />
          <HArrow label="request" />
          <Node
            title="Proxy container"
            subtitle="holds the token"
            bullets={[
              "matches host wiki.local",
              "injects Authorization header",
            ]}
            tone="warn"
            isMono
            minWidth={210}
          />
          <HArrow label="authenticated" />
          <Node
            title="Mock service"
            subtitle="requires Authorization"
            bullets={["returns 200"]}
            tone="info"
            isMono
            minWidth={190}
          />
        </div>
      </div>
      <Caption>
        The worker reaches the service and gets a response, but the real
        wiki-token only ever lives on the host and inside the proxy. A
        prompt-injected agent in the worker can read its per-run proxy bearer —
        but never receives the raw service token the proxy injects.
      </Caption>
    </DiagramFrame>
  );
}

// ------------------------------------------------------------
// Diagram 15: Two service-call paths (Stage 5)
// ------------------------------------------------------------

export function ServiceCallPathsDiagram() {
  return (
    <DiagramFrame>
      <div style={{ ...col, gap: 12, minWidth: 640 }}>
        <div style={{ display: "flex", justifyContent: "center" }}>
          <Node
            title="PydanticAI agent"
            subtitle="inside the Kitaru durable flow · picks a path per call"
            tone="accent"
            isMono
            minWidth={320}
          />
        </div>
        <div style={{ display: "flex", justifyContent: "center" }}>
          <VArrow />
        </div>

        <Subgraph
          label="exec — shell-shaped work"
          tone="success"
          annotation="inspect files · run commands"
        >
          <div style={{ ...row, alignItems: "center" }}>
            <Node title="agent" tone="success" isMono minWidth={90} />
            <HArrow />
            <Node
              title="Docker sandbox shell"
              subtitle="proxy injects Authorization on egress (Stage 4)"
              tone="success"
              isMono
              minWidth={240}
            />
            <HArrow />
            <Node
              title="command output"
              subtitle="raw bytes the agent parses"
              tone="muted"
              isMono
              minWidth={180}
            />
          </div>
        </Subgraph>

        <Subgraph
          label="exec_service — structured work"
          tone="info"
          annotation="publish · look up a record · file a ticket"
        >
          <div style={{ ...row, alignItems: "center" }}>
            <Node title="agent" tone="info" isMono minWidth={90} />
            <HArrow />
            <Node
              title="host-side typed handler"
              subtitle="host resolves kitaru.get_secret() directly · no proxy"
              tone="info"
              isMono
              minWidth={240}
            />
            <HArrow />
            <Node
              title="typed Pydantic result"
              subtitle="validated fields, not bytes"
              tone="info"
              isMono
              minWidth={180}
            />
          </div>
        </Subgraph>
      </div>
      <Legend
        items={[
          {
            tone: "success",
            label: "exec — sandboxed shell, proxy-injected auth",
          },
          {
            tone: "info",
            label: "exec_service — host-side typed call, direct creds",
          },
        ]}
      />
      <Caption>
        <code>exec</code> is right when the agent reasons about shell output.{" "}
        <code>exec_service</code> is right when the structured result matters
        more than the bytes — and its credentials never enter the sandbox.
      </Caption>
    </DiagramFrame>
  );
}

// ------------------------------------------------------------
// Diagram 16: Durable human pause (Stage 6)
// ------------------------------------------------------------

export function HitlPauseResumeDiagram() {
  return (
    <DiagramFrame>
      <div style={{ ...col, gap: 14 }}>
        <div
          style={{
            fontSize: 11,
            fontFamily: mono,
            color: "var(--color-fd-muted-foreground)",
            letterSpacing: "0.08em",
            textTransform: "uppercase",
          }}
        >
          Running · compute active
        </div>
        <div style={{ ...row, alignItems: "center" }}>
          <Node
            title="exec_service"
            subtitle="lookup_wiki"
            tone="info"
            isMono
          />
          <HArrow label="draft summary" />
          <Node
            title="ask_question(...)"
            subtitle="agent calls it like any tool"
            tone="warn"
            isMono
          />
        </div>

        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 14,
            padding: "12px 16px",
            border: "1px dashed var(--color-fd-border)",
            borderRadius: 10,
            background:
              "color-mix(in oklab, var(--color-fd-muted-foreground) 5%, transparent)",
            color: "var(--color-fd-muted-foreground)",
            fontSize: 12.5,
            flexWrap: "wrap",
          }}
        >
          <span
            style={{
              fontFamily: mono,
              fontSize: 11,
              letterSpacing: "0.06em",
              textTransform: "uppercase",
            }}
          >
            Paused
          </span>
          <span>
            kitaru.wait() · flow suspended durably · wait record{" "}
            <code>ask_question:1:abc12345</code>
          </span>
          <span style={{ marginLeft: "auto", fontFamily: mono, fontSize: 11 }}>
            seconds → days
          </span>
        </div>

        <div style={{ ...row, alignItems: "center" }}>
          <Node
            title="operator answers"
            subtitle="terminal · dashboard · CLI · REST"
            tone="accent"
            isMono
          />
          <HArrow label="resume" />
          <Node
            title="exec_service"
            subtitle="publish_summary"
            tone="info"
            isMono
          />
          <HArrow />
          <Node title="published" tone="success" isMono minWidth={110} />
        </div>
      </div>
      <Caption>
        To the agent, <code>ask_question(...)</code> is a tool call that returns
        a string. Underneath, Kitaru turns it into a durable wait — the run
        holds its place, for seconds or days, until a human answers from any
        surface.
      </Caption>
    </DiagramFrame>
  );
}

// ------------------------------------------------------------
// Diagram 17: Cached output vs shell side effects (Stage 2)
// ------------------------------------------------------------

export function SandboxReplaySideEffectsDiagram() {
  return (
    <DiagramFrame>
      <div style={{ ...col, gap: 18, minWidth: 640 }}>
        <div>
          <div
            style={{
              fontSize: 11,
              fontFamily: mono,
              color: "var(--color-fd-muted-foreground)",
              letterSpacing: "0.08em",
              textTransform: "uppercase",
              marginBottom: 10,
            }}
          >
            First run · FORCE_FAILURE=1
          </div>
          <div style={{ ...col, gap: 10 }}>
            <div style={{ ...row, alignItems: "center" }}>
              <Node
                title="default"
                subtitle="turn 1 · runs in the sandbox shell"
                tone="success"
                isMono
                minWidth={200}
              />
              <HArrow label="output checkpointed" />
              <Node
                title="✗ crash"
                subtitle="dies before default_2"
                tone="danger"
                isMono
                minWidth={170}
              />
              <Node
                title="default_2"
                subtitle="turn 2 · never reached"
                tone="muted"
                isMono
                minWidth={150}
              />
            </div>
            <Node
              title="shell side effects · live container"
              subtitle="happen now, but never enter the checkpoint"
              bullets={["cd /tmp", "exports, temp files, background jobs"]}
              tone="warn"
              isMono
              fullWidth
            />
          </div>
        </div>

        <div
          style={{
            padding: "8px 14px",
            border: "1px dashed var(--color-fd-border)",
            borderRadius: 8,
            fontSize: 12,
            color: "var(--color-fd-muted-foreground)",
            textAlign: "center",
          }}
        >
          re-run · <code>python stage_2_sandboxed_exec.py</code>
        </div>

        <div>
          <div
            style={{
              fontSize: 11,
              fontFamily: mono,
              color: "var(--color-fd-muted-foreground)",
              letterSpacing: "0.08em",
              textTransform: "uppercase",
              marginBottom: 10,
            }}
          >
            Retry · no flag
          </div>
          <div style={{ ...col, gap: 10 }}>
            <div style={{ ...row, alignItems: "center" }}>
              <Node
                title="default"
                subtitle="↺ cache hit · saved output returned · $0"
                tone="muted"
                isMono
                minWidth={220}
              />
              <HArrow />
              <Node
                title="default_2"
                subtitle="● runs fresh · shell starts at /workspace"
                tone="accent"
                isMono
                minWidth={220}
              />
              <HArrow />
              <Node
                title="result"
                subtitle="flow finishes"
                tone="success"
                isMono
                minWidth={120}
              />
            </div>
            <Node
              title="shell side effects · not replayed"
              subtitle="the cache returns saved output, not live shell state"
              bullets={[
                "cd /tmp · skipped",
                "writes outside /workspace · not restored",
              ]}
              tone="warn"
              isMono
              fullWidth
              conceptual
            />
          </div>
        </div>

        <Node
          title="/workspace · or a checkpointed value"
          subtitle="keep state a later turn depends on here, so a cached turn can't strand it"
          tone="info"
          isMono
          fullWidth
        />
      </div>
      <Legend
        items={[
          { tone: "success", label: "Ran · output checkpointed" },
          { tone: "muted", label: "Cache hit · output returned ($0)" },
          { tone: "accent", label: "Re-executed fresh" },
          { tone: "warn", label: "Shell side effect · happened live" },
          {
            tone: "warn",
            label: "Side effect · not replayed",
            conceptual: true,
          },
          { tone: "danger", label: "Crash" },
        ]}
      />
      <Caption>
        Kitaru caches each turn&rsquo;s output, so the retry hands turn
        1&rsquo;s result back for free. It does not re-run the shell. The{" "}
        <code>cd /tmp</code> and any non-persisted shell side effects from that
        cached turn ran once, in a container the retry never touches. Writes
        already placed in <code>/workspace</code> may still be there, but the
        write command itself is not replayed. Any state a later turn needs has
        to live in <code>/workspace</code> or a checkpointed value, because the
        shell a cached turn leaves behind is gone.
      </Caption>
    </DiagramFrame>
  );
}

// ------------------------------------------------------------
// Diagram 18: Skill procedure flow (Stage 3)
// ------------------------------------------------------------

export function SkillProcedureFlowDiagram() {
  return (
    <DiagramFrame>
      <div style={{ ...col, gap: 14, minWidth: 640 }}>
        <Node
          title="Profile"
          subtitle="one per agent · mostly configuration"
          bullets={[
            'allowed_tools = {"skill", "exec"}',
            "skill_source = skills/basic/default-agent/",
          ]}
          tone="default"
          isMono
          fullWidth
        />
        <div style={{ display: "flex", justifyContent: "center" }}>
          <VArrow label="configures one durable agent" />
        </div>

        <Subgraph
          label="Kitaru durable flow"
          tone="accent"
          annotation="each turn checkpointed · replayable"
        >
          <div style={{ ...col, alignItems: "center", gap: 10 }}>
            <Node
              title="PydanticAI agent"
              subtitle={
                'system prompt shrinks to: "find your skill and follow it"'
              }
              tone="accent"
              isMono
              minWidth={360}
            />
            <VArrow label={'1 · skill(action="list") → skill(action="read")'} />
            <div
              style={{ ...row, alignItems: "center", justifyContent: "center" }}
            >
              <Node
                title="skill tool"
                subtitle="host-side Python · not in the sandbox"
                tone="muted"
                isMono
                minWidth={210}
              />
              <HArrow label="reads" />
              <Node
                title="SKILL.md"
                subtitle="skills/basic/default-agent/ · operator edits in place"
                tone="muted"
                isMono
                minWidth={250}
              />
            </div>
            <VArrow label="procedure text returned to the agent" />
            <Node
              title="agent receives the procedure text"
              subtitle="then picks the commands the skill describes"
              tone="accent"
              isMono
              minWidth={360}
            />
            <VArrow label="2 · exec(command)" />
            <div style={{ width: "100%" }}>
              <Subgraph
                label="Docker sandbox"
                tone="success"
                annotation="Stage 2 boundary"
              >
                <Node
                  title="exec → shell command"
                  subtitle="runs the procedure's steps inside the container"
                  tone="success"
                  isMono
                  fullWidth
                />
              </Subgraph>
            </div>
          </div>
        </Subgraph>
      </div>
      <Legend
        items={[
          { tone: "accent", label: "Kitaru durable flow + agent" },
          { tone: "muted", label: "Host-side skill file (operator-editable)" },
          { tone: "success", label: "Sandboxed shell (exec)" },
        ]}
      />
      <Caption>
        The Profile names a <code>skill_source</code>. The host-side{" "}
        <code>skill</code> tool reads <code>SKILL.md</code> from it and hands
        the procedure text to the agent, which only then runs those steps
        through <code>exec</code> in the Docker sandbox. Edit the markdown and
        the agent behaves differently on the next run, with no Python change.
      </Caption>
    </DiagramFrame>
  );
}
