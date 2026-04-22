import type { ReactNode } from 'react';

// Shared primitives ---------------------------------------------------------

type Tone =
  | 'default'
  | 'accent'
  | 'muted'
  | 'warn'
  | 'success'
  | 'danger'
  | 'info';

const toneBorder = (tone: Tone) => {
  switch (tone) {
    case 'accent':
      return 'color-mix(in oklab, var(--color-fd-primary) 55%, transparent)';
    case 'warn':
      return 'color-mix(in oklab, #f59e0b 55%, transparent)';
    case 'success':
      return 'color-mix(in oklab, #10b981 55%, transparent)';
    case 'danger':
      return 'color-mix(in oklab, #ef4444 55%, transparent)';
    case 'info':
      return 'color-mix(in oklab, #6366f1 55%, transparent)';
    case 'muted':
      return 'var(--color-fd-border)';
    default:
      return 'var(--color-fd-border)';
  }
};
const toneFill = (tone: Tone) => {
  switch (tone) {
    case 'accent':
      return 'color-mix(in oklab, var(--color-fd-primary) 10%, transparent)';
    case 'warn':
      return 'color-mix(in oklab, #f59e0b 10%, transparent)';
    case 'success':
      return 'color-mix(in oklab, #10b981 10%, transparent)';
    case 'danger':
      return 'color-mix(in oklab, #ef4444 10%, transparent)';
    case 'info':
      return 'color-mix(in oklab, #6366f1 10%, transparent)';
    case 'muted':
      return 'color-mix(in oklab, var(--color-fd-muted-foreground) 5%, transparent)';
    default:
      return 'var(--color-fd-card)';
  }
};
const toneText = (tone: Tone) =>
  tone === 'muted'
    ? 'var(--color-fd-muted-foreground)'
    : 'var(--color-fd-foreground)';

const mono =
  'var(--font-mono, ui-monospace, SFMono-Regular, Menlo, monospace)';

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
        margin: '28px 0',
        padding: compact ? '20px 16px' : '28px 20px',
        border: '1px solid var(--color-fd-border)',
        borderRadius: 14,
        background:
          'color-mix(in oklab, var(--color-fd-muted-foreground) 3%, transparent)',
        overflowX: 'auto',
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
  tone = 'default',
  isMono,
  minWidth = 180,
  fullWidth,
}: {
  title: ReactNode;
  subtitle?: ReactNode;
  bullets?: string[];
  tone?: Tone;
  isMono?: boolean;
  minWidth?: number;
  fullWidth?: boolean;
}) {
  return (
    <div
      style={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'flex-start',
        gap: 4,
        padding: '10px 14px',
        minWidth: fullWidth ? undefined : minWidth,
        width: fullWidth ? '100%' : undefined,
        border: `1px solid ${toneBorder(tone)}`,
        background: toneFill(tone),
        color: toneText(tone),
        borderRadius: 10,
        fontSize: 13,
        lineHeight: 1.35,
      }}
    >
      <span
        style={{
          fontFamily: isMono ? mono : 'inherit',
          fontWeight: 600,
          color: 'var(--color-fd-foreground)',
          fontSize: 13.5,
        }}
      >
        {title}
      </span>
      {subtitle ? (
        <span
          style={{
            fontSize: 11.5,
            color: 'var(--color-fd-muted-foreground)',
            fontWeight: 400,
          }}
        >
          {subtitle}
        </span>
      ) : null}
      {bullets && bullets.length > 0 ? (
        <ul
          style={{
            margin: '6px 0 0',
            padding: 0,
            listStyle: 'none',
            display: 'flex',
            flexDirection: 'column',
            gap: 2,
            fontSize: 11.5,
            color: 'var(--color-fd-muted-foreground)',
          }}
        >
          {bullets.map((b, i) => (
            <li
              key={i}
              style={{
                display: 'flex',
                gap: 6,
                alignItems: 'flex-start',
              }}
            >
              <span
                aria-hidden
                style={{
                  color: 'var(--color-fd-muted-foreground)',
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
  tone = 'default',
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
        position: 'relative',
        border: `1.5px dashed ${toneBorder(tone)}`,
        borderRadius: 14,
        padding: '22px 16px 16px',
        background: 'transparent',
      }}
    >
      <span
        style={{
          position: 'absolute',
          top: -10,
          left: 18,
          padding: '2px 10px',
          background: 'var(--color-fd-background)',
          border: `1px solid ${toneBorder(tone)}`,
          borderRadius: 6,
          fontSize: 11,
          fontFamily: mono,
          fontWeight: 600,
          letterSpacing: '0.04em',
          textTransform: 'uppercase',
          color: 'var(--color-fd-foreground)',
        }}
      >
        {label}
      </span>
      {annotation ? (
        <span
          style={{
            position: 'absolute',
            top: -10,
            right: 18,
            padding: '2px 10px',
            background: 'var(--color-fd-background)',
            fontSize: 11,
            fontStyle: 'italic',
            color: 'var(--color-fd-muted-foreground)',
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
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        color: 'var(--color-fd-muted-foreground)',
        fontSize: 11,
        lineHeight: 1,
        padding: '4px 0',
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
            color: 'var(--color-fd-muted-foreground)',
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
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        color: 'var(--color-fd-muted-foreground)',
        fontSize: 11,
        lineHeight: 1,
        padding: '0 4px',
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
            color: 'var(--color-fd-muted-foreground)',
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
        textAlign: 'center',
        fontSize: 12.5,
        color: 'var(--color-fd-muted-foreground)',
        fontStyle: 'italic',
      }}
    >
      {children}
    </figcaption>
  );
}

function Legend({
  items,
}: {
  items: { tone: Tone; label: string }[];
}) {
  return (
    <div
      style={{
        display: 'flex',
        gap: 14,
        flexWrap: 'wrap',
        justifyContent: 'center',
        marginTop: 18,
        fontSize: 11.5,
        color: 'var(--color-fd-muted-foreground)',
      }}
    >
      {items.map((i, idx) => (
        <span
          key={idx}
          style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}
        >
          <span
            aria-hidden
            style={{
              width: 12,
              height: 12,
              borderRadius: 3,
              background: toneFill(i.tone),
              border: `1px solid ${toneBorder(i.tone)}`,
            }}
          />
          {i.label}
        </span>
      ))}
    </div>
  );
}

const row: React.CSSProperties = {
  display: 'flex',
  gap: 12,
  flexWrap: 'wrap',
};
const col: React.CSSProperties = {
  display: 'flex',
  flexDirection: 'column',
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
          display: 'flex',
          flexDirection: 'column',
          gap: 14,
          minWidth: 680,
        }}
      >
        {/* Consumers row */}
        <div style={{ ...row, justifyContent: 'center' }}>
          <Node
            title="Consumer"
            subtitle="user · service · upstream agent"
            tone="muted"
            minWidth={240}
          />
        </div>
        <div style={{ display: 'flex', justifyContent: 'center' }}>
          <VArrow label="invoke" />
        </div>

        {/* Invocation API */}
        <div style={{ display: 'flex', justifyContent: 'center' }}>
          <Node
            title="Kitaru invocation API"
            subtitle="CLI · SDK · MCP · HTTP"
            tone="default"
            isMono
            minWidth={320}
          />
        </div>
        <div style={{ display: 'flex', justifyContent: 'center' }}>
          <VArrow />
        </div>

        {/* Control plane subgraph */}
        <Subgraph label="Control plane" tone="accent" annotation="long-lived, shared">
          <div
            style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fit, minmax(170px, 1fr))',
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
        <div style={{ display: 'flex', justifyContent: 'center' }}>
          <VArrow label="schedules run on your stack" />
        </div>

        {/* Orchestration plane subgraph */}
        <Subgraph label="Orchestration plane" tone="warn" annotation="per-run, durable">
          <Node
            title="Runner"
            subtitle="the durable brain of one execution"
            bullets={[
              'loads the selected flow snapshot',
              'controls checkpoint order',
              'persists state after every checkpoint',
              'retry · replay · resume · wait',
              'can wait for days without burning compute',
            ]}
            tone="warn"
            fullWidth
            isMono
          />
        </Subgraph>
        <div style={{ display: 'flex', justifyContent: 'center' }}>
          <VArrow label="delegates each checkpoint" />
        </div>

        {/* Execution plane subgraph */}
        <Subgraph label="Execution plane" tone="success" annotation="where your code actually runs">
          <div
            style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fit, minmax(170px, 1fr))',
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
            />
            <Node
              title="External / MCP tool"
              subtitle="remote capability or API"
              tone="success"
              isMono
            />
          </div>
        </Subgraph>
        <div style={{ display: 'flex', justifyContent: 'center' }}>
          <VArrow label="persists outputs" />
        </div>

        {/* Persistence */}
        <Subgraph label="Persistence" tone="info" annotation="your cloud">
          <div
            style={{
              display: 'grid',
              gridTemplateColumns: '1fr 1fr',
              gap: 10,
            }}
          >
            <Node
              title="Artifact / state store"
              subtitle="your S3 / GCS / Azure Blob"
              bullets={[
                'checkpoint outputs',
                'files · errors · logs',
                'replay lineage',
              ]}
              tone="info"
            />
            <Node
              title="Metadata DB"
              subtitle="runs · versions · statuses"
              tone="info"
            />
          </div>
        </Subgraph>

        {/* Ops rail */}
        <div style={{ display: 'flex', justifyContent: 'center' }}>
          <VArrow label="read by" />
        </div>
        <Subgraph label="Operations">
          <div
            style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fit, minmax(130px, 1fr))',
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
          { tone: 'accent', label: 'Control plane' },
          { tone: 'warn', label: 'Orchestration plane' },
          { tone: 'success', label: 'Execution plane' },
          { tone: 'info', label: 'Persistence' },
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
            <Node title="Sandbox" tone="success" minWidth={110} isMono />
            <Node title="External / MCP tool" tone="success" minWidth={180} isMono />
            <Node title="Custom backend" tone="success" minWidth={160} isMono />
          </div>
        </Subgraph>
      </div>
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
        <div style={{ ...col, alignItems: 'center', gap: 6, maxWidth: 560, margin: '0 auto' }}>
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
  state: 'ok' | 'fail' | 'cached' | 'rerun';
}) {
  const tone: Tone =
    state === 'ok'
      ? 'success'
      : state === 'fail'
        ? 'danger'
        : state === 'cached'
          ? 'muted'
          : 'accent';
  const badge = {
    ok: '✓ ran',
    fail: '✗ failed',
    cached: '↺ cache hit',
    rerun: '● re-runs',
  }[state];
  return (
    <div style={{ ...col, alignItems: 'center', gap: 6, minWidth: 110 }}>
      <Node title={label} subtitle={badge} tone={tone} isMono minWidth={110} />
      {state !== 'fail' ? (
        <>
          <svg width="10" height="16" viewBox="0 0 10 16" aria-hidden style={{ color: 'var(--color-fd-muted-foreground)' }}>
            <path d="M5 0 V12 M2 9 L5 12 L8 9" fill="none" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
          <div
            style={{
              fontSize: 10.5,
              fontFamily: mono,
              padding: '3px 8px',
              borderRadius: 6,
              border: '1px dashed var(--color-fd-border)',
              color: 'var(--color-fd-muted-foreground)',
              background: 'var(--color-fd-background)',
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
            padding: '3px 8px',
            borderRadius: 6,
            border: `1px dashed ${toneBorder('danger')}`,
            color: toneBorder('danger'),
            background: toneFill('danger'),
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
              color: 'var(--color-fd-muted-foreground)',
              letterSpacing: '0.08em',
              textTransform: 'uppercase',
              marginBottom: 10,
            }}
          >
            First run · fails at c4
          </div>
          <div style={{ ...row, alignItems: 'flex-start' }}>
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
            padding: '8px 14px',
            border: '1px dashed var(--color-fd-border)',
            borderRadius: 8,
            fontSize: 12,
            color: 'var(--color-fd-muted-foreground)',
            textAlign: 'center',
          }}
        >
          fix code or inputs · <code>kitaru executions replay &lt;exec-id&gt;</code>
        </div>

        <div>
          <div
            style={{
              fontSize: 11,
              fontFamily: mono,
              color: 'var(--color-fd-muted-foreground)',
              letterSpacing: '0.08em',
              textTransform: 'uppercase',
              marginBottom: 10,
            }}
          >
            Replay · c1–c3 return cached outputs, c4 re-executes
          </div>
          <div style={{ ...row, alignItems: 'flex-start' }}>
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
          { tone: 'success', label: 'Ran this time' },
          { tone: 'muted', label: 'Cache hit (skipped)' },
          { tone: 'accent', label: 'Re-executed' },
          { tone: 'danger', label: 'Failed' },
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
        <div style={{ ...row, alignItems: 'flex-start', justifyContent: 'center' }}>
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
          <Node
            title="synthesis"
            subtitle="not yet run"
            tone="muted"
            isMono
          />
        </div>

        <div style={{ display: 'flex', justifyContent: 'center' }}>
          <VArrow label="failure becomes durable context" />
        </div>

        <Subgraph label="Recovery paths" tone="accent">
          <div
            style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
              gap: 10,
            }}
          >
            <Node title="Retry" subtitle="same input, same code" tone="accent" />
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
            color: 'var(--color-fd-muted-foreground)',
            letterSpacing: '0.08em',
            textTransform: 'uppercase',
          }}
        >
          Running · compute active
        </div>
        <div style={{ ...row, alignItems: 'center' }}>
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
            display: 'flex',
            alignItems: 'center',
            gap: 14,
            padding: '12px 16px',
            border: '1px dashed var(--color-fd-border)',
            borderRadius: 10,
            background:
              'color-mix(in oklab, var(--color-fd-muted-foreground) 5%, transparent)',
            color: 'var(--color-fd-muted-foreground)',
            fontSize: 12.5,
            flexWrap: 'wrap',
          }}
        >
          <span
            style={{
              fontFamily: mono,
              fontSize: 11,
              letterSpacing: '0.06em',
              textTransform: 'uppercase',
            }}
          >
            Waiting
          </span>
          <span>compute released · server holds durable state</span>
          <span
            style={{
              marginLeft: 'auto',
              fontFamily: mono,
              fontSize: 11,
              color: 'var(--color-fd-muted-foreground)',
            }}
          >
            seconds → days
          </span>
        </div>

        <div style={{ ...row, alignItems: 'center' }}>
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
            display: 'grid',
            gridTemplateColumns: '1fr auto 1fr',
            gap: 18,
            alignItems: 'stretch',
          }}
        >
          <Node
            title="Client"
            subtitle="your laptop, CI, or service"
            bullets={['SDK', 'CLI', 'UI', 'MCP']}
          />
          <div style={{ alignSelf: 'center' }}>
            <HArrow label="submit · input · replay" />
          </div>
          <Node
            title="Server"
            subtitle="central coordination"
            tone="accent"
            bullets={[
              'execution metadata',
              'checkpoint state',
              'log metadata',
              'auth + credentials',
            ]}
          />
        </div>
        <div style={{ display: 'flex', justifyContent: 'center' }}>
          <VArrow label="state · logs · results" />
        </div>
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: '1fr 1fr',
            gap: 18,
          }}
        >
          <Node
            title="Runner"
            subtitle="your Python process or pod"
            tone="warn"
            bullets={[
              'runs checkpoints',
              'calls kitaru.llm()',
              'writes artifacts',
              'can wait + resume',
            ]}
          />
          <Node
            title="Cloud storage"
            subtitle="your bucket"
            tone="info"
            bullets={['S3', 'GCS', 'Azure Blob', 'local fs (dev)']}
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
          alignItems: 'stretch',
          gap: 10,
          maxWidth: 620,
          margin: '0 auto',
        }}
      >
        <Node title="Consumer" subtitle="internal user · product · upstream agent" tone="default" />
        <VArrow />
        <Node
          title="Your gateway / product API"
          subtitle="what your org owns"
          bullets={[
            'auth · entitlements',
            'rate limits · policy',
            'interceptors · guardrails',
            'product-specific endpoints',
          ]}
          tone="default"
        />
        <VArrow />
        <Node
          title="Kitaru invocation API"
          subtitle="the runtime primitive"
          bullets={[
            'version / tag resolution',
            'schema validation',
            'run record + FlowHandle',
            'credential brokering',
          ]}
          tone="accent"
          isMono
        />
        <VArrow />
        <Node
          title="Runner on your stack"
          subtitle="durable execution"
          bullets={[
            'checkpoint order',
            'replay · resume · wait',
            'artifacts + state',
            'retry + isolation',
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
// Diagram 9: Harness / Runtime / Platform — the three-layer split
// ============================================================

export function HarnessRuntimePlatformDiagram() {
  return (
    <DiagramFrame>
      <div style={{ ...col, gap: 12 }}>
        <Subgraph
          label="Harness layer"
          tone="muted"
          annotation="how the agent thinks"
        >
          <div style={{ ...row }}>
            <Node title="Pydantic AI" tone="muted" minWidth={130} isMono />
            <Node title="Deep Agents" tone="muted" minWidth={130} isMono />
            <Node title="LangGraph" tone="muted" minWidth={120} isMono />
            <Node title="Claude Agent SDK" tone="muted" minWidth={170} isMono />
            <Node title="OpenAI Agents SDK" tone="muted" minWidth={170} isMono />
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
            <Node title="Versions + tag routing" tone="accent" minWidth={200} isMono />
            <Node title="Invocation" tone="accent" minWidth={130} isMono />
            <Node title="Artifacts + state" tone="accent" minWidth={160} isMono />
            <Node title="Isolated execution" tone="accent" minWidth={180} isMono />
          </div>
        </Subgraph>

        <Subgraph
          label="Platform layer"
          tone="muted"
          annotation="how the org governs · stays yours"
        >
          <div style={{ ...row }}>
            <Node title="Auth + entitlements" tone="muted" minWidth={180} />
            <Node title="Interceptors + guardrails" tone="muted" minWidth={210} />
            <Node title="Observability" tone="muted" minWidth={140} />
            <Node title="Product UI" tone="muted" minWidth={120} />
            <Node title="Policy" tone="muted" minWidth={90} />
          </div>
        </Subgraph>
      </div>
      <Legend
        items={[
          { tone: 'muted', label: 'Composable (your choice)' },
          { tone: 'accent', label: 'Where Kitaru lives' },
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
          display: 'grid',
          gridTemplateColumns: '1fr 1fr',
          gap: 18,
        }}
      >
        <Node
          title="Individual or small team building one agent"
          subtitle="optimize for velocity"
          bullets={[
            'pick a harness (PydanticAI, Deep Agents, Claude SDK…)',
            'adopt its runtime if it has one',
            'Kitaru is probably overkill',
          ]}
          tone="muted"
        />
        <Node
          title="Platform team supporting many agent teams"
          subtitle="optimize for durability + portability"
          bullets={[
            'teams pick their own harness',
            'durable execution must be harness-independent',
            'infra must be self-hosted',
            'Kitaru is the right size primitive',
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
