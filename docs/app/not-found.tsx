import Link from 'next/link';

export default function NotFound() {
  return (
    <div className="flex flex-col items-center justify-center min-h-[60vh] px-6 text-center">
      <span className="text-[clamp(72px,12vw,120px)] font-bold tracking-tighter leading-none text-fd-muted-foreground/30">
        404
      </span>
      <h1 className="text-[clamp(24px,4vw,36px)] font-bold tracking-tight mt-2 mb-3">
        Page not found
      </h1>
      <p className="text-fd-muted-foreground text-base max-w-sm mb-8">
        The docs page you&apos;re looking for doesn&apos;t exist or has been
        moved.
      </p>
      <div className="flex gap-3 flex-wrap justify-center">
        <Link
          href="/"
          className="inline-flex items-center gap-2 rounded-lg bg-fd-primary text-fd-primary-foreground px-5 py-2.5 text-sm font-semibold transition-opacity hover:opacity-85"
        >
          Browse docs
        </Link>
        <a
          href="https://kitaru.ai"
          className="inline-flex items-center gap-2 rounded-lg border border-fd-border bg-fd-background px-5 py-2.5 text-sm font-medium text-fd-muted-foreground transition-colors hover:text-fd-foreground hover:border-fd-muted-foreground"
        >
          Back to home
        </a>
      </div>
    </div>
  );
}
