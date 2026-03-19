/**
 * Shared waitlist form handler for Hero and CTA sections.
 * Submits to /api/waitlist and tracks via Segment analytics.
 */
export function setupWaitlistForm(formId: string, source: string) {
  const form = document.getElementById(formId) as HTMLFormElement | null;
  if (!form) return;

  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    const btn = form.querySelector('button')!;
    const input = form.querySelector('input')! as HTMLInputElement;
    const email = input.value.trim();

    btn.textContent = 'Submitting...';
    btn.setAttribute('disabled', 'true');

    try {
      const res = await fetch('/api/waitlist', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email }),
      });

      if (res.ok) {
        btn.textContent = "You're in \u2713";
        btn.style.background = 'var(--color-success)';
        input.disabled = true;
        window.analytics?.track('Waitlist Signup', {
          email,
          source,
          page: window.location.pathname,
        });
        window.analytics?.identify(email, { email });
      } else {
        btn.textContent = 'Something went wrong — try again';
        btn.removeAttribute('disabled');
      }
    } catch {
      btn.textContent = 'No connection — try again';
      btn.removeAttribute('disabled');
    }
  });
}
