/** Copy text to clipboard with fallback for restricted environments. */
export function copyToClipboard(text: string): Promise<void> {
  if (navigator.clipboard?.writeText) {
    return navigator.clipboard.writeText(text);
  }
  // Fallback: temporary textarea + execCommand
  const ta = document.createElement('textarea');
  ta.value = text;
  ta.style.cssText = 'position:fixed;left:-9999px;top:-9999px;opacity:0';
  document.body.appendChild(ta);
  ta.select();
  try {
    if (!document.execCommand('copy')) {
      document.body.removeChild(ta);
      return Promise.reject();
    }
  } catch {
    document.body.removeChild(ta);
    return Promise.reject();
  }
  document.body.removeChild(ta);
  return Promise.resolve();
}
