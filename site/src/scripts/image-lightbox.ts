/**
 * Click-to-expand lightbox for blog images.
 *
 * Targets the hero image and any `<img>` rendered inside the MDX article body.
 * Avatars and other UI images outside `.blog-post` are intentionally excluded.
 */

const TARGET_SELECTOR = '.blog-post-image, .blog-post-body img';
const ZOOMABLE_CLASS = 'is-zoomable';

interface LightboxRefs {
  dialog: HTMLDialogElement;
  img: HTMLImageElement;
}

function buildLightbox(): LightboxRefs {
  const dialog = document.createElement('dialog');
  dialog.className = 'image-lightbox';
  dialog.setAttribute('aria-label', 'Image preview');
  dialog.innerHTML = `
    <button type="button" class="image-lightbox__close" aria-label="Close image preview">&times;</button>
    <img class="image-lightbox__img" alt="" />
  `;
  document.body.appendChild(dialog);

  const img = dialog.querySelector<HTMLImageElement>('.image-lightbox__img')!;
  const closeBtn = dialog.querySelector<HTMLButtonElement>('.image-lightbox__close')!;

  // Backdrop region is the dialog itself; clicks on the inner image bubble up
  // to here, so we filter by event.target to keep image clicks from closing.
  dialog.addEventListener('click', (event) => {
    if (event.target === dialog) dialog.close();
  });
  closeBtn.addEventListener('click', () => dialog.close());

  return { dialog, img };
}

function init(): void {
  const targets = document.querySelectorAll<HTMLImageElement>(TARGET_SELECTOR);
  if (targets.length === 0) return;

  targets.forEach((source) => source.classList.add(ZOOMABLE_CLASS));

  const { dialog, img } = buildLightbox();

  document.addEventListener('click', (event) => {
    const source = (event.target as Element | null)?.closest<HTMLImageElement>(
      `img.${ZOOMABLE_CLASS}`,
    );
    if (!source) return;
    img.src = source.currentSrc || source.src;
    img.alt = source.alt || '';
    dialog.showModal();
  });
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', init);
} else {
  init();
}
