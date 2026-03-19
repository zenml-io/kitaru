// Intersection Observer for scroll-triggered reveals
const revealElements = document.querySelectorAll<HTMLElement>('[data-reveal]');

// Skip animations for users who prefer reduced motion
if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
  revealElements.forEach((el) => el.classList.add('revealed'));
} else {
  const observer = new IntersectionObserver(
    (entries) => {
      entries.forEach((entry) => {
        if (entry.isIntersecting) {
          entry.target.classList.add('revealed');
          observer.unobserve(entry.target);
        }
      });
    },
    { threshold: 0.1, rootMargin: '0px 0px -40px 0px' }
  );

  revealElements.forEach((el) => {
    observer.observe(el);
  });
}

// Number counter animation for [data-counter] elements
if (!window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
  const counterElements = document.querySelectorAll<HTMLElement>('[data-counter]');

  const counterObserver = new IntersectionObserver(
    (entries) => {
      entries.forEach((entry) => {
        if (entry.isIntersecting) {
          const el = entry.target as HTMLElement;
          const target = parseInt(el.getAttribute('data-target') || '0', 10);
          const duration = 1200;
          const start = performance.now();

          function update(now: number) {
            const elapsed = now - start;
            const progress = Math.min(elapsed / duration, 1);
            // Ease-out cubic
            const eased = 1 - Math.pow(1 - progress, 3);
            el.textContent = Math.round(eased * target).toString();
            if (progress < 1) {
              requestAnimationFrame(update);
            }
          }

          requestAnimationFrame(update);
          counterObserver.unobserve(el);
        }
      });
    },
    { threshold: 0.1 }
  );

  counterElements.forEach((el) => {
    counterObserver.observe(el);
  });
} else {
  // For reduced motion, show the target number immediately
  document.querySelectorAll<HTMLElement>('[data-counter]').forEach((el) => {
    el.textContent = el.getAttribute('data-target') || '0';
  });
}
