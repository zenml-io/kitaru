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
