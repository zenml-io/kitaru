export const FORM_TYPES = {
  DEMO_REQUEST: 'demo-request',
  WAITLIST: 'waitlist',
} as const;

export type FormType = typeof FORM_TYPES[keyof typeof FORM_TYPES];
