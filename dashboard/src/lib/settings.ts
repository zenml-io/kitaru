import { useSyncExternalStore } from "react";

const API_KEY_STORAGE_KEY = "kitaru-dashboard.api-key";

const listeners = new Set<() => void>();

export function getApiKey(): string | null {
  return localStorage.getItem(API_KEY_STORAGE_KEY);
}

export function setApiKey(value: string | null): void {
  if (value && value.trim() !== "") {
    localStorage.setItem(API_KEY_STORAGE_KEY, value.trim());
  } else {
    localStorage.removeItem(API_KEY_STORAGE_KEY);
  }
  for (const listener of listeners) {
    listener();
  }
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function useApiKey(): string | null {
  return useSyncExternalStore(subscribe, getApiKey);
}
