// Evaluator model policies match `model_provider` exactly, so the recorded value
// must stay the bare provider family. SDKs report transport-qualified strings
// such as "openai.responses"; the qualified original survives as
// `attributes.provider_id`.
export function providerFamily(provider: string): string {
  const separator = provider.indexOf(".");
  return separator === -1 ? provider : provider.slice(0, separator);
}
