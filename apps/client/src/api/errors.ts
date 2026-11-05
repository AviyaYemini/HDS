/**
 * The API's refusals as translation keys: every problem answer carries a stable `code`, which is
 * also the key of its text in `src/locales` (Constitution VIII).
 */
import { useTranslation } from "react-i18next";

/** A request the API refused, or one that never reached it (`errors.network`). */
export class ApiError extends Error {
  constructor(
    readonly code: string,
    readonly status: number,
  ) {
    super(code);
    this.name = "ApiError";
  }
}

/** The translation key for `error`: its `code` when it has one, else `errors.unknown`. */
export function codeOf(error: unknown): string {
  if (typeof error === "object" && error !== null && "code" in error) {
    const code = (error as { code: unknown }).code;
    if (typeof code === "string" && code.length > 0) return code;
  }
  return "errors.unknown";
}

interface Outcome<T> {
  data?: T;
  error?: unknown;
  response: Response;
}

/** The data of a typed client call, or an {@link ApiError} with the API's code. */
export async function call<T>(request: Promise<Outcome<T>>): Promise<T> {
  let outcome: Outcome<T>;
  try {
    outcome = await request;
  } catch {
    throw new ApiError("errors.network", 0);
  }
  if (!outcome.response.ok) throw new ApiError(codeOf(outcome.error), outcome.response.status);
  return outcome.data as T;
}

/** A function from an error code to its text, `errors.unknown` for a code with none. */
export function useErrorText(): (code: string) => string {
  const { t, i18n } = useTranslation();
  return (code) => (i18n.exists(code) ? t(code) : t("errors.unknown"));
}
