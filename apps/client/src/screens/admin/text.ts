/**
 * Small text helpers the admin forms share.
 */

/** Empty text as null, for the API's optional fields. */
export const orNull = (value: string | undefined): string | null => (value && value.trim() ? value.trim() : null);
