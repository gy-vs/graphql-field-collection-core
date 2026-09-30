import type { ExecContext } from './types.js';

export const DEFER_BRAND: unique symbol = Symbol('graphql.incremental.defer');
export const STREAM_BRAND: unique symbol = Symbol('graphql.incremental.stream');

/**
 * A plain value, a promise of one, or a thunk producing either. Thunks
 * receive the execution context so they can observe `ctx.signal` and abort
 * their own work when the execution is cancelled.
 */
export type DeferredSource<T> = T | Promise<T> | ((ctx: ExecContext) => T | Promise<T>);

/** Marker a resolver returns to declare that this field's value arrives later in a patch. */
export interface DeferredValue<T = unknown> {
  readonly [DEFER_BRAND]: true;
  readonly label?: string;
  readonly value: DeferredSource<T>;
}

/** Marker a resolver returns to declare that this list field is delivered incrementally. */
export interface StreamValue<T = unknown> {
  readonly [STREAM_BRAND]: true;
  readonly label?: string;
  readonly items: AsyncIterable<T> | Iterable<T>;
}

export function defer<T>(value: DeferredSource<T>, label?: string): DeferredValue<T> {
  return { [DEFER_BRAND]: true, label, value };
}

export function stream<T>(items: AsyncIterable<T> | Iterable<T>, label?: string): StreamValue<T> {
  return { [STREAM_BRAND]: true, label, items };
}

export function isDeferred(value: unknown): value is DeferredValue {
  return typeof value === 'object' && value !== null && (value as Record<PropertyKey, unknown>)[DEFER_BRAND] === true;
}

export function isStream(value: unknown): value is StreamValue {
  return typeof value === 'object' && value !== null && (value as Record<PropertyKey, unknown>)[STREAM_BRAND] === true;
}
