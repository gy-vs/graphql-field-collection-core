export type TypeRef = { kind: 'named' | 'list' | 'nonNull'; ofType?: TypeRef };

/**
 * Stable identity of a node in the execution tree. Path arrays are frozen
 * once created; the same node is referenced by the same path value across
 * the initial result, patches and errors.
 */
export type Path = ReadonlyArray<string | number>;

export interface ExecutionError {
  message: string;
  path: Path;
}

/** Context handed to resolvers and deferred thunks so they can observe cancellation. */
export interface ExecContext {
  readonly signal: AbortSignal;
}

export interface InitialResult {
  /** Immediately available data; deferred fields are omitted, streamed lists start empty. */
  data: unknown;
  errors?: ExecutionError[];
  /** false means no incremental work is pending and the patch iterator is already done. */
  hasNext: boolean;
}

export interface IncrementalPatch {
  /** Path of the deferred field or streamed list this patch belongs to. */
  path: Path;
  /** Resolver-declared label; labels are inert metadata, duplicates are allowed. */
  label?: string;
  /** Completed value of a deferred field (absent when the deferred work failed). */
  data?: unknown;
  /** Next chunk of list items for a streamed field. */
  items?: unknown[];
  /** Sequence number of the first item in `items`, monotonic per stream path. */
  itemIndex?: number;
  errors?: ExecutionError[];
}

export interface TerminalEvent {
  /** Emitted exactly once, after every pending task has been published or cleaned up. */
  hasNext: false;
}

export type PatchEvent = IncrementalPatch | TerminalEvent;

export function isTerminal(event: PatchEvent): event is TerminalEvent {
  return 'hasNext' in event;
}

export interface IncrementalExecution {
  /** Resolves with the first payload as soon as non-deferred data is ready. */
  initial: Promise<InitialResult>;
  /** Yields patches in completion order, then a single terminal `{ hasNext: false }`. */
  patches: AsyncIterableIterator<PatchEvent>;
  /**
   * Cancels the execution: aborts the resolver signal, retracts all pending
   * patches, finishes the patch iterator, and resolves once every in-flight
   * task has settled (late completions are dropped, never published).
   */
  cancel(): Promise<void>;
}
