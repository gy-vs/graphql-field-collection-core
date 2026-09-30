import type {
  ExecContext,
  ExecutionError,
  IncrementalExecution,
  IncrementalPatch,
  InitialResult,
  Path,
  PatchEvent,
  TypeRef,
} from './types.js';
import { isDeferred, isStream, type DeferredValue, type StreamValue } from './markers.js';

/**
 * One field in the execution tree. `resolve` may return a plain value, a
 * promise of one, or a defer()/stream() marker to declare incremental
 * delivery. When omitted, the resolver reads `source[key]`.
 */
export interface FieldNode {
  key: string;
  type: TypeRef;
  resolve?: (source: unknown, ctx: ExecContext) => unknown;
  children?: readonly FieldNode[];
}

const ROOT_PATH: Path = Object.freeze([]);
const NAMED: TypeRef = { kind: 'named' };

/** Internal unwind signal: a non-null position resolved to null. */
const BUBBLE = Symbol('BUBBLE');
/** Internal marker: the field's value is deferred and omitted from its parent object. */
const OMITTED = Symbol('OMITTED');

function toError(cause: unknown, path: Path): ExecutionError {
  return { message: cause instanceof Error ? cause.message : String(cause), path };
}

function childPath(path: Path, key: string | number): Path {
  return Object.freeze([...path, key]);
}

function nullableOf(type: TypeRef): TypeRef {
  return type.kind === 'nonNull' ? type.ofType! : type;
}

function listItemType(type: TypeRef): TypeRef | null {
  const nullable = nullableOf(type);
  return nullable.kind === 'list' ? nullable.ofType! : null;
}

function isThenable(value: unknown): value is PromiseLike<unknown> {
  return (
    ((typeof value === 'object' && value !== null) || typeof value === 'function') &&
    typeof (value as { then?: unknown }).then === 'function'
  );
}

function withLabel(label: string | undefined): { label?: string } {
  return label === undefined ? {} : { label };
}

function toAsyncIterator(items: AsyncIterable<unknown> | Iterable<unknown>): AsyncIterator<unknown> {
  if (Symbol.asyncIterator in items) return items[Symbol.asyncIterator]();
  const sync = items[Symbol.iterator]();
  const iterator: AsyncIterator<unknown> = {
    next: () => Promise.resolve(sync.next()),
  };
  if (sync.return) {
    iterator.return = () => {
      sync.return!();
      return Promise.resolve({ done: true, value: undefined });
    };
  }
  return iterator;
}

/**
 * Cancellation scope. Every object node in the execution tree gets one, and
 * deferred/stream tasks register in the scope of the node that declared
 * them. Cancelling a scope cancels its tasks and child scopes recursively,
 * which is how a failed parent field retracts its not-yet-published
 * descendant patches.
 *
 * A scope also carries a publication barrier: a promise resolved once the
 * position this scope belongs to has been published. Tasks wait on it before
 * emitting, so a patch never overtakes the patch that established its
 * parent position.
 */
class Scope {
  private readonly tasks = new Set<Task>();
  private readonly children = new Set<Scope>();
  private cancelled = false;
  readonly barrier: Promise<void>;
  private resolveBarrier: () => void = () => undefined;

  constructor(parent: Scope | null, ownBarrier = false) {
    parent?.children.add(this);
    if (ownBarrier) {
      this.barrier = new Promise<void>((resolve) => {
        this.resolveBarrier = resolve;
      });
    } else {
      this.barrier = parent ? parent.barrier : Promise.resolve();
    }
  }

  /** Mark this scope's position as published, unblocking descendant emissions. */
  publish(): void {
    this.resolveBarrier();
  }

  register(task: Task): void {
    if (this.cancelled) {
      task.cancel();
      return;
    }
    this.tasks.add(task);
  }

  cancel(): void {
    if (this.cancelled) return;
    this.cancelled = true;
    for (const task of [...this.tasks]) task.cancel();
    for (const child of [...this.children]) child.cancel();
    this.tasks.clear();
    this.children.clear();
  }
}

type TaskKind = 'defer' | 'stream';

/** A pending unit of incremental work, counted towards termination. */
class Task {
  cancelled = false;
  private finished = false;
  payloadScope: Scope | null = null;
  iterator: AsyncIterator<unknown> | null = null;
  /** Publication barrier of the position this task's patch targets. */
  readonly barrier: Promise<void>;
  /** Resolves when the task is cancelled, unblocking any barrier wait. */
  readonly cancelSignal: Promise<void>;
  private resolveCancel!: () => void;

  constructor(
    readonly kind: TaskKind,
    readonly path: Path,
    readonly label: string | undefined,
    readonly parentScope: Scope,
    private readonly owner: Executor,
  ) {
    this.barrier = parentScope.barrier;
    this.cancelSignal = new Promise<void>((resolve) => {
      this.resolveCancel = resolve;
    });
  }

  cancel(): void {
    if (this.cancelled) return;
    this.cancelled = true;
    this.resolveCancel();
    this.payloadScope?.cancel();
    const iterator = this.iterator;
    if (iterator && typeof iterator.return === 'function') {
      try {
        void Promise.resolve(iterator.return()).catch(() => undefined);
      } catch {
        // best-effort: a broken iterator must not escape cancellation
      }
    }
    this.finish();
  }

  /** Release the pending count exactly once, however the task ended. */
  finish(): void {
    if (this.finished) return;
    this.finished = true;
    this.owner.taskFinished();
  }
}

/**
 * Buffered producer/consumer queue behind the patch iterator. Producers
 * The terminal event is queued like any other event; nothing is delivered after it.
 */
class EventQueue implements AsyncIterableIterator<PatchEvent> {
  private readonly buffer: PatchEvent[] = [];
  private readonly waiters: Array<() => void> = [];
  private closed = false;
  private returned = false;

  constructor(
    private readonly gate: Promise<void>,
    private readonly onReturn: () => void,
  ) {}

  push(event: PatchEvent): void {
    if (this.closed || this.returned) return;
    this.buffer.push(event);
    this.flush();
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.flush();
  }

  private flush(): void {
    for (const wake of this.waiters.splice(0)) wake();
  }

  async next(): Promise<IteratorResult<PatchEvent>> {
    // The initial result always comes first; patches wait behind the gate.
    await this.gate;
    for (;;) {
      if (this.returned) return { done: true, value: undefined };
      const head = this.buffer.shift();
      if (head !== undefined) return { done: false, value: head };
      if (this.closed) return { done: true, value: undefined };
      await new Promise<void>((resolve) => this.waiters.push(resolve));
    }
  }

  async return(): Promise<IteratorResult<PatchEvent>> {
    if (!this.returned) {
      this.returned = true;
      this.onReturn();
      this.flush();
    }
    return { done: true, value: undefined };
  }

  [Symbol.asyncIterator](): AsyncIterableIterator<PatchEvent> {
    return this;
  }
}

class Executor {
  private readonly abort = new AbortController();
  private readonly ctx: ExecContext;
  private readonly rootScope = new Scope(null);
  private readonly queue: EventQueue;
  private readonly gate: Promise<void>;
  private openGate!: () => void;
  private readonly inflight = new Set<Promise<unknown>>();
  private pendingCount = 0;
  private patchesEmitted = false;
  private initialDone = false;
  private terminated = false;
  private cancelled = false;

  constructor(
    private readonly fields: readonly FieldNode[],
    private readonly rootValue: unknown,
  ) {
    this.ctx = { signal: this.abort.signal };
    this.gate = new Promise<void>((resolve) => {
      this.openGate = resolve;
    });
    this.queue = new EventQueue(this.gate, () => {
      void this.cancel();
    });
  }

  start(): IncrementalExecution {
    const initial = this.runInitial();
    // Open the gate no matter how the initial result turned out.
    initial.then(this.openGate, this.openGate);
    return {
      initial,
      patches: this.queue,
      cancel: () => this.cancel(),
    };
  }

  /** Cancel the whole execution and resolve once in-flight tasks have settled. */
  async cancel(): Promise<void> {
    if (this.cancelled) return;
    this.cancelled = true;
    this.abort.abort();
    this.rootScope.cancel();
    this.queue.close();
    // Drain late completions: cancelled tasks still settle their underlying
    // promises here, but their results are dropped, never published.
    await Promise.allSettled([...this.inflight]);
  }

  taskFinished(): void {
    this.pendingCount -= 1;
    this.maybeTerminate();
  }

  private maybeTerminate(): void {
    if (this.cancelled || !this.initialDone || this.pendingCount !== 0) return;
    this.terminateNow();
  }

  private terminateNow(): void {
    if (this.terminated) return;
    this.terminated = true;
    this.queue.push({ hasNext: false });
    this.queue.close();
  }

  private emit(patch: IncrementalPatch): void {
    if (this.terminated || this.cancelled) return;
    this.patchesEmitted = true;
    this.queue.push(patch);
  }

  private track(done: Promise<unknown>): void {
    this.inflight.add(done);
    const forget = () => {
      this.inflight.delete(done);
    };
    done.then(forget, forget);
  }

  private async runInitial(): Promise<InitialResult> {
    const errors: ExecutionError[] = [];
    let data: unknown = null;
    try {
      data = await this.completeValue(NAMED, this.rootValue ?? {}, ROOT_PATH, this.rootScope, this.fields, errors);
    } catch (e) {
      if (e !== BUBBLE) errors.push(toError(e, ROOT_PATH));
      data = null;
      this.rootScope.cancel();
    }
    this.initialDone = true;
    const hasNext = !this.cancelled && (this.pendingCount > 0 || this.patchesEmitted);
    if (!hasNext) {
      this.terminated = true;
      this.queue.close();
    } else if (this.pendingCount === 0) {
      // Every task settled while the initial tree was still completing.
      this.terminateNow();
    }
    return { data, ...(errors.length > 0 ? { errors } : {}), hasNext };
  }

  private async completeValue(
    type: TypeRef,
    value: unknown,
    path: Path,
    scope: Scope,
    children: readonly FieldNode[] | undefined,
    errors: ExecutionError[],
  ): Promise<unknown> {
    if (type.kind === 'nonNull') {
      const completed = await this.completeValue(type.ofType!, value, path, scope, children, errors);
      if (completed === null) {
        errors.push(toError(new Error('non-null field resolved to null'), path));
        throw BUBBLE;
      }
      return completed;
    }
    if (value === null || value === undefined) return null;
    if (type.kind === 'list') {
      const itemType = type.ofType!;
      const values = Array.isArray(value) ? value : [value];
      return Promise.all(
        values.map(async (item, index) => {
          try {
            return await this.completeValue(itemType, item, childPath(path, index), scope, children, errors);
          } catch (e) {
            if (e !== BUBBLE) throw e;
            if (itemType.kind === 'nonNull') throw BUBBLE;
            return null;
          }
        }),
      );
    }
    if (children !== undefined) {
      if (typeof value !== 'object') {
        errors.push(toError(new Error('expected an object value'), path));
        throw BUBBLE;
      }
      return this.completeObject(value as Record<string, unknown>, path, scope, children, errors);
    }
    return value;
  }

  private async completeObject(
    source: Record<string, unknown>,
    path: Path,
    scope: Scope,
    fields: readonly FieldNode[],
    errors: ExecutionError[],
  ): Promise<Record<string, unknown>> {
    const nodeScope = new Scope(scope);
    // Wait for every field, even after a failure, so no field completion can
    // register tasks or record errors after this node has been torn down.
    const results = await Promise.all(
      fields.map((field) =>
        this.completeField(field, source, childPath(path, field.key), nodeScope, errors).then(
          (value) => ({ field, value }),
          (error: unknown) => ({ field, error }),
        ),
      ),
    );
    const obj: Record<string, unknown> = {};
    let bubbled = false;
    for (const result of results) {
      if ('error' in result) {
        if (result.error !== BUBBLE) errors.push(toError(result.error, childPath(path, result.field.key)));
        bubbled = true;
        continue;
      }
      if (result.value !== OMITTED) obj[result.field.key] = result.value;
    }
    if (bubbled) {
      // A non-null field failed: this node becomes null and its unpublished
      // descendant patches are retracted.
      nodeScope.cancel();
      throw BUBBLE;
    }
    return obj;
  }

  private async completeField(
    field: FieldNode,
    source: Record<string, unknown>,
    path: Path,
    scope: Scope,
    errors: ExecutionError[],
  ): Promise<unknown> {
    const nonNull = field.type.kind === 'nonNull';
    let raw: unknown;
    try {
      raw = field.resolve ? field.resolve(source, this.ctx) : source[field.key];
    } catch (e) {
      errors.push(toError(e, path));
      if (nonNull) throw BUBBLE;
      return null;
    }
    try {
      let value = raw;
      for (;;) {
        if (isDeferred(value)) {
          this.registerDefer(scope, path, field, value);
          return OMITTED;
        }
        if (isStream(value)) {
          const itemType = listItemType(field.type);
          if (itemType === null) {
            errors.push(toError(new Error('stream marker on a non-list field'), path));
            if (nonNull) throw BUBBLE;
            return null;
          }
          this.registerStream(scope, path, field, value, itemType);
          return [];
        }
        if (isThenable(value)) {
          value = await value;
          continue;
        }
        break;
      }
      return await this.completeValue(field.type, value, path, scope, field.children, errors);
    } catch (e) {
      if (e !== BUBBLE) errors.push(toError(e, path));
      if (nonNull) throw BUBBLE;
      return null;
    }
  }

  private newTask(kind: TaskKind, path: Path, label: string | undefined, scope: Scope): Task {
    this.pendingCount += 1;
    const task = new Task(kind, path, label, scope, this);
    scope.register(task);
    return task;
  }

  private registerDefer(scope: Scope, path: Path, field: FieldNode, marker: DeferredValue): void {
    const task = this.newTask('defer', path, marker.label, scope);
    if (task.cancelled) return;
    let settled: Promise<unknown>;
    try {
      const source =
        typeof marker.value === 'function'
          ? (marker.value as (ctx: ExecContext) => unknown)(this.ctx)
          : marker.value;
      settled = Promise.resolve(source);
    } catch (e) {
      settled = Promise.reject(e);
    }
    const done = settled.then(
      (value) => this.settleDefer(task, field, path, value),
      (cause) => this.settleDeferError(task, path, cause),
    );
    this.track(done);
  }

  private async settleDefer(task: Task, field: FieldNode, path: Path, value: unknown): Promise<void> {
    try {
      // Never publish before the position this patch targets is published.
      await Promise.race([task.barrier, task.cancelSignal]);
      if (task.cancelled) return;
      const payloadScope = new Scope(task.parentScope, true);
      task.payloadScope = payloadScope;
      const errors: ExecutionError[] = [];
      let data: unknown = null;
      let failed = false;
      try {
        data = await this.completeValue(field.type, value, path, payloadScope, field.children, errors);
      } catch (e) {
        // The defer boundary stops non-null propagation: the failure is
        // reported on this patch instead of nulling already-delivered data.
        if (e !== BUBBLE) errors.push(toError(e, path));
        failed = true;
      }
      if (task.cancelled) {
        payloadScope.cancel();
        return;
      }
      if (failed) {
        payloadScope.cancel();
        this.emit({ path, ...withLabel(task.label), errors });
      } else {
        this.emit({ path, ...withLabel(task.label), data, ...(errors.length > 0 ? { errors } : {}) });
      }
      // Unblock nested tasks only after this patch is queued.
      payloadScope.publish();
    } finally {
      task.finish();
    }
  }

  private async settleDeferError(task: Task, path: Path, cause: unknown): Promise<void> {
    try {
      // Error patches respect the same publication order as data patches.
      await Promise.race([task.barrier, task.cancelSignal]);
      if (task.cancelled) return;
      this.emit({ path, ...withLabel(task.label), errors: [toError(cause, path)] });
    } finally {
      task.finish();
    }
  }

  private registerStream(
    scope: Scope,
    path: Path,
    field: FieldNode,
    marker: StreamValue,
    itemType: TypeRef,
  ): void {
    const task = this.newTask('stream', path, marker.label, scope);
    if (task.cancelled) return;
    // Items of this stream complete in their own scope so that a stream
    // failure retracts the pending nested work of already-delivered items.
    const streamScope = new Scope(scope);
    task.payloadScope = streamScope;
    const iterator = toAsyncIterator(marker.items);
    task.iterator = iterator;
    this.track(this.runStream(task, field, path, itemType, iterator, streamScope));
  }

  private async runStream(
    task: Task,
    field: FieldNode,
    path: Path,
    itemType: TypeRef,
    iterator: AsyncIterator<unknown>,
    streamScope: Scope,
  ): Promise<void> {
    let index = 0;
    try {
      // Never publish before the position the list lives at is published.
      await Promise.race([task.barrier, task.cancelSignal]);
      if (task.cancelled) return;
      for (;;) {
        if (task.cancelled) return;
        const next = await iterator.next();
        if (next.done || task.cancelled) return;
        const itemPath = childPath(path, index);
        // Nested work of this item publishes only after the item's own patch.
        const itemScope = new Scope(streamScope, true);
        const errors: ExecutionError[] = [];
        let item: unknown = null;
        let listFailed = false;
        try {
          item = await this.completeValue(itemType, next.value, itemPath, itemScope, field.children, errors);
        } catch (e) {
          if (e !== BUBBLE) errors.push(toError(e, itemPath));
          // A violated non-null item type fails the whole list; a nullable
          // item collapses to null and the stream continues.
          listFailed = itemType.kind === 'nonNull';
          item = null;
        }
        if (task.cancelled) {
          itemScope.cancel();
          return;
        }
        if (listFailed) {
          itemScope.cancel();
          streamScope.cancel();
          this.emit({ path, ...withLabel(task.label), errors });
          return;
        }
        this.emit({
          path,
          ...withLabel(task.label),
          items: [item],
          itemIndex: index,
          ...(errors.length > 0 ? { errors } : {}),
        });
        itemScope.publish();
        index += 1;
      }
    } catch (e) {
      if (!task.cancelled) {
        streamScope.cancel();
        this.emit({ path, ...withLabel(task.label), errors: [toError(e, path)] });
      }
    } finally {
      if (typeof iterator.return === 'function') {
        try {
          await iterator.return();
        } catch {
          // best-effort cleanup of the underlying iterable
        }
      }
      task.finish();
    }
  }
}

/**
 * Execute the field tree against `rootValue`. The returned `initial` promise
 * resolves with the immediately available data (deferred fields omitted,
 * streamed lists empty) without waiting for any deferred work; subsequent
 * results are delivered as patches through the async iterator, followed by a
 * single terminal `{ hasNext: false }` once every task has been cleaned up.
 */
export function execute(fields: readonly FieldNode[], rootValue: unknown = {}): IncrementalExecution {
  return new Executor(fields, rootValue).start();
}
