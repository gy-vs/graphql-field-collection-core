import { describe, expect, it } from 'vitest';
import {
  defer,
  execute,
  isTerminal,
  stream,
  type FieldNode,
  type IncrementalPatch,
  type PatchEvent,
  type TypeRef,
} from '../src/index.js';

const T: TypeRef = { kind: 'named' };
const nn = (ofType: TypeRef): TypeRef => ({ kind: 'nonNull', ofType });
const listOf = (ofType: TypeRef): TypeRef => ({ kind: 'list', ofType });

/** A promise whose settlement is controlled by the test. */
function manual<T>() {
  let resolve!: (value: T | Promise<T>) => void;
  let reject!: (cause: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

/** An async iterable whose items are pushed by the test; cancellation-safe. */
function controllable<T>() {
  const queue: T[] = [];
  const waiters: Array<() => void> = [];
  let finished = false;
  let failure: { cause: unknown } | null = null;
  let returned = false;
  const wake = () => {
    for (const resolve of waiters.splice(0)) resolve();
  };
  const iterator: AsyncIterator<T> = {
    async next(): Promise<IteratorResult<T>> {
      for (;;) {
        if (returned) return { done: true, value: undefined };
        if (queue.length > 0) return { done: false, value: queue.shift()! };
        if (failure) throw failure.cause;
        if (finished) return { done: true, value: undefined };
        await new Promise<void>((resolve) => waiters.push(resolve));
      }
    },
    async return(): Promise<IteratorResult<T>> {
      returned = true;
      wake();
      return { done: true, value: undefined };
    },
  };
  return {
    iterable: { [Symbol.asyncIterator]: () => iterator },
    push(value: T) {
      queue.push(value);
      wake();
    },
    close() {
      finished = true;
      wake();
    },
    fail(cause: unknown) {
      failure = { cause };
      wake();
    },
    isReturned: () => returned,
  };
}

async function drain(patches: AsyncIterableIterator<PatchEvent>): Promise<PatchEvent[]> {
  const events: PatchEvent[] = [];
  for await (const event of patches) events.push(event);
  return events;
}

function onlyPatches(events: PatchEvent[]): IncrementalPatch[] {
  return events.filter((event): event is IncrementalPatch => !isTerminal(event));
}

describe('incremental delivery', () => {
  it('delivers a deferred field in a patch without blocking the initial payload', async () => {
    const slow = manual<string>();
    const fields: FieldNode[] = [
      { key: 'fast', type: T, resolve: () => 'F' },
      { key: 'slow', type: T, resolve: () => defer(slow.promise, 'slowLabel') },
    ];
    const exec = execute(fields, {});
    // The first payload must resolve while the deferred promise is still pending.
    const initial = await exec.initial;
    expect(initial.hasNext).toBe(true);
    expect(initial.data).toEqual({ fast: 'F' });
    expect('slow' in (initial.data as object)).toBe(false);

    slow.resolve('S');
    expect(await drain(exec.patches)).toEqual([
      { path: ['slow'], label: 'slowLabel', data: 'S' },
      { hasNext: false },
    ]);
  });

  it('still awaits non-deferred async resolvers for the initial payload', async () => {
    const gate = manual<string>();
    const exec = execute([{ key: 'a', type: T, resolve: () => gate.promise }], {});
    let settled = false;
    void exec.initial.then(() => {
      settled = true;
    });
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(settled).toBe(false);
    gate.resolve('A');
    expect((await exec.initial).data).toEqual({ a: 'A' });
  });

  it('nests deferred payloads with stable paths', async () => {
    const outer = manual<unknown>();
    const inner = manual<string>();
    const fields: FieldNode[] = [
      {
        key: 'a',
        type: T,
        resolve: () => defer(outer.promise, 'outer'),
        children: [
          { key: 'x', type: T },
          { key: 'b', type: T, resolve: () => defer(inner.promise, 'inner') },
        ],
      },
    ];
    const exec = execute(fields, {});
    expect((await exec.initial).data).toEqual({});

    outer.resolve({ x: 1 });
    const it = exec.patches;
    expect((await it.next()).value).toEqual({ path: ['a'], label: 'outer', data: { x: 1 } });

    inner.resolve('deep');
    expect((await it.next()).value).toEqual({ path: ['a', 'b'], label: 'inner', data: 'deep' });
    expect((await it.next()).value).toEqual({ hasNext: false });
    expect((await it.next()).done).toBe(true);
  });

  it('orders a nested deferred rejection after its parent patch', async () => {
    const payload = manual<unknown>();
    const nested = manual<string>();
    const exec = execute(
      [
        {
          key: 'a',
          type: T,
          resolve: () => defer(payload.promise, 'a'),
          children: [
            { key: 'x', type: T },
            { key: 'b', type: T, resolve: () => defer(nested.promise, 'nested') },
          ],
        },
      ],
      {},
    );
    await exec.initial;
    payload.resolve({ x: 1 });
    // Rejects while the parent payload is still being completed; the error
    // patch must still come after the parent patch that establishes ['a'].
    nested.reject(new Error('nested boom'));
    expect(await drain(exec.patches)).toEqual([
      { path: ['a'], label: 'a', data: { x: 1 } },
      { path: ['a', 'b'], label: 'nested', errors: [{ message: 'nested boom', path: ['a', 'b'] }] },
      { hasNext: false },
    ]);
  });

  it('supports streams inside deferred payloads', async () => {
    const payload = manual<unknown>();
    const items = controllable<number>();
    const fields: FieldNode[] = [
      {
        key: 'a',
        type: T,
        resolve: () => defer(payload.promise, 'a'),
        children: [{ key: 'nums', type: listOf(T), resolve: () => stream(items.iterable, 's') }],
      },
    ];
    const exec = execute(fields, {});
    await exec.initial;
    payload.resolve({});
    const it = exec.patches;
    expect((await it.next()).value).toEqual({ path: ['a'], label: 'a', data: { nums: [] } });

    items.push(7);
    items.close();
    expect((await it.next()).value).toEqual({ path: ['a', 'nums'], label: 's', items: [7], itemIndex: 0 });
    expect((await it.next()).value).toEqual({ hasNext: false });
  });

  it('streams list items with per-path sequence numbers', async () => {
    const items = controllable<number>();
    const exec = execute(
      [{ key: 'nums', type: listOf(T), resolve: () => stream(items.iterable, 'nums') }],
      {},
    );
    const initial = await exec.initial;
    expect(initial.data).toEqual({ nums: [] });
    expect(initial.hasNext).toBe(true);

    items.push(1);
    items.push(2);
    const it = exec.patches;
    expect((await it.next()).value).toEqual({ path: ['nums'], label: 'nums', items: [1], itemIndex: 0 });
    expect((await it.next()).value).toEqual({ path: ['nums'], label: 'nums', items: [2], itemIndex: 1 });

    items.push(3);
    items.close();
    expect((await it.next()).value).toEqual({ path: ['nums'], label: 'nums', items: [3], itemIndex: 2 });
    expect((await it.next()).value).toEqual({ hasNext: false });
    expect((await it.next()).done).toBe(true);
  });

  it('accepts synchronous iterables as streams', async () => {
    const exec = execute([{ key: 'nums', type: listOf(T), resolve: () => stream([1, 2, 3]) }], {});
    await exec.initial;
    expect(await drain(exec.patches)).toEqual([
      { path: ['nums'], items: [1], itemIndex: 0 },
      { path: ['nums'], items: [2], itemIndex: 1 },
      { path: ['nums'], items: [3], itemIndex: 2 },
      { hasNext: false },
    ]);
  });

  it('delivers patches with duplicate labels independently', async () => {
    const first = manual<string>();
    const second = manual<string>();
    const exec = execute(
      [
        { key: 'a', type: T, resolve: () => defer(first.promise, 'dup') },
        { key: 'b', type: T, resolve: () => defer(second.promise, 'dup') },
      ],
      {},
    );
    await exec.initial;
    // Arrival order follows completion order, not declaration order.
    second.resolve('B');
    first.resolve('A');
    const events = await drain(exec.patches);
    expect(onlyPatches(events)).toEqual([
      { path: ['b'], label: 'dup', data: 'B' },
      { path: ['a'], label: 'dup', data: 'A' },
    ]);
    expect(events.at(-1)).toEqual({ hasNext: false });
  });

  it('keeps per-path sequence numbers when concurrent streams interleave', async () => {
    const left = controllable<number>();
    const right = controllable<number>();
    const exec = execute(
      [
        { key: 'left', type: listOf(T), resolve: () => stream(left.iterable) },
        { key: 'right', type: listOf(T), resolve: () => stream(right.iterable) },
      ],
      {},
    );
    await exec.initial;
    // Interleave arrivals across the two streams.
    left.push(1);
    right.push(10);
    left.push(2);
    right.push(20);
    right.push(30);
    left.push(3);
    left.close();
    right.close();
    const events = await drain(exec.patches);
    const patches = onlyPatches(events);
    const byPath = (key: string) => patches.filter((p) => p.path[0] === key);
    expect(byPath('left').map((p) => p.itemIndex)).toEqual([0, 1, 2]);
    expect(byPath('left').map((p) => p.items)).toEqual([[1], [2], [3]]);
    expect(byPath('right').map((p) => p.itemIndex)).toEqual([0, 1, 2]);
    expect(byPath('right').map((p) => p.items)).toEqual([[10], [20], [30]]);
    expect(events.at(-1)).toEqual({ hasNext: false });
  });

  it('supports deferred fields inside streamed items with indexed paths', async () => {
    const items = controllable<unknown>();
    const extra = manual<string>();
    const exec = execute(
      [
        {
          key: 'items',
          type: listOf(T),
          resolve: () => stream(items.iterable, 's'),
          children: [
            { key: 'id', type: T },
            { key: 'extra', type: T, resolve: () => defer(extra.promise, 'x') },
          ],
        },
      ],
      {},
    );
    await exec.initial;
    items.push({ id: 1 });
    items.close();
    const it = exec.patches;
    expect((await it.next()).value).toEqual({ path: ['items'], label: 's', items: [{ id: 1 }], itemIndex: 0 });

    // The terminal event must wait for the item's deferred field too.
    extra.resolve('E');
    expect((await it.next()).value).toEqual({ path: ['items', 0, 'extra'], label: 'x', data: 'E' });
    expect((await it.next()).value).toEqual({ hasNext: false });
  });
});

describe('errors and non-null propagation', () => {
  it('records synchronous resolver throws and propagates non-null violations', async () => {
    const fields: FieldNode[] = [
      { key: 'ok', type: T, resolve: () => 1 },
      {
        key: 'bad',
        type: T,
        resolve: () => {
          throw new Error('boom');
        },
      },
      {
        key: 'obj',
        type: T,
        resolve: () => ({}),
        children: [
          {
            key: 'c',
            type: nn(T),
            resolve: () => {
              throw new Error('deep');
            },
          },
        ],
      },
    ];
    const exec = execute(fields, {});
    const initial = await exec.initial;
    expect(initial.data).toEqual({ ok: 1, bad: null, obj: null });
    expect(initial.errors).toEqual([
      { message: 'boom', path: ['bad'] },
      { message: 'deep', path: ['obj', 'c'] },
    ]);
    expect(initial.hasNext).toBe(false);
    expect((await exec.patches.next()).done).toBe(true);
  });

  it('collects async rejections of plain resolvers into the initial result', async () => {
    const exec = execute([{ key: 'a', type: T, resolve: () => Promise.reject(new Error('async boom')) }], {});
    const initial = await exec.initial;
    expect(initial.data).toEqual({ a: null });
    expect(initial.errors).toEqual([{ message: 'async boom', path: ['a'] }]);
    expect(initial.hasNext).toBe(false);
  });

  it('puts async rejections into the corresponding patch and keeps siblings', async () => {
    const bad = manual<string>();
    const good = manual<string>();
    const exec = execute(
      [
        { key: 'bad', type: T, resolve: () => defer(bad.promise, 'bad') },
        { key: 'good', type: T, resolve: () => defer(good.promise, 'good') },
      ],
      {},
    );
    await exec.initial;
    bad.reject(new Error('nope'));
    good.resolve('G');
    expect(await drain(exec.patches)).toEqual([
      { path: ['bad'], label: 'bad', errors: [{ message: 'nope', path: ['bad'] }] },
      { path: ['good'], label: 'good', data: 'G' },
      { hasNext: false },
    ]);
  });

  it('reports synchronous throws of deferred thunks as patch errors', async () => {
    const exec = execute(
      [
        {
          key: 'a',
          type: T,
          resolve: () =>
            defer(() => {
              throw new Error('thunk boom');
            }, 'a'),
        },
      ],
      {},
    );
    await exec.initial;
    expect(await drain(exec.patches)).toEqual([
      { path: ['a'], label: 'a', errors: [{ message: 'thunk boom', path: ['a'] }] },
      { hasNext: false },
    ]);
  });

  it('treats a deferred payload as a propagation boundary and cancels its nested defers on failure', async () => {
    const payload = manual<unknown>();
    const nested = manual<string>();
    const exec = execute(
      [
        {
          key: 'a',
          type: T,
          resolve: () => defer(payload.promise, 'a'),
          children: [
            { key: 'vital', type: nn(T), resolve: () => Promise.reject(new Error('inner fatal')) },
            { key: 'nested', type: T, resolve: () => defer(nested.promise, 'nested') },
          ],
        },
      ],
      {},
    );
    await exec.initial;
    payload.resolve({});
    nested.resolve('never published');
    expect(await drain(exec.patches)).toEqual([
      { path: ['a'], label: 'a', errors: [{ message: 'inner fatal', path: ['a', 'vital'] }] },
      { hasNext: false },
    ]);
  });

  it('delivers partial data with errors when a nullable field inside a deferred payload fails', async () => {
    const payload = manual<unknown>();
    const exec = execute(
      [
        {
          key: 'a',
          type: T,
          resolve: () => defer(payload.promise, 'a'),
          children: [
            { key: 'ok', type: T, resolve: () => 1 },
            { key: 'bad', type: T, resolve: () => Promise.reject(new Error('nullable boom')) },
          ],
        },
      ],
      {},
    );
    await exec.initial;
    payload.resolve({});
    expect(await drain(exec.patches)).toEqual([
      {
        path: ['a'],
        label: 'a',
        data: { ok: 1, bad: null },
        errors: [{ message: 'nullable boom', path: ['a', 'bad'] }],
      },
      { hasNext: false },
    ]);
  });

  it('cancels unpublished descendant patches when a parent field fails', async () => {
    const doomed = manual<string>();
    const exec = execute(
      [
        {
          key: 'obj',
          type: T,
          resolve: () => ({}),
          children: [
            { key: 'later', type: T, resolve: () => defer(doomed.promise, 'doomed') },
            {
              key: 'c',
              type: nn(T),
              resolve: () => {
                throw new Error('kill parent');
              },
            },
          ],
        },
        { key: 'safe', type: T, resolve: () => defer(Promise.resolve('kept'), 'safe') },
      ],
      {},
    );
    const initial = await exec.initial;
    expect(initial.data).toEqual({ obj: null });
    expect(initial.errors).toEqual([{ message: 'kill parent', path: ['obj', 'c'] }]);
    expect(initial.hasNext).toBe(true);

    // The doomed deferred field resolves late; its patch must never appear.
    doomed.resolve('too late');
    expect(await drain(exec.patches)).toEqual([
      { path: ['safe'], label: 'safe', data: 'kept' },
      { hasNext: false },
    ]);
  });

  it('nulls the root and cancels all pending work when a non-null root field fails', async () => {
    const pending = manual<string>();
    const exec = execute(
      [
        { key: 'pending', type: T, resolve: () => defer(pending.promise) },
        {
          key: 'vital',
          type: nn(T),
          resolve: () => {
            throw new Error('fatal');
          },
        },
      ],
      {},
    );
    const initial = await exec.initial;
    expect(initial.data).toBeNull();
    expect(initial.errors).toEqual([{ message: 'fatal', path: ['vital'] }]);
    expect(initial.hasNext).toBe(false);
    pending.resolve('late');
    expect((await exec.patches.next()).done).toBe(true);
  });

  it('reports stream iterable failures in a patch at the list path', async () => {
    const items = controllable<number>();
    const exec = execute([{ key: 'nums', type: listOf(T), resolve: () => stream(items.iterable, 's') }], {});
    await exec.initial;
    items.push(1);
    items.fail(new Error('stream broke'));
    expect(await drain(exec.patches)).toEqual([
      { path: ['nums'], label: 's', items: [1], itemIndex: 0 },
      { path: ['nums'], label: 's', errors: [{ message: 'stream broke', path: ['nums'] }] },
      { hasNext: false },
    ]);
  });

  it('fails the whole stream when a non-null item violates its type', async () => {
    const items = controllable<unknown>();
    const exec = execute(
      [
        {
          key: 'items',
          type: listOf(nn(T)),
          resolve: () => stream(items.iterable),
          children: [{ key: 'v', type: nn(T) }],
        },
      ],
      {},
    );
    await exec.initial;
    items.push({ v: 1 });
    items.push({ v: null });
    items.push({ v: 3 }); // never delivered: the stream already failed
    items.close();
    expect(await drain(exec.patches)).toEqual([
      { path: ['items'], items: [{ v: 1 }], itemIndex: 0 },
      { path: ['items'], errors: [{ message: 'non-null field resolved to null', path: ['items', 1, 'v'] }] },
      { hasNext: false },
    ]);
    expect(items.isReturned()).toBe(true);
  });

  it('delivers null for a failed nullable item and continues the stream', async () => {
    const items = controllable<unknown>();
    const exec = execute(
      [
        {
          key: 'items',
          type: listOf(T),
          resolve: () => stream(items.iterable),
          children: [{ key: 'v', type: nn(T) }],
        },
      ],
      {},
    );
    await exec.initial;
    items.push({ v: 1 });
    items.push({ v: null });
    items.push({ v: 3 });
    items.close();
    expect(await drain(exec.patches)).toEqual([
      { path: ['items'], items: [{ v: 1 }], itemIndex: 0 },
      {
        path: ['items'],
        items: [null],
        itemIndex: 1,
        errors: [{ message: 'non-null field resolved to null', path: ['items', 1, 'v'] }],
      },
      { path: ['items'], items: [{ v: 3 }], itemIndex: 2 },
      { hasNext: false },
    ]);
  });

  it('rejects stream markers on non-list fields', async () => {
    const exec = execute([{ key: 'a', type: T, resolve: () => stream([1, 2]) }], {});
    const initial = await exec.initial;
    expect(initial.data).toEqual({ a: null });
    expect(initial.errors).toEqual([{ message: 'stream marker on a non-list field', path: ['a'] }]);
    expect(initial.hasNext).toBe(false);
  });
});

describe('cancellation', () => {
  it('supports client cancellation and drops late completions', async () => {
    const pending = manual<string>();
    const exec = execute([{ key: 'a', type: T, resolve: () => defer(pending.promise, 'a') }], {});
    expect((await exec.initial).hasNext).toBe(true);

    const drained = exec.cancel();
    expect((await exec.patches.next()).done).toBe(true);

    // The deferred promise settles after cancellation: no patch, no crash,
    // and cancel() resolves only once the late settlement has been drained.
    pending.resolve('late');
    await drained;
    expect((await exec.patches.next()).done).toBe(true);
  });

  it('aborts the execution signal so resolvers can cancel their own work', async () => {
    let observed: AbortSignal | undefined;
    const exec = execute(
      [
        {
          key: 'a',
          type: T,
          resolve: () =>
            defer((ctx) => {
              observed = ctx.signal;
              return new Promise<string>((_, reject) => {
                ctx.signal.addEventListener('abort', () => reject(new Error('aborted')));
              });
            }),
        },
      ],
      {},
    );
    await exec.initial;
    await exec.cancel();
    expect(observed?.aborted).toBe(true);
    expect((await exec.patches.next()).done).toBe(true);
  });

  it('cancels execution when the consumer stops iterating', async () => {
    const items = controllable<number>();
    const exec = execute([{ key: 'nums', type: listOf(T), resolve: () => stream(items.iterable) }], {});
    await exec.initial;
    items.push(1);
    const it = exec.patches;
    expect((await it.next()).value).toEqual({ path: ['nums'], items: [1], itemIndex: 0 });

    await it.return!();
    expect(items.isReturned()).toBe(true);
    expect((await it.next()).done).toBe(true);
  });

  it('never emits patches after the terminal event', async () => {
    const pending = manual<string>();
    const exec = execute([{ key: 'a', type: T, resolve: () => defer(pending.promise) }], {});
    await exec.initial;
    pending.resolve('A');
    const events = await drain(exec.patches);
    expect(events).toEqual([{ path: ['a'], data: 'A' }, { hasNext: false }]);
    // The iterator stays done forever.
    expect((await exec.patches.next()).done).toBe(true);
    expect((await exec.patches.next()).done).toBe(true);
  });
});

describe('backpressure', () => {
  it('buffers patches for slow consumers without losing or reordering them', async () => {
    const items = controllable<number>();
    const extra = manual<string>();
    const exec = execute(
      [
        { key: 'nums', type: listOf(T), resolve: () => stream(items.iterable) },
        { key: 'a', type: T, resolve: () => defer(extra.promise) },
      ],
      {},
    );
    await exec.initial;
    // Produce everything while nobody is consuming.
    items.push(1);
    items.push(2);
    items.push(3);
    items.close();
    extra.resolve('A');
    await new Promise((resolve) => setTimeout(resolve, 20));

    const events = await drain(exec.patches);
    const patches = onlyPatches(events);
    const streamPatches = patches.filter((patch) => patch.path.length === 1 && patch.path[0] === 'nums');
    expect(streamPatches.map((patch) => patch.itemIndex)).toEqual([0, 1, 2]);
    expect(streamPatches.map((patch) => patch.items)).toEqual([[1], [2], [3]]);
    expect(patches.some((patch) => 'data' in patch && patch.data === 'A')).toBe(true);
    expect(events.at(-1)).toEqual({ hasNext: false });
  });
});
