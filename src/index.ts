import type { TypeRef } from './types.js';

export type { TypeRef } from './types.js';

export async function completeValue(type:TypeRef,value:unknown):Promise<unknown>{if(type.kind==='nonNull'){const result=await completeValue(type.ofType!,value);if(result==null)throw new Error('non-null field resolved to null');return result}if(value==null)return null;if(type.kind==='list'){const values=Array.isArray(value)?value:[value];return Promise.all(values.map(item=>completeValue(type.ofType!,item).catch(()=>null)))}return value}

export type {
  Path,
  ExecutionError,
  ExecContext,
  InitialResult,
  IncrementalPatch,
  TerminalEvent,
  PatchEvent,
  IncrementalExecution,
} from './types.js';
export { isTerminal } from './types.js';
export { defer, stream, isDeferred, isStream } from './markers.js';
export type { DeferredValue, StreamValue, DeferredSource } from './markers.js';
export { execute } from './executor.js';
export type { FieldNode } from './executor.js';
