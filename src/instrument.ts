/*!
 * Copyright 2024 Google LLC. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {
  SEMATTRS_DB_NAME,
  SEMATTRS_DB_STATEMENT,
  SEMATTRS_DB_SYSTEM,
  SEMATTRS_DB_SQL_TABLE,
} from '@opentelemetry/semantic-conventions';

import {
  Span,
  SpanStatusCode,
  trace,
  INVALID_SPAN_CONTEXT,
  SpanAttributes,
  TimeInput,
  TracerProvider,
  Link,
  Exception,
  SpanContext,
  SpanStatus,
  SpanKind,
} from '@opentelemetry/api';

import {EventEmitter} from 'events';
import {finished} from 'stream';
import {PartialResultStream} from './partial-result-stream';

const optedInPII: boolean =
  process.env.SPANNER_ENABLE_EXTENDED_TRACING === 'true';

interface SQLStatement {
  sql: string;
}

interface observabilityOptions {
  tracerProvider: TracerProvider;
  enableExtendedTracing: boolean;
}

export type {observabilityOptions as ObservabilityOptions};

const TRACER_NAME = 'cloud.google.com/nodejs/spanner';
const TRACER_VERSION = '7.14.0'; // Manually hard coded, TODO: remove

/**
 * getTracer fetches the tracer from the provided tracerProvider.
 * @param {TracerProvider} [tracerProvider] optional custom tracer provider
 * to use for fetching the tracer. If not provided, the global provider will be used.
 *
 * @returns {Tracer} The tracer instance associated with the provided or global provider.
 */
export function getTracer(tracerProvider?: TracerProvider) {
  if (tracerProvider) {
    return tracerProvider.getTracer(TRACER_NAME, TRACER_VERSION);
  }
  // Otherwise use the global tracer.
  return trace.getTracer(TRACER_NAME, TRACER_VERSION);
}

interface traceConfig {
  sql?: string | SQLStatement;
  tableName?: string;
  dbName?: string;
  opts?: observabilityOptions;
  that?: Object;
}

const SPAN_NAMESPACE_PREFIX = 'CloudSpanner'; // TODO: discuss & standardize this prefix.
export {SPAN_NAMESPACE_PREFIX};

/**
 * startTrace begins an active span in the current active context
 * and passes it back to the set callback function. Each span will
 * be prefixed with "cloud.google.com/nodejs/spanner". It is the
 * responsibility of the caller to invoke [span.end] when finished tracing.
 *
 * @returns {Span} The created span.
 */
export function startTrace<T>(
  spanNameSuffix: string,
  config: traceConfig | undefined,
  cb: (span: Span) => T
): T {
  const origConfig = config;
  if (!config) {
    config = {} as traceConfig;
  }

  return getTracer(config.opts?.tracerProvider).startActiveSpan(
    SPAN_NAMESPACE_PREFIX + '.' + spanNameSuffix,
    {kind: SpanKind.CLIENT},
    span => {
      span.setAttribute(SEMATTRS_DB_SYSTEM, 'spanner');

      if (config.tableName) {
        span.setAttribute(SEMATTRS_DB_SQL_TABLE, config.tableName);
      }
      if (config.dbName) {
        span.setAttribute(SEMATTRS_DB_NAME, config.dbName);
      }

      const allowExtendedTracing =
        optedInPII || config.opts?.enableExtendedTracing;
      if (config.sql && allowExtendedTracing) {
        const sql = config.sql;
        if (typeof sql === 'string') {
          span.setAttribute(SEMATTRS_DB_STATEMENT, sql as string);
        } else {
          const stmt = sql as SQLStatement;
          span.setAttribute(SEMATTRS_DB_STATEMENT, stmt.sql);
        }
      }

      if (config.that) {
        const fn = cb.bind(config.that);
        return fn(span);
      } else {
        return cb(span);
      }
    }
  );
}

/**
 * Sets the span status with err, if non-null onto the span with
 * status.code=ERROR and the message of err.toString()
 *
 * @returns {boolean} to signify if the status was set.
 */
export function setSpanError(span: Span, err: Error | String): boolean {
  if (!err || !span) {
    return false;
  }

  span.setStatus({
    code: SpanStatusCode.ERROR,
    message: err.toString(),
  });
  return true;
}

/**
 * Sets err, if non-null onto the span with
 * status.code=ERROR and the message of err.toString()
 * as well as recording an exception on the span.
 * @param {Span} [span] the subject span
 * @param {Error} [err] the error whose message to use to record
 * the span error and the span exception.
 *
 * @returns {boolean} to signify if the status and exception were set.
 */
export function setSpanErrorAndException(
  span: Span,
  err: Error | String
): boolean {
  if (setSpanError(span, err)) {
    span.recordException(err as Error);
    return true;
  }
  return false;
}

/**
 * getActiveOrNoopSpan queries the global tracer for the currently active
 * span and returns it, otherwise if there is no active span available, it'll
 * simply create a NoopSpan. This is important in the cases where we don't
 * want to create a new span, such as in sensitive and frequently called code
 * for which the new spans would be too many and thus pollute the trace,
 * but yet we'd like to record an important annotation.
 *
 * @returns {Span} the non-null span.
 */
export function getActiveOrNoopSpan(): Span {
  const span = trace.getActiveSpan();
  if (span) {
    return span;
  }
  return new noopSpan();
}

/**
 * noopSpan is a pass-through Span that does nothing and shall not
 * be exported, nor added into any context. It serves as a placeholder
 * to allow calls in sensitive areas like sessionPools to transparently
 * add attributes to spans without lots of ugly null checks.
 *
 * It exists because OpenTelemetry-JS does not seem to export the NoopSpan.
 */
class noopSpan implements Span {
  constructor() {}

  spanContext(): SpanContext {
    return INVALID_SPAN_CONTEXT;
  }

  setAttribute(key: string, value: unknown): this {
    return this;
  }

  setAttributes(attributes: SpanAttributes): this {
    return this;
  }

  addEvent(name: string, attributes?: SpanAttributes): this {
    return this;
  }

  addLink(link: Link): this {
    return this;
  }

  addLinks(links: Link[]): this {
    return this;
  }

  setStatus(status: SpanStatus): this {
    return this;
  }

  end(endTime?: TimeInput): void {}

  isRecording(): boolean {
    return false;
  }

  recordException(exc: Exception, timeAt?: TimeInput): void {}

  updateName(name: string): this {
    return this;
  }
}

interface TraceWrapped extends Function {
  traceWrapped?: boolean;
}

function performWrap(spanName: string, original: TraceWrapped) {
  /*
  original would be for example:
    const [rows] = await database.run('SELECT 1');
  or
    database.run('SELECT 1', (err, rows) => {
        ...
    });
  */
  if (original.traceWrapped) {
    return original;
  }

  const traced: TraceWrapped = function (
    this: any
  ):
    | void
    | Promise<unknown>
    | PartialResultStream
    | EventEmitter
    | AsyncIterable<unknown> {
    const lastArg = arguments[arguments.length - 1];
    const hasCallback = typeof lastArg === 'function';

    return startTrace(spanName, {}, span => {
      // Case 1. We have a callback function, plainly wrap it
      // with a passthrough function that starts the span on invocation
      // and then ends it once results are returned from the original.
      if (hasCallback) {
        const callback = lastArg;
        const wrappedFn = function (...args: any) {
          const errIndex = Array.from(arguments).findIndex(
            arg => arg instanceof Error
          );
          const err: Error = errIndex !== -1 ? arguments[errIndex] : null;
          setSpanError(span, err);
          span.end();
          callback(...args);
        };

        // Let's copy the name of the wrapping callback
        // to be that of the original callback function.
        Object.defineProperty(wrappedFn, 'name', {value: callback.name});
        arguments[arguments.length - 1] = wrappedFn;

        original.apply(this, arguments);
        return;
      }

      const originalResult = original.apply(this, arguments);
      // console.log('className', Object.entries(originalResult));

      // Case 2. We have a PartialResultStream, check the event emitters
      // and end the span once the finished event is issued.
      if (originalResult instanceof PartialResultStream) {
        const originalStream = originalResult as PartialResultStream;
        if (originalStream.on) {
          originalStream.on('error', err => {
            setSpanError(span, err);
          });
        }

        finished(originalStream, err => {
          if (err) {
            setSpanError(span, err);
          }
          span.end();
        });

        const eventNames = originalStream.eventNames();

        const closeRelatedEvents = [
          'close',
          'complete',
          'end',
          'finished',
          'prefinish',
        ];
        closeRelatedEvents.forEach(eventName => {
          originalStream.on(eventName, () => span.end());
        });
        return originalStream;
      }

      // Case 3. We have an EventEmitter so for each event that is related
      // to completion should be listened on, allow span ending.
      if (originalResult instanceof EventEmitter) {
        const originalStream = originalResult as EventEmitter;
        originalStream.on('error', err => {
          setSpanError(span, err);
        });
        const closeRelatedEvents = [
          'close',
          'complete',
          'end',
          'finished',
          'prefinish',
        ];
        closeRelatedEvents.forEach(eventName => {
          originalStream.on(eventName, () => span.end());
        });
        return originalStream;
      }

      // Case 4. We have a Promise and for that create a wrapping
      // Promise with a finally event that'll end the span..
      if (originalResult.constructor.name === 'Promise') {
        // console.log('as promise', spanName);
        // This is a promise
        const originalPromise = originalResult;
        // Extract the original promise then create a fresh one.
        // TODO: Retrieve the runtime type of original and bound it to the promise
        // type returnType = ReturnType<typeof original>;
        const passThroughPromise = new Promise((resolve, reject) => {
          originalPromise
            .then(result => {
              resolve(result);
            })
            .catch((err: Error) => {
              setSpanErrorAndException(span, err);
              reject(err);
            })
            .finally(() => {
              span.end();
            });
        });

        return passThroughPromise;
      }

      console.log(typeof originalResult);
      console.dir(originalResult, {depth: 100, colors: true, showHidden: true});
      // Case 5. We don't know how to handle this type, so throw an exception
      // complaining loudly so that we can handle it.
      throw new Error(
        `Unknown trace wrapper for ${originalResult.constructor.name}`
      );
    });
  };

  traced.traceWrapped = true;
  return traced;
}

export function traceWrap(klass: Function, methodNames: string[]) {
  const canWrap =
    klass && klass.prototype && methodNames && methodNames.length > 0;
  if (!canWrap) {
    return;
  }

  methodNames.forEach(methodName => {
    const original = klass.prototype[methodName];
    const spanName = klass.name + '.' + methodName;
    if (!original) {
      throw new Error(`Missing/mispelled method ${spanName}`);
    }

    // There are some return types for which we don't want
    // to trace for example AsyncIterable, for which we'd have.
    /*
    type returnType = ReturnType<typeof original>;
    if (returnType.prototype implements AsyncIterable<any>) {
        console.log('asyncIterable');
        return;
    }
    */

    klass.prototype[methodName] = performWrap(spanName, original);
  });
}
