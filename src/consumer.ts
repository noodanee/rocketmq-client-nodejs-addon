import { AsyncLocalStorage } from 'async_hooks';
import { EventEmitter } from 'events';
import binding, { NativePushConsumer } from './binding';
import { LogLevel, Status } from './constants';

const START_OR_SHUTDOWN = Symbol('RocketMQPushConsumer#startOrShutdown');

interface HandlerContext {
  consumer: RocketMQPushConsumer;
  acked: boolean;
}

export interface PushConsumerOptions {
  nameServer?: string;
  groupName?: string;
  threadCount?: number;
  maxBatchSize?: number;
  maxReconsumeTimes?: number;
  logLevel?: LogLevel | keyof typeof LogLevel;
  logDir?: string;
  logFileSize?: number;
  logFileNum?: number;
}

export interface Message {
  topic: string;
  tags: string;
  keys: string;
  body: string;
  msgId: string;
}

export interface ConsumerAck {
  done(success?: boolean): void;
}

type Callback<T = void> = (err?: Error | null, result?: T) => void;

let consumerRef = 0;
let timer: NodeJS.Timeout | undefined;
const keepAlive = (): void => {};

export interface RocketMQPushConsumerEvents {
  message: (msg: Message, ack: ConsumerAck) => void;
  error: (err: Error, msg?: Message, ack?: ConsumerAck) => void;
}

export declare interface RocketMQPushConsumer {
  on<K extends keyof RocketMQPushConsumerEvents>(
    event: K,
    listener: RocketMQPushConsumerEvents[K]
  ): this;
  emit<K extends keyof RocketMQPushConsumerEvents>(
    event: K,
    ...args: Parameters<RocketMQPushConsumerEvents[K]>
  ): boolean;
}

export class RocketMQPushConsumer extends EventEmitter {
  public core: NativePushConsumer;
  public status: Status;
  private operationQueue: Promise<void>;
  private pendingOperations = 0;
  private outstandingAcks = 0;
  private ackGeneration = 0;
  private shutdownCompleted = false;
  private handlerZone = new AsyncLocalStorage<HandlerContext>();
  private nativeListenerFn!: (msg: Message, ack: ConsumerAck) => void;

  on<K extends keyof RocketMQPushConsumerEvents>(
    event: K,
    listener: RocketMQPushConsumerEvents[K]
  ): this;
  on(event: string | symbol, listener: (...args: any[]) => void): this;
  on(event: string | symbol, listener: (...args: any[]) => void): this {
    return super.on(event, listener);
  }

  addListener<K extends keyof RocketMQPushConsumerEvents>(
    event: K,
    listener: RocketMQPushConsumerEvents[K]
  ): this;
  addListener(event: string | symbol, listener: (...args: any[]) => void): this;
  addListener(event: string | symbol, listener: (...args: any[]) => void): this {
    return this.on(event as any, listener);
  }

  once<K extends keyof RocketMQPushConsumerEvents>(
    event: K,
    listener: RocketMQPushConsumerEvents[K]
  ): this;
  once(event: string | symbol, listener: (...args: any[]) => void): this;
  once(event: string | symbol, listener: (...args: any[]) => void): this {
    return super.once(event, listener);
  }

  prependListener<K extends keyof RocketMQPushConsumerEvents>(
    event: K,
    listener: RocketMQPushConsumerEvents[K]
  ): this;
  prependListener(event: string | symbol, listener: (...args: any[]) => void): this;
  prependListener(event: string | symbol, listener: (...args: any[]) => void): this {
    return super.prependListener(event, listener);
  }

  prependOnceListener<K extends keyof RocketMQPushConsumerEvents>(
    event: K,
    listener: RocketMQPushConsumerEvents[K]
  ): this;
  prependOnceListener(event: string | symbol, listener: (...args: any[]) => void): this;
  prependOnceListener(event: string | symbol, listener: (...args: any[]) => void): this {
    return super.prependOnceListener(event, listener);
  }

  constructor(groupId: string, instanceName?: string | PushConsumerOptions, options?: PushConsumerOptions) {
    super();

    let actualInstanceName: string | null = null;
    let actualOptions: PushConsumerOptions;

    if (typeof instanceName !== 'string') {
      actualOptions = instanceName || {};
    } else {
      actualInstanceName = instanceName;
      actualOptions = options || {};
    }

    if (actualOptions.logLevel && typeof actualOptions.logLevel === 'string') {
      actualOptions.logLevel = LogLevel[actualOptions.logLevel.toUpperCase() as keyof typeof LogLevel] ?? LogLevel.INFO;
    }

    this.core = new binding.PushConsumer(groupId, actualInstanceName, actualOptions);
    this.nativeListenerFn = (msg, ack) => {
      const handlerCtx: HandlerContext = { consumer: this, acked: false };
      this.handlerZone.run(handlerCtx, () => {
        const gen = this.ackGeneration;
        this.outstandingAcks++;
        let acked = false;
        const wrappedAck = {
          done: (success?: boolean) => {
            if (acked) return;
            try {
              if (ack && typeof ack.done === 'function') {
                ack.done(success);
              }
            } finally {
              acked = true;
              handlerCtx.acked = true;
              if (gen === this.ackGeneration) {
                this.outstandingAcks--;
              }
            }
          }
        };
        const asyncPromises: Promise<unknown>[] = [];
        try {
          const listeners = this.rawListeners('message');
          if (listeners.length === 0) {
            wrappedAck.done(false);
            return;
          }
          for (const listener of listeners) {
            const ret = (listener as Function).call(this, msg, wrappedAck);
            if (ret && typeof (ret as any).catch === 'function') {
              asyncPromises.push(
                (ret as Promise<unknown>).catch((err: Error) => {
                  this.handleListenerError(err, msg, wrappedAck);
                })
              );
            }
          }
        } catch (err) {
          this.handleListenerError(err as Error, msg, wrappedAck);
        } finally {
          if (asyncPromises.length > 0) {
            Promise.all(asyncPromises).then(() => {});
          }
        }
      });
    };
    this.core.setListener(this.nativeListenerFn);
    this.status = Status.STOPPED;
    this.operationQueue = Promise.resolve();
  }

  setSessionCredentials(accessKey: string, secretKey: string, onsChannel: string): boolean {
    if (typeof accessKey !== 'string') throw new TypeError('accessKey must be a string');
    if (typeof secretKey !== 'string') throw new TypeError('secretKey must be a string');
    if (typeof onsChannel !== 'string') throw new TypeError('onsChannel must be a string');

    this.core.setSessionCredentials(accessKey, secretKey, onsChannel);
    return true;
  }

  private [START_OR_SHUTDOWN](method: 'start' | 'shutdown'): Promise<void>;
  private [START_OR_SHUTDOWN](method: 'start' | 'shutdown', callback: Callback): void;
  private [START_OR_SHUTDOWN](method: 'start' | 'shutdown', callback?: Callback): void | Promise<void> {
    let promise: Promise<void> | undefined;
    let resolve!: (value?: void) => void;
    let reject!: (err: Error) => void;
    let initiated = false;
    const queueBusy = this.pendingOperations > 0;

    const handlerCtx = method === 'shutdown' ? this.handlerZone.getStore() : undefined;
    const fromHandler = handlerCtx?.consumer === this;
    const callerAcked = handlerCtx?.acked === true;

    if (!callback) {
      promise = new Promise<void>((_resolve, _reject) => {
        resolve = _resolve;
        reject = _reject;
      });
    } else {
      resolve = () => callback(null);
      reject = (err: Error) => { queueMicrotask(() => callback(err)); };
    }

    if (!queueBusy) {
      if (method === 'start') {
        if (this.shutdownCompleted) {
          reject(new Error('Consumer cannot be restarted after shutdown'));
          return promise;
        }
        if (this.status === Status.STARTED) {
          reject(new Error('Consumer is already started'));
          return promise;
        }
        if (this.status === Status.STARTING) {
          reject(new Error('Consumer is already starting'));
          return promise;
        }
        if (this.status === Status.STOPPING) {
          reject(new Error('Consumer is stopping, please wait for shutdown to complete'));
          return promise;
        }
        if (this.status === Status.STOPPED) {
          this.status = Status.STARTING;
          initiated = true;
        }
      } else {
        if (this.status === Status.STOPPED) {
          reject(new Error('Consumer is already stopped'));
          return promise;
        }
        if (this.status === Status.STARTING) {
          reject(new Error('Consumer is starting, please wait for start to complete'));
          return promise;
        }
        if (this.status === Status.STARTED) {
          this.status = Status.STOPPING;
          initiated = true;
        }
      }
    }

    this.pendingOperations++;
    this.operationQueue = this.operationQueue
      .catch((err) => {
        try {
          process.emitWarning(String(err), 'RocketMQ');
        } catch (_) {
          // ignore
        }
      })
      .then(() => {
        return new Promise<void>((queueResolve) => {
          const finalizeQueue = (): void => {
            this.pendingOperations = Math.max(0, this.pendingOperations - 1);
            queueResolve();
          };
          let statusError: Error | undefined;
          if (method === 'start') {
            if (this.shutdownCompleted) {
              statusError = new Error('Consumer cannot be restarted after shutdown');
            } else if (this.status === Status.STARTED) {
              statusError = new Error('Consumer is already started');
            } else if (this.status === Status.STOPPING) {
              statusError = new Error('Consumer is stopping, please wait for shutdown to complete');
            } else if (this.status === Status.STARTING && !initiated) {
              statusError = new Error('Consumer is already starting');
            } else {
              if (this.status === Status.STOPPED) {
                this.status = Status.STARTING;
                initiated = true;
              }
            }
          } else {
            if (this.status === Status.STOPPED) {
              statusError = new Error('Consumer is already stopped');
            } else if (this.status === Status.STARTING) {
              statusError = new Error('Consumer is starting, please wait for start to complete');
            } else {
              if (this.status === Status.STARTED) {
                this.status = Status.STOPPING;
                initiated = true;
              }
            }
          }

          if (statusError) {
            finalizeQueue();
            reject(statusError);
            return;
          }

          let settled = false;

          const finishError = (err: Error): void => {
            if (settled) return;
            settled = true;
            if (method === 'start') {
              this.status = Status.STOPPED;
              this.shutdownCompleted = true;
            } else {
              this.ackGeneration++;
              this.outstandingAcks = 0;
              this.status = Status.STOPPED;
              this.shutdownCompleted = true;
              consumerRef--;
              if (!consumerRef && timer) {
                clearInterval(timer);
                timer = undefined;
              }
            }
            finalizeQueue();
            reject(err);
          };
          const finishSuccess = (): void => {
            if (settled) return;
            settled = true;
            if (method === 'start') {
              this.status = Status.STARTED;
              if (!consumerRef) {
                keepAlive();
                timer = setInterval(keepAlive, 24 * 3600 * 1000);
              }
              consumerRef++;
              finalizeQueue();
              resolve();
            } else {
              const exemptAcks = (fromHandler && !callerAcked) ? 1 : 0;
              const cleanupShutdown = (): void => {
                this.ackGeneration++;
                this.outstandingAcks = 0;
                this.shutdownCompleted = true;
                this.status = Status.STOPPED;
                consumerRef--;
                if (!consumerRef && timer) {
                  clearInterval(timer);
                  timer = undefined;
                }
                finalizeQueue();
              };
              this.waitForOutstandingAcks(30_000, exemptAcks).then(() => {
                cleanupShutdown();
                resolve();
              }).catch((ackErr: Error) => {
                cleanupShutdown();
                reject(ackErr);
              });
            }
          };

          try {
            if (method === 'start') {
              this.core.setListener(this.nativeListenerFn);
            }
            this.core[method]((err) => {
              if (err) {
                finishError(err);
                return;
              }
              finishSuccess();
            });
          } catch (error) {
            finishError(error instanceof Error ? error : new Error(String(error)));
          }
        });
      })
      .catch((err) => {
        try {
          process.emitWarning(String(err), 'RocketMQ');
        } catch (_) {
          // ignore
        }
      });

    return promise;
  }

  start(): Promise<void>;
  start(callback: Callback): void;
  start(callback?: Callback): void | Promise<void> {
    return this[START_OR_SHUTDOWN]('start', callback as any);
  }

  shutdown(): Promise<void>;
  shutdown(callback: Callback): void;
  shutdown(callback?: Callback): void | Promise<void> {
    return this[START_OR_SHUTDOWN]('shutdown', callback as any);
  }

  subscribe(topic: string, expression: string = ''): void {
    if (!topic || typeof topic !== 'string') {
      throw new Error('Topic must be a non-empty string');
    }
    if (arguments.length > 1 && expression != null && typeof expression !== 'string') {
      throw new Error('Expression must be a string if provided');
    }

    const actualExpression = expression == null ? '' : expression;

    if (this.status === Status.STOPPING) {
      throw new Error('Cannot subscribe while consumer is stopping');
    }

    this.core.subscribe(topic, actualExpression);
  }

  // Design: Follows Node.js EventEmitter error convention.
  // 1. Nack first (done(false)) so the broker can redeliver.
  // 2. Emit 'error' if listeners exist; if an error listener itself throws,
  //    log via console.error (non-fatal).
  // 3. No listeners → log via console.error (non-fatal).
  private handleListenerError(err: Error, msg: any, wrappedAck: ConsumerAck): void {
    try {
      wrappedAck.done(false);
    } catch (_) {
      // ignore
    }
    if (this.listenerCount('error') > 0) {
      try {
        this.emit('error', err, msg, wrappedAck);
      } catch (listenerErr) {
        console.error('[RocketMQ] error listener threw:', listenerErr);
      }
    } else {
      console.error('[RocketMQ] unhandled message listener error:', err);
    }
  }

  private waitForOutstandingAcks(timeout = 30_000, exemptAcks = 0): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const deadline = Date.now() + timeout;
      let delay = 5;
      const MAX_DELAY = 100;
      const check = (): void => {
        if (this.outstandingAcks <= exemptAcks) {
          resolve();
          return;
        }
        if (Date.now() > deadline) {
          reject(new Error(
            `Shutdown timed out: ${this.outstandingAcks - exemptAcks} outstanding ack(s) did not complete within ${timeout}ms`
          ));
          return;
        }
        setTimeout(check, delay);
        delay = Math.min(delay * 2, MAX_DELAY);
      };
      check();
    });
  }

}

export default RocketMQPushConsumer;
