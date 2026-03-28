import binding, { NativeProducer } from './binding';
import { LogLevel, Status } from './constants';

const START_OR_SHUTDOWN = Symbol('RocketMQProducer#startOrShutdown');
const DRAIN_PENDING_SENDS = Symbol('RocketMQProducer#drainPendingSends');

interface PendingSendSettler {
  reject: (err: Error) => void;
  settled: boolean;
}

export enum SendResultStatus {
  OK = 0,
  FLUSH_DISK_TIMEOUT = 1,
  FLUSH_SLAVE_TIMEOUT = 2,
  SLAVE_NOT_AVAILABLE = 3,
}

const SEND_RESULT_STATUS_STR: Record<number, string> = {
  0: 'OK',
  1: 'FLUSH_DISK_TIMEOUT',
  2: 'FLUSH_SLAVE_TIMEOUT',
  3: 'SLAVE_NOT_AVAILABLE'
};


export interface ProducerOptions {
  nameServer?: string;
  groupName?: string;
  maxMessageSize?: number;
  compressLevel?: number;
  sendMessageTimeout?: number;
  logLevel?: LogLevel | keyof typeof LogLevel;
  logDir?: string;
  logFileSize?: number;
  logFileNum?: number;
}

export interface SendOptions {
  tags?: string;
  keys?: string;
}

export interface SendResult {
  status: number;
  statusStr: string;
  msgId: string;
  offset: number;
}

type Callback<T = void> = (err?: Error | null, result?: T) => void;

let producerRef = 0;
let timer: NodeJS.Timeout | undefined;
const keepAlive = (): void => {};

export class RocketMQProducer {
  public core: NativeProducer;
  status: Status;
  private operationQueue: Promise<void>;
  private pendingOperations = 0;
  private pendingSendSettlers = new Set<PendingSendSettler>();
  private shutdownCompleted = false;
  private pendingShutdownPromise: Promise<void> | null = null;

  /**
   * RocketMQ Producer constructor
   * @param groupId the group id
   * @param instanceName the instance name
   * @param options the options
   */
  constructor(groupId: string, instanceName?: string | ProducerOptions, options?: ProducerOptions) {
    let actualInstanceName: string | null = null;
    let actualOptions: ProducerOptions;

    if (typeof instanceName !== 'string') {
      actualOptions = instanceName || {};
    } else {
      actualInstanceName = instanceName;
      actualOptions = options || {};
    }

    if (actualOptions.logLevel && typeof actualOptions.logLevel === 'string') {
      actualOptions.logLevel = LogLevel[actualOptions.logLevel.toUpperCase() as keyof typeof LogLevel] ?? LogLevel.INFO;
    }

    this.core = new binding.Producer(groupId, actualInstanceName, actualOptions);
    this.status = Status.STOPPED;
    this.operationQueue = Promise.resolve();
  }

  /**
   * Set session credentials (usually used in Alibaba MQ)
   * @param accessKey the access key
   * @param secretKey the secret key
   * @param onsChannel the ons channel
   * @return the result
   */
  setSessionCredentials(accessKey: string, secretKey: string, onsChannel: string): boolean {
    if (typeof accessKey !== 'string') throw new TypeError('accessKey must be a string');
    if (typeof secretKey !== 'string') throw new TypeError('secretKey must be a string');
    if (typeof onsChannel !== 'string') throw new TypeError('onsChannel must be a string');

    this.core.setSessionCredentials(accessKey, secretKey, onsChannel);
    return true;
  }

  private getStatusName(status: Status = this.status): string {
    switch (status) {
    case Status.STOPPED: return 'STOPPED';
    case Status.STARTED: return 'STARTED';
    case Status.STARTING: return 'STARTING';
    case Status.STOPPING: return 'STOPPING';
    default: return 'UNKNOWN';
    }
  }

  private [START_OR_SHUTDOWN](method: 'start' | 'shutdown'): Promise<void>;
  private [START_OR_SHUTDOWN](method: 'start' | 'shutdown', callback: Callback): void;
  private [START_OR_SHUTDOWN](method: 'start' | 'shutdown', callback?: Callback): void | Promise<void> {
    let promise: Promise<void> | undefined;
    let resolve!: (value?: void) => void;
    let reject!: (err: Error) => void;
    let initiated = false;
    const queueBusy = this.pendingOperations > 0;

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
          reject(new Error('Producer cannot be restarted after shutdown'));
          return promise;
        }
        if (this.status === Status.STARTED) {
          reject(new Error('Producer is already started'));
          return promise;
        }
        if (this.status === Status.STARTING) {
          reject(new Error('Producer is already starting'));
          return promise;
        }
        if (this.status === Status.STOPPING) {
          reject(new Error('Producer is stopping, please wait for shutdown to complete'));
          return promise;
        }
        if (this.status === Status.STOPPED) {
          this.status = Status.STARTING;
          initiated = true;
        }
      } else {
        if (this.status === Status.STOPPED) {
          reject(new Error('Producer is already stopped'));
          return promise;
        }
        if (this.status === Status.STARTING) {
          reject(new Error('Producer is starting, please wait for start to complete'));
          return promise;
        }
        if (this.status === Status.STOPPING) {
          initiated = true;
        }
        if (this.status === Status.STARTED) {
          this.status = Status.STOPPING;
          initiated = true;
        }
      }
    }

    // 将操作加入队列，确保串行执行
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
              statusError = new Error('Producer cannot be restarted after shutdown');
            } else if (this.status === Status.STARTED) {
              statusError = new Error('Producer is already started');
            } else if (this.status === Status.STOPPING) {
              statusError = new Error('Producer is stopping, please wait for shutdown to complete');
            } else {
              if (this.status === Status.STOPPED) {
                this.status = Status.STARTING;
                initiated = true;
              }
            }
          } else {
            if (this.status === Status.STOPPED) {
              statusError = new Error('Producer is already stopped');
            } else if (this.status === Status.STOPPING && !initiated) {
              initiated = true;
            } else if (this.status === Status.STARTING) {
              statusError = new Error('Producer is starting, please wait for start to complete');
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

          const finishError = (error: Error): void => {
            if (settled) return;
            settled = true;
            if (method === 'start') {
              this.status = Status.STOPPED;
              this.shutdownCompleted = true;
            } else {
              this.status = Status.STOPPED;
              this.shutdownCompleted = true;
              producerRef--;
              if (!producerRef && timer) {
                clearInterval(timer);
                timer = undefined;
              }
              this[DRAIN_PENDING_SENDS]();
            }
            finalizeQueue();
            reject(error);
          };

          const finishSuccess = (): void => {
            if (settled) return;
            settled = true;
            try {
              if (method === 'start') {
                this.status = Status.STARTED;
                if (!producerRef) {
                  keepAlive();
                  timer = setInterval(keepAlive, 24 * 3600 * 1000);
                }
                producerRef++;
              } else {
                this.status = Status.STOPPED;
                this.shutdownCompleted = true;
                producerRef--;
                if (!producerRef && timer) {
                  clearInterval(timer);
                  timer = undefined;
                }
              }
            } finally {
              if (method === 'shutdown') {
                this[DRAIN_PENDING_SENDS]();
              }
              finalizeQueue();
              resolve();
            }
          };

          try {
            this.core[method]((err) => {
              if (err) {
                finishError(err);
                return;
              }
              finishSuccess();
            });
          } catch (err) {
            finishError(err instanceof Error ? err : new Error(String(err)));
          }
        });
      })
      .catch((err) => {
        try {
          process.emitWarning(String(err), 'RocketMQ');
        } catch (_) {
          // ignore
        }
      }); // 防止队列中断

    return promise;
  }

  /**
   * Start the producer
   * @param callback the callback function
   * @return returns a Promise if no callback
   */
  start(): Promise<void>;
  start(callback: Callback): void;
  start(callback?: Callback): void | Promise<void> {
    return this[START_OR_SHUTDOWN]('start', callback as any);
  }

  /**
   * Shutdown the producer
   * @param callback the callback function
   * @return returns a Promise if no callback
   */
  shutdown(): Promise<void>;
  shutdown(callback: Callback): void;
  shutdown(callback?: Callback): void | Promise<void> {
    if (!callback) {
      if (!this.pendingShutdownPromise) {
        this.pendingShutdownPromise = this[START_OR_SHUTDOWN]('shutdown') as Promise<void>;
        this.pendingShutdownPromise.then(
          () => { this.pendingShutdownPromise = null; },
          () => { this.pendingShutdownPromise = null; }
        );
      }
      return this.pendingShutdownPromise;
    }
    return this[START_OR_SHUTDOWN]('shutdown', callback);
  }

  /**
   * Send a message
   * @param topic the topic
   * @param body the body
   * @param options the options
   * @param callback the callback function
   * @return returns a Promise if no callback
   */
  send(topic: string, body: string | Buffer, options?: SendOptions): Promise<SendResult>;
  send(topic: string, body: string | Buffer, callback: Callback<SendResult>): void;
  send(topic: string, body: string | Buffer, options: SendOptions, callback: Callback<SendResult>): void;
  send(
    topic: string,
    body: string | Buffer,
    options?: SendOptions | Callback<SendResult>,
    callback?: Callback<SendResult>
  ): void | Promise<SendResult> {
    if (typeof topic !== 'string') throw new TypeError('topic must be a string');
    if (typeof body !== 'string' && !Buffer.isBuffer(body)) {
      throw new TypeError('body must be a string or Buffer');
    }

    let actualOptions: SendOptions = {};
    let actualCallback: Callback<SendResult> | undefined;

    if (typeof options === 'function') {
      actualCallback = options;
    } else {
      actualOptions = options || {};
      actualCallback = callback;
    }

    // 检查 Producer 状态
    if (this.status !== Status.STARTED) {
      const err = new Error(`Producer must be started before sending messages. Current status: ${this.getStatusName()}`);
      if (actualCallback) {
        queueMicrotask(() => actualCallback(err));
        return;
      }
      return Promise.reject(err);
    }

    if (!body.length) {
      const ret: SendResult = { status: -1, statusStr: 'EMPTY_BODY', msgId: '', offset: 0 };
      if (actualCallback) {
        queueMicrotask(() => actualCallback(null, ret));
        return;
      }
      return Promise.resolve(ret);
    }

    let promise: Promise<SendResult> | undefined;
    let resolve!: (value: SendResult) => void;
    let reject!: (err: Error) => void;

    if (!actualCallback) {
      promise = new Promise<SendResult>((_resolve, _reject) => {
        resolve = _resolve;
        reject = _reject;
      });
    } else {
      resolve = (result: SendResult) => actualCallback(null, result);
      reject = (err: Error) => { queueMicrotask(() => actualCallback(err)); };
    }

    const settler: PendingSendSettler = { reject, settled: false };
    this.pendingSendSettlers.add(settler);

    try {
      this.core.send(topic, body, actualOptions, (err, status, msgId, offset) => {
        if (settler.settled) return;
        settler.settled = true;
        this.pendingSendSettlers.delete(settler);

        if (err) {
          reject(err);
          return;
        }

        const ret: SendResult = {
          status: status || 0,
          statusStr: SEND_RESULT_STATUS_STR[status || 0] || 'UNKNOWN',
          msgId: msgId || '',
          offset: offset || 0
        };
        resolve(ret);
      });
    } catch (err) {
      settler.settled = true;
      this.pendingSendSettlers.delete(settler);
      reject(err instanceof Error ? err : new Error(String(err)));
    }

    return promise;
  }

  static SEND_RESULT = SendResultStatus;

  private [DRAIN_PENDING_SENDS](): void {
    const cancelErr = new Error('Send cancelled: producer shutdown timeout');
    const callbackErrors: Error[] = [];
    for (const settler of this.pendingSendSettlers) {
      if (!settler.settled) {
        settler.settled = true;
        try {
          settler.reject(cancelErr);
        } catch (e) {
          callbackErrors.push(e instanceof Error ? e : new Error(String(e)));
        }
      }
    }
    this.pendingSendSettlers.clear();
    if (callbackErrors.length) {
      for (const e of callbackErrors) {
        queueMicrotask(() => { throw e; });
      }
    }
  }
}

export default RocketMQProducer;

// CommonJS compatibility
// module.exports = RocketMQProducer;
// module.exports.default = RocketMQProducer;
