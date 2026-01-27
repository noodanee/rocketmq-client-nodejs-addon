import binding, { NativeProducer } from './binding';
import { LogLevel, Status } from './constants';

const START_OR_SHUTDOWN = Symbol('RocketMQProducer#startOrShutdown');

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

export class RocketMQProducer {
  public core: NativeProducer;
  status: Status;
  private operationQueue: Promise<void>;

  /**
   * RocketMQ Producer constructor
   * @param groupId the group id
   * @param instanceName the instance name
   * @param options the options
   */
  constructor(groupId: string, instanceName?: string | ProducerOptions, options?: ProducerOptions) {
    let actualInstanceName: string | null = null;
    let actualOptions: ProducerOptions = {};

    if (typeof instanceName !== 'string') {
      actualOptions = instanceName || {};
    } else {
      actualInstanceName = instanceName;
      actualOptions = options || {};
    }

    if (actualOptions.logLevel && typeof actualOptions.logLevel === 'string') {
      actualOptions.logLevel = LogLevel[actualOptions.logLevel.toUpperCase() as keyof typeof LogLevel] || LogLevel.INFO;
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
    let resolve: (value?: void) => void;
    let reject: (err: Error) => void;

    if (!callback) {
      promise = new Promise<void>((_resolve, _reject) => {
        resolve = _resolve;
        reject = _reject;
      });
    } else {
      resolve = () => callback(null);
      reject = callback;
    }

    // 将操作加入队列，确保串行执行
    this.operationQueue = this.operationQueue
      .then(() => {
        return new Promise<void>((queueResolve) => {
          // 在队列内部进行状态检查和更新，确保原子性
          if (method === 'start') {
            if (this.status === Status.STARTING) {
              const err = new Error('Producer is already starting');
              queueResolve();
              return reject(err);
            }
            if (this.status === Status.STARTED) {
              const err = new Error('Producer is already started');
              queueResolve();
              return reject(err);
            }
            if (this.status === Status.STOPPING) {
              const err = new Error('Producer is stopping, please wait for shutdown to complete');
              queueResolve();
              return reject(err);
            }
            // 设置中间状态
            this.status = Status.STARTING;
          } else { // shutdown
            if (this.status === Status.STOPPED) {
              const err = new Error('Producer is already stopped');
              queueResolve();
              return reject(err);
            }
            if (this.status === Status.STOPPING) {
              const err = new Error('Producer is already stopping');
              queueResolve();
              return reject(err);
            }
            if (this.status === Status.STARTING) {
              const err = new Error('Producer is starting, please wait for start to complete');
              queueResolve();
              return reject(err);
            }
            // 设置中间状态
            this.status = Status.STOPPING;
          }

          this.core[method]((err) => {
            if (err) {
              // 回滚状态
              if (method === 'start') {
                this.status = Status.STOPPED;
              } else {
                this.status = Status.STARTED;
              }
              queueResolve();
              return reject(err);
            }

            if (method === 'start') {
              this.status = Status.STARTED;
              if (!producerRef) timer = setInterval(() => {}, 24 * 3600 * 1000);
              producerRef++;
            } else {
              this.status = Status.STOPPED;
              producerRef--;
              if (!producerRef && timer) {
                clearInterval(timer);
                timer = undefined;
              }
            }

            queueResolve();
            resolve();
          });
        });
      })
      .catch((err) => {
        if (process.env.NODE_ENV !== 'production') {
          console.error('[RocketMQ] Operation queue error:', err);
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
    return this[START_OR_SHUTDOWN]('shutdown', callback as any);
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
      const statusName =
        this.status === Status.STOPPED
          ? 'STOPPED'
          : this.status === Status.STARTING
            ? 'STARTING'
            : 'STOPPING';
      const err = new Error(`Producer must be started before sending messages. Current status: ${statusName}`);
      return actualCallback ? actualCallback(err) : Promise.reject(err);
    }

    if (!body.length) {
      const ret: SendResult = { status: -1, statusStr: 'EMPTY_BODY', msgId: '', offset: 0 };
      return actualCallback ? actualCallback(null, ret) : Promise.resolve(ret);
    }

    let promise: Promise<SendResult> | undefined;
    let resolve: (value: SendResult) => void;
    let reject: (err: Error) => void;

    if (!actualCallback) {
      promise = new Promise<SendResult>((_resolve, _reject) => {
        resolve = _resolve;
        reject = _reject;
      });
    } else {
      resolve = (result: SendResult) => actualCallback(null, result);
      reject = actualCallback;
    }

    this.core.send(topic, body, actualOptions, (err, status, msgId, offset) => {
      if (err) {
        return reject(err);
      }

      const ret: SendResult = {
        status: status || 0,
        statusStr: SEND_RESULT_STATUS_STR[status || 0] || 'UNKNOWN',
        msgId: msgId || '',
        offset: offset || 0
      };
      resolve(ret);
    });

    return promise;
  }

  static SEND_RESULT = SendResultStatus;
}

export default RocketMQProducer;

// CommonJS compatibility
// module.exports = RocketMQProducer;
// module.exports.default = RocketMQProducer;
