import { EventEmitter } from 'events';
import binding, { NativePushConsumer } from './binding';
import { LogLevel, Status } from './constants';

const START_OR_SHUTDOWN = Symbol('RocketMQPushConsumer#startOrShutdown');

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

  /**
   * RocketMQ PushConsumer constructor
   * @param groupId the group id
   * @param instanceName the instance name
   * @param options the options
   */
  constructor(groupId: string, instanceName?: string | PushConsumerOptions, options?: PushConsumerOptions) {
    super();

    let actualInstanceName: string | null = null;
    let actualOptions: PushConsumerOptions = {};

    if (typeof instanceName !== 'string') {
      actualOptions = instanceName || {};
    } else {
      actualInstanceName = instanceName;
      actualOptions = options || {};
    }

    if (actualOptions.logLevel && typeof actualOptions.logLevel === 'string') {
      actualOptions.logLevel = LogLevel[actualOptions.logLevel.toUpperCase() as keyof typeof LogLevel] || LogLevel.INFO;
    }

    this.core = new binding.PushConsumer(groupId, actualInstanceName, actualOptions);
    this.core.setListener((msg, ack) => {
      try {
        this.emit('message', msg, ack);
      } catch (err) {
        try {
          if (ack && typeof ack.done === 'function') {
            ack.done(false);
          }
        } catch (_) {
          // ignore
        }
        if (this.listenerCount('error') > 0) {
          try {
            this.emit('error', err as Error, msg, ack);
          } catch (_) {
            // ignore
          }
        }
      }
    });
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
              const err = new Error('Consumer is already starting');
              queueResolve();
              return reject(err);
            }
            if (this.status === Status.STARTED) {
              const err = new Error('Consumer is already started');
              queueResolve();
              return reject(err);
            }
            if (this.status === Status.STOPPING) {
              const err = new Error('Consumer is stopping, please wait for shutdown to complete');
              queueResolve();
              return reject(err);
            }
            // 设置中间状态
            this.status = Status.STARTING;
          } else { // shutdown
            if (this.status === Status.STOPPED) {
              const err = new Error('Consumer is already stopped');
              queueResolve();
              return reject(err);
            }
            if (this.status === Status.STOPPING) {
              const err = new Error('Consumer is already stopping');
              queueResolve();
              return reject(err);
            }
            if (this.status === Status.STARTING) {
              const err = new Error('Consumer is starting, please wait for start to complete');
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
              if (!consumerRef) timer = setInterval(() => {}, 24 * 3600 * 1000);
              consumerRef++;
            } else {
              this.status = Status.STOPPED;
              consumerRef--;
              if (!consumerRef && timer) {
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
   * Start the push consumer
   * @param callback the callback function
   * @return returns a Promise if no callback
   */
  start(): Promise<void>;
  start(callback: Callback): void;
  start(callback?: Callback): void | Promise<void> {
    return this[START_OR_SHUTDOWN]('start', callback as any);
  }

  /**
   * Shutdown the push consumer
   * @param callback the callback function
   * @return returns a Promise if no callback
   */
  shutdown(): Promise<void>;
  shutdown(callback: Callback): void;
  shutdown(callback?: Callback): void | Promise<void> {
    return this[START_OR_SHUTDOWN]('shutdown', callback as any);
  }

  /**
   * subscribe a topic
   * @param topic the topic to be subscribed
   * @param expression the additional expression to be subscribed
   * @return the subscribe status result
   */
  subscribe(topic: string, expression: string = ''): void {
    if (!topic || typeof topic !== 'string') {
      throw new Error('Topic must be a non-empty string');
    }
    if (expression && typeof expression !== 'string') {
      throw new Error('Expression must be a string if provided');
    }
    
    // C++ 代码只检查 destroyed 和 shutting_down 状态
    // 允许在 STOPPED 和 STARTED 状态下订阅
    if (this.status === Status.STOPPING) {
      throw new Error('Cannot subscribe while consumer is stopping');
    }
    
    this.core.subscribe(topic, expression);
  }
}

export default RocketMQPushConsumer;

// CommonJS compatibility
// module.exports = RocketMQPushConsumer;
// module.exports.default = RocketMQPushConsumer;
