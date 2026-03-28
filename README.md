# Apache RocketMQ Client for Node.js

Node.js client for [Apache RocketMQ](https://rocketmq.apache.org/), implemented as native addons.

## Installation

```bash
npm install rocketmq-nodejs
```

## Prerequisites

Node.js 18.17.0+ / 20.3.0+ / 22.14.0+

## Usage

### Producer

#### Constructor
```javascript
const { Producer } = require('rocketmq-nodejs');

// Basic usage
const producer = new Producer('GID_GROUP', {
    nameServer: '127.0.0.1:9876'
});

// With all options
const producer = new Producer('GID_GROUP', 'INSTANCE_NAME', {
    nameServer: '127.0.0.1:9876',
    groupName: 'GROUP_NAME',
    compressLevel: 5,          // 0-9, default 5
    sendMessageTimeout: 3000,  // ms, default 3000
    maxMessageSize: 131072,    // bytes, default 128KB
    logDir: '$HOME/logs/rocketmq',
    logFileNum: 3,
    logFileSize: 104857600,    // bytes, default 100MB
    logLevel: 'info'           // fatal|error|warn|info|debug|trace|num
});
```

#### Methods

##### start([callback])
```javascript
// With callback
producer.start((err) => {
    if (err) {
        console.error(err);
        return;
    }
    console.log('Producer started');
});

// With Promise
await producer.start();
```

##### send(topic, message, [options], [callback])
```javascript
// Basic send
producer.send('TP_TOPIC', 'Hello RocketMQ', (err, result) => {
    if (err) {
        console.error(err);
        return;
    }
    console.log(result);
    // Result example:
    // {
    //   status: 0,
    //   statusStr: 'OK',
    //   msgId: '0101007F0000367E0000309DD68B0700',
    //   offset: 0
    // }
});

// With options
producer.send('TP_TOPIC', 'Hello RocketMQ', {
    keys: 'key1',
    tags: 'TagA',
}, callback);

// With Promise
const result = await producer.send('TP_TOPIC', 'Hello RocketMQ');
```

Send Status Codes:
| `status` | `statusStr`         |
|----------|-----------------------|
| `0`      | `OK`                  |
| `1`      | `FLUSH_DISK_TIMEOUT`  |
| `2`      | `FLUSH_SLAVE_TIMEOUT` |
| `3`      | `SLAVE_NOT_AVAILABLE` |

##### shutdown([callback])
```javascript
// With callback
producer.shutdown((err) => {
    if (err) {
        console.error(err);
        return;
    }
    console.log('Producer shutdown');
});

// With Promise
await producer.shutdown();
```

> **Note:** Producer is a one-shot object. Once `shutdown()` is called, the instance cannot be restarted. Create a new `Producer` instance if you need to send messages again.

### PushConsumer

#### Constructor
```javascript
const { PushConsumer } = require('rocketmq-nodejs');

// Basic usage
const consumer = new PushConsumer('GID_GROUP', {
    nameServer: '127.0.0.1:9876'
});

// With all options
const consumer = new PushConsumer('GID_GROUP', 'INSTANCE_NAME', {
    nameServer: '127.0.0.1:9876',
    threadCount: 3,            // Number of consumer threads
    maxBatchSize: 32,         // Max batch size for consuming messages
    maxReconsumeTimes: 16,    // Max retries before the message is dropped (default 16)
    logDir: '$HOME/logs/rocketmq',
    logFileNum: 3,
    logFileSize: 104857600,    // bytes, default 100MB
    logLevel: 'info'           // fatal|error|warn|info|debug|trace|num
});
```

#### Methods

##### start([callback])
```javascript
// With callback
consumer.start((err) => {
    if (err) {
        console.error(err);
        return;
    }
    console.log('Consumer started');
});

// With Promise
await consumer.start();
```

##### subscribe(topic, [expression])
```javascript
// Subscribe to all messages
consumer.subscribe('TP_TOPIC', '*');

// Subscribe with expression
consumer.subscribe('TP_TOPIC', 'TagA');
```

##### Message Event Handler
```javascript
consumer.on('message', (msg, ack) => {
    console.log(msg);
    // Message example:
    // {
    //   topic: 'TopicTest',
    //   tags: 'TagA',
    //   keys: 'key1',
    //   body: 'Hello RocketMQ',
    //   msgId: '0101007F0000367E0000339DD68B0800'
    // }

    // Acknowledge success
    ack.done();
    
    // Or acknowledge failure
    // ack.done(false);
});
```

##### shutdown([callback])
```javascript
// With callback
consumer.shutdown((err) => {
    if (err) {
        console.error(err);
        return;
    }
    console.log('Consumer shutdown');
});

// With Promise
await consumer.shutdown();
```

> **Note:** PushConsumer is a one-shot object. Once `shutdown()` is called, the instance cannot be restarted. Create a new `PushConsumer` instance if you need to consume messages again.

### Aliyun RocketMQ

For Aliyun RocketMQ, additional configuration is required:

```javascript
// Producer
const producer = new Producer('GID_GROUP', {
    nameServer: 'onsaddr.cn-hangzhou.mq.aliyuncs.com:80'
});

producer.setSessionCredentials(
    'YOUR_ACCESS_KEY',      // Access Key ID
    'YOUR_SECRET_KEY',      // Access Key Secret
    'ALIYUN'               // Channel
);

// Consumer
const consumer = new PushConsumer('GID_GROUP', {
    nameServer: 'onsaddr.cn-hangzhou.mq.aliyuncs.com:80'
});

consumer.setSessionCredentials(
    'YOUR_ACCESS_KEY',      // Access Key ID
    'YOUR_SECRET_KEY',      // Access Key Secret
    'ALIYUN'               // Channel
);
```
