import { config } from './common';
import { PushConsumer, Message, ConsumerAck } from '../src';

void function () {
  const consumer = new PushConsumer('GID_GROUP', {
    nameServer: config.nameServer
  });

  // consumer.setSessionCredentials('accessKey', 'secretKey', 'ALIYUN');

  consumer.subscribe('TP_TOPIC', '*');
  consumer.on('message', function(msg: Message, ack: ConsumerAck) {
    console.log(msg);
    ack.done(true);
  });

  consumer.start(function() {
    console.log('consumer started');
  });
}();
