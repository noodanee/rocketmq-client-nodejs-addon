import * as readline from 'readline';
import { config } from './common';
import { Producer } from '../src';

if (typeof (Promise as any).withResolvers === 'undefined') {
  (Promise as any).withResolvers = function () {
    let resolve: ((value?: any) => void) | null = null;
    let reject: ((reason?: any) => void) | null = null;
    const promise = new Promise((res, rej) => {
      resolve = res;
      reject = rej;
    });
    return { promise, resolve, reject };
  };
}

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});

void async function () {
  const producer = new Producer('GID_GROUP', {
    nameServer: config.nameServer
  });

  // producer.setSessionCredentials('accessKey', 'secretKey', 'ALIYUN')

  await producer.start();

  while(1) {
    const { promise, resolve, reject } = (Promise as any).withResolvers();
    rl.question('Enter input: ', async (input: string) => {
      await producer.send('TP_TOPIC', input).catch(reject);
      resolve();
    });
    await promise;
  }

  // producer.start(async function() {
  //     console.log('producer started');
  //     while(1) {
  //         const { promise, resolve, reject } = Promise.withResolvers()
  //         rl.question('Enter input: ', input => {
  //             producer.send('TP_TOPIC', input, function(err, ret) {
  //                 if (err) {
  //                     console.error(err);
  //                     reject(err)
  //                 }
  //                 console.log(ret);
  //                 resolve(ret)
  //             });
  //         });
  //         await promise.catch(console.error);
  //     }
  // });

}();
