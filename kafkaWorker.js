const Kafka = require('node-rdkafka');
const v4 = require('uuid/v4');
const kafkaHost = '111.111.111.42:9092';
const kafkaGroupId = `plotter-${v4()}`;
let consumer;

console.log(__dirname);
console.log(process.version);
console.log(process.versions);
console.log(process.argv);

startStream();

function startStream() {
  const partitionNumber = 5;
  const topicName = 'topic';
  consumer = Kafka.KafkaConsumer({
    'metadata.broker.list': kafkaHost,
    'rebalance_cb': function (err, assignment) {
      try {
        consumer.assign([{topic: topicName, partition: partitionNumber}]);
      } catch (e) {
        console.log(e);
      }
    },
    'group.id': kafkaGroupId,
    'socket.keepalive.enable': true,
    'enable.auto.commit': false,
    'debug': 'all'
  });

  consumer.on('ready', function (arg) {
    console.log('ready');
    consumer.subscribe([topicName]);
    consumer.consume();
  });

  consumer.on('data', function (message) {
    console.log(message);
  });

  consumer.on('event.error', (err) => {
    console.log(err);
  });

  consumer.on('error', (err) => {
    console.log(err);
  });

  consumer.on('disconnected', function (arg) {
    console.log('disconnect');
    process.exit();
  });

  consumer.connect();
}

process.on('uncaughtException', (err) => {
  console.log('uncaught exception in the process. Stop data stream...');
});

process.on('exit', function (code, signal) {
  console.log(code, signal);
});
