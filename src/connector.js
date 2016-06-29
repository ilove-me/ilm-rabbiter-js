var RabbiterError = require('./rabbiterError.js');

var logger = require('winston');
var Amqp = require('amqplib');
var _ = require('lodash');
var PromiseBlue = require('bluebird');

var Publisher = null; //postpone this to connection create
var Callbacks = null;

// The normal priority is on the middle, to ensure one can increase/decrease
// priorities in future scenarios
var PRIORITY = {
  NORMAL: 5,
  LOW: 2,
  HIGH: 8
};

class Connector {
  constructor() {

    this.options = {
      connection: {
        user: process.env.RABBIT_USER,
        pass: process.env.RABBIT_PASS,
        host: process.env.RABBIT_HOST,
        port: process.env.RABBIT_PORT
      },
      queueName: process.env.RABBIT_QUEUE_TYPE || 'all',
      exchangeName: process.env.RABBIT_EXCHANGE || '',
      prefetch: 0,
      durable: false,
      autoDelete: true,
      name: process.env.SERVICE_NAME
    };
    this.exchange = null;
    this.channel = null;
    this.randomNumber = Math.round(Math.random() * 10000000000);
  }

  getConnectionUri() {
    var auth = this.options.connection;
    return 'amqp://' + encodeURIComponent(auth.user) + ':' + encodeURIComponent(auth.pass) + '@' + auth.host + ':' + auth.port;
  }


  getQueueName(priority) {
    return this.options.name + '_' + {5: 'normal', 2: 'low', 8: 'high'}[priority];
  }

  getResponseQueueName() {
    return this.options.name + '_' + this.randomNumber.toString();
  }


  amqpServerConnect(url, retries) {

    var self = this;

    logger.info('Connecting to RabbitMQ Server');
    return Amqp.connect(url)
      .then(function (conn) {
        if (conn) {
          logger.info('Successfully connected to RabbitMQ Server');
          return conn;
        }

        PromiseBlue.reject('');

      })
      .catch(function () {
        logger.info('RabbitMQ Server Connection failed');
        return new PromiseBlue(function (resolve) {
          setTimeout(function () {
            resolve(self.amqpServerConnect(url, (retries || 0) + 1));
          }, 5000);

        });
      });
  }

  getQueues() {
    var normalQueue = {
      name: this.getQueueName(PRIORITY.NORMAL),
      priority: PRIORITY.NORMAL
    };

    var highQueue = {
      name: this.getQueueName(PRIORITY.HIGH),
      priority: PRIORITY.HIGH
    };

    var lowQueue = {
      name: this.getQueueName(PRIORITY.LOW),
      priority: PRIORITY.LOW
    };

    var responseQueue = {
      name: this.getResponseQueueName(),
      priority: PRIORITY.HIGH
    };

    var queues = [normalQueue, highQueue, lowQueue, responseQueue];

    return queues;
  }

  createConnection(opts) {
    if (this.exchange) {
      return PromiseBlue.resolve(this);
    }

    Publisher = require('./publisher.js');
    Callbacks = require('./callbacks.js');

    _.extend(this.options, opts);

    var amqpUri = this.getConnectionUri();
    return this.amqpServerConnect(amqpUri)
      .then(conn => conn.createChannel().then(ch => {
          this.channel = ch;
          return ch.prefetch(this.options.prefetch || 0);
        })
      ).then(() => {
        var queues = this.getQueues();

        // Check which priority queues need to be created => NOT USED FOR NOW
        // THIS IS COOL TO USE WHEN WE NEED DIFFERENT PRIORITY QUEUES

        return PromiseBlue.map(queues, queue => {
          var name = queue.name;
          var priority = queue.priority;

          return [
            this.channel.assertQueue(name, {
              durable: this.options.durable,
              autoDelete: this.options.autoDelete
            }),
            this.channel.consume(name, this.receiverDispatcher.bind(this), { //set the queue consumer
              noAck: false,
              priority: priority
            }),
            PromiseBlue.resolve() //bind the exchange
              .then(() => this.channel.assertExchange(this.options.exchangeName, 'direct', { durable: this.options.durable }))
              .then(() => this.channel.bindQueue(name, this.options.exchangeName, name))
          ];
        }).all();

      });
  }


  receiverDispatcher(rabbitMsg) {

    if (!rabbitMsg) {
      return PromiseBlue.reject(new RabbiterError('Rabbit Message is Null'));
    }

    var self = this,
      messageProperties = rabbitMsg.properties,
      messageId = messageProperties.messageId,
      correlationId = messageProperties.correlationId;

    return PromiseBlue.bind(this)
      .then(function () {

        logger.info('%s [Received <-] %s %s', this.options.queueName, correlationId || 'no correlation id');

        //get callback function by message id
        var callbackFunction = Callbacks.removeSent(correlationId);


        if (!callbackFunction) {
          callbackFunction = Callbacks.removeMessage(messageId);
        }

        if (!callbackFunction) {
          //return Promise.reject(new VError("Non existing callback for keys: %s | %s", messageId, correlationId));
          return PromiseBlue.resolve(false);
        }


        //execute callback and send response if there is a queue and id to respond to
        return PromiseBlue.resolve(callbackFunction(JSON.parse(rabbitMsg.content)))
          .then(function(msg){
            return Publisher.respondSuccess(msg, messageProperties);
          })
          .catch(function(err){
            return Publisher.respondError(err, messageProperties);
          });
      })
      .catch(function (err) {
          return Publisher.respondError(new RabbiterError('RABBITER CONNECTOR - CallbackDispatcher :' + err.message), messageProperties);
      })
      .finally(function () {
        // acknowledge the message
        self.channel.ack(rabbitMsg);

        // clean the binding after the response is sent
        //this.channel.unbindQueue(originalQueue, this.options.exchangeName, originalMessageId);
      });
  }
}

module.exports = new Connector();
