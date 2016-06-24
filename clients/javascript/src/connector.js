
var RabbiterError = require('./rabbiterError.js');

var logger = require('ilm-node-logger');
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

let connectorSingleton = Symbol();
let connectorEnforcer = Symbol();

class Connector {
  constructor(enforcer) {

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

    if (enforcer !== connectorEnforcer) {
      throw 'Cannot construct singleton';
    }

    this.init();
  }


  init() {

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

  createConnection(opts) {
    var self = this;

    if (this.exchange) {
      return PromiseBlue.resolve(self);
    }

    Publisher = require('./publisher.js');
    Callbacks = require('./callbacks.js');

    _.extend(this.options, opts);

    return this.amqpServerConnect(this.getConnectionUri())
      .then(function (conn) {

        return conn.createChannel()
          .then(function (ch) {
            self.channel = ch;

            return ch.prefetch(self.options.prefetch || 0);
          });
      })
      .then(function () {

        var normalQueue = {
            name: self.getQueueName(PRIORITY.NORMAL),
            priority: PRIORITY.NORMAL
          },
          highQueue = {
            name: self.getQueueName(PRIORITY.HIGH),
            priority: PRIORITY.HIGH
          },
          lowQueue = {
            name: self.getQueueName(PRIORITY.LOW),
            priority: PRIORITY.LOW
          },
          responseQueue = {
            name: self.getResponseQueueName(),
            priority: PRIORITY.HIGH
          },
          queues = [normalQueue, highQueue, lowQueue, responseQueue];

        // Check which priority queues need to be created => NOT USED FOR NOW
        // THIS IS COOL TO USE WHEN WE NEED DIFFERENT PRIORITY QUEUES


        return PromiseBlue.map(queues, function (queue) {
          var name = queue.name, priority = queue.priority;

          return [
            self.channel.assertQueue(name, {
              durable: self.options.durable,
              autoDelete: self.options.autoDelete
            }),
            self.channel.consume(name, _.partialRight(self.receiverDispatcher.bind(self)), { //set the queue consumer
              noAck: false,
              priority: priority
            }),
            PromiseBlue.resolve() //bind the exchange
              .then(function () {
                return self.channel.assertExchange(self.options.exchangeName, 'direct', {
                  durable: self.options.durable
                });
              })
              .then(function () {
                return self.channel.bindQueue(name, self.options.exchangeName, name);
              })
          ];
        }).all();

      });

  }


  receiverDispatcher(rabbitMsg) {

    if (!rabbitMsg) {
      return PromiseBlue.reject(RabbiterError.create('Rabbit Message is Null'));
    }

    var messageProperties = rabbitMsg.properties,
      messageId = messageProperties.messageId,
      correlationId = messageProperties.correlationId;

    return PromiseBlue.bind(this)
      .then(function () {

        logger.info('\n\n%s [Received <-] %s %s', this.options.queueName, correlationId || 'no correlation id');

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

        /*
         NEWRELIC STUFF

         function executeTheRabbitMQCallback() {
         return callbackFunction(msg[key]);
         }
         // we're the discarding the instrumentation of adhoc callbacks because
         // their key is random and that would result in useless traces
         var instrument = isRouting;

         // execute it
         var cb = instrument? newrelic.createBackgroundTransaction(key, 'RabbitMQ', executeTheRabbitMQCallback): executeTheRabbitMQCallback;
         return Promise.resolve(cb())
         .tap(function endNewrelicTransaction() {
         if (instrument) {
         newrelic.endTransaction();
         }
         })*/

      })
      .catch(function (err) {
          return Publisher.respondError(RabbiterError.create('RABBITER CONNECTOR - CallbackDispatcher :' + err.message), messageProperties);
      })
      .finally(function () {
        // acknowledge the message
        Connector.instance.channel.ack(rabbitMsg);

        // clean the binding after the response is sent
        //this.channel.unbindQueue(originalQueue, this.options.exchangeName, originalMessageId);
      });
  }

  static get instance() {
    if (!this[connectorSingleton]) {
      this[connectorSingleton] = new Connector(connectorEnforcer);
    }
    return this[connectorSingleton];
  }
}


module.exports = Connector.instance;