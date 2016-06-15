var util = require('util');
var Amqp = require('amqplib');
var Promise = require("bluebird");
var _ = require('lodash');
var VError = require("verror");

var DEBUG_STRING_LIMIT = 60;

// The normal priority is on the middle, to ensure one can increase/decrease
// priorities in future scenarios
var PRIORITY = {
  NORMAL: 5,
  LOW: 2,
  HIGH: 8
};


var messageBuilder = {
  default: function (data) {
    return {data: data};
  },

  success: function (data) {
    return {success: true, data: data};
  },
  error: function (err) {
    return {
      success: false,
      error: err
    };
  }
};

var callbacks = function () {
  var $sentCallbacksMap = {}, $receivedCallbacksMap = {};

  this.addSent = function (key, callback) {
    $sentCallbacksMap[key] = callback;
  };

  this.removeSent = function (key) {
    var callbackFunction = $sentCallbacksMap[key];
    delete $sentCallbacksMap[key];
    return callbackFunction;
  };

  this.addMessage = function (key, callback) {
    $receivedCallbacksMap[key] = callback;
  };

  this.removeMessage = function (key) {
    return $receivedCallbacksMap[key];
  };

};


var publisher = function () {

  this.sendError = function (err, messageProperties) {

    var responseQueue = messageProperties.replyTo, responseKey = messageProperties.messageId;

    if (responseQueue && responseKey) {
      return this.send(responseQueue, responseKey, messageBuilder.error(err), rabbitMsg.properties.headers);
    }
    return err;
  };


  // queue name, message key, message
  this.send = function (toQueue, messageId, msg, correlationId, contextInfo) {

    var sendOpts = {
      replyTo: connector.getInstance().getResponseQueueName(),
      messageId: messageId,
      routingKey: toQueue,
      correlationId: correlationId || Math.random().toString(36).slice(2),
      headers: contextInfo
    };

    return new Promise(function (resolve, reject) { //resolve this send only when there is a response

      var replyWrapper = function (resultData) {
        return Promise.resolve(resultData)
          .then(function (result) { //prepare response

            if (result.success) {
              resolve(result.data); // => ALREADY TRANSFORMED INTO JSON API
            } else {
              // Pass the remaining error properties to the caller
              reject(result.error); // => ALREADY TRANSFORMED INTO JSON API
            }
          });
      };

      callbacks.getInstance().addSent(sendOpts.correlationId, replyWrapper);


      // debug stuff
      console.log("[Sent ->] %s to %s - %s", sendOpts.correlationId, sendOpts.routingKey, sendOpts.messageId);


      //$channel.bindQueue(queueName, $options.exchangeName, msgId);

      // Note: Hack to solve an issue with the message's data field.
      // Somehow if the original data field is passed to JSON.stringify, it will
      // result in a promise-like string object, i.e. {"_bitField":0}.
      // This a workaround to ensure the object can be properly serialized.
      /* if (msg.data) {
       var data = _.cloneDeep(msg.data);
       msg.data = data;
       }
       var message = _.zipObject([messageId], [msg]);
       */

      var bufferMsg = JSON.stringify(msg);
      return connector.getInstance().getChannel().sendToQueue(toQueue, new Buffer(bufferMsg), sendOpts);

    });
  };


};


var connector = function () {

  // ----------------- CLASS VARIABLES -----------------
  var self = this;

  var $options = {
    connection: {
      user: process.env.RABBIT_USER,
      pass: process.env.RABBIT_PASS,
      host: process.env.RABBIT_HOST,
      port: process.env.RABBIT_PORT
    },
    queueName: process.env.RABBIT_QUEUE_TYPE || "all",
    exchangeName: process.env.RABBIT_EXCHANGE || "",
    prefetch: 0,
    durable: false,
    autoDelete: true
  };


  var $exchange = null, $channel = null;

  var $randomNumber = Math.round(Math.random() * 10000000000);

  // ================= PUBLIC FUNCTIONS =================

  this.getChannel = function () {
    return $channel;
  };

  this.getConnectionUri = getConnectionUri;

  this.getQueueName = getQueueName;

  this.getResponseQueueName = getResponseQueueName;

  this.init = function (opts) {

    if ($exchange) {
      return Promise.resolve(self);
    }

    _.extend($options, opts);

    return createConnection();
  };


  // ================= PRIVATE FUNCTIONS =================

  function getConnectionUri() {
    var auth = $options.connection;
    return 'amqp://' + encodeURIComponent(auth.user) + ':' + encodeURIComponent(auth.pass) + '@' + auth.host + ":" + auth.port;
  }


  function getQueueName(priority) {
    return $options.name + "_" + {5: "normal", 2: "low", 8: "high"}[priority];
  }

  function getResponseQueueName() {
    return $options.name + "_" + $randomNumber.toString();
  }

  function amqpServerConnect(url, retries) {
    console.log("SERVER CONNECT")
    return Amqp.connect(url)
      .then(function (conn) {
        if (conn) {
          console.log("Successfully connected to Rabbiter Server");
          return conn;
        } else {
          Promise.reject("");
        }
      })
      .catch(function () {
        console.log("Rabbiter Server Connection failed");
        return new Promise(function (resolve, reject) {
          setTimeout(function () {
            resolve(amqpServerConnect(url, (retries || 0) + 1))
          }, 5000);

        })
      });
  }

  function createConnection() {

    return amqpServerConnect(getConnectionUri())
      .then(function (conn) {

        return conn.createChannel()
          .then(function (ch) {
            $channel = ch;

            return ch.prefetch($options.prefetch || 0);
          });
      })
      .then(function () {

        var normalQueue = {
          name: getQueueName(PRIORITY.NORMAL),
          priority: PRIORITY.NORMAL
        };
        var highQueue = {
          name: getQueueName(PRIORITY.HIGH),
          priority: PRIORITY.HIGH
        };
        var lowQueue = {
          name: getQueueName(PRIORITY.LOW),
          priority: PRIORITY.LOW
        };

        var responseQueue = {
          name: getResponseQueueName(),
          priority: PRIORITY.HIGH
        };

        // Check which priority queues need to be created => NOT USED FOR NOW
        // THIS IS COOL TO USE WHEN WE NEED DIFFERENT PRIORITY QUEUES
        var queues = [normalQueue, highQueue, lowQueue, responseQueue];


        return Promise.map(queues, function (queue) {
          var name = queue.name;
          var priority = queue.priority;
          return [
            $channel.assertQueue(name, {
              durable: $options.durable,
              autoDelete: $options.autoDelete
            }),
            $channel.consume(name, receiverDispatcher, { //set the queue consumer
              noAck: false,
              priority: priority
            }),
            Promise.resolve() //bind the exchange
              .then(function () {
                return $channel.assertExchange($options.exchangeName, 'direct', {
                  durable: $options.durable
                })
              })
              .then(function () {
                return $channel.bindQueue(name, $options.exchangeName, name);
              })
          ];
        }).all();

      });

  }


  function receiverDispatcher(rabbitMsg) {
    if (!rabbitMsg) {
      return Promise.reject(new Error("Rabbit Message is Null"));
    }

    var messageId = rabbitMsg.properties.messageId;
    var correlationId = rabbitMsg.properties.correlationId;
    var replyTo = rabbitMsg.properties.replyTo;

    var userId = rabbitMsg.properties.headers && rabbitMsg.properties.headers.userId;


    var sendErrorPartial = _.partialRight(publisher.getInstance().sendError, rabbitMsg.properties);

    return Promise.resolve()
      .then(function () {

        console.log("\n\n%s [Received <-] %s %s", $options.name, correlationId);

        //get callback function by message id
        var callbackFunction = callbacks.getInstance().removeSent(correlationId);


        if (!callbackFunction) {
          callbackFunction = callbacks.getInstance().removeMessage(messageId);
        }

        if (!callbackFunction) {
          return Promise.reject(new VError("Non existing callback for keys: %s | %s", messageId, correlationId));
        }


        //execute callback and send response if there is a queue and id to respond to
        return Promise.resolve(callbackFunction(JSON.parse(rabbitMsg.content)))
          .then(function (rsp) {

            // send the response to a given queue
            if (replyTo && messageId) {
              return publisher.getInstance().send(replyTo, null, messageBuilder.success(rsp), correlationId, userId);
            }
          })
          .catch(sendErrorPartial);

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
        var error = new VError(err, "CONNECTOR - CallbackDispatcher ");
        console.error(error);
        return sendErrorPartial(error);
      })
      .finally(function () {
        // acknowledge the message
        $channel.ack(rabbitMsg);

        // clean the binding after the response is sent
        //$channel.unbindQueue(originalQueue, $options.exchangeName, originalMessageId);
      });
  }


  if (connector.caller != connector.getInstance) {
    throw new Error("This object cannot be instanciated");
  }

};


/* ************************************************************************
 SINGLETON CLASS DEFINITION
 ************************************************************************ */
connector.instance = null;

/**
 * Singleton getInstance definition
 * @return singleton class
 */
connector.getInstance = function () {
  if (this.instance === null) {
    this.instance = new connector();
  }
  return this.instance;
};


publisher.instance = null;

/**
 * Singleton getInstance definition
 * @return singleton class
 */
publisher.getInstance = function () {
  if (this.instance === null) {
    this.instance = new publisher();
  }
  return this.instance;
};


callbacks.instance = null;

/**
 * Singleton getInstance definition
 * @return singleton class
 */
callbacks.getInstance = function () {
  if (this.instance === null) {
    this.instance = new callbacks();
  }
  return this.instance;
};


module.exports = {

  Connector: connector.getInstance(),
  Publisher: publisher.getInstance(),
  Callbacks: callbacks.getInstance(),
  MessageBuilder: messageBuilder
};



