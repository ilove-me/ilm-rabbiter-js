var MessageBuilder = require('./messageBuilder.js');
var Connector = require('./connector.js');
var Callbacks = require('./callbacks.js');
var RabbiterError = require('./rabbiterError.js');

var logger = require('ilm-node-logger');
var PromiseBlue = require('bluebird');
// var _ = require('lodash');


let publisherSingleton = Symbol();
let publisherEnforcer = Symbol();

class Publisher {
  constructor(enforcer) {

    this.messageBuilder = MessageBuilder.create();

    if (enforcer !== publisherEnforcer) {
      throw 'Cannot construct singleton';
    }

    //this.redis = nil;
    this.init();
  }

  init() {
    //this.log('Hello ', this.session.userId, JSON.stringify(this.session));

    //this.redis = new Redis(config.redis);
  }

  respondError(err, messageProperties) {

    var responseQueue = messageProperties.replyTo,
      responseKey = messageProperties.messageId,
      responseId = messageProperties.correlationId,
      responseContext = messageProperties.headers;

    if (responseQueue && responseId) {
      return this.send(responseQueue, null, this.messageBuilder.error(err), false, responseId, responseContext);
    }
    return false;
  }


  respondSuccess(msg, messageProperties) {

    var responseQueue = messageProperties.replyTo,
      responseKey = messageProperties.messageId,
      responseId = messageProperties.correlationId,
      responseContext = messageProperties.headers;
    //context = rabbitMsg.properties.headers && {userId: rabbitMsg.properties.headers.userId}, //send just the userId

    if (responseQueue && responseId) {
      return this.send(responseQueue, null, this.messageBuilder.success(msg), false, responseId,responseContext);
    }

    return false;
  }


  send(toQueue, messageId, msg, waitResponse, correlationId, contextInfo) {
    if (typeof waitResponse === 'undefined') {
      waitResponse = true;
    }

    var sendOpts = {
      replyTo: Connector.getResponseQueueName(),
      messageId: messageId,
      routingKey: toQueue,
      correlationId: (correlationId && correlationId.toString()) || (waitResponse && Math.random().toString(36).slice(2)),
      headers: contextInfo
    };

    return new PromiseBlue(function (resolve, reject) { //resolve this send only when there is a response

      var bufferMsg = JSON.stringify(msg);

      //add timeout response if it is to wait for one
      if(waitResponse){

        var responseTimeout = sendOpts.correlationId && setTimeout(function () {
            Callbacks.removeSent(sendOpts.correlationId);

            reject(RabbiterError.create('Timeout - waiting for too long for response ' + sendOpts.correlationId));
          }, 7000);


        var replyWrapper = function (resultData) {
          return PromiseBlue.resolve(resultData)
            .then(function (result) { //prepare response

              clearTimeout(responseTimeout);

              if (result.success) {
                resolve(result.data);
              } else {
                // Pass the remaining error properties to the caller
                reject(result.error);
              }
            });
        };

        Callbacks.addSent(sendOpts.correlationId, replyWrapper);
      }
      // debug stuff
      logger.info('[Sent ->] %s to %s - %s', sendOpts.correlationId || 'no response required', sendOpts.routingKey, sendOpts.messageId);


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


      return Connector.channel.sendToQueue(toQueue, new Buffer(bufferMsg), sendOpts);

    });
  }

  static get instance() {
    if (!this[publisherSingleton]) {
      this[publisherSingleton] = new Publisher(publisherEnforcer);
    }
    return this[publisherSingleton];
  }
}

module.exports = Publisher.instance;
