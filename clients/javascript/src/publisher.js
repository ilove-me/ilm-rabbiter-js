var MessageBuilder = require('./messageBuilder.js');
var Connector = require('./connector.js');
var Callbacks = require('./callbacks.js');

var logger = require('ilm-node-logger');
var Promise = require('bluebird');
var _ = require('lodash');


let publisherSingleton = Symbol();
let publisherEnforcer = Symbol();

class Publisher {
  constructor(enforcer) {

    this.messageBuilder = MessageBuilder.create();

    if (enforcer !== publisherEnforcer) {
      throw "Cannot construct singleton"
    }

    //this.redis = nil;
    this.init();
  }

  init() {
    //this.log('Hello ', this.session.userId, JSON.stringify(this.session));

    //this.redis = new Redis(config.redis);
  }

  respondError(err, messageProperties) {
    this.messageBuilder = require('./messageBuilder.js').create();


    var responseQueue = messageProperties.replyTo,
      responseKey = messageProperties.correlationId,
      responseId = messageProperties.correlationId,
      responseContext = messageProperties.headers;

    if (responseQueue && responseKey) {
      return this.send(responseQueue, null, this.messageBuilder.error(err), false, responseId, responseContext);
    }
    return false;
  }

  respondSuccess(msg, messageProperties) {

    var self = this;

    var responseQueue = messageProperties.replyTo,
      responseKey = messageProperties.messageId,
      responseId = messageProperties.correlationId,
      responseContext = messageProperties.headers;
    //context = rabbitMsg.properties.headers && {userId: rabbitMsg.properties.headers.userId}, //send just the userId

    if (responseQueue && responseKey) {
      return this.send(responseQueue, null, this.messageBuilder.success(msg), false, responseId,responseContext);
    }

    return false;
  }


  send(toQueue, messageId, msg, waitResponse, correlationId, contextInfo) {
    if (typeof waitResponse === 'undefined') waitResponse = true;

    var sendOpts = {
      replyTo: Connector.getResponseQueueName(),
      messageId: messageId,
      routingKey: toQueue,
      correlationId: (correlationId && correlationId.toString()) || (waitResponse && Math.random().toString(36).slice(2)),
      headers: contextInfo
    };

    return new Promise(function (resolve, reject) { //resolve this send only when there is a response

      //add timeout response
      var responseTimeout = sendOpts.correlationId && setTimeout(function () {
          Callbacks.removeSent(sendOpts.correlationId);

          reject(new Error('Timeout - waiting for too long for response ' + sendOpts.correlationId));
        }, 7000);

      var bufferMsg = JSON.stringify(msg);

      var replyWrapper = function (resultData) {
        return Promise.resolve(resultData)
          .then(function (result) { //prepare response

            clearTimeout(responseTimeout);

            if (result.success) {
              resolve(result.data); // => ALREADY TRANSFORMED INTO JSON API
            } else {
              // Pass the remaining error properties to the caller
              reject(result.error); // => ALREADY TRANSFORMED INTO JSON API
            }
          });
      };

      Callbacks.addSent(sendOpts.correlationId, replyWrapper);

      // debug stuff
      console.log('[Sent ->] %s to %s - %s', sendOpts.correlationId || "no response required", sendOpts.routingKey, sendOpts.messageId);


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

  /* log() {
   let args = Array.prototype.slice.call( arguments );
   args.unshift(`${this.id} - `);
   if(this.subscriptions){
   args.push('subs: ');
   args.push(JSON.stringify(this.subscriptions));
   }

   logger.info(args.join(' '));
   }
   */
}

module.exports = Publisher.instance;