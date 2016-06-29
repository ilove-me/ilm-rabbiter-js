var MessageBuilder = require('./messageBuilder.js');
var Connector = require('./connector.js');
var Callbacks = require('./callbacks.js');
var RabbiterError = require('./rabbiterError.js');

var logger = require('winston');
var PromiseBlue = require('bluebird');

class Publisher {
  constructor() {
    this.messageBuilder = new MessageBuilder();
  }

  respondError(err, messageProperties) {

    var responseQueue = messageProperties.replyTo;
    // var responseKey = messageProperties.messageId;
    var responseId = messageProperties.correlationId;
    var responseContext = messageProperties.headers;

    if (responseQueue && responseId) {
      return this.send(
        responseQueue,
        null,
        this.messageBuilder.error(err),
        false,
        responseId,
        responseContext
      );
    }
    return false;
  }


  respondSuccess(msg, messageProperties) {

    var responseQueue = messageProperties.replyTo;
    // var responseKey = messageProperties.messageId;
    var responseId = messageProperties.correlationId;
    var responseContext = messageProperties.headers;
    //context = rabbitMsg.properties.headers && {userId: rabbitMsg.properties.headers.userId}, //send just the userId

    if (responseQueue && responseId) {
      return this.send(
        responseQueue,
        null,
        this.messageBuilder.success(msg),
        false,
        responseId,
        responseContext
      );
    }

    return false;
  }


  send(toQueue, messageId, msg, waitResponse, correlationId, contextInfo) {
    if (typeof waitResponse === 'undefined') {
      waitResponse = true;
    }

    correlationId = (correlationId && correlationId.toString()) || (waitResponse && Math.random().toString(36).slice(2));

    var sendOpts = {
      replyTo: Connector.getResponseQueueName(),
      messageId: messageId,
      routingKey: toQueue,
      correlationId: correlationId,
      headers: contextInfo
    };

    return new PromiseBlue(function (resolve, reject) { //resolve this send only when there is a response

      var bufferMsg = JSON.stringify(msg);

      //add timeout response if it is to wait for one
      if(waitResponse){

        var responseTimeout = sendOpts.correlationId && setTimeout(function () {
            Callbacks.removeSent(sendOpts.correlationId);

            reject(new RabbiterError('Timeout - waiting for too long for response ' + sendOpts.correlationId));
          }, 7000);


        var replyWrapper = function (resultData) {
          return PromiseBlue.resolve(resultData)
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
      }

      logger.info(
        '[Sent ->] %s to %s - %s',
        sendOpts.correlationId || 'no response required',
        sendOpts.routingKey,
        sendOpts.messageId
      );

      return Connector.channel.sendToQueue(toQueue, new Buffer(bufferMsg), sendOpts);
    });
  }
}

module.exports = new Publisher();
