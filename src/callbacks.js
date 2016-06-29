var logger = require('winston');

class Callbacks {
  constructor() {
    this.sentCallbacksMap = {};
    this.receivedCallbacksMap = {};
  }

  addSent(key, callback) {
    logger.info('Callbacks addSent', key);
    if(key){
      this.sentCallbacksMap[key] = callback;
    }
  }

  removeSent(key) {
    logger.info('Callbacks removeSent', key);
    var callbackFunction = this.sentCallbacksMap[key];
    delete this.sentCallbacksMap[key];
    return callbackFunction;
  }

  addMessage(key, callback) {
    logger.info('Callbacks addMessage', key);
    this.receivedCallbacksMap[key] = callback;
  }

  removeMessage(key) {
    logger.info('Callbacks removeMessage', key);
    return this.receivedCallbacksMap[key];
  }
}

module.exports = new Callbacks();
