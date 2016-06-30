var logger = require('winston');

class Callbacks {
  constructor() {
    this.sentCallbacksMap = {};
    this.receivedCallbacksMap = {};
  }

  addSent(key, callback) {
    if(key){
      this.sentCallbacksMap[key] = callback;
    }
  }

  removeSent(key) {
    var callbackFunction = this.sentCallbacksMap[key];
    delete this.sentCallbacksMap[key];
    return callbackFunction;
  }

  addMessage(key, callback) {
    this.receivedCallbacksMap[key] = callback;
  }

  removeMessage(key) {
    return this.receivedCallbacksMap[key];
  }
}

module.exports = new Callbacks();
