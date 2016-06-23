
var logger = require('ilm-node-logger');

let callbacksSingleton = Symbol();
let callbacksEnforcer = Symbol();

class Callbacks {
  constructor(enforcer) {
    this.sentCallbacksMap = {};
    this.receivedCallbacksMap = {};

    if (enforcer !== callbacksEnforcer) {
      throw "Cannot construct singleton"
    }

    this.init();
  }

  init() {

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

  static get instance() {
    if (!this[callbacksSingleton]) {
      this[callbacksSingleton] = new Callbacks(callbacksEnforcer);
    }
    return this[callbacksSingleton];
  }
  /*
  log() {
    let args = Array.prototype.slice.call( arguments );
    args.unshift(`{this.id} - `);
    if(this.subscriptions){
      args.push('subs: ');
      args.push(JSON.stringify(this.subscriptions));
    }

    logger.info(args.join(' '));
  }
  */
}


module.exports = Callbacks.instance;