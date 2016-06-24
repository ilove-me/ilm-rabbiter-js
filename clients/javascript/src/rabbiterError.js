
function IlmRabbiterError(message) {
  this.name = 'RabbiterError';
  this.message = (message || '');
}
IlmRabbiterError.prototype = Error.prototype;

class RabbiterError {
  constructor() {

    this.init();
  }

  init(){

  }

  static create(msg){
    return new IlmRabbiterError(msg);
  }

}


module.exports = RabbiterError;