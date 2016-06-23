
var logger = require('ilm-node-logger');

class MessageBuilder {
  constructor() {

    this.init();
  }

  init() {

  }

  default(data) {
    return {data: data};
  };

  success(data) {
    return {success: true, data: data};
  };

  error(err) {
    return {
      success: false,
      error: err
    };
  }
}


function create() {
  return new MessageBuilder();
}

module.exports = {
  create: create
};