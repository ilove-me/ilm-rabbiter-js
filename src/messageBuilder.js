class MessageBuilder {
  normal(data) {
    return { data };
  }

  success(data) {
    return { data, success: true };
  }

  error(error) {
    return { error, success: false };
  }
}

module.exports = MessageBuilder;
