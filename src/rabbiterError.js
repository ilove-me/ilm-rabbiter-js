class RabbiterError extends Error {
  constructor(message, extra){
    super();

    Error.captureStackTrace( this, this.constructor );

    this.name = 'RabbiterError';
    this.message = message || '';

    if ( extra ) {
      this.extra = extra;
    }
  }
}

module.exports = RabbiterError;
