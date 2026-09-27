var db = require('../db'),
  streams = require('../db/streams')

module.exports = function getRecords (store, data, cb) {
  var iterator = streams.parseIterator(data.ShardIterator)
  if (!iterator) return cb(db.validationError('Invalid ShardIterator'))

  var age = Date.now() - iterator.createdAt
  if (age > streams.ITERATOR_TTL_MS) {
    return cb(streams.clientError('ExpiredIteratorException', 'Iterator expired. The iterator was created ' +
      Math.round(age / 1000) + ' seconds ago; shard iterators are valid for 15 minutes.'))
  }

  streams.getStream(store, iterator.streamArn, function (err, stream) {
    if (err) return cb(err)

    streams.readRecords(store, stream, iterator.position, data.Limit || 1000, function (err, read) {
      if (err) return cb(err)

      var result = { Records: read.records }
      // A closed shard that has been read to its end has no next iterator
      if (!read.closed) result.NextShardIterator = streams.createIterator(stream.StreamArn, iterator.shardId, read.next)
      cb(null, result)
    })
  })
}
