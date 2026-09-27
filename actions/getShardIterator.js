var streams = require('../db/streams')

module.exports = function getShardIterator (store, data, cb) {
  streams.getStream(store, data.StreamArn, function (err, stream) {
    if (err) return cb(err)

    var shard = stream.Shards.find(function (shard) { return shard.ShardId == data.ShardId })
    if (!shard) {
      return cb(streams.notFound('Requested resource not found: Shard does not exist'))
    }

    if (data.ShardIteratorType == 'TRIM_HORIZON') return done(1)
    if (data.ShardIteratorType == 'AT_SEQUENCE_NUMBER') return done(parseInt(data.SequenceNumber, 10))
    if (data.ShardIteratorType == 'AFTER_SEQUENCE_NUMBER') return done(parseInt(data.SequenceNumber, 10) + 1)

    // LATEST: just past the newest record, so only new writes are read
    streams.lastSequence(store, stream.StreamArn, function (err, last) {
      if (err) return cb(err)
      done(last + 1)
    })

    function done (position) {
      cb(null, { ShardIterator: streams.createIterator(stream.StreamArn, shard.ShardId, position) })
    }
  })
}
