var streams = require('../db/streams')

module.exports = function describeStream (store, data, cb) {
  streams.getStream(store, data.StreamArn, function (err, stream) {
    if (err) return cb(err)

    var description = streams.describe(stream)
    // One shard per stream, so paging past it leaves nothing
    if (data.ExclusiveStartShardId) {
      var ix = description.Shards.findIndex(function (shard) { return shard.ShardId == data.ExclusiveStartShardId })
      description.Shards = ix == -1 ? description.Shards : description.Shards.slice(ix + 1)
    }
    if (data.Limit) description.Shards = description.Shards.slice(0, data.Limit)

    cb(null, { StreamDescription: description })
  })
}
