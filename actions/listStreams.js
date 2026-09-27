var db = require('../db')

module.exports = function listStreams (store, data, cb) {
  var limit = data.Limit || 100

  db.lazy(store.streamDb.createValueStream(), cb).join(function (streams) {
    streams = streams
      .filter(function (stream) { return !data.TableName || stream.TableName == data.TableName })
      .sort(function (a, b) { return a.StreamArn < b.StreamArn ? -1 : a.StreamArn > b.StreamArn ? 1 : 0 })

    if (data.ExclusiveStartStreamArn) {
      streams = streams.filter(function (stream) { return stream.StreamArn > data.ExclusiveStartStreamArn })
    }

    var page = streams.slice(0, limit), result = {
      Streams: page.map(function (stream) {
        return { StreamArn: stream.StreamArn, TableName: stream.TableName, StreamLabel: stream.StreamLabel }
      }),
    }
    if (streams.length > limit) result.LastEvaluatedStreamArn = page[page.length - 1].StreamArn

    cb(null, result)
  })
}
