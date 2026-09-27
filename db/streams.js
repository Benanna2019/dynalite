// DynamoDB Streams support.
//
// The storage and iterator design is adapted from kinesalite
// (https://github.com/mhart/kinesalite, MIT, Michael Hart): each stream's
// records live in their own sub-db keyed by a zero-padded sequence number, so
// LevelDB's key order is stream order, and shard iterators are encrypted,
// expiring position tokens. Kinesis-specific sequence encoding is not carried
// over; DynamoDB Streams clients treat sequence numbers as opaque numeric
// strings.
//
// Scope: one shard per stream (DynamoDB allows this; it never promises a
// shard count), records are written before the write request responds, and
// records older than 24 hours are hidden from reads.

var crypto = require('crypto'),
  once = require('once')

var SEQUENCE_DIGITS = 21
var RETENTION_MS = 24 * 60 * 60 * 1000
var ITERATOR_TTL_MS = 15 * 60 * 1000
var ITERATOR_KEY = Buffer.from('3f9c2b1d8e7a6f5041c3d2e1f0a9b8c7d6e5f4a3b2c1d0e9f8a7b6c5d4e3f2a1', 'hex')
var ITERATOR_IV = Buffer.from('a1b2c3d4e5f60718293a4b5c6d7e8f90', 'hex')

exports.VIEW_TYPES = [ 'NEW_IMAGE', 'OLD_IMAGE', 'NEW_AND_OLD_IMAGES', 'KEYS_ONLY' ]
exports.createStream = createStream
exports.disableStream = disableStream
exports.recordChange = recordChange
exports.lastSequence = lastSequence
exports.formatSequence = formatSequence
exports.createIterator = createIterator
exports.parseIterator = parseIterator
exports.readRecords = readRecords
exports.describe = describe
exports.getStream = getStream
exports.clientError = clientError
exports.notFound = notFound
exports.RETENTION_MS = RETENTION_MS

/**
 * Builds the stream for a table and stores it. `previousLabel` keeps labels
 * unique when a stream is disabled and re-enabled within the same millisecond.
 */
function createStream (store, table, viewType, previousLabel, cb) {
  var now = Date.now()
  var label = new Date(now).toISOString().slice(0, -1)
  if (label === previousLabel) label = new Date(now + 1).toISOString().slice(0, -1)
  var stream = {
    StreamArn: table.TableArn + '/stream/' + label,
    StreamLabel: label,
    StreamStatus: 'ENABLED',
    StreamViewType: viewType,
    CreationRequestDateTime: now / 1000,
    TableName: table.TableName,
    KeySchema: table.KeySchema,
    Shards: [ {
      ShardId: 'shardId-' + ('00000000000000000000' + now).slice(-20) + '-' + crypto.randomBytes(4).toString('hex'),
      SequenceNumberRange: { StartingSequenceNumber: formatSequence(1) },
    } ],
  }
  store.streamDb.put(stream.StreamArn, stream, function (err) {
    if (err) return cb(err)
    cb(null, stream)
  })
}

/**
 * Closes a stream: no new records, and its shard gets an ending sequence
 * number so readers know when they have drained it.
 */
function disableStream (store, streamArn, cb) {
  store.streamDb.lock(streamArn, function (release) {
    cb = release(cb)
    store.streamDb.get(streamArn, function (err, stream) {
      if (err && err.name != 'NotFoundError') return cb(err)
      if (!stream || stream.StreamStatus == 'DISABLED') return cb()
      lastSequence(store, streamArn, function (err, last) {
        if (err) return cb(err)
        stream.StreamStatus = 'DISABLED'
        stream.Shards[0].SequenceNumberRange.EndingSequenceNumber = formatSequence(last)
        store.streamDb.put(streamArn, stream, cb)
      })
    })
  })
}

/**
 * Appends a stream record for one item write. Called with the item lock held,
 * so records for the same item keep their write order. Writes that don't
 * change the item produce no record, matching DynamoDB.
 */
function recordChange (store, table, oldItem, newItem, cb) {
  var spec = table.StreamSpecification
  if (!spec || !spec.StreamEnabled || !table.LatestStreamArn) return cb()
  if (!oldItem && !newItem) return cb()
  if (oldItem && newItem && canonical(oldItem) === canonical(newItem)) return cb()

  var streamArn = table.LatestStreamArn, viewType = spec.StreamViewType
  // Appends are serialized per stream so a record is stored before the next
  // sequence number is handed out; otherwise a reader could see record 5 while
  // record 4 is still being written, and skip 4 forever.
  store.streamDb.lock('append~' + streamArn, function (release) {
    cb = release(cb)
    lastSequence(store, streamArn, function (err, last) {
      if (err) return cb(err)
      var seq = last + 1, item = newItem || oldItem
      var record = {
        eventID: crypto.randomBytes(16).toString('hex'),
        eventName: !oldItem ? 'INSERT' : !newItem ? 'REMOVE' : 'MODIFY',
        eventVersion: '1.1',
        eventSource: 'aws:dynamodb',
        awsRegion: store.tableDb.awsRegion,
        dynamodb: {
          ApproximateCreationDateTime: Math.floor(Date.now() / 1000),
          Keys: keysOf(item, table.KeySchema),
        },
      }
      if (newItem && (viewType == 'NEW_IMAGE' || viewType == 'NEW_AND_OLD_IMAGES')) record.dynamodb.NewImage = newItem
      if (oldItem && (viewType == 'OLD_IMAGE' || viewType == 'NEW_AND_OLD_IMAGES')) record.dynamodb.OldImage = oldItem
      record.dynamodb.SequenceNumber = formatSequence(seq)
      record.dynamodb.SizeBytes = Buffer.byteLength(JSON.stringify(record.dynamodb))
      record.dynamodb.StreamViewType = viewType
      record._createdAt = Date.now()
      store.getStreamRecordDb(streamArn).put(formatSequence(seq), record, function (err) {
        if (err) return cb(err)
        store.streamSequences[streamArn] = seq
        cb()
      })
    })
  })
}

/**
 * The last sequence number stored on a stream (0 when it has none). Cached in
 * memory, seeded once from the newest stored record so persistent (--path)
 * stores keep counting after a restart.
 */
function lastSequence (store, streamArn, cb) {
  var seqs = store.streamSequences
  if (seqs[streamArn] != null) return cb(null, seqs[streamArn])
  cb = once(cb)
  var found = null
  var reader = store.getStreamRecordDb(streamArn).createValueStream({ reverse: true, limit: 1 })
  reader.on('data', function (record) { found = record })
  reader.on('error', cb)
  reader.on('end', function () {
    if (seqs[streamArn] == null) seqs[streamArn] = found ? parseInt(found.dynamodb.SequenceNumber, 10) : 0
    cb(null, seqs[streamArn])
  })
}

function formatSequence (n) {
  return (new Array(SEQUENCE_DIGITS + 1).join('0') + n).slice(-SEQUENCE_DIGITS)
}

function createIterator (streamArn, shardId, position) {
  var plain = JSON.stringify([ Date.now(), streamArn, shardId, position ])
  var cipher = crypto.createCipheriv('aes-256-cbc', ITERATOR_KEY, ITERATOR_IV)
  return Buffer.concat([ cipher.update(plain, 'utf8'), cipher.final() ]).toString('base64')
}

/** Returns { createdAt, streamArn, shardId, position } or null if it isn't one of ours. */
function parseIterator (iterator) {
  try {
    var decipher = crypto.createDecipheriv('aes-256-cbc', ITERATOR_KEY, ITERATOR_IV)
    var plain = Buffer.concat([ decipher.update(Buffer.from(iterator, 'base64')), decipher.final() ]).toString('utf8')
    var parts = JSON.parse(plain)
    if (!Array.isArray(parts) || parts.length != 4) return null
    return { createdAt: parts[0], streamArn: parts[1], shardId: parts[2], position: parts[3] }
  }
  catch {
    return null
  }
}

/**
 * Reads up to `limit` records from `position` on. Returns the records and the
 * position to continue from; `closed` is true once a disabled stream's shard
 * has been read to its end.
 */
function readRecords (store, stream, position, limit, cb) {
  cb = once(cb)
  var cutoff = Date.now() - RETENTION_MS, records = [], next = position
  var reader = store.getStreamRecordDb(stream.StreamArn).createValueStream({ gte: formatSequence(position) })
  reader.on('data', function (record) {
    if (records.length >= limit) return reader.destroy()
    next = parseInt(record.dynamodb.SequenceNumber, 10) + 1
    if (record._createdAt < cutoff) return
    var out = Object.assign({}, record)
    delete out._createdAt
    records.push(out)
  })
  reader.on('error', cb)
  reader.on('close', done)
  reader.on('end', done)
  function done () {
    var ending = stream.Shards[0].SequenceNumberRange.EndingSequenceNumber
    var closed = ending != null && next > parseInt(ending, 10)
    cb(null, { records: records, next: next, closed: closed })
  }
}

function getStream (store, streamArn, cb) {
  store.streamDb.get(streamArn, function (err, stream) {
    // Level returns undefined for a missing key; older versions threw NotFoundError
    if (err && err.name != 'NotFoundError') return cb(err)
    if (!stream) return cb(notFound('Requested resource not found: Stream: ' + streamArn + ' not found'))
    cb(null, stream)
  })
}

/** The public shape of a stream, as DescribeStream returns it. */
function describe (stream) {
  return {
    StreamArn: stream.StreamArn,
    StreamLabel: stream.StreamLabel,
    StreamStatus: stream.StreamStatus,
    StreamViewType: stream.StreamViewType,
    CreationRequestDateTime: stream.CreationRequestDateTime,
    TableName: stream.TableName,
    KeySchema: stream.KeySchema,
    Shards: stream.Shards,
  }
}

function clientError (type, msg) {
  var err = new Error(msg)
  err.statusCode = 400
  err.body = { __type: 'com.amazonaws.dynamodb.v20120810#' + type, message: msg }
  return err
}

function notFound (msg) {
  return clientError('ResourceNotFoundException', msg)
}

function keysOf (item, keySchema) {
  return keySchema.reduce(function (keys, key) {
    keys[key.AttributeName] = item[key.AttributeName]
    return keys
  }, {})
}

// Stable stringify so attribute order doesn't make identical items look different
function canonical (val) {
  if (Array.isArray(val)) return '[' + val.map(canonical).join(',') + ']'
  if (val && typeof val == 'object') {
    return '{' + Object.keys(val).sort().map(function (k) { return JSON.stringify(k) + ':' + canonical(val[k]) }).join(',') + '}'
  }
  return JSON.stringify(val)
}

exports.ITERATOR_TTL_MS = ITERATOR_TTL_MS
