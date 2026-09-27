var should = require('should'),
  helpers = require('./helpers')

// DynamoDB Streams is served from a different endpoint in AWS
// (streams.dynamodb.<region>.amazonaws.com), which these helpers don't target.
var describeLocal = process.env.REMOTE ? describe.skip : describe

function call (api, target, data) {
  return new Promise(function (resolve, reject) {
    helpers.request({
      headers: {
        'Content-Type': 'application/x-amz-json-1.0',
        'X-Amz-Target': api + '.' + target,
      },
      body: JSON.stringify(data),
    }, function (err, res) {
      if (err) return reject(err)
      resolve(res)
    })
  })
}

function dynamo (target, data) {
  return call('DynamoDB_20120810', target, data).then(ok)
}

function streams (target, data) {
  return call('DynamoDBStreams_20120810', target, data).then(ok)
}

function ok (res) {
  if (res.statusCode != 200) throw new Error(res.statusCode + ' ' + JSON.stringify(res.body))
  return res.body
}

function createTable (viewType) {
  var table = {
    TableName: helpers.randomName(),
    AttributeDefinitions: [ { AttributeName: 'id', AttributeType: 'S' } ],
    KeySchema: [ { AttributeName: 'id', KeyType: 'HASH' } ],
    BillingMode: 'PAY_PER_REQUEST',
  }
  if (viewType) table.StreamSpecification = { StreamEnabled: true, StreamViewType: viewType }
  return new Promise(function (resolve, reject) {
    helpers.createAndWait(table, function (err) {
      if (err) return reject(err)
      dynamo('DescribeTable', { TableName: table.TableName }).then(function (res) { resolve(res.Table) }, reject)
    })
  })
}

async function iterator (streamArn, type, sequenceNumber) {
  var stream = (await streams('DescribeStream', { StreamArn: streamArn })).StreamDescription
  var req = { StreamArn: streamArn, ShardId: stream.Shards[0].ShardId, ShardIteratorType: type }
  if (sequenceNumber) req.SequenceNumber = sequenceNumber
  return (await streams('GetShardIterator', req)).ShardIterator
}

async function readAll (streamArn) {
  return (await streams('GetRecords', { ShardIterator: await iterator(streamArn, 'TRIM_HORIZON') })).Records
}

function item (id, extra) {
  return Object.assign({ id: { S: id } }, extra)
}

describeLocal('streams', function () {

  describe('tables', function () {

    it('should describe the stream on a table created with one', async function () {
      var table = await createTable('NEW_AND_OLD_IMAGES')
      table.StreamSpecification.should.eql({ StreamEnabled: true, StreamViewType: 'NEW_AND_OLD_IMAGES' })
      table.LatestStreamArn.should.equal(table.TableArn + '/stream/' + table.LatestStreamLabel)
      table.LatestStreamLabel.should.match(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}$/)
    })

    it('should not describe a stream on a table created without one', async function () {
      var table = await createTable()
      should.not.exist(table.StreamSpecification)
      should.not.exist(table.LatestStreamArn)
    })

    it('should require a view type when enabling a stream', async function () {
      var res = await call('DynamoDB_20120810', 'CreateTable', {
        TableName: helpers.randomName(),
        AttributeDefinitions: [ { AttributeName: 'id', AttributeType: 'S' } ],
        KeySchema: [ { AttributeName: 'id', KeyType: 'HASH' } ],
        BillingMode: 'PAY_PER_REQUEST',
        StreamSpecification: { StreamEnabled: true },
      })
      res.statusCode.should.equal(400)
      res.body.__type.should.equal('com.amazon.coral.validate#ValidationException')
      res.body.message.should.match(/StreamViewType is required/)
    })

    it('should reject an unknown view type', async function () {
      var res = await call('DynamoDB_20120810', 'CreateTable', {
        TableName: helpers.randomName(),
        AttributeDefinitions: [ { AttributeName: 'id', AttributeType: 'S' } ],
        KeySchema: [ { AttributeName: 'id', KeyType: 'HASH' } ],
        BillingMode: 'PAY_PER_REQUEST',
        StreamSpecification: { StreamEnabled: true, StreamViewType: 'EVERYTHING' },
      })
      res.statusCode.should.equal(400)
      res.body.message.should.match(/streamSpecification\.streamViewType/)
    })
  })

  describe('listStreams and describeStream', function () {

    it('should list streams for a table and describe them', async function () {
      var table = await createTable('KEYS_ONLY')
      var list = await streams('ListStreams', { TableName: table.TableName })
      list.Streams.should.eql([ { StreamArn: table.LatestStreamArn, TableName: table.TableName, StreamLabel: table.LatestStreamLabel } ])

      var desc = (await streams('DescribeStream', { StreamArn: table.LatestStreamArn })).StreamDescription
      desc.StreamStatus.should.equal('ENABLED')
      desc.StreamViewType.should.equal('KEYS_ONLY')
      desc.TableName.should.equal(table.TableName)
      desc.KeySchema.should.eql(table.KeySchema)
      desc.Shards.should.have.length(1)
      desc.Shards[0].ShardId.should.match(/^shardId-\d{20}-[0-9a-f]{8}$/)
      should.not.exist(desc.Shards[0].SequenceNumberRange.EndingSequenceNumber)
    })

    it('should page through streams', async function () {
      var a = await createTable('KEYS_ONLY'), b = await createTable('KEYS_ONLY')
      var all = (await streams('ListStreams', {})).Streams.map(function (s) { return s.StreamArn })
      all.should.containEql(a.LatestStreamArn)
      all.should.containEql(b.LatestStreamArn)

      var first = await streams('ListStreams', { Limit: 1 })
      first.Streams.should.have.length(1)
      var second = await streams('ListStreams', { Limit: 1, ExclusiveStartStreamArn: first.LastEvaluatedStreamArn })
      second.Streams[0].StreamArn.should.not.equal(first.Streams[0].StreamArn)
    })

    it('should return ResourceNotFoundException for an unknown stream', async function () {
      var arn = 'arn:aws:dynamodb:us-east-1:000000000000:table/nope/stream/2020-01-01T00:00:00.000'
      var res = await call('DynamoDBStreams_20120810', 'DescribeStream', { StreamArn: arn })
      res.statusCode.should.equal(400)
      res.body.__type.should.equal('com.amazonaws.dynamodb.v20120810#ResourceNotFoundException')
    })
  })

  describe('records', function () {

    it('should record inserts, modifications and removals in order', async function () {
      var table = await createTable('NEW_AND_OLD_IMAGES'), name = table.TableName
      await dynamo('PutItem', { TableName: name, Item: item('a', { n: { N: '1' } }) })
      await dynamo('UpdateItem', { TableName: name, Key: item('a'), UpdateExpression: 'SET n = :n', ExpressionAttributeValues: { ':n': { N: '2' } } })
      await dynamo('DeleteItem', { TableName: name, Key: item('a') })

      var records = await readAll(table.LatestStreamArn)
      records.map(function (r) { return r.eventName }).should.eql([ 'INSERT', 'MODIFY', 'REMOVE' ])

      records[0].eventSource.should.equal('aws:dynamodb')
      records[0].eventVersion.should.equal('1.1')
      records[0].awsRegion.should.equal(helpers.awsRegion)
      records[0].dynamodb.Keys.should.eql(item('a'))
      records[0].dynamodb.NewImage.should.eql(item('a', { n: { N: '1' } }))
      should.not.exist(records[0].dynamodb.OldImage)
      records[0].dynamodb.StreamViewType.should.equal('NEW_AND_OLD_IMAGES')
      records[0].dynamodb.ApproximateCreationDateTime.should.be.a.Number()
      records[0].dynamodb.SizeBytes.should.be.above(0)

      records[1].dynamodb.OldImage.should.eql(item('a', { n: { N: '1' } }))
      records[1].dynamodb.NewImage.should.eql(item('a', { n: { N: '2' } }))

      records[2].dynamodb.OldImage.should.eql(item('a', { n: { N: '2' } }))
      should.not.exist(records[2].dynamodb.NewImage)

      var seqs = records.map(function (r) { return r.dynamodb.SequenceNumber })
      seqs.should.eql(seqs.slice().sort())
      new Set(records.map(function (r) { return r.eventID })).size.should.equal(3)
    })

    it('should include only the images the view type asks for', async function () {
      var expected = {
        KEYS_ONLY: [ false, false ],
        NEW_IMAGE: [ true, false ],
        OLD_IMAGE: [ false, true ],
      }
      for (var viewType of Object.keys(expected)) {
        var table = await createTable(viewType)
        await dynamo('PutItem', { TableName: table.TableName, Item: item('a', { n: { N: '1' } }) })
        await dynamo('PutItem', { TableName: table.TableName, Item: item('a', { n: { N: '2' } }) })
        var modify = (await readAll(table.LatestStreamArn))[1]
        modify.dynamodb.Keys.should.eql(item('a'))
        should(modify.dynamodb.NewImage != null).equal(expected[viewType][0], viewType + ' NewImage')
        should(modify.dynamodb.OldImage != null).equal(expected[viewType][1], viewType + ' OldImage')
      }
    })

    it('should not record writes that change nothing', async function () {
      var table = await createTable('NEW_IMAGE'), name = table.TableName
      await dynamo('PutItem', { TableName: name, Item: item('a', { x: { S: 'y' }, n: { N: '1' } }) })
      await dynamo('PutItem', { TableName: name, Item: item('a', { n: { N: '1' }, x: { S: 'y' } }) })
      await dynamo('DeleteItem', { TableName: name, Key: item('missing') })
      ;(await readAll(table.LatestStreamArn)).should.have.length(1)
    })

    it('should record batch writes', async function () {
      var table = await createTable('KEYS_ONLY'), name = table.TableName, req = {}
      req[name] = [ { PutRequest: { Item: item('a') } }, { PutRequest: { Item: item('b') } } ]
      await dynamo('BatchWriteItem', { RequestItems: req })
      req[name] = [ { DeleteRequest: { Key: item('a') } } ]
      await dynamo('BatchWriteItem', { RequestItems: req })

      var records = await readAll(table.LatestStreamArn)
      records.map(function (r) { return r.eventName }).sort().should.eql([ 'INSERT', 'INSERT', 'REMOVE' ])
    })

    it('should not record writes to tables without a stream', async function () {
      var table = await createTable()
      await dynamo('PutItem', { TableName: table.TableName, Item: item('a') })
      var list = await streams('ListStreams', { TableName: table.TableName })
      list.Streams.should.eql([])
    })

    it('should keep sequence order under concurrent writes', async function () {
      var table = await createTable('KEYS_ONLY'), name = table.TableName, writes = []
      for (var i = 0; i < 25; i++) writes.push(dynamo('PutItem', { TableName: name, Item: item('k' + i) }))
      await Promise.all(writes)
      var seqs = (await readAll(table.LatestStreamArn)).map(function (r) { return parseInt(r.dynamodb.SequenceNumber, 10) })
      seqs.should.eql(Array.from({ length: 25 }, function (_, i) { return i + 1 }))
    })
  })

  describe('iterators', function () {

    it('should page with Limit and NextShardIterator', async function () {
      var table = await createTable('KEYS_ONLY'), name = table.TableName
      for (var id of [ 'a', 'b', 'c' ]) await dynamo('PutItem', { TableName: name, Item: item(id) })

      var page1 = await streams('GetRecords', { ShardIterator: await iterator(table.LatestStreamArn, 'TRIM_HORIZON'), Limit: 2 })
      page1.Records.map(function (r) { return r.dynamodb.Keys.id.S }).should.eql([ 'a', 'b' ])
      var page2 = await streams('GetRecords', { ShardIterator: page1.NextShardIterator, Limit: 2 })
      page2.Records.map(function (r) { return r.dynamodb.Keys.id.S }).should.eql([ 'c' ])
      var page3 = await streams('GetRecords', { ShardIterator: page2.NextShardIterator })
      page3.Records.should.eql([])
      should.exist(page3.NextShardIterator)

      // An open shard's iterator picks up later writes
      await dynamo('PutItem', { TableName: name, Item: item('d') })
      var page4 = await streams('GetRecords', { ShardIterator: page3.NextShardIterator })
      page4.Records.map(function (r) { return r.dynamodb.Keys.id.S }).should.eql([ 'd' ])
    })

    it('should start LATEST iterators after existing records', async function () {
      var table = await createTable('KEYS_ONLY'), name = table.TableName
      await dynamo('PutItem', { TableName: name, Item: item('old') })
      var latest = await iterator(table.LatestStreamArn, 'LATEST')
      await dynamo('PutItem', { TableName: name, Item: item('new') })
      var res = await streams('GetRecords', { ShardIterator: latest })
      res.Records.map(function (r) { return r.dynamodb.Keys.id.S }).should.eql([ 'new' ])
    })

    it('should support AT_ and AFTER_SEQUENCE_NUMBER', async function () {
      var table = await createTable('KEYS_ONLY'), name = table.TableName
      for (var id of [ 'a', 'b', 'c' ]) await dynamo('PutItem', { TableName: name, Item: item(id) })
      var seq = (await readAll(table.LatestStreamArn))[1].dynamodb.SequenceNumber

      var at = await streams('GetRecords', { ShardIterator: await iterator(table.LatestStreamArn, 'AT_SEQUENCE_NUMBER', seq) })
      at.Records.map(function (r) { return r.dynamodb.Keys.id.S }).should.eql([ 'b', 'c' ])
      var after = await streams('GetRecords', { ShardIterator: await iterator(table.LatestStreamArn, 'AFTER_SEQUENCE_NUMBER', seq) })
      after.Records.map(function (r) { return r.dynamodb.Keys.id.S }).should.eql([ 'c' ])
    })

    it('should require a SequenceNumber for AT_SEQUENCE_NUMBER', async function () {
      var table = await createTable('KEYS_ONLY')
      var desc = (await streams('DescribeStream', { StreamArn: table.LatestStreamArn })).StreamDescription
      var res = await call('DynamoDBStreams_20120810', 'GetShardIterator', {
        StreamArn: table.LatestStreamArn, ShardId: desc.Shards[0].ShardId, ShardIteratorType: 'AT_SEQUENCE_NUMBER',
      })
      res.statusCode.should.equal(400)
      res.body.message.should.match(/SequenceNumber is required/)
    })

    it('should return ResourceNotFoundException for an unknown shard', async function () {
      var table = await createTable('KEYS_ONLY')
      var res = await call('DynamoDBStreams_20120810', 'GetShardIterator', {
        StreamArn: table.LatestStreamArn, ShardId: 'shardId-00000000000000000000-00000000', ShardIteratorType: 'LATEST',
      })
      res.statusCode.should.equal(400)
      res.body.__type.should.equal('com.amazonaws.dynamodb.v20120810#ResourceNotFoundException')
    })

    it('should reject iterators it did not issue', async function () {
      var res = await call('DynamoDBStreams_20120810', 'GetRecords', { ShardIterator: 'bm90IGFuIGl0ZXJhdG9y' })
      res.statusCode.should.equal(400)
      res.body.__type.should.equal('com.amazon.coral.validate#ValidationException')
    })

    it('should expire iterators after 15 minutes', async function () {
      var table = await createTable('KEYS_ONLY')
      var it = await iterator(table.LatestStreamArn, 'TRIM_HORIZON')
      var realNow = Date.now
      Date.now = function () { return realNow() + (16 * 60 * 1000) }
      try {
        var res = await call('DynamoDBStreams_20120810', 'GetRecords', { ShardIterator: it })
      }
      finally {
        Date.now = realNow
      }
      res.statusCode.should.equal(400)
      res.body.__type.should.equal('com.amazonaws.dynamodb.v20120810#ExpiredIteratorException')
    })
  })

  describe('disabling and re-enabling', function () {

    it('should close the stream, then start a new one', async function () {
      var table = await createTable('KEYS_ONLY'), name = table.TableName, firstArn = table.LatestStreamArn
      await dynamo('PutItem', { TableName: name, Item: item('before') })
      var it = await iterator(firstArn, 'TRIM_HORIZON')

      var disabled = (await dynamo('UpdateTable', { TableName: name, StreamSpecification: { StreamEnabled: false } })).TableDescription
      should.not.exist(disabled.StreamSpecification)
      disabled.LatestStreamArn.should.equal(firstArn)

      await dynamo('PutItem', { TableName: name, Item: item('while-disabled') })

      var desc = (await streams('DescribeStream', { StreamArn: firstArn })).StreamDescription
      desc.StreamStatus.should.equal('DISABLED')
      desc.Shards[0].SequenceNumberRange.EndingSequenceNumber.should.equal('000000000000000000001')

      // Readers drain what was written, then the closed shard ends
      var res = await streams('GetRecords', { ShardIterator: it })
      res.Records.map(function (r) { return r.dynamodb.Keys.id.S }).should.eql([ 'before' ])
      should.not.exist(res.NextShardIterator)

      var enabled = (await dynamo('UpdateTable', {
        TableName: name, StreamSpecification: { StreamEnabled: true, StreamViewType: 'NEW_IMAGE' },
      })).TableDescription
      enabled.StreamSpecification.should.eql({ StreamEnabled: true, StreamViewType: 'NEW_IMAGE' })
      enabled.LatestStreamArn.should.not.equal(firstArn)

      await dynamo('PutItem', { TableName: name, Item: item('after') })
      var records = await readAll(enabled.LatestStreamArn)
      records.map(function (r) { return r.dynamodb.Keys.id.S }).should.eql([ 'after' ])
      ;(await streams('ListStreams', { TableName: name })).Streams.should.have.length(2)
    })

    it('should refuse to enable a stream twice or disable a missing one', async function () {
      var withStream = await createTable('KEYS_ONLY'), without = await createTable()
      var res = await call('DynamoDB_20120810', 'UpdateTable', {
        TableName: withStream.TableName, StreamSpecification: { StreamEnabled: true, StreamViewType: 'KEYS_ONLY' },
      })
      res.statusCode.should.equal(400)
      res.body.message.should.match(/already has an enabled stream/)

      res = await call('DynamoDB_20120810', 'UpdateTable', { TableName: without.TableName, StreamSpecification: { StreamEnabled: false } })
      res.statusCode.should.equal(400)
      res.body.message.should.match(/does not have an enabled stream/)
    })

    it('should keep the stream readable after the table is deleted', async function () {
      var table = await createTable('KEYS_ONLY')
      await dynamo('PutItem', { TableName: table.TableName, Item: item('a') })
      await dynamo('DeleteTable', { TableName: table.TableName })

      var desc = (await streams('DescribeStream', { StreamArn: table.LatestStreamArn })).StreamDescription
      desc.StreamStatus.should.equal('DISABLED')
      ;(await readAll(table.LatestStreamArn)).should.have.length(1)
    })
  })

  describe('routing', function () {

    it('should not serve Streams operations under the DynamoDB API', async function () {
      var res = await call('DynamoDB_20120810', 'ListStreams', {})
      res.statusCode.should.equal(400)
      res.body.__type.should.equal('com.amazon.coral.service#UnknownOperationException')
    })

    it('should not serve DynamoDB operations under the Streams API', async function () {
      var res = await call('DynamoDBStreams_20120810', 'ListTables', {})
      res.statusCode.should.equal(400)
      res.body.__type.should.equal('com.amazon.coral.service#UnknownOperationException')
    })
  })
})
