var db = require('../db'),
  streams = require('../db/streams')

module.exports = function updateTable (store, data, cb) {

  var key = data.TableName, tableDb = store.tableDb

  tableDb.lock(key, function (release) {
    cb = release(cb)

    store.getTable(key, false, function (err, table) {
      if (err) return cb(err)

      var tableBillingMode = (table.BillingModeSummary || {}).BillingMode || 'PROVISIONED'

      if (data.ProvisionedThroughput && (data.BillingMode || tableBillingMode) == 'PAY_PER_REQUEST') {
        return cb(db.validationError('One or more parameter values were invalid: ' +
          'Neither ReadCapacityUnits nor WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST'))
      }

      var updates, i, update, dataThroughput, tableThroughput, readDiff, writeDiff

      try {
        updates = getThroughputUpdates(data, table)
      }
      catch (err) {
        return cb(err)
      }

      for (i = 0; i < updates.length; i++) {
        update = updates[i]
        dataThroughput = update.dataThroughput
        tableThroughput = update.tableThroughput
        readDiff = dataThroughput.ReadCapacityUnits - tableThroughput.ReadCapacityUnits
        writeDiff = dataThroughput.WriteCapacityUnits - tableThroughput.WriteCapacityUnits

        if (!readDiff && !writeDiff)
          return cb(db.validationError(
            'The provisioned throughput for the table will not change. The requested value equals the current value. ' +
            'Current ReadCapacityUnits provisioned for the table: ' + tableThroughput.ReadCapacityUnits +
            '. Requested ReadCapacityUnits: ' + dataThroughput.ReadCapacityUnits + '. ' +
            'Current WriteCapacityUnits provisioned for the table: ' + tableThroughput.WriteCapacityUnits +
            '. Requested WriteCapacityUnits: ' + dataThroughput.WriteCapacityUnits + '. ' +
            'Refer to the Amazon DynamoDB Developer Guide for current limits and how to request higher limits.'))

        update.setStatus('UPDATING')

        if (readDiff > 0 || writeDiff > 0) tableThroughput.LastIncreaseDateTime = Date.now() / 1000
        if (readDiff < 0 || writeDiff < 0) tableThroughput.LastDecreaseDateTime = Date.now() / 1000

        update.readDiff = readDiff
        update.writeDiff = writeDiff

        if (data.BillingMode == 'PROVISIONED' && tableBillingMode != 'PROVISIONED') {
          tableThroughput.ReadCapacityUnits = dataThroughput.ReadCapacityUnits
          tableThroughput.WriteCapacityUnits = dataThroughput.WriteCapacityUnits
        }
      }

      if (data.BillingMode == 'PAY_PER_REQUEST' && tableBillingMode != 'PAY_PER_REQUEST') {
        table.TableStatus = 'UPDATING'
        table.BillingModeSummary = table.BillingModeSummary || {}
        table.BillingModeSummary.BillingMode = 'PAY_PER_REQUEST'
        table.TableThroughputModeSummary = table.TableThroughputModeSummary || {}
        table.TableThroughputModeSummary.TableThroughputMode = 'PAY_PER_REQUEST'
        table.ProvisionedThroughput = table.ProvisionedThroughput || {}
        table.ProvisionedThroughput.LastDecreaseDateTime = Date.now() / 1000
        table.ProvisionedThroughput.NumberOfDecreasesToday = table.ProvisionedThroughput.NumberOfDecreasesToday || 0
        table.ProvisionedThroughput.ReadCapacityUnits = 0
        table.ProvisionedThroughput.WriteCapacityUnits = 0
        if (table.GlobalSecondaryIndexes) {
          table.GlobalSecondaryIndexes.forEach(function (index) {
            index.IndexStatus = 'UPDATING'
            index.ProvisionedThroughput = index.ProvisionedThroughput || {}
            index.ProvisionedThroughput.NumberOfDecreasesToday = index.ProvisionedThroughput.NumberOfDecreasesToday || 0
            index.ProvisionedThroughput.ReadCapacityUnits = 0
            index.ProvisionedThroughput.WriteCapacityUnits = 0
          })
        }
      }
      else if (data.BillingMode == 'PROVISIONED' && tableBillingMode != 'PROVISIONED') {
        table.BillingModeSummary = table.BillingModeSummary || {}
        table.BillingModeSummary.BillingMode = 'PROVISIONED'
        table.TableThroughputModeSummary = table.TableThroughputModeSummary || {}
        table.TableThroughputModeSummary.TableThroughputMode = 'PROVISIONED'
      }

      applyStreamUpdate(store, table, data.StreamSpecification, function (err) {
        if (err) return cb(err)

        tableDb.put(key, table, function (err) {
          if (err) return cb(err)

          setTimeout(function () {

            // Shouldn't need to lock/fetch as nothing should have changed
            updates.forEach(function (update) {
              dataThroughput = update.dataThroughput
              tableThroughput = update.tableThroughput

              update.setStatus('ACTIVE')

              if (update.readDiff > 0 || update.writeDiff > 0) {
                tableThroughput.LastIncreaseDateTime = Date.now() / 1000
              }
              else if (update.readDiff < 0 || update.writeDiff < 0) {
                tableThroughput.LastDecreaseDateTime = Date.now() / 1000
                tableThroughput.NumberOfDecreasesToday++
              }

              tableThroughput.ReadCapacityUnits = dataThroughput.ReadCapacityUnits
              tableThroughput.WriteCapacityUnits = dataThroughput.WriteCapacityUnits
            })

            if (data.BillingMode == 'PAY_PER_REQUEST' && tableBillingMode != 'PAY_PER_REQUEST') {
              table.TableStatus = 'ACTIVE'
              table.BillingModeSummary.LastUpdateToPayPerRequestDateTime = Date.now() / 1000
              table.TableThroughputModeSummary.LastUpdateToPayPerRequestDateTime = Date.now() / 1000
              delete table.ProvisionedThroughput.LastDecreaseDateTime
              if (table.GlobalSecondaryIndexes) {
                table.GlobalSecondaryIndexes.forEach(function (index) {
                  index.IndexStatus = 'ACTIVE'
                  index.ProvisionedThroughput.NumberOfDecreasesToday++
                  index.ProvisionedThroughput.LastDecreaseDateTime = Date.now() / 1000
                })
              }
            }

            tableDb.put(key, table, function (err) {

              if (err && !/Database is (not open|closed)/.test(err)) console.error(err.stack || err)
            })

          }, store.options.updateTableMs)

          cb(null, { TableDescription: table })
        })
      })
    })
  })

}

// Enables a new stream or disables the current one. Re-enabling creates a new
// stream with a new label and ARN; the old one stays readable until it ages out.
function applyStreamUpdate (store, table, spec, cb) {
  if (!spec) return cb()
  var enabled = Boolean(table.StreamSpecification && table.StreamSpecification.StreamEnabled)

  if (spec.StreamEnabled && enabled) {
    return cb(db.validationError('Table already has an enabled stream: TableName: ' + table.TableName))
  }
  if (!spec.StreamEnabled && !enabled) {
    return cb(db.validationError('Table does not have an enabled stream: TableName: ' + table.TableName))
  }

  if (spec.StreamEnabled) {
    return streams.createStream(store, table, spec.StreamViewType, table.LatestStreamLabel, function (err, stream) {
      if (err) return cb(err)
      table.StreamSpecification = { StreamEnabled: true, StreamViewType: spec.StreamViewType }
      table.LatestStreamArn = stream.StreamArn
      table.LatestStreamLabel = stream.StreamLabel
      cb()
    })
  }

  streams.disableStream(store, table.LatestStreamArn, function (err) {
    if (err) return cb(err)
    delete table.StreamSpecification
    cb()
  })
}

function getThroughputUpdates (data, table) {
  var tableBillingMode = (table.BillingModeSummary || {}).BillingMode || 'PROVISIONED'
  var remainingIndexes = (table.GlobalSecondaryIndexes || []).reduce(function (map, index) {
    map[index.IndexName] = true
    return map
  }, Object.create(null))
  var updates = []
  if (data.ProvisionedThroughput) {
    updates.push({
      dataThroughput: data.ProvisionedThroughput,
      tableThroughput: table.ProvisionedThroughput,
      setStatus: function (status) { table.TableStatus = status },
    })
  }
  var globalUpdates = data.GlobalSecondaryIndexUpdates || []
  if (globalUpdates.length > 5) throw db.limitError('Subscriber limit exceeded: Only 1 online index can be created or deleted simultaneously per table')
  globalUpdates.forEach(function (update) {
    var dataThroughput = update.Update && update.Update.ProvisionedThroughput
    if (!dataThroughput) {
      return
    }
    if (dataThroughput.ReadCapacityUnits > 1000000000000 || dataThroughput.WriteCapacityUnits > 1000000000000) {
      throw db.validationError('This operation cannot be performed with given input values. Please contact DynamoDB service team for more info: Action Blocked: IndexUpdate')
    }
    (table.GlobalSecondaryIndexes || []).forEach(function (index) {
      if (index.IndexName == update.Update.IndexName) {
        delete remainingIndexes[index.IndexName]
        updates.push({
          dataThroughput: dataThroughput,
          tableThroughput: index.ProvisionedThroughput,
          setStatus: function (status) { index.IndexStatus = status },
        })
      }
    })
  })
  if (data.BillingMode == 'PROVISIONED' && tableBillingMode != 'PROVISIONED' && Object.keys(remainingIndexes).length) {
    throw db.validationError('One or more parameter values were invalid: ' +
      'ProvisionedThroughput must be specified for index: ' + Object.keys(remainingIndexes).join(','))
  }
  return updates
}
