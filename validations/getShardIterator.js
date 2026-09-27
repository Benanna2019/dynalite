exports.types = {
  StreamArn: {
    type: 'String',
    required: true,
    lengthGreaterThanOrEqual: 37,
    lengthLessThanOrEqual: 1024,
  },
  ShardId: {
    type: 'String',
    required: true,
    lengthGreaterThanOrEqual: 28,
    lengthLessThanOrEqual: 65,
  },
  ShardIteratorType: {
    type: 'String',
    required: true,
    enum: [ 'TRIM_HORIZON', 'LATEST', 'AT_SEQUENCE_NUMBER', 'AFTER_SEQUENCE_NUMBER' ],
  },
  SequenceNumber: {
    type: 'String',
    lengthGreaterThanOrEqual: 21,
    lengthLessThanOrEqual: 40,
  },
}

exports.custom = function (data) {
  var needsSequence = data.ShardIteratorType == 'AT_SEQUENCE_NUMBER' || data.ShardIteratorType == 'AFTER_SEQUENCE_NUMBER'
  if (needsSequence && data.SequenceNumber == null) {
    return 'SequenceNumber is required for ShardIteratorType ' + data.ShardIteratorType
  }
  if (!needsSequence && data.SequenceNumber != null) {
    return 'SequenceNumber is not allowed for ShardIteratorType ' + data.ShardIteratorType
  }
  if (data.SequenceNumber != null && !/^\d+$/.test(data.SequenceNumber)) {
    return 'Invalid SequenceNumber: ' + data.SequenceNumber
  }
}
