exports.types = {
  StreamArn: {
    type: 'String',
    required: true,
    lengthGreaterThanOrEqual: 37,
    lengthLessThanOrEqual: 1024,
  },
  Limit: {
    type: 'Integer',
    greaterThanOrEqual: 1,
    lessThanOrEqual: 100,
  },
  ExclusiveStartShardId: {
    type: 'String',
    lengthGreaterThanOrEqual: 28,
    lengthLessThanOrEqual: 65,
  },
}
