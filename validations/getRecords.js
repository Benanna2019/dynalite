exports.types = {
  ShardIterator: {
    type: 'String',
    required: true,
    lengthGreaterThanOrEqual: 1,
    lengthLessThanOrEqual: 2048,
  },
  Limit: {
    type: 'Integer',
    greaterThanOrEqual: 1,
    lessThanOrEqual: 1000,
  },
}
