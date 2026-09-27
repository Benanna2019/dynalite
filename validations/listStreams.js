exports.types = {
  TableName: {
    type: 'String',
    regex: '[a-zA-Z0-9_.-]+',
    lengthGreaterThanOrEqual: 3,
    lengthLessThanOrEqual: 255,
  },
  Limit: {
    type: 'Integer',
    greaterThanOrEqual: 1,
    lessThanOrEqual: 100,
  },
  ExclusiveStartStreamArn: {
    type: 'String',
    lengthGreaterThanOrEqual: 37,
    lengthLessThanOrEqual: 1024,
  },
}
