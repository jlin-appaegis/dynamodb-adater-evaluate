module.exports = {
  tables: [
    {
      TableName: "LargeItem",
      KeySchema: [{ AttributeName: "id", KeyType: "HASH" }],
      AttributeDefinitions: [{ AttributeName: "id", AttributeType: "S" }],
      ProvisionedThroughput: {
        ReadCapacityUnits: 1e6,
        WriteCapacityUnits: 1e6,
      },
      data: require("./data"),
    },
  ],
};
