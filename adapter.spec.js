// https://docs.aws.amazon.com/sdk-for-javascript/v3/developer-guide/dynamodb-example-table-read-write.html
// https://github.com/awslabs/dynamodb-data-mapper-js/tree/master/packages/dynamodb-auto-marshaller
const { Marshaller } = require("@aws/dynamodb-auto-marshaller");
const {
  DynamoDBClient,
  GetItemCommand,
  ScanCommand,
  QueryCommand,
} = require("@aws-sdk/client-dynamodb");
// https://dynamoosejs.com/getting_started/Introduction
const dynamoose = require("dynamoose");
// https://github.com/awslabs/dynamodb-data-mapper-js/tree/master/packages/dynamodb-data-mapper
// https://github.com/awslabs/dynamodb-data-mapper-js/blob/3ffe4bfb9187cf21bf87f72212526fa72172e555/packages/dynamodb-data-marshaller/src/unmarshallItem.ts#L50
const {
  DataMapper,
  DynamoDbTable,
  DynamoDbSchema,
} = require("@aws/dynamodb-data-mapper");
const { equals, greaterThan } = require("@aws/dynamodb-expressions");
const DynamoDB = require("aws-sdk/clients/dynamodb");

const data = require("./data");

const TABLE_NAME = "LargeItem";

jest.setTimeout(1e6);

describe("DynamoDB adapters", () => {
  const sorter = (a, b) => (a.id === b.id ? 0 : a.id < b.id ? 1 : -1);
  const dynamodbConfig = {
    region: "local",
    endpoint: process.env.MOCK_DYNAMODB_ENDPOINT,
  };

  // Native
  const client = new DynamoDBClient(dynamodbConfig);
  const marshaller = new Marshaller();

  // Dynamoose
  dynamoose.aws.ddb.set(new dynamoose.aws.ddb.DynamoDB(dynamodbConfig));
  const model = dynamoose.model(
    TABLE_NAME,
    new dynamoose.Schema({
      id: {
        type: String,
        required: true,
        hashKey: true,
      },
      boolean: {
        type: Boolean,
      },
      string: {
        type: String,
      },
      nullable: {
        type: [String, dynamoose.type.NULL],
      },
      number: {
        type: Number,
      },
      externalIdList: {
        type: Array,
        schema: [String],
      },
      numberList: {
        type: Array,
        schema: [Number],
      },
      nested: {
        type: Object,
        schema: {
          any: {
            type: Object,
            schema: {
              level: {
                type: Object,
                schema: {
                  supported: {
                    type: Boolean,
                  },
                },
              },
            },
          },
        },
      },
    })
  );

  // DataMapper
  const mapper = new DataMapper({
    client: new DynamoDB(dynamodbConfig),
  });
  class LargeItem {
    id;
    boolean;
    string;
    nullable;
    number;
    externalIdList;
    numberList;
    nested;
  }
  Object.defineProperties(LargeItem.prototype, {
    [DynamoDbTable]: { value: TABLE_NAME },
    [DynamoDbSchema]: {
      value: {
        id: { type: "String", keyType: "HASH" },
        boolean: { type: "Boolean" },
        string: { type: "String" },
        nullable: {
          type: "Custom",
          marshall(input) {
            if (input === "") {
              return { NULL: true };
            }
            return { S: input };
          },
          unmarshall(persistedValue) {
            if (persistedValue.NULL === true) {
              return null;
            }
            return persistedValue.S;
          },
        },
        number: { type: "Number" },
        externalIdList: { type: "List", memberType: { type: "String" } },
        numberList: { type: "List", memberType: { type: "Number" } },
        nested: { type: "Any" },
      },
    },
  });

  describe("Get 10000 times", () => {
    const NUM_ROUND = 10000;
    const targetData = data[Math.floor(data.length / 2)];

    // Native
    const command = new GetItemCommand({
      Key: { id: { S: targetData.id } },
      TableName: TABLE_NAME,
    });

    test("Verify all the same", async () => {
      expect(
        JSON.parse(
          JSON.stringify(
            marshaller.unmarshallItem((await client.send(command)).Item)
          )
        )
      ).toMatchObject(targetData);
      expect(await model.get(targetData.id)).toMatchObject(targetData);
      expect(
        await mapper.get(Object.assign(new LargeItem(), { id: targetData.id }))
      ).toMatchObject(targetData);
    });

    test("Native SDK", async () => {
      for (let i = 0; i < NUM_ROUND; ++i) {
        marshaller.unmarshallItem((await client.send(command)).Item);
      }
    });

    test("Dynamoose@3", async () => {
      for (let i = 0; i < NUM_ROUND; ++i) {
        await model.get(targetData.id);
      }
    });

    test("DataMapper", async () => {
      for (let i = 0; i < NUM_ROUND; ++i) {
        await mapper.get(Object.assign(new LargeItem(), { id: targetData.id }));
      }
    });
  });

  describe("Scan 50 times", () => {
    const NUM_ROUND = 50;
    const { string: scanString, number: scanNumber } = data[0];

    // Native
    const command = new ScanCommand({
      FilterExpression:
        "#stringName = :stringValue AND #numberName > :numberValue",
      ExpressionAttributeValues: {
        ":stringValue": { S: scanString },
        ":numberValue": { N: String(scanNumber) },
      },
      ExpressionAttributeNames: {
        "#stringName": "string",
        "#numberName": "number",
      },
      TableName: TABLE_NAME,
    });

    // DataMapper
    const condition = {
      type: "And",
      conditions: [
        {
          subject: "string",
          ...equals(scanString),
        },
        {
          subject: "number",
          ...greaterThan(scanNumber),
        },
      ],
    };

    test("Verify all the same", async () => {
      const expected = data
        .filter(
          ({ string, number }) => string === scanString && number > scanNumber
        )
        .sort(sorter);
      // Native
      expect(
        (await client.send(command)).Items.map((item) =>
          JSON.parse(JSON.stringify(marshaller.unmarshallItem(item)))
        ).sort(sorter)
      ).toMatchObject(expected);
      // Dynamoose
      expect(
        Array.from(
          await model
            .scan()
            .filter("string")
            .eq(scanString)
            .filter("number")
            .gt(scanNumber)
            .exec()
        ).sort(sorter)
      ).toMatchObject(expected);
      // DataMapper
      const iterator = await mapper.scan(LargeItem, { filter: condition });
      const results = [];
      for await (const res of iterator) {
        results.push(res);
      }
      expect(results.sort(sorter)).toMatchObject(expected);
    });

    test("Native SDK", async () => {
      for (let i = 0; i < NUM_ROUND; ++i) {
        (await client.send(command)).Items.map((item) =>
          marshaller.unmarshallItem(item)
        );
      }
    });

    test("Dynamoose@3", async () => {
      for (let i = 0; i < NUM_ROUND; ++i) {
        await model
          .scan()
          .filter("string")
          .eq(scanString)
          .filter("number")
          .gt(scanNumber)
          .exec();
      }
    });

    test("DataMapper", async () => {
      for (let i = 0; i < NUM_ROUND; ++i) {
        const iterator = await mapper.scan(LargeItem, { filter: condition });
        for await (const res of iterator) {
        }
      }
    });
  });

  describe("Query 1000 times", () => {
    const NUM_ROUND = 1000;
    const targetData = data[0];

    // Native
    const command = new QueryCommand({
      KeyConditionExpression: "#idName = :idValue",
      ExpressionAttributeValues: {
        ":idValue": { S: targetData.id },
      },
      ExpressionAttributeNames: {
        "#idName": "id",
      },
      TableName: TABLE_NAME,
    });

    test("Verify all the same", async () => {
      // Native
      expect(
        JSON.parse(
          JSON.stringify(
            marshaller.unmarshallItem((await client.send(command)).Items[0])
          )
        )
      ).toMatchObject(targetData);
      // Dynamoose
      expect(
        (await model.query().filter("id").eq(targetData.id).exec())[0]
      ).toMatchObject(targetData);
      // DataMapper
      const iterator = await mapper.query(LargeItem, { id: targetData.id });
      expect((await iterator.next()).value).toMatchObject(targetData);
    });

    test("Native SDK", async () => {
      for (let i = 0; i < NUM_ROUND; ++i) {
        (await client.send(command)).Items.map((item) =>
          marshaller.unmarshallItem(item)
        );
      }
    });

    test("Dynamoose@3", async () => {
      for (let i = 0; i < NUM_ROUND; ++i) {
        await model.query().filter("id").eq(targetData.id).exec();
      }
    });

    test("DataMapper", async () => {
      for (let i = 0; i < NUM_ROUND; ++i) {
        const iterator = await mapper.query(LargeItem, { id: targetData.id });
        for await (const res of iterator) {
        }
      }
    });
  });
});
