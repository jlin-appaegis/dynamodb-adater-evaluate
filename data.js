const { v4: uuidv4 } = require("uuid");

const animals = ["Lion", "Monkey", "Elephant"];

const generateTenant = () => ({
  id: uuidv4(),
  boolean: false,
  string: animals[Math.floor(animals.length * Math.random())],
  nullable: null,
  number: Math.floor(1e6 * Math.random()),
  externalIdList: [
    "some-external-id-1",
    "some-external-id-2",
    "some-external-id-3",
  ],
  numberList: [1, 2, 3],
  nested: {
    any: {
      level: {
        supported: true,
      },
    },
  },
});

module.exports = Array.from({ length: 1e3 }).map(generateTenant);
