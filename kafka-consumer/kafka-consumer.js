const { Kafka } = require("kafkajs");

const run = async () => {
  try {
    const kafka = new Kafka({
      brokers: ["localhost:9092"],
    });

    const consumer = kafka.consumer({"groupId": "test-kafka"});
    console.log("Connecting.....");
    await consumer.connect();
    console.log("Connected!");

    await consumer.subscribe({
      topic: "Users",
      fromBeginning: true
    })

    await consumer.run({
      eachMessage: async result => {
        console.log(`Received message: ${result.message.value} `)
      }
    })

  } catch (err) {
    console.log(`Something bad happened ${err}`);
  }
};

run();