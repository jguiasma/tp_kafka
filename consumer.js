//consommer des messages depuis le même topic
const { Kafka } = require('kafkajs');
const mongoose = require('mongoose');
const Message = require('./models/Message');

// Connexion MongoDB
mongoose.connect('mongodb://localhost:27017/kafka_messages', {
  useNewUrlParser: true,
  useUnifiedTopology: true
}).then(() => console.log('MongoDB connecté'))
  .catch(err => console.error('Erreur MongoDB', err));

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'test-group' });

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'test-topic', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const value = message.value.toString();
      console.log("Message reçu:", value);

      // Sauvegarde dans MongoDB
      const newMsg = new Message({ value });
      await newMsg.save();
    },
  });
};

run().catch(console.error);
