const express = require('express');
const mongoose = require('mongoose');
const Message = require('./models/Message');

const app = express();
const PORT = 3000;

// Connexion MongoDB
mongoose.connect('mongodb://localhost:27017/kafka_messages', {
  useNewUrlParser: true,
  useUnifiedTopology: true
}).then(() => console.log('MongoDB connecté'))
  .catch(err => console.error('Erreur MongoDB', err));

// Route GET pour récupérer tous les messages
app.get('/messages', async (req, res) => {
  try {
    const messages = await Message.find().sort({ timestamp: -1 });
    res.json(messages);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.listen(PORT, () => {
  console.log(`Serveur Express démarré sur http://localhost:${PORT}`);
});
