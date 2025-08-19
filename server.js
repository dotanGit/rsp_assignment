const express = require('express');
const log4js = require('log4js');
const { Kafka } = require('kafkajs');
const { db, initDB, connectDB } = require('./database');

const app = express();
const PORT = 3001;

// Simple logging setup
log4js.configure({
  appenders: { console: { type: 'console' } },
  categories: { default: { appenders: ['console'], level: 'info' } }
});
const logger = log4js.getLogger();

// Kafka setup
const kafka = new Kafka({
  clientId: 'sre-app',
  brokers: [process.env.KAFKA_BROKER || 'kafka:9092']
});

const producer = kafka.producer();

app.use(express.json());
app.use(express.static('public'));

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'ok', message: 'Server is running!' });
});

// Database health check
app.get('/db-health', async (req, res) => {
  try {
    await db.ping();
    res.json({ status: 'ok', message: 'Database connected!' });
  } catch (error) {
    res.status(500).json({ status: 'error', message: 'Database connection failed' });
  }
});

// Login endpoint
app.post('/api/login', async (req, res) => {
  try {
    const { username, password } = req.body;
    
    // Simple validation
    if (!username || !password) {
      return res.status(400).json({ error: 'Username and password required' });
    }

    // Check user credentials
    const [users] = await db.execute(
      'SELECT * FROM users WHERE username = ? AND password = ?',
      [username, password]
    );

    if (users.length === 0) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }

    const user = users[0];
    
    // Generate simple token
    const token = 'token_' + Date.now() + '_' + user.id;
    
    // Store token in database
    await db.execute(
      'INSERT INTO user_tokens (user_id, token, expires_at) VALUES (?, ?, DATE_ADD(NOW(), INTERVAL 1 HOUR))',
      [user.id, token]
    );

    // Send login event to Kafka
    await producer.send({
      topic: 'user-login',
      messages: [
        {
          key: user.id.toString(),
          value: JSON.stringify({
            userId: user.id,
            username: user.username,
            action: 'login',
            timestamp: new Date().toISOString(),
            ipAddress: req.ip
          })
        }
      ]
    });

    // Log activity
    logger.info(JSON.stringify({
      timestamp: new Date().toISOString(),
      userId: user.id,
      action: 'login',
      ipAddress: req.ip
    }));

    res.json({ 
      message: 'Login successful',
      token: token,
      user: { id: user.id, username: user.username }
    });

  } catch (error) {
    logger.error('Login error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Start server
async function startServer() {
  try {
    await connectDB();
    await initDB();
    
    // Start the server first (don't wait for Kafka)
    app.listen(PORT, () => {
      logger.info(`Server running on port ${PORT}`);
    });
    
    // Try to connect to Kafka in the background
    setTimeout(async () => {
      try {
        await producer.connect();
        logger.info('Kafka producer connected');
      } catch (error) {
        logger.warn('Kafka connection failed, continuing without Kafka:', error.message);
      }
    }, 5000); // Wait 5 seconds before trying Kafka
    
  } catch (error) {
    logger.error('Failed to start server:', error);
    process.exit(1);
  }
}

startServer();