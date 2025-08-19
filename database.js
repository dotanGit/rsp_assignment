const mysql = require('mysql2/promise');
const log4js = require('log4js');

const logger = log4js.getLogger('database');

// Database connection with retry
async function connectDB() {
  let connection;
  for (let i = 0; i < 10; i++) {
    try {
      connection = await mysql.createConnection({
        host: process.env.DB_HOST || 'tidb',
        port: process.env.DB_PORT || 4000,
        user: process.env.DB_USER || 'root',
        password: process.env.DB_PASSWORD || '',
      });
      
      // Create database if not exists
      await connection.execute('CREATE DATABASE IF NOT EXISTS sre_assignment');
      await connection.execute('USE sre_assignment');
      
      logger.info('Database connected successfully');
      return connection;
    } catch (error) {
      logger.warn(`Database connection attempt ${i + 1} failed:`, error.message);
      if (i < 9) {
        await new Promise(resolve => setTimeout(resolve, 2000));
      }
    }
  }
  throw new Error('Failed to connect to database after 10 attempts');
}

const db = mysql.createPool({
  host: process.env.DB_HOST || 'localhost',
  port: process.env.DB_PORT || 4000,
  user: process.env.DB_USER || 'root',
  password: process.env.DB_PASSWORD || '',
  database: process.env.DB_NAME || 'sre_assignment'
});

async function initDB() {
  try {
    await db.execute(`
      CREATE TABLE IF NOT EXISTS users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        username VARCHAR(50) UNIQUE NOT NULL,
        email VARCHAR(100) UNIQUE NOT NULL,
        password VARCHAR(255) NOT NULL
      )
    `);
    
    await db.execute(`
      CREATE TABLE IF NOT EXISTS user_tokens (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL,
        token VARCHAR(500) NOT NULL,
        expires_at TIMESTAMP NOT NULL
      )
    `);

    // Create default user with plain password
    const [users] = await db.execute('SELECT id FROM users WHERE username = ?', ['admin']);
    if (users.length === 0) {
      await db.execute(
        'INSERT INTO users (username, email, password) VALUES (?, ?, ?)',
        ['admin', 'admin@example.com', 'admin123']
      );
      logger.info('Default user created');
    }
    
    logger.info('Database initialized successfully');
  } catch (error) {
    logger.error('Database init error:', error);
    throw error;
  }
}

module.exports = { db, initDB, connectDB };