const { Kafka } = require('kafkajs');
const log4js = require('log4js');

// Configure logging
log4js.configure({
  appenders: { console: { type: 'console' } },
  categories: { default: { appenders: ['console'], level: 'info' } }
});
const logger = log4js.getLogger('consumer');

// Kafka setup
const kafka = new Kafka({
  clientId: 'sre-consumer',
  brokers: [process.env.KAFKA_BROKER || 'kafka:9092']
});

const consumer = kafka.consumer({ groupId: 'sre-consumer-group' });

async function startConsumer() {
  // Wait for Kafka to be ready
  for (let i = 0; i < 10; i++) {
    try {
      await consumer.connect();
      await consumer.subscribe({ topic: 'user-login', fromBeginning: true });
      
      logger.info('Consumer started and subscribed to user-login topic');
      
      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          try {
            const data = JSON.parse(message.value.toString());
            
            // Process the database change message
            logger.info(JSON.stringify({
              timestamp: new Date().toISOString(),
              topic: topic,
              partition: partition,
              offset: message.offset,
              userId: data.userId,
              username: data.username,
              action: data.action,
              ipAddress: data.ipAddress
            }));
            
          } catch (error) {
            logger.error('Error processing message:', error);
          }
        },
      });
      
      break; // Success, exit the retry loop
      
    } catch (error) {
      logger.warn(`Consumer connection attempt ${i + 1} failed:`, error.message);
      if (i < 9) {
        await new Promise(resolve => setTimeout(resolve, 3000)); // Wait 3 seconds
      } else {
        logger.error('Failed to connect to Kafka after 10 attempts');
        process.exit(1);
      }
    }
  }
}

startConsumer();
