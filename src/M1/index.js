const express = require('express');
const winston = require('winston');

const app = express();
const port = 4001;

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.json(),
  defaultMeta: { service: 'microservice-1' },
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: 'microservice-1.log' }), // Логирование в файл
  ],
});

// Принимаем JSON в теле запроса
app.use(express.json());

// Обработка входящих HTTP запросов
app.post('/process', (req, res) => {
  try {
    // Получаем данные из тела запроса
    const requestData = req.body;

    // Подготавливаем задание для RabbitMQ
    const task = JSON.stringify(requestData);

    logger.info('Получен HTTP запрос:', { request: requestData });

    // Подключаемся к RabbitMQ и отправляем задание в очередь
    const amqp = require('amqplib');
    const queue = 'task_queue'; // Имя очереди, куда будут отправляться задания

    async function sendTaskToQueue() {
      try {
        const connection = await amqp.connect('amqp://localhost');
        const channel = await connection.createChannel();
        await channel.assertQueue(queue, { durable: true });
        channel.sendToQueue(queue, Buffer.from(task), { persistent: true });
        logger.info('Получен HTTP запрос:', { request: requestData });
      } catch (err) {
        logger.error('Ошибка обработки запроса:', { error: err.message });
        res.status(500).json({ error: 'Ошибка обработки запроса' });
      }
    }

    sendTaskToQueue();

    res.status(200).json({ message: 'Запрос успешно принят и отправлен на обработку' });
  } catch (err) {
    logger.error('Ошибка обработки запроса:', err.message);
    res.status(500).json({ error: 'Ошибка обработки запроса' });
  }
});

// Запускаем сервер
app.listen(port, () => {
  logger.info(`Сервер запущен и слушает порт ${port}`);
});
