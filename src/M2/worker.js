const amqp = require('amqplib');
const winston = require('winston');

const queue = 'task_queue'; // Имя очереди, из которой будут получаться задания
const resultQueue = 'result_queue'; // Имя очереди, в которую будут помещаться результаты обработки

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.json(),
  defaultMeta: { service: 'microservice-2' },
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: 'microservice-2.log' }), // Логирование в файл
  ],
});

// Функция обработки задания
function processTask(task) {
  return new Promise((resolve, reject) => {
    try {
      // Вместо setTimeout здесь должен быть ваш код обработки задания
      // В данном примере просто имитируем обработку задания
      setTimeout(() => {
        logger.info('Задание обработано:', { task }); // Логирование успешной обработки задания
        resolve({ result: 'Обработка успешно завершена', originalTask: task });
      }, 3000);
    } catch (error) {
      logger.error('Ошибка при обработке задания:', { task, error });
      reject(error);
    }
  });
}

// Подключение к RabbitMQ и обработка заданий
async function startWorker() {
  try {
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();

    // Убедимся, что очереди существуют
    await channel.assertQueue(queue, { durable: true });
    await channel.assertQueue(resultQueue, { durable: true });

    // Указываем, что можно получать не более одного сообщения за раз
    channel.prefetch(1);

    // Обработка заданий
    channel.consume(queue, async (msg) => {
      if (!msg) return;

      const task = JSON.parse(msg.content.toString());

      try {
        const result = await processTask(task);

        // Помещаем результат обработки в другую очередь
        channel.sendToQueue(resultQueue, Buffer.from(JSON.stringify(result)), { persistent: true });

        logger.info('Задание успешно обработано:', { task, result });

        // Подтверждаем, что задание было обработано успешно
        channel.ack(msg);
      } catch (error) {
        logger.error('Ошибка при обработке задания:', error.message);

        // Если возникла ошибка, возвращаем задание в очередь для повторной обработки
        channel.nack(msg, false, true);
      }
    });
  } catch (err) {
    logger.error('Ошибка при подключении к RabbitMQ:', err.message);
  }
}

startWorker();