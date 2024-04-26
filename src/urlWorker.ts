import { Redis } from "ioredis";
import { Job, Queue, Worker } from "bullmq";
import { App } from "./app";
import { pino } from "pino";

const QUEUE_NAME = "urls";
export const INDEX_URL_JOB_NAME = "indexUrl";

export function getUrlWorker(
  app: App,
  redis: Redis,
  log: pino.Logger,
  concurrency = 1
) {
  const worker = new Worker(
    QUEUE_NAME,
    async (job: Job) => {
      if (job.name === INDEX_URL_JOB_NAME) {
        const start = Date.now();
        const url = job.data.url;
        await app.indexUrl(url);
        const elapsed = (Date.now() - start) / 1000;
        log.info(`Indexed ${url} in ${elapsed} seconds`);
      }
    },
    {
      autorun: false, // Don't start yet
      useWorkerThreads: concurrency > 1,
      concurrency,
      connection: redis,
    }
  );

  return worker;
}

export function getUrlQueue(redis: Redis) {
  return new Queue(QUEUE_NAME, {
    connection: redis,
    defaultJobOptions: {
      attempts: 3,
      backoff: { delay: 1000, type: "exponential" },
    },
  });
}
