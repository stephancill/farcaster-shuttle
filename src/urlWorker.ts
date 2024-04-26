import { Redis } from "ioredis";
import { Job, Queue, Worker } from "bullmq";
import { App } from "./app";
import { pino } from "pino";

const URL_QUEUE_NAME = "urls";
const URL_BATCH_QUEUE_NAME = "urlBatches";
export const INDEX_URL_JOB_NAME = "indexUrl";
export const UNBATCH_JOB_NAME = "unbatch";

export function getUrlWorker(
  app: App,
  redis: Redis,
  log: pino.Logger,
  concurrency = 1
) {
  const worker = new Worker(
    URL_QUEUE_NAME,
    async (job: Job) => {
      if (job.name === INDEX_URL_JOB_NAME) {
        const start = Date.now();
        const url = job.data.url;
        const result = await app.indexUrl(url);
        if (result) {
          const elapsed = (Date.now() - start) / 1000;
          log.info(`Indexed ${url} in ${elapsed} seconds`);
        }
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

export function getUnbatchWorker(redis: Redis) {
  return new Worker(
    URL_BATCH_QUEUE_NAME,
    async (job: Job<{ urls: { url: string }[] }>) => {
      if (job.name === UNBATCH_JOB_NAME) {
        const urls = job.data.urls;
        console.log("Unbatching", urls.length, "urls");
        const urlQueue = getUrlQueue(redis);
        await Promise.all(
          urls.map(({ url }) => urlQueue.add(INDEX_URL_JOB_NAME, { url }))
        );
      }
    },
    {
      connection: redis,
      autorun: false,
    }
  );
}

export function getUrlQueue(redis: Redis) {
  return new Queue(URL_QUEUE_NAME, {
    connection: redis,
    defaultJobOptions: {
      attempts: 3,
      backoff: { delay: 1000, type: "exponential" },
    },
  });
}

export function getUrlBatchQueue(redis: Redis) {
  return new Queue(URL_BATCH_QUEUE_NAME, {
    connection: redis,
  });
}
