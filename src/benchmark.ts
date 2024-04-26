import { Command } from "@commander-js/extra-typings";
import { App } from "app";
import { readFileSync } from "fs";
import * as process from "node:process";
import url from "node:url";
import { AppDb } from "./db";
import {
  BACKFILL_FIDS,
  CONCURRENCY,
  FETCH_CONCURRENCY,
  HUB_HOST,
  HUB_SSL,
  POSTGRES_URL,
  REDIS_URL,
} from "./env";
import { log } from "./log";
import { getQueue, getWorker } from "./worker";
import { getUnbatchWorker, getUrlWorker } from "./urlWorker";

//If the module is being run directly, start the shuttle
if (
  import.meta.url.endsWith(url.pathToFileURL(process.argv[1] || "").toString())
) {
  async function benchmarkSingleFid() {
    log.info(
      `Creating app connecting to: ${POSTGRES_URL}, ${REDIS_URL}, ${HUB_HOST}`
    );
    const app = App.create(POSTGRES_URL, REDIS_URL, HUB_HOST, HUB_SSL);
    await app.ensureMigrations();
    const fids = BACKFILL_FIDS
      ? BACKFILL_FIDS.split(",").map((fid) => parseInt(fid))
      : [];
    log.info(`Benchmarking fids: ${fids}`);
    const start = Date.now();
    await app.reconcileFids(fids);
    const elapsed = (Date.now() - start) / 1000;
    log.info(`Reconciled ${fids.length} in ${elapsed}s`);

    process.exit(0);
  }

  async function benchmarkBackfill() {
    log.info(
      `Creating app connecting to: ${POSTGRES_URL}, ${REDIS_URL}, ${HUB_HOST}`
    );
    const app = App.create(POSTGRES_URL, REDIS_URL, HUB_HOST, HUB_SSL);
    await app.ensureMigrations();
    const fids = BACKFILL_FIDS
      ? BACKFILL_FIDS.split(",").map((fid) => parseInt(fid))
      : [];
    log.info(`Benchmarking backfill fids: ${fids}`);
    const backfillQueue = getQueue(app.redis.client);
    const start = Date.now();
    await app.backfillFids(fids, backfillQueue);
    const worker = getWorker(app, app.redis.client, log, CONCURRENCY);

    async function checkQueueAndExecute() {
      const queue = getQueue(app.redis.client);
      const count = await queue.getActiveCount();
      if (count === 0) {
        // Get total rows in cast table
        const rows = await (app.db as unknown as AppDb)
          .selectFrom("casts")
          .execute();

        const elapsed = (Date.now() - start) / 1000;
        log.info(`[benchmark complete] ${elapsed},${rows.length}`);

        process.exit(0);
      }
    }

    setInterval(() => {
      checkQueueAndExecute();
    }, 1000); // Checks every second

    await worker.run();
    // const elapsed = (Date.now() - start) / 1000;
    // log.info(`Backfilled ${fids.length} in ${elapsed}s`);
  }

  async function benchmarkIndexUrlsSingleFidReconcile() {
    log.info(
      `Creating app connecting to: ${POSTGRES_URL}, ${REDIS_URL}, ${HUB_HOST}`
    );
    const app = App.create(POSTGRES_URL, REDIS_URL, HUB_HOST, HUB_SSL);
    await app.ensureMigrations();
    const fids = BACKFILL_FIDS
      ? BACKFILL_FIDS.split(",").map((fid) => parseInt(fid))
      : [];
    log.info(`Benchmarking fids: ${fids}`);
    const start = Date.now();
    await app.reconcileFids(fids);
    const elapsed = (Date.now() - start) / 1000;
    log.info(`Reconciled ${fids.length} in ${elapsed}s`);

    const unbatchWorker = getUnbatchWorker(app.redis.client);
    unbatchWorker.run();
    log.info("Unbatch worker started");
    const worker = getUrlWorker(app, app.redis.client, log, FETCH_CONCURRENCY);
    await worker.run();

    process.exit(0);
  }

  async function indexUrl(url: string) {
    log.info(
      `Creating app connecting to: ${POSTGRES_URL}, ${REDIS_URL}, ${HUB_HOST}`
    );
    const app = App.create(POSTGRES_URL, REDIS_URL, HUB_HOST, HUB_SSL);
    await app.ensureMigrations();
    const start = Date.now();
    await app.urlIndexer.push(url).catch((e) => {
      log.error(e);
      process.exit(1);
    });
    const elapsed = (Date.now() - start) / 1000;
    log.info(`Indexed ${url} in ${elapsed}s`);

    process.exit(0);
  }

  // for (const signal of ["SIGINT", "SIGTERM", "SIGHUP"]) {
  //   process.on(signal, async () => {
  //     log.info(`Received ${signal}. Shutting down...`);
  //     (async () => {
  //       await sleep(10_000);
  //       log.info(`Shutdown took longer than 10s to complete. Forcibly terminating.`);
  //       process.exit(1);
  //     })();
  //     await app?.stop();
  //     process.exit(1);
  //   });
  // }
  const program = new Command()
    .name("shuttle")
    .description("Synchronizes a Farcaster Hub with a Postgres database")
    .version(JSON.parse(readFileSync("./package.json").toString()).version);

  program
    .command("bench-reconcile")
    .description("Benchmark reconcile")
    .action(benchmarkSingleFid);
  program
    .command("bench-backfill")
    .description("Benchmark backfill")
    .action(benchmarkBackfill);
  program
    .command("bench-index-urls")
    .description("Benchmark index urls")
    .action(benchmarkIndexUrlsSingleFidReconcile);
  program
    .command("index-url")
    .description("Test index url")
    .requiredOption("-u, --url <url>", "URL to index")
    .action(({ url }) => indexUrl(url));

  program.parse(process.argv);
}
