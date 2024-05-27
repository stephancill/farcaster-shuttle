import "dotenv/config";

export const COLORIZE =
  process.env["COLORIZE"] === "true" ? true : process.env["COLORIZE"] === "false" ? false : process.stdout.isTTY;
export const LOG_LEVEL = process.env["LOG_LEVEL"] || "info";

export const HUB_HOST = process.env["HUB_HOST"] || "localhost:2283";
export const HUB_SSL = process.env["HUB_SSL"]  === "true";

export const POSTGRES_URL = process.env["POSTGRES_URL"] || "postgres://localhost:5432";
export const INSTANCE_CONNECTION_NAME = process.env["INSTANCE_CONNECTION_NAME"] || "";
export const SERVICE_ACCOUNT = process.env["SERVICE_ACCOUNT"] || "";
export const DATABASE = process.env["DATABASE"] || "shuttle";

export const REDIS_URL = process.env["REDIS_URL"] || "redis://localhost:6379";

export const TOTAL_SHARDS = Number.parseInt(process.env["SHARDS"] || "0");
export const SHARD_INDEX = Number.parseInt(process.env["SHARD_NUM"] || "0");

export const BACKFILL_FIDS = process.env["FIDS"] || "";
export const MAX_FID = process.env["MAX_FID"];

export const STATSD_HOST = process.env["STATSD_HOST"];
export const STATSD_METRICS_PREFIX = process.env["STATSD_METRICS_PREFIX"] || "shuttle.";

export const CONCURRENCY = Number.parseInt(process.env["CONCURRENCY"] || "2");

// Number of partitions to create (partitioned by FID).
// 0 = no partitioning.
// Highly experimental. Don't use in production.
export const PARTITIONS = Number(process.env["PARTITIONS"] || "0");