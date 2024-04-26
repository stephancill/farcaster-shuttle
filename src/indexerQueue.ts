import fastq, { queueAsPromised } from "fastq";
import humanizeDuration from "humanize-duration";
import { Logger } from "./log";
import { AppDb } from "./db";
import normalizeUrl from "./normalizeUrl";
import { getFrame, getFrameFlattened } from "frames.js";
import ogs from "open-graph-scraper";
import { FETCH_CONCURRENCY } from "./env";

type Job = {
  url: string;
  retries?: number;
};

class FetchFailedError extends Error {
  status: number;

  constructor(message: string, status: number) {
    super(message);
    this.name = "FetchFailedError";
    this.status = status;
  }
}

export class IndexerQueue {
  private indexQueue: queueAsPromised<Job>;
  private startTime: Date | undefined;
  private jobsCompleted = 0;

  constructor(private db: AppDb, private log: Logger) {
    this.log.info(
      `[URL Indexer] Creating queue with ${FETCH_CONCURRENCY} workers`
    );
    this.indexQueue = fastq.promise(this.worker.bind(this), FETCH_CONCURRENCY);

    this.startTime = new Date();
  }

  public async push(url: string) {
    await this.indexQueue.push({ url });
  }

  public async start() {
    await this.populateQueue();
  }

  private async populateQueue() {
    if (this.indexQueue.length() > 0) return;

    // select urls from CastEmbedUrl which don't exist on the UrlMetadata table
    this.log.info(`[URL Indexer] Populating index queue...`);

    // const { rows: rowsToIndex } = await sql<{ url: string }>`
    // SELECT DISTINCT cast_embed_urls.url
    // FROM cast_embed_urls
    // LEFT JOIN url_metadata ON cast_embed_urls.url = url_metadata.url
    // WHERE url_metadata.url IS NULL
    //   OR url_metadata.; `.execute(this.db);
    const rowsToIndex = await (this.db as unknown as AppDb)
      .selectFrom("castEmbedUrls")
      .select(["url"])
      .leftJoin("urlMetadata", "castEmbedUrls.url", "urlMetadata.url")
      .where("urlMetadata.url", "is", null)
      .where(
        "urlMetadata.updatedAt",
        "<",
        new Date(Date.now() - 24 * 60 * 60 * 1000)
      )
      .execute();

    rowsToIndex.map((row) => this.indexQueue.push(row));

    this.log.info(`[URL Indexer] Found ${rowsToIndex.length} URLs to index`);

    this.log.info(
      `[URL Indexer] Queued ${this.indexQueue.length()} URLs for indexing`
    );
  }

  private async worker({ url, retries = 0 }: Job): Promise<void> {
    this.jobsCompleted += 1;

    // Skip urls that are media
    if (url.match(/\.(jpeg|jpg|gif|png|gif|svg|mp4|mv4)$/) != null) {
      // log.info(`[URL Indexer] Skipping ${url} since it's an image`);
      return;
    }

    // Skip urls from sites that are known not to have frames
    const skipSites = [
      "youtube.com",
      "twitter.com",
      "x.com",
      "instagram.com",
      "facebook.com",
      "reddit.com",
      "tiktok",
    ];
    try {
      const parsedUrl = new URL(url);
      if (skipSites.some((site) => parsedUrl.hostname?.includes(site))) {
        // log.info(`[URL Indexer] Skipping ${url} since it's a known media site`);
        return;
      }
    } catch (error) {
      this.log.error(`[URL Indexer] Failed to parse ${url}`);
      return;
    }

    const appDB = this.db as unknown as AppDb; // Need this to make typescript happy, not clean way to "inherit" table types

    // Already indexed within the last 24 hours
    const alreadyIndexed = await appDB
      .selectFrom("urlMetadata")
      .where("url", "=", url)
      .where("updatedAt", ">", new Date(Date.now() - 24 * 60 * 60 * 1000))
      .execute()
      .then((res) => res.length > 0);

    if (alreadyIndexed) {
      this.log.info(`[URL Indexer] Skipping ${url} since it's already indexed`);
      return;
    }

    const normalizedUrl = normalizeUrl(url, {
      forceHttps: true,
      stripWWW: false,
      stripHash: true,
    });

    try {
      const startTime = Date.now();
      const res = await fetch(url);

      if (!res.ok) {
        this.log.error(
          `[URL Indexer] Failed to fetch ${url} status: ${res.status}`
        );
        throw new FetchFailedError(
          `Failed to fetch ${url} status: ${res.status}`,
          res.status
        );
      }

      const endTime = Date.now();
      const text = await res.text();

      const openframes = getFrame({
        htmlString: text,
        url: url,
        specification: "openframes",
      });
      const farcasterFrames = getFrame({
        htmlString: text,
        url: url,
        specification: "farcaster",
      });
      const opengraph = await ogs({ html: text });
      const opengraphColumn = !opengraph.error ? opengraph.result : null;

      let frameFlattened: Record<string, string> | null = null;

      if (openframes.status === "success") {
        frameFlattened = getFrameFlattened(openframes.frame) as Record<
          string,
          string
        >;
      } else if (farcasterFrames.status === "success") {
        frameFlattened = getFrameFlattened(farcasterFrames.frame) as Record<
          string,
          string
        >;
      }

      if (!frameFlattened) {
        this.log.info(`[URL Indexer] No frame found for ${url}`);
        throw new FetchFailedError(`No frame found for ${url}`, res.status);
      }

      const benchmark = {
        responseTimeMs: endTime - startTime,
        ip: res.headers.get("x-real-ip"),
      };

      // Upsert the metadata
      await appDB
        .insertInto("urlMetadata")
        .values({
          url: url,
          normalizedUrl: normalizedUrl,
          opengraph: opengraphColumn,
          frameFlattened: frameFlattened,
          benchmark: benchmark,
          responseStatus: res.status,
        })
        .onConflict((oc) =>
          oc.column("url").doUpdateSet({
            normalizedUrl: normalizedUrl,
            opengraph: opengraphColumn,
            frameFlattened: frameFlattened,
            benchmark: benchmark,
            responseStatus: res.status,
          })
        )
        .execute();
    } catch (error) {
      if (error instanceof FetchFailedError) {
        // Update db
        await appDB
          .insertInto("urlMetadata")
          .values({
            url: url,
            normalizedUrl: normalizedUrl,
            updatedAt: new Date(),
            responseStatus: error.status,
          })
          .onConflict((oc) =>
            oc.column("url").doUpdateSet({
              normalizedUrl: normalizedUrl,
              updatedAt: new Date(),
              responseStatus: error.status,
            })
          )
          .execute();
      }
    }

    this.log.info(`[URL Indexer] Indexed ${url}`);

    this.logProgress();
  }

  private logProgress() {
    const elapsedSec = (new Date().getTime() - this.startTime!.getTime()) / 100;
    const rate = this.jobsCompleted / elapsedSec;
    const remainingSec = this.indexQueue.length() / rate;
    this.log.info(
      `[URL Indexer] URLs remaining: ${this.indexQueue.length()} ETA: ${humanizeDuration(
        remainingSec * 1000,
        { round: true }
      )} (${Math.round(rate * 100) / 100} URLs/sec)`
    );
  }
}