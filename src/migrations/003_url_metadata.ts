import { Kysely, sql } from "kysely";

export const up = async (db: Kysely<any>) => {
  await db.schema
    .createTable("castEmbedUrls")
    .addColumn("id", "bigint", (col) =>
      col.generatedAlwaysAsIdentity().primaryKey()
    )
    .addColumn("castHash", "bytea", (col) => col.notNull())
    .addColumn("url", "text", (col) => col.notNull())
    .$call((qb) =>
      qb.addUniqueConstraint("castEmbedUrls_hash_url_unique", [
        "url",
        "castHash",
      ])
    )
    .execute();

  await db.schema
    .createIndex("cast_embed_urls_cast_hash_index")
    .on("castEmbedUrls")
    .column("castHash")
    .execute();

  await db.schema
    .createIndex("cast_embed_urls_cast_url_index")
    .on("castEmbedUrls")
    .column("url")
    .execute();

  await db.schema
    .createTable("urlMetadata")
    .addColumn("url", "text", (col) => col.notNull().primaryKey())
    .addColumn("normalizedUrl", "text", (col) => col.notNull())
    .addColumn("createdAt", "timestamptz", (col) =>
      col.notNull().defaultTo(sql`current_timestamp`)
    )
    .addColumn("updatedAt", "timestamptz")
    .addColumn("responseStatus", "integer")
    .addColumn("opengraph", "jsonb")
    .addColumn("frameFlattened", "jsonb")
    .addColumn("benchmark", "jsonb")
    .$call((qb) => qb.addUniqueConstraint("urlMetadata_url", ["url"]))
    .execute();

  await db.schema
    .createIndex("url_metadata_url_index")
    .on("urlMetadata")
    .column("url")
    .execute();
};

export const down = async (db: Kysely<any>) => {
  // Delete in reverse order of above so that foreign keys are not violated.
  await db.schema.dropTable("castEmbedUrls").ifExists().execute();
  await db.schema.dropTable("urlMetadata").ifExists().execute();
};
