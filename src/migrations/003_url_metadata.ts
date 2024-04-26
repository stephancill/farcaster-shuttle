import { Kysely, sql } from "kysely";

export const up = async (db: Kysely<any>) => {
  await db.schema
    .createTable("urlMetadata")
    .addColumn("url", "text", (col) => col.notNull().primaryKey())
    .addColumn("normalizedUrl", "text", (col) => col.notNull())
    .addColumn("createdAt", "timestamptz", (col) =>
      col.notNull().defaultTo(sql`current_timestamp`)
    )
    .addColumn("updatedAt", "timestamptz", (col) =>
      col.notNull().defaultTo(sql`current_timestamp`)
    )
    .addColumn("opengraph", "jsonb")
    .addColumn("frameFlattened", "jsonb")
    .addColumn("performance", "jsonb")
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
