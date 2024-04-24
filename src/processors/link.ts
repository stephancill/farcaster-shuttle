import { Message, fromFarcasterTime } from "@farcaster/hub-nodejs";
import { AppDb } from "../db";
import { formatLinks } from "./utils";
import { log } from "../log";

export async function insertLinks(msgs: Message[], db: AppDb) {
  const links = formatLinks(msgs);

  try {
    await db
      .insertInto("links")
      .values(links)
      .onConflict((oc) => oc.column("hash").doNothing())
      .execute();

    log.debug(`LINKS INSERTED`);
  } catch (error) {
    log.error(error, "ERROR INSERTING LINK");
  }
}

export async function deleteLinks(msgs: Message[], db: AppDb) {
  try {
    for (const msg of msgs) {
      const data = msg.data!;

      await db
        .updateTable("links")
        .set({
          deletedAt: new Date(
            fromFarcasterTime(data.timestamp)._unsafeUnwrap()
          ),
        })
        .where("fid", "=", data.fid)
        .where("targetFid", "=", data.linkBody!.targetFid!)
        .execute();
    }

    log.debug(`LINKS DELETED`);
  } catch (error) {
    log.error(error, "ERROR DELETING LINK");
  }
}
