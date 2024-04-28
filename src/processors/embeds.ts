import { Message, isCastAddMessage } from "@farcaster/hub-nodejs";
import { AppDb } from "../db";

export async function insertEmbeds(messages: Message[], db: AppDb) {
  const castAddMessages = messages.filter(isCastAddMessage);
  const urlEmbeds = castAddMessages
    .map(
      (msg) =>
        msg.data?.castAddBody?.embeds.map((e) => ({
          castHash: msg.hash,
          embed: e,
        })) || []
    )
    .flat()
    .filter(({ embed }) => "url" in embed && embed.url);

  if (urlEmbeds.length > 0) {
    await db
      .insertInto("castEmbedUrls")
      .values(
        urlEmbeds.map(({ embed, castHash }) => {
          return {
            castHash,
            url: embed.url!,
          };
        })
      )
      .onConflict((oc) => oc.doNothing())
      .execute();
  }
}
