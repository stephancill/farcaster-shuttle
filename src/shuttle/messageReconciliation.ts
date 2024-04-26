import { HubRpcClient, Message, MessageType } from "@farcaster/hub-nodejs";
import { DB, MessageRow } from "./db";
import { pino } from "pino";

const MAX_PAGE_SIZE = 3_000;

type MessageWithSoftDelete = Message & { missingInDb?:boolean, wasPruned?: boolean, wasRevoked?: boolean };

// Ensures that all messages for a given FID are present in the database. Can be used for both backfilling and reconciliation.
export class MessageReconciliation {
  private client: HubRpcClient;
  private db: DB;
  private log: pino.Logger;

  constructor(client: HubRpcClient, db: DB, log: pino.Logger) {
    this.client = client;
    this.db = db;
    this.log = log;
  }

  async reconcileMessagesForFid(
    fid: number,
    onHubMessages: (messages: MessageWithSoftDelete[], type: MessageType) => Promise<void>,
  ) {
    for (const type of [
        MessageType.CAST_ADD,
        MessageType.REACTION_ADD,
        MessageType.LINK_ADD,
        MessageType.VERIFICATION_ADD_ETH_ADDRESS,
        MessageType.USER_DATA_ADD,
      ]) {
      // this.log.info(`Reconciling messages for FID ${fid} of type ${type}`);
      await this.reconcileMessagesOfTypeForFid(fid, type, onHubMessages);
    }
        
    this.log.info(`Reconciliation for FID ${fid} complete`);
  }

  async reconcileMessagesOfTypeForFid(
    fid: number,
    type: MessageType,
    // onHubMessage: (message: Message, missingInDb: boolean, prunedInDb: boolean, revokedInDb: boolean) => Promise<void>,
    onHubMessages: (messages: MessageWithSoftDelete[], type: MessageType) => Promise<void>,
  ) {
    // todo: Username proofs, and on chain events

    const hubMessagesByHash: Record<string, Message> = {};
    for await (const messages of this.allHubMessagesOfTypeForFid(fid, type)) {
      const messageHashes = messages.map((msg) => msg.hash);

      if (messageHashes.length === 0) {
        // this.log.info(`No messages of type ${type} for FID ${fid}`);
        continue;
      }

      const dbMessages = await this.db
        .selectFrom("messages")
        .select(["prunedAt", "revokedAt", "hash", "fid", "type", "raw"])
        .where("hash", "in", messageHashes)
        .execute();

      const dbMessageHashes = dbMessages.reduce((acc, msg) => {
        const key = Buffer.from(msg.hash).toString("hex");
        acc[key] = msg;
        return acc;
      }, {} as Record<string, Pick<MessageRow, "revokedAt" | "prunedAt" | "fid" | "type" | "hash" | "raw">>);

      const hubMessages: MessageWithSoftDelete[] = [];

      for (const message of messages) {
        let messageWithSoftDelete: MessageWithSoftDelete = {...message}

        const msgHashKey = Buffer.from(message.hash).toString("hex");
        hubMessagesByHash[msgHashKey] = message;

        const dbMessage = dbMessageHashes[msgHashKey];
        if (dbMessage === undefined) {
          // await onHubMessage(message, true, false, false);
          hubMessages.push({...messageWithSoftDelete, missingInDb: true});
        } else {
          let wasPruned = false;
          let wasRevoked = false;
          if (dbMessage?.prunedAt) {
            wasPruned = true;
          }
          if (dbMessage?.revokedAt) {
            wasRevoked = true;
          }
          hubMessages.push({...messageWithSoftDelete, missingInDb: false, wasPruned, wasRevoked});
          // await onHubMessage(message, false, wasPruned, wasRevoked);
        }
      }

      await onHubMessages(hubMessages, type);
    }
  }

  private async *allHubMessagesOfTypeForFid(fid: number, type: MessageType) {
    let start = Date.now()
    let fn;
    switch (type) {
      case MessageType.CAST_ADD:
        fn = this.getAllCastMessagesByFidInBatchesOf;
        break;
      case MessageType.REACTION_ADD:
        fn = this.getAllReactionMessagesByFidInBatchesOf;
        break;
      case MessageType.LINK_ADD:
        fn = this.getAllLinkMessagesByFidInBatchesOf;
        break;
      case MessageType.VERIFICATION_ADD_ETH_ADDRESS:
        fn = this.getAllVerificationMessagesByFidInBatchesOf;
        break;
      case MessageType.USER_DATA_ADD:
        fn = this.getAllUserDataMessagesByFidInBatchesOf;
        break;
      default:
        throw `Unknown message type ${type}`;
    }
    for await (const messages of fn.call(this, fid, MAX_PAGE_SIZE)) {
      yield messages as Message[];
    }
  }

  private async *getAllCastMessagesByFidInBatchesOf(fid: number, pageSize: number | undefined) {
    let result = await this.client.getCastsByFid({ pageSize, fid });
    for (;;) {
      if (result.isErr()) {
        throw new Error(`Unable to get all casts for FID ${fid}: ${result.error?.message}`);
      }

      const { messages, nextPageToken: pageToken } = result.value;

      yield messages;

      if (!pageToken?.length) break;
      result = await this.client.getCastsByFid({ pageSize, pageToken, fid });
    }
  }

  private async *getAllReactionMessagesByFidInBatchesOf(fid: number, pageSize: number | undefined) {
    let result = await this.client.getReactionsByFid({ pageSize, fid });
    for (;;) {
      if (result.isErr()) {
        throw new Error(`Unable to get all reactions for FID ${fid}: ${result.error?.message}`);
      }

      const { messages, nextPageToken: pageToken } = result.value;

      yield messages;

      if (!pageToken?.length) break;
      result = await this.client.getReactionsByFid({ pageSize, pageToken, fid });
    }
  }

  private async *getAllLinkMessagesByFidInBatchesOf(fid: number, pageSize: number | undefined) {
    let result = await this.client.getLinksByFid({ pageSize, fid });
    for (;;) {
      if (result.isErr()) {
        throw new Error(`Unable to get all links for FID ${fid}: ${result.error?.message}`);
      }

      const { messages, nextPageToken: pageToken } = result.value;

      yield messages;

      if (!pageToken?.length) break;
      result = await this.client.getLinksByFid({ pageSize, pageToken, fid });
    }
  }

  private async *getAllVerificationMessagesByFidInBatchesOf(fid: number, pageSize: number | undefined) {
    let result = await this.client.getVerificationsByFid({ pageSize, fid });
    for (;;) {
      if (result.isErr()) {
        throw new Error(`Unable to get all verifications for FID ${fid}: ${result.error?.message}`);
      }

      const { messages, nextPageToken: pageToken } = result.value;

      yield messages;

      if (!pageToken?.length) break;
      result = await this.client.getVerificationsByFid({ pageSize, pageToken, fid });
    }
  }

  private async *getAllUserDataMessagesByFidInBatchesOf(fid: number, pageSize: number | undefined) {
    let result = await this.client.getUserDataByFid({ pageSize, fid });
    for (;;) {
      if (result.isErr()) {
        throw new Error(`Unable to get all user data messages for FID ${fid}: ${result.error?.message}`);
      }

      const { messages, nextPageToken: pageToken } = result.value;

      yield messages;

      if (!pageToken?.length) break;
      result = await this.client.getUserDataByFid({ pageSize, pageToken, fid });
    }
  }
}
