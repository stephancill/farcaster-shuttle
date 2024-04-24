import {
  HubEvent,
  isCastAddMessage,
  isCastRemoveMessage,
  isLinkAddMessage,
  isLinkRemoveMessage,
  isMergeMessageHubEvent,
  isPruneMessageHubEvent,
  isReactionAddMessage,
  isReactionRemoveMessage,
  isRevokeMessageHubEvent,
  isVerificationAddAddressMessage,
  isVerificationRemoveMessage,
  Message,
  MessageType,
} from "@farcaster/hub-nodejs";
import { DB } from "./db";
import { MessageProcessor } from "./messageProcessor";
import { MessageHandler, MessageState, StoreMessageOperation } from "./";
import { log } from "../log";

export class HubEventProcessor {
  static async processHubEvent(db: DB, event: HubEvent, handler: MessageHandler) {
    if (isMergeMessageHubEvent(event)) {
      await this.processMessage(
        db,
        event.mergeMessageBody.message,
        handler,
        "merge",
        event.mergeMessageBody.deletedMessages,
      );
    } else if (isRevokeMessageHubEvent(event)) {
      await this.processMessage(db, event.revokeMessageBody.message, handler, "revoke");
    } else if (isPruneMessageHubEvent(event)) {
      await this.processMessage(db, event.pruneMessageBody.message, handler, "prune");
    }
  }

  static async handleMissingMessage(db: DB, message: Message, handler: MessageHandler) {
    await this.processMessage(db, message, handler, "merge", [], true);
  }

  static async handleMissingMessagesOfType(db: DB, messages: Message[], type: MessageType, handler: MessageHandler) {
    await this.processMessagesOfType(db, messages, type, handler, "merge");
  }

  private static async processMessagesOfType(
    db: DB,
    messages: Message[],
    type: MessageType,
    handler: MessageHandler,
    operation: StoreMessageOperation,
  ) {
    await MessageProcessor.storeMessages(messages, db, log);
    await handler.handleMessagesMergeOfType(messages, type, db);
  }

  private static async processMessage(
    db: DB,
    message: Message,
    handler: MessageHandler,
    operation: StoreMessageOperation,
    deletedMessages: Message[] = [],
    wasMissed = false,
  ) {
    await db.transaction().execute(async (trx) => {
      if (deletedMessages.length > 0) {
        for (const deletedMessage of deletedMessages) {
          const isNew = await MessageProcessor.storeMessage(deletedMessage, trx, "delete", log);

          if (isNew === null)
            continue;

          const state = this.getMessageState(deletedMessage, "delete");
          // Log info and handle the message if needed
          // log.info(`Handling deleted message ${deletedMessage.data?.fid}`)
          await handler.handleMessageMerge(deletedMessage, trx, "delete", state, isNew, wasMissed);
        }
      }
      const isNew = await MessageProcessor.storeMessage(message, trx, operation, log);
      if (isNew === null)
        return;
      const state = this.getMessageState(message, operation);
      await handler.handleMessageMerge(message, trx, operation, state, isNew, wasMissed);
    });
  }

  public static getMessageState(message: Message, operation: StoreMessageOperation): MessageState {
    const isAdd = operation === "merge";
    // Casts
    if (isAdd && isCastAddMessage(message)) {
      return "created";
    } else if ((isAdd && isCastRemoveMessage(message)) || (!isAdd && isCastAddMessage(message))) {
      return "deleted";
    }
    // Links
    if (isAdd && isLinkAddMessage(message)) {
      return "created";
    } else if ((isAdd && isLinkRemoveMessage(message)) || (!isAdd && isLinkAddMessage(message))) {
      return "deleted";
    }
    // Reactions
    if (isAdd && isReactionAddMessage(message)) {
      return "created";
    } else if ((isAdd && isReactionRemoveMessage(message)) || (!isAdd && isReactionAddMessage(message))) {
      return "deleted";
    }
    // Verifications
    if (isAdd && isVerificationAddAddressMessage(message)) {
      return "created";
    } else if (
      (isAdd && isVerificationRemoveMessage(message)) ||
      (!isAdd && isVerificationAddAddressMessage(message))
    ) {
      return "deleted";
    }

    // The above are 2p sets, so we have the consider whether they are add or remove messages to determine the state
    // The rest are 1p sets, so we can just check the operation

    return isAdd ? "created" : "deleted";
  }
}
