import { AppDb } from "../db"
import { log } from "../log"

/**
 * Insert an event ID in the database
 * @param eventId Hub event ID
 */
export async function insertEvent(eventId: number, db: AppDb) {
  try {
    await db
      .insertInto('events')
      .values({ id: eventId })
      .onConflict((oc) => oc.column('id').doNothing())
      .execute()

    log.debug(`EVENT INSERTED -- ${eventId}`)
  } catch (error) {
    log.error(error, 'ERROR INSERTING EVENT')
  }
}

/**
 * Get the latest event ID from the database
 * @returns Latest event ID
 */
export async function getLatestEvent(db: AppDb): Promise<number | undefined> {
  try {
    const event = await db
      .selectFrom('events')
      .selectAll()
      .orderBy('id', 'desc')
      .limit(1)
      .executeTakeFirst()

    return event?.id
  } catch (error) {
    log.error(error, 'ERROR GETTING LATEST EVENT')
  }
}
