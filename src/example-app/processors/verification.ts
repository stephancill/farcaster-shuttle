import { Message, fromFarcasterTime } from '@farcaster/hub-nodejs'
import { AppDb } from '../db'
import { formatVerifications } from './utils'
import { log } from '../log'

/**
 * Insert a new verification in the database
 * @param msg Hub event in JSON format
 */
export async function insertVerifications(msgs: Message[], db: AppDb) {
  const verifications = formatVerifications(msgs)

  try {
    await db
      .insertInto('verifications')
      .values(verifications)
      .onConflict((oc) => oc.columns(['fid', 'signerAddress']).doNothing())
      .execute()

    log.debug(`VERIFICATIONS INSERTED`)
  } catch (error) {
    log.error(error, 'ERROR INSERTING VERIFICATION')
  }
}

/**
 * Delete a verification from the database
 * @param msg Hub event in JSON format
 */
export async function deleteVerifications(msgs: Message[], db: AppDb) {
  try {
    await db.transaction().execute(async (trx) => {
      for (const msg of msgs) {
        const data = msg.data!
        const address = data.verificationRemoveBody!.address

        await trx
          .updateTable('verifications')
          .set({
            deletedAt: new Date(
              fromFarcasterTime(data.timestamp)._unsafeUnwrap()
            ),
          })
          .where('signerAddress', '=', address)
          .where('fid', '=', data.fid)
          .execute()
      }
    })

    log.debug('VERIFICATIONS DELETED')
  } catch (error) {
    log.error(error, 'ERROR DELETING VERIFICATION')
  }
}
