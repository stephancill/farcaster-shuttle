import { Message } from '@farcaster/hub-nodejs'
import { formatUserDatas } from './utils'
import { AppDb } from '../db'
import { log } from '../log'

export async function insertUserDatas(msgs: Message[], db: AppDb) {
  const userDatas = formatUserDatas(msgs)

  try {
    await db
      .insertInto('userData')
      .values(userDatas)
      .onConflict((oc) =>
        oc.columns(['fid', 'type']).doUpdateSet((eb) => ({
          value: eb.ref('excluded.value'),
        }))
      )
      .execute()

    log.debug(`USER DATA INSERTED`)
  } catch (error) {
    log.error(error, 'ERROR INSERTING USER DATA')
  }
}
