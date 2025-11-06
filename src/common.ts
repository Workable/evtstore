import { EventMeta, Provider, StoreEvent, Event } from './types'
import { Knex } from 'knex'

export function toMeta(ev: StoreEvent<any>): EventMeta {
  return {
    aggregateId: ev.aggregateId,
    position: ev.position,
    stream: ev.stream,
    timestamp: new Date(ev.timestamp),
    version: ev.version,
    processed: ev.processed
  }
}

export const MemoryBookmark = '@@MemoryBookmark'

export async function getAllEventsFor<E extends Event>(
  provider: Provider<any>,
  stream: string,
  id: string,
  from?: any,
  trx?: Knex.Transaction
) {
  const events: StoreEvent<E>[] = []
  let current = from
  do {
    const stored = await provider.getEventsFor(stream, id, current, trx)
    events.push(...stored)
    if (stored.length === 0) return events
    if (!provider.limit) return events
    if (stored.length < provider.limit) return events

    const last = stored.slice(-1)[0]
    current = last.position
  } while (true)
}
