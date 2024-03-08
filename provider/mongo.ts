import { Collection, Timestamp, MongoError, Filter } from 'mongodb'
import { Event, StoreEvent, Provider, ErrorCallback } from '../src/types'
import { VersionError } from './error'
import { createEventsMapper, toArray } from './util'

export type Bookmark = {
  bookmark: string
  position: Timestamp
}

export type Options<E extends Event> = {
  limit?: number
  events: Collection<StoreEvent<E>> | Promise<Collection<StoreEvent<E>>>
  bookmarks: Collection<Bookmark> | Promise<Collection<Bookmark>>
  onError?: ErrorCallback
}

export function createProvider<E extends Event>(opts: Options<E>): Provider<E> {
  const events = Promise.resolve(opts.events)
  const bookmarks = Promise.resolve(opts.bookmarks)
  const onError =
    opts.onError ||
    (() => {
      /* NOOP */
    })

  const createEvents = createEventsMapper<E>(new Timestamp({ t: 0, i: 0 }))
  return {
    limit: opts.limit,
    driver: 'mongo',
    onError,
    getPosition: (bm) => getPos(bm, bookmarks),
    setPosition: (bm, pos) => setPos(bm, pos, bookmarks),
    getEventsFor: async (stream, id, fromPosition) => {
      const query: Filter<StoreEvent<E>> = {
        stream,
        aggregateId: id,
      }

      if (fromPosition !== undefined) {
        query.position = { $gt: fromPosition }
      }

      const results = await events.then((coll) => coll.find(query).sort({ position: 1 }).toArray())

      return results
    },
    getLastEventFor: async (stream, id) => {
      const query: Filter<StoreEvent<E>> = {}
      query.stream = { $in: toArray(stream) }

      if (id) {
        query.aggregateId = id
      }

      const results = await events.then((coll) =>
        coll.find(query).sort({ position: -1 }).limit(1).toArray()
      )

      return results[0]
    },

    getEventsFrom: async (stream, position, lim) =>
      events.then((coll) => {
        const filter = {
          stream: { $in: toArray(stream) },
          position: { $gt: position },
        } as Filter<StoreEvent<E>>
        const query = coll.find(filter).sort({ position: 1 })

        const limit = lim ?? opts.limit
        if (limit) {
          query.limit(limit)
        }

        return query.toArray()
      }),
    // @ts-ignore
    markEvent(stream: string | string[], aggregateId: string, position: any): Promise<void> {
      // not-implemented
    },
    createEvents,

    append: async (_stream, _aggId, _version, newEvents) => {
      try {
        await events.then((coll) => coll.insertMany(newEvents))
        return newEvents
      } catch (ex) {
        if (ex instanceof MongoError && ex.code === 11000) throw new VersionError()
        throw ex
      }
    },
  }
}

export async function migrate(
  events: Collection<StoreEvent<any>> | Promise<Collection<StoreEvent<any>>>,
  bookmarks: Collection<Bookmark> | Promise<Collection<Bookmark>>
) {
  const eventColl = await events
  const bookmarkColl = await bookmarks

  await bookmarkColl.createIndex({ bookmark: 1 }, { name: 'bookmark-index', unique: true })
  await eventColl.createIndex(
    { stream: 1, position: 1 },
    { name: 'stream-position-index', unique: true }
  )

  await eventColl.createIndex(
    { stream: 1, aggregateId: 1, version: 1 },
    { name: 'stream-id-version-index', unique: true }
  )
}

async function getPos(bm: string, bookmarks: Promise<Collection<Bookmark>>) {
  const record = await bookmarks.then((coll) => coll.findOne({ bookmark: bm }))
  if (record) return record.position
  return new Timestamp({ t: 1, i: 0 })
}

async function setPos(bm: string, pos: Timestamp, bookmarks: Promise<Collection<Bookmark>>) {
  await bookmarks.then((coll) =>
    coll.updateOne({ bookmark: bm }, { $set: { position: pos } }, { upsert: true })
  )
}
