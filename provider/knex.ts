import { Knex as knex } from 'knex'
import { Event, Provider, StoreEvent, ErrorCallback } from '../src/types'
import { VersionError } from './error'
import { createEventsMapper, toArray } from './util'

export type Bookmark = {
  bookmark: string
  position: number
}

export type MigrateOptions = {
  client: knex
  events?: string
  bookmarks?: string
}

export type Options = {
  limit?: number
  onError?: ErrorCallback
  bookmarks: () => knex.QueryBuilder<any, any>
  events: () => knex.QueryBuilder<any, any>
}

export function createProvider<E extends Event>(opts: Options): Provider<E> {
  const onError =
    opts.onError ||
    (() => {
      /* NOOP */
    })
  return {
    limit: opts.limit,
    driver: 'knex',
    onError,
    getPosition: async (bm) => {
      const result = await opts.bookmarks().select().where('bookmark', bm).first()
      if (result) return result.position
      return 0
    },
    setPosition: async (bm, pos) => {
      const updates = await opts.bookmarks().update({ position: pos }).where('bookmark', bm)

      if (updates === 0) {
        await opts.bookmarks().insert({ bookmark: bm, position: pos })
      }
    },
    getEventsFor: async (stream, aggregateId, fromPosition) => {
      const query = opts
        .events()
        .select()
        .where({ stream, aggregate_id: aggregateId })
        .orderBy('version', 'asc');

      const handleOutOfOrderEvents =
        (process.env.HANDLE_OUT_OF_ORDER_EVENTS || 'no') === 'yes';
      if (handleOutOfOrderEvents || fromPosition !== undefined) {
        query.where(builder => {
          if (fromPosition !== undefined) {
            builder.where('position', '>', fromPosition)
            if (handleOutOfOrderEvents)
              builder.orWhere({processed: false})
          } else if (handleOutOfOrderEvents) {
            builder.where({processed: false})
          }
        })
      }

      const rows = await query
      return rows.map(mapToEvent)
    },
    getLastEventFor: async (stream, aggregateId) => {
      let query = opts
        .events()
        .select()
        .whereIn('stream', toArray(stream))
        .orderBy('position', 'desc')
        .limit(1)

      if (aggregateId) {
        query = query.andWhere({ aggregate_id: aggregateId })
      }

      const rows = await query
      return rows.map(mapToEvent)[0]
    },
    getEventsFrom: async (stream, position, lim) => {
      const limit = lim ?? opts.limit
      const handleOutOfOrderEvents =
        (process.env.HANDLE_OUT_OF_ORDER_EVENTS || 'no') === 'yes';
      const query = opts
        .events()
        .select()
        .whereIn('stream', toArray(stream))
        .where(builder => {
          builder.where('position', '>', position);
          if (handleOutOfOrderEvents)
            builder.orWhere({ processed: false })
        })
        .orderBy('position', 'asc')

      if (limit) query.limit(limit)

      const events = await query

      return events.map(mapToEvent)
    },
    markEvent: async (stream, aggregateId, position) => {
      await opts.events()
        .update({ processed: true })
        .whereIn('stream', toArray(stream))
        .where({  aggregate_id: aggregateId, position });
    },
    createEvents: createEventsMapper<E>(0),
    append: async (_stream, _aggregateId, _version, newEvents, trx) => {
      try {
        const toInsert = newEvents.map((storeEvent) => ({
          stream: storeEvent.stream,
          aggregate_id: storeEvent.aggregateId,
          event: JSON.stringify(storeEvent.event),
          version: storeEvent.version,
          timestamp: storeEvent.timestamp,
        }))
        const query = opts.events().insert(toInsert, ['position']);
        if (trx) {
          query.transacting(trx);
        }

        let index = 0
        const results = await query;
        for (const result of results) {
          newEvents[index].position = result
          index++
        }

        return newEvents
      } catch (ex: any) {
        // TODO: Verify version conflict error
        throw new VersionError(ex.message)
      }
    },
  }
}

export async function migrate(opts: MigrateOptions) {
  if (!opts.bookmarks && !opts.events) return

  await opts.client.transaction(async (trx) => {
    if (opts.events) {
      const eventsExists = await trx.schema.hasTable(opts.events)
      if (!eventsExists) {
        const q1 = trx.schema.createTable(opts.events, (tbl) => {
          tbl.bigIncrements('position').primary()
          tbl.integer('version')
          tbl.string('stream')
          tbl.string('aggregate_id')
          tbl.dateTime('timestamp')
          tbl.text('event')
          tbl.boolean('processed').defaultTo(false)
        })
        const q2 = trx.schema.table(opts.events, (tbl) => {
          tbl.unique(['stream', 'position'])
          tbl.unique(['stream', 'aggregate_id', 'version'])
        })
        await q1
        await q2
      }
    }

    if (opts.bookmarks) {
      const bookmarkExists = await trx.schema.hasTable(opts.bookmarks)

      if (!bookmarkExists) {
        const q1 = trx.schema.createTable(opts.bookmarks, (tbl) => {
          tbl.string('bookmark').primary()
          tbl.bigInteger('position')
        })
        await q1
      }
    }

    await trx.commit()
  })
}

function mapToEvent<E extends Event>(row: any): StoreEvent<E> {
  return {
    aggregateId: row.aggregate_id,
    event: JSON.parse(row.event),
    position: row.position,
    stream: row.stream,
    timestamp: row.timestamp,
    version: row.version,
    processed: row.processed
  }
}
