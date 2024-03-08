import { Event, Provider, StoreEvent, ErrorCallback } from '../src/types'
import { VersionError } from './error'
import { createEventsMapper, toArray } from './util'

export function createProvider<E extends Event>(
  initEvents?: Array<StoreEvent<E>>,
  onError: ErrorCallback = () => {}
): Provider<E> {
  const events: Array<StoreEvent<E>> = initEvents || []
  const bms = new Map<string, number>()
  let position = 0

  const getPosition = async (bm: string) => bms.get(bm) || 0

  const setPosition = async (bm: string, pos: number) => {
    bms.set(bm, pos)
  }

  const getEventsFrom = async (stream: string | string[], pos: number, limit: number) => {
    const results = events.filter((ev) => toArray(stream).includes(ev.stream) && ev.position > pos)
    return limit ? results.slice(0, limit) : results
  }

  const getLastEventFor = async (stream: string | string[], id?: string) => {
    const streams = toArray(stream)

    for (let i = events.length - 1; i >= 0; i--) {
      const evt = events[i]
      if (id && streams.includes(evt.stream) && evt.aggregateId === id) return evt
      if (streams.includes(evt.stream)) return evt
    }
  }

  const getEventsFor = async (stream: string, id: string, fromPosition?: number) => {
    const filter =
      fromPosition === undefined
        ? (ev: StoreEvent<E>) => ev.stream === stream && ev.aggregateId === id
        : (ev: StoreEvent<E>) =>
            ev.stream === stream && ev.aggregateId === id && ev.position > fromPosition

    return events.filter(filter)
  }

  const createEvents = createEventsMapper<E>(0)

  const append = async (
    stream: string,
    aggregateId: string,
    version: number,
    newEvents: StoreEvent<E>[]
  ) => {
    const aggEvents = await getEventsFor(stream, aggregateId)
    for (const ev of aggEvents) {
      if (ev.version === version) throw new VersionError()
    }

    const storeEvents: Array<StoreEvent<E>> = newEvents.map((event) => ({
      ...event,
      position: ++position,
    }))

    events.push(...storeEvents)
    return storeEvents
  }

  return {
    driver: 'memory',
    onError,
    getPosition,
    setPosition,
    getEventsFor,
    getEventsFrom,
    getLastEventFor,
    // @ts-ignore
    markEvent(stream: string | string[], aggregateId: string, position: any): Promise<void> {
      throw Error('not implemented')
    },
    createEvents,
    append,
  }
}
