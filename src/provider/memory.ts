import { UserEvt, Provider, StoredEvt } from '../types'
import { VersionError } from './error'

export function createProvider<E extends UserEvt>(initEvents?: Array<StoredEvt<E>>): Provider<E> {
  const events: Array<StoredEvt<E>> = initEvents || []
  const bms = new Map<string, number>()

  const getPosition = async (bm: string) => bms.get(bm) || 0

  const setPosition = async (bm: string, pos: number) => {
    bms.set(bm, pos)
  }

  const getEventsFrom = async (stream: string, pos: number) => {
    return events.filter(ev => ev.stream === stream && ev.position >= pos)
  }

  const getEventsFor = async (stream: string, id: string) => {
    return events.filter(ev => ev.stream === stream && ev.aggregateId === id)
  }

  const append = async (stream: string, event: E, aggregateId: string, version: number) => {
    const aggEvents = await getEventsFor(stream, aggregateId)
    for (const ev of aggEvents) {
      if (ev.version === version) throw new VersionError()
    }

    events.push({
      stream,
      event,
      version,
      position: events.length,
      aggregateId,
      timestamp: new Date(Date.now())
    })
    return
  }

  return {
    getPosition,
    setPosition,
    getEventsFor,
    getEventsFrom,
    append
  }
}
