import {
  Event,
  Aggregate,
  Fold,
  Provider,
  Domain,
  Command,
  CmdBody,
  CommandHandler,
  StoreEvent,
  BaseAggregate,
  ExecutableAggregate,
  Ext,
  EventMeta,
} from './types'
import { EventHandler } from './handler'
import { toMeta } from './common'

export type DomainOptions<E extends Event, A extends Aggregate> = {
  aggregate: () => A
  stream: string
  fold: Fold<E, A>
  provider: Provider<E> | Promise<Provider<E>>
  useCache?: boolean
}

export type StreamsHandler<T extends { [key: string]: Event }> = <
  TStream extends keyof T,
  TType extends T[TStream]['type']
>(
  stream: TStream,
  type: TType,
  handler: (id: string, event: Ext<T[TStream], TType>, meta: EventMeta) => any
) => void

export function createHandler<Body extends { [key: string]: Event }>(
  bookmark: string,
  streams: Array<keyof Body>,
  provider: Provider<Event>
) {
  const handler = new EventHandler({
    bookmark,
    provider,
    stream: streams as string[],
  })

  type CB = (id: string, event: Event, meta: EventMeta) => any
  const callbacks = new Map<string, CB>()

  const handle: StreamsHandler<Body> = (stream, type, callback) => {
    callbacks.set(`${stream}-${type}`, callback as any)

    handler.handle(type, (id, event, meta) => {
      const cb = callbacks.get(`${meta.stream}-${event.type}`)
      if (!cb) return

      return cb(id, event as any, meta)
    })
  }

  return {
    handle,
    start: handler.start,
    stop: handler.stop,
    runOnce: handler.runOnce,
    run: handler.run,
    setPosition: handler.setPosition,
    getPosition: handler.getPosition,
    reset: handler.reset,
  }
}

export function createDomain<Evt extends Event, Agg extends Aggregate, Cmd extends Command>(
  opts: DomainOptions<Evt, Agg>,
  cmd: CommandHandler<Evt, Agg, Cmd>
): Domain<Evt, Agg, Cmd> {
  function handler(bookmark: string) {
    return new EventHandler({
      bookmark,
      provider: opts.provider,
      stream: opts.stream,
    })
  }

  return {
    stream: opts.stream,
    handler,
    ...wrapCmd(opts, cmd),
  }
}

function wrapCmd<E extends Event, A extends Aggregate, C extends Command>(
  opts: DomainOptions<E, A>,
  cmd: CommandHandler<E, A, C>
) {
  const keys = Object.keys(cmd) as Array<C['type']>
  const command: CmdBody<C, A & BaseAggregate> = {} as any
  const providerAsync = Promise.resolve(opts.provider)

  if ('aggregate' in cmd) {
    throw new Error(`Invalid command body: Command handler function cannot be named "aggregate"`)
  }

  const aggregateCache = new Map<
    string,
    { aggregate: A & { aggregateId: string; version: number }; position: any }
  >()

  async function getAggregate(id: string) {
    const provider = await providerAsync

    const cached = opts.useCache && aggregateCache.get(id)
    if (cached) {
      const events = await provider.getEventsFor(opts.stream, id, cached.position)
      if (!events.length) {
        return cached.aggregate
      }

      const lastEvent = events.slice(-1)[0]
      const aggregate = events.reduce(toNextAggregate, cached.aggregate)
      aggregateCache.set(id, { aggregate, position: lastEvent.position })
      return aggregate
    }

    const events = await provider.getEventsFor(opts.stream, id)

    const next = { ...opts.aggregate(), aggregateId: id, version: 0 }
    const aggregate = events.reduce(toNextAggregate, next)
    if (events.length > 0) {
      const lastEvent = events.slice(-1)[0]
      aggregateCache.set(id, { aggregate, position: lastEvent.position })
    }
    return aggregate
  }

  async function getExecAggregate(id: string) {
    const aggregate = await getAggregate(id)
    const body: ExecutableAggregate<C, A> = {} as any

    for (const type of keys) {
      body[type] = async (cmdBody) => {
        const cmdResult = await cmd[type]({ ...cmdBody, aggregateId: id, type }, aggregate)
        const nextAggregate = await handleCommandResult(cmdResult, aggregate)

        return { ...body, aggregate: nextAggregate }
      }
    }

    return { ...body, aggregate }
  }

  // Prepare the command handlers that accept an aggregateId and a command body
  for (const type of keys) {
    command[type] = async (id, body) => {
      const agg = await getAggregate(id)

      const cmdResult = await cmd[type]({ ...body, aggregateId: id, type }, agg)
      const nextAggregate = await handleCommandResult(cmdResult, agg)
      return nextAggregate
    }
  }

  async function handleCommandResult(cmdResult: E | E[] | void, aggregate: A & BaseAggregate) {
    const id = aggregate.aggregateId
    let nextAggregate = { ...aggregate }

    if (cmdResult) {
      const events = Array.isArray(cmdResult) ? cmdResult : [cmdResult]
      const provider = await providerAsync
      let nextVersion = aggregate.version + 1

      const storeEvents = await provider.append(opts.stream, id, nextVersion, events)
      const nextAggregate = storeEvents.reduce(toNextAggregate, aggregate)
      return nextAggregate
    }

    return nextAggregate
  }

  function toNextAggregate(next: A & BaseAggregate, ev: StoreEvent<E>): A & BaseAggregate {
    return {
      ...next,
      ...opts.fold(ev.event, next, toMeta(ev)),
      version: ev.version,
      aggregateId: ev.aggregateId,
    }
  }

  return { command, getAggregate: getExecAggregate }
}
