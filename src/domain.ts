import {
  UserEvt,
  UserAgg,
  Fold,
  Provider,
  Domain,
  Command,
  UserCmd,
  CmdBody,
  BaseAgg,
} from './types'
import { Handler } from './handler'

type DomainOptions<E extends UserEvt, A extends UserAgg> = {
  aggregate: () => A
  stream: string
  fold: Fold<E, A>
  provider: Provider<E>
}

export function createDomain<
  Evt extends UserEvt,
  Agg extends UserAgg,
  Cmd extends UserCmd
>(
  opts: DomainOptions<Evt, Agg>,
  cmd: Command<Evt, Agg, Cmd>
): Domain<Evt, Agg, Cmd> {
  function handler(bookmark: string) {
    return new Handler({
      bookmark,
      provider: opts.provider,
      stream: opts.stream,
    })
  }

  return {
    handler,
    ...wrapCmd(opts, cmd),
  }
}

function wrapCmd<E extends UserEvt, A extends UserAgg, C extends UserCmd>(
  opts: DomainOptions<E, A>,
  cmd: Command<E, A, C>
) {
  const keys = Object.keys(cmd) as Array<C['type']>
  const command: CmdBody<C> = {} as any
  const cache = new Map<string, A & BaseAgg>()

  async function getAggregate(id: string) {
    {
      const agg = cache.get(id)
      if (agg) return agg
    }

    const events = await opts.provider.getEventsFor(opts.stream, id)
    const next = { ...opts.aggregate(), aggregateId: id, version: 0 }
    const agg = events.reduce((next, ev, version) => {
      return { ...next, ...opts.fold(ev.event, next), version: version + 1 }
    }, next)
    cache.set(id, agg)
    return agg
  }

  function updateAgg(ev: E, agg: A & BaseAgg) {
    const next = { ...agg, ...opts.fold(ev, agg), version: agg.version + 1 }
    cache.set(agg.aggregateId, next)
  }

  for (const type of keys) {
    command[type] = async (id, body) => {
      const agg = await getAggregate(id)
      const result = await cmd[type]({ ...body, aggregateId: id }, agg)

      if (result) {
        await opts.provider.append(
          opts.stream,
          { ...result, aggregateId: id },
          agg.version
        )
        updateAgg(result, agg)
      }
    }
  }

  return { command, getAggregate }
}
