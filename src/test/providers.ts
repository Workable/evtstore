import * as memory from '../../provider/memory'
import { ExampleEv } from './example'
import { Provider } from '../types'

export const providers: ProviderTest[] = [
  { name: 'memory', provider: () => Promise.resolve(memory.createProvider<ExampleEv>()) }
]

type ProviderTest = {
  name: string
  provider: () => Promise<Provider<ExampleEv>>
  cache?: Provider<ExampleEv>
}
