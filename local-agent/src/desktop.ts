import { randomBytes, createHash } from 'node:crypto'
import { mkdir, writeFile, unlink, readFile } from 'node:fs/promises'
import { fileURLToPath } from 'node:url'
import type { AddressInfo } from 'node:net'
import { createAgent } from './server.ts'
import { createDesktopServer } from './desktopServer.ts'
import { Environments } from './environments.ts'
const port = 4318, origin = `http://127.0.0.1:${port}`
const secret = randomBytes(32).toString('hex'), agentToken = randomBytes(32).toString('hex')
const statePath = fileURLToPath(new URL('../.local/desktop.json', import.meta.url))
const webRoot = fileURLToPath(new URL('../../web/dist', import.meta.url))
await readFile(webRoot + '/index.html')
const container = process.env.KAFKA_CONTAINER ?? 'ksl-kafka'
const environments = new Environments(fileURLToPath(new URL('../.local/environments.json', import.meta.url)), container)
await environments.load()
const target = () => ({ mode: 'docker' as const, container: environments.container, image: process.env.KAFKA_CLI_IMAGE ?? 'apache/kafka:4.0.0', kafkaBin: '' })
const agent = createAgent({ token: agentToken, origins: [origin], bootstrap: environments.list().selected === 'existing' ? process.env.KAFKA_BOOTSTRAP_SERVER ?? 'localhost:9092' : 'localhost:9092', runner: target() })
await new Promise<void>((resolve, reject) => { agent.server.once('error', reject); agent.server.listen(0, '127.0.0.1', resolve) })
let stopping = false
async function shutdown() {
  if (stopping) return
  stopping = true
  await agent.shutdown()
  await new Promise<void>(resolve => server.close(() => resolve()))
  try { const current = JSON.parse(await readFile(statePath, 'utf8')); if (current.secret === secret) await unlink(statePath) } catch { /* already removed */ }
}
const server = createDesktopServer({ origin, secret, agentToken, agentUrl: `http://127.0.0.1:${(agent.server.address() as AddressInfo).port}`, webRoot, broker: environments, environments, targetChanged: () => agent.setTarget(target(), environments.list().selected === 'existing' ? process.env.KAFKA_BOOTSTRAP_SERVER ?? 'localhost:9092' : 'localhost:9092'), disconnectAll: () => agent.disconnectAll(), exit: () => void shutdown().then(() => process.exit(0)) })
server.requestTimeout = 10000; server.headersTimeout = 10000
try {
  await new Promise<void>((resolve, reject) => { server.once('error', reject); server.listen(port, '127.0.0.1', resolve) })
  await mkdir(fileURLToPath(new URL('../.local', import.meta.url)), { recursive: true, mode: 0o700 })
  await writeFile(statePath, JSON.stringify({ origin, secret, identity: createHash('sha256').update(secret).digest('hex') }), { mode: 0o600 })
  console.log('Kafka Studio 준비 완료: ' + origin)
} catch (e) { await agent.shutdown(); console.error(e); process.exit(1) }
for (const signal of ['SIGTERM', 'SIGINT'] as const) process.on(signal, () => void shutdown().then(() => process.exit(0)))
