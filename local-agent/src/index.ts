import { randomBytes } from 'node:crypto'
import { mkdir, writeFile } from 'node:fs/promises'
import { fileURLToPath } from 'node:url'
import { createAgent } from './server.ts'

const token = randomBytes(32).toString('hex')
const tokenPath = fileURLToPath(new URL('../.local/token', import.meta.url))
const mode = process.env.KAFKA_MODE ?? 'docker'
if (!['docker', 'local'].includes(mode)) throw new Error('KAFKA_MODE는 docker 또는 local이어야 합니다.')
if (mode === 'local' && !process.env.KAFKA_BIN) throw new Error('로컬 설치의 bin 디렉터리를 KAFKA_BIN으로 설정하세요.')
const origins = (process.env.ALLOWED_ORIGINS ?? 'http://127.0.0.1:5173,http://localhost:5173').split(',')
const port = Number(process.env.PORT ?? 4317)
const agent = createAgent({ token, origins, bootstrap: process.env.KAFKA_BOOTSTRAP_SERVER ?? 'localhost:9092', runner: {
  mode: mode as 'docker' | 'local', container: process.env.KAFKA_CONTAINER ?? 'ksl-kafka', image: process.env.KAFKA_CLI_IMAGE ?? 'apache/kafka:4.0.0', kafkaBin: process.env.KAFKA_BIN ?? '',
} })
agent.server.on('error', error => { console.error(error.message); process.exitCode = 1 })
agent.server.listen(port, '127.0.0.1', async () => {
  await mkdir(fileURLToPath(new URL('../.local', import.meta.url)), { recursive: true, mode: 0o700 })
  await writeFile(tokenPath, token, { mode: 0o600 })
  console.log(`Kafka local agent: http://127.0.0.1:${port} (${mode})\n연결 토큰 파일: ${tokenPath}\n허용 Origin: ${origins.join(', ')}`)
})
let closing = false
for (const signal of ['SIGINT', 'SIGTERM'] as const) process.on(signal, () => {
  if (closing) return
  closing = true
  void agent.shutdown().then(() => process.exit(0))
})
