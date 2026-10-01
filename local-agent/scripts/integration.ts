import { readFile } from 'node:fs/promises'
import assert from 'node:assert/strict'
const token = (await readFile(new URL('../.local/token', import.meta.url), 'utf8')).trim()
const base = 'http://127.0.0.1:4317'
let id = '', cursor = 0
const events: { type: string; sessionId?: string; stream?: string; text?: string; status?: string }[] = []
async function call(path: string, data?: unknown) {
  const response = await fetch(base + path, { method: data === undefined ? 'GET' : 'POST', headers: { Origin: 'http://127.0.0.1:5173', Authorization: `Bearer ${token}`, 'Content-Type': 'application/json', ...(id ? { 'X-Connection-Id': id } : {}) }, body: data === undefined ? undefined : JSON.stringify(data), signal: AbortSignal.timeout(35000) })
  const result = await response.json()
  if (!response.ok) throw new Error(result.error)
  return result
}
const connected = await call('/connect', {})
id = connected.connectionId
let polling = false
const heartbeat = setInterval(async () => {
  if (polling) return
  polling = true
  try { const result = await call(`/events?after=${cursor}`); cursor = result.cursor; events.push(...result.events.map((item: { event: object }) => item.event)) }
  catch { /* main request reports connection failure */ }
  finally { polling = false }
}, 300)
try {
  const topics = await call('/topics/list', {}) as { name: string }[]
  if (!topics.some(topic => topic.name === 'kts-practice')) await call('/topics/create', { name: 'kts-practice', partitions: 3, replicationFactor: 1 })
  const common = { topic: 'kts-practice', bootstrapServer: connected.bootstrapServer }
  await call('/sessions/start', { ...common, id: 'check-consumer', name: 'Check consumer', role: 'consumer', group: `kts-check-${Date.now()}`, fromBeginning: true })
  await call('/sessions/start', { ...common, id: 'check-producer', name: 'Check producer', role: 'producer', acks: 'all', parseKey: true })
  const message = `한글 실제 Kafka 검증 ${Date.now()}`
  await call('/sessions/input', { id: 'check-producer', text: `order-1:${message}` })
  const deadline = Date.now() + 30000
  while (Date.now() < deadline && !events.some(e => e.sessionId === 'check-consumer' && e.stream === 'stdout' && e.text?.includes(message))) await new Promise(resolve => setTimeout(resolve, 300))
  assert.ok(events.some(e => e.sessionId === 'check-consumer' && e.stream === 'stdout' && e.text?.includes(message)), '실제 Consumer 수신 실패: ' + events.filter(e => e.stream === 'stderr').map(e => e.text).join('\n'))
  await call('/sessions/stop', { id: 'check-producer' })
  await call('/sessions/stop', { id: 'check-consumer' })
  await assert.rejects(call('/sessions/input', { id: 'check-producer', text: 'after-stop' }))
  console.log('PASS: 실제 토픽 조회/준비, Producer key 입력, Consumer 한글 수신, 중지 후 입력 차단')
} finally {
  clearInterval(heartbeat)
  await call('/disconnect', {})
}
