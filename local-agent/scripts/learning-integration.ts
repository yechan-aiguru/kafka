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
  const topic = `kts-learning-${Date.now()}`, group = topic + '-group'
  await call('/topics/create', { name: topic, partitions: 3, replicationFactor: 1 })
  const common = { topic, bootstrapServer: connected.bootstrapServer }
  const consumer = { ...common, id: 'learn-c1', name: 'Learn C1', role: 'consumer', group, fromBeginning: true, assignor: 'round-robin' }
  await call('/sessions/start', consumer)
  await call('/sessions/start', { ...common, id: 'learn-p', name: 'Learn P', role: 'producer', acks: 'all', parseKey: true, partitioner: 'round-robin' })
  for (let i = 0; i < 9; i++) await call('/sessions/input', { id: 'learn-p', text: `same-key:한글-${i}` })
  async function waitFor(check: () => Promise<boolean>) { const deadline = Date.now() + 35000; while (Date.now() < deadline) { if (await check()) return; await new Promise(r => setTimeout(r, 1000)) } throw new Error('조건 대기 시간 초과') }
  await waitFor(async () => events.some(e => e.sessionId === 'learn-c1' && e.stream === 'stdout' && e.text?.includes('한글-8')))
  let state = await call('/groups/describe', { group })
  assert.equal(state.partitions.length, 3)
  assert.ok(state.partitions.every((p: { consumerId: string }) => p.consumerId.includes('learn-c1')))
  await call('/sessions/start', { ...consumer, id: 'learn-c2', name: 'Learn C2' })
  await waitFor(async () => { state = await call('/groups/describe', { group }); return new Set(state.partitions.map((p: { consumerId: string }) => p.consumerId)).size === 2 })
  await assert.rejects(call('/groups/reset', { group, topic }), /Consumer|Empty/)
  await call('/sessions/stop', { id: 'learn-c1' }); await call('/sessions/stop', { id: 'learn-c2' })
  await call('/sessions/stop', { id: 'learn-p' })
  await call('/groups/reset', { group, topic })
  state = await call('/groups/describe', { group })
  assert.ok(state.partitions.every((p: { currentOffset: number }) => p.currentOffset === 0))
  assert.equal(state.partitions.reduce((sum: number, p: { lag: number }) => sum + p.lag, 0), 9)
  const endOffsets = state.partitions.map((p: { logEndOffset: number }) => p.logEndOffset)
  events.length = 0
  await call('/sessions/start', consumer)
  await waitFor(async () => events.some(e => e.sessionId === 'learn-c1' && e.stream === 'stdout' && e.text?.includes('한글-8')))
  await call('/sessions/stop', { id: 'learn-c1' })
  console.log('PASS: 실제 RoundRobin, classic assignor, 1→2 consumer rebalance, active reset 차단, inactive reset, lag=9, replay', { topic, group, endOffsets })
} finally {
  clearInterval(heartbeat)
  await call('/disconnect', {})
}
