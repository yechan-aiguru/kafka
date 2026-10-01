import { readFile } from 'node:fs/promises'
import assert from 'node:assert/strict'
const token = (await readFile(new URL('../.local/token', import.meta.url), 'utf8')).trim()
const base = 'http://127.0.0.1:4317'
let id = '', cursor = 0, polling = false
async function call(path: string, data?: unknown) {
  const response = await fetch(base + path, { method: data === undefined ? 'GET' : 'POST', headers: { Origin: 'http://127.0.0.1:5173', Authorization: `Bearer ${token}`, 'Content-Type': 'application/json', ...(id ? { 'X-Connection-Id': id } : {}) }, body: data === undefined ? undefined : JSON.stringify(data), signal: AbortSignal.timeout(70000) })
  const result = await response.json()
  if (!response.ok) throw new Error(result.error)
  return result
}
id = (await call('/connect', {})).connectionId
const heartbeat = setInterval(async () => {
  if (polling) return
  polling = true
  try { const result = await call(`/events?after=${cursor}`); cursor = result.cursor }
  catch { /* main request reports failures */ } finally { polling = false }
}, 300)
try {
  const cluster = await call('/cluster/info', {})
  assert.ok(cluster.brokerIds.length >= 1)
  const name = `kts-partitions.${Date.now()}`
  await assert.rejects(call('/topics/create', { name: name + '-invalid', partitions: 1, replicationFactor: cluster.brokerIds.length + 1 }), /브로커 수/)
  await call('/topics/create', { name, partitions: 1, replicationFactor: 1 })
  const before = await call('/topics/describe', { name })
  assert.equal(before.partitions, 1)
  assert.equal(before.partitionDetails.length, 1)
  assert.ok(before.partitionDetails[0].isr.length > 0)
  await call('/topics/increase', { name, count: 3 })
  const after = await call('/topics/describe', { name })
  assert.equal(after.partitions, 3)
  assert.equal(after.partitionDetails.length, 3)
  assert.ok(after.partitionDetails.every((p: { leader: number; replicas: number[] }) => p.leader >= 0 && p.replicas.length === 1))
  await assert.rejects(call('/topics/increase', { name, count: 2 }), /큰 파티션/)
  await assert.rejects(call('/topics/create', { name, partitions: 1, replicationFactor: 1 }))
  console.log(`PASS: ${name} · 브로커 ${cluster.brokerIds.length}개 · 생성 1 → 증가 3 · Leader/Replicas/ISR · 복제 수/감소/중복 검증`)
} finally { clearInterval(heartbeat); await call('/disconnect', {}) }
