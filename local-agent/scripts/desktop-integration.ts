import { readFile } from 'node:fs/promises'
import assert from 'node:assert/strict'
const state = JSON.parse(await readFile(new URL('../.local/desktop.json', import.meta.url), 'utf8'))
const { origin, secret } = state
const ticketResponse = await fetch(origin + '/desktop/ticket', { method: 'POST', headers: { Origin: origin, Authorization: 'Bearer ' + secret } })
const { ticket } = await ticketResponse.json()
const opened = await fetch(origin + '/open?ticket=' + ticket, { redirect: 'manual' })
assert.equal(opened.status, 303)
const cookie = opened.headers.get('set-cookie')!.split(';')[0]
let id = '', cursor = 0
const outputs: { sessionId: string; stream: string; text: string }[] = []
async function call(path: string, body?: unknown) {
 const response = await fetch(origin + path, { method: body === undefined ? 'GET' : 'POST', headers: { Origin: origin, Cookie: cookie, 'Content-Type': 'application/json', 'X-Connection-Id': id }, body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(70000) })
 const data = await response.json(); if (!response.ok) throw new Error(data.error); return data
}
const status = await call('/desktop/status', {})
assert.equal(status.running, true)
id = (await call('/connect', {})).connectionId
let polling = false
const heartbeat = setInterval(async () => { if (polling) return; polling = true; try { const result = await call(`/events?after=${cursor}`); cursor = result.cursor; outputs.push(...result.events.map((e: { event: object }) => e.event)) } finally { polling = false } }, 400)
try {
 const topics = await call('/topics/list', {})
 if (!topics.some((t: {name: string}) => t.name === 'kts-practice')) await call('/topics/create', { name: 'kts-practice', partitions: 3, replicationFactor: 1 })
 const base = { topic: 'kts-practice', bootstrapServer: 'localhost:9092' }
 await call('/sessions/start', { ...base, id: 'desktop-consumer', name: 'Desktop Consumer', role: 'consumer', group: 'kts-desktop-' + Date.now(), fromBeginning: true })
 await call('/sessions/start', { ...base, id: 'desktop-producer', name: 'Desktop Producer', role: 'producer', acks: 'all', parseKey: false })
 const message = '통합 실행기 검증 ' + Date.now()
 await call('/sessions/input', { id: 'desktop-producer', text: message })
 const deadline = Date.now() + 30000
 while (Date.now() < deadline && !outputs.some(o => o.sessionId === 'desktop-consumer' && o.stream === 'stdout' && o.text.includes(message))) await new Promise(r => setTimeout(r, 300))
 assert.ok(outputs.some(o => o.sessionId === 'desktop-consumer' && o.stream === 'stdout' && o.text.includes(message)))
 console.log('PASS: 실행기 티켓 → HttpOnly 쿠키 → 통합 서버 → 실제 Kafka 한글 송수신')
} finally { clearInterval(heartbeat); await call('/disconnect', {}) }
