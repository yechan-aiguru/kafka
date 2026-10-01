import { makeCase, probe } from '../../web/src/features/experiments/cases.ts'
import type { ExecutionClient } from '../../web/src/execution/types.ts'
import { readFile } from 'node:fs/promises'
import assert from 'node:assert/strict'
const state = JSON.parse(await readFile(new URL('../.local/desktop.json', import.meta.url), 'utf8'))
const { origin, secret } = state
const ticketResponse = await fetch(origin + '/desktop/ticket', { method: 'POST', headers: { Origin: origin, Authorization: 'Bearer ' + secret } })
const { ticket } = await ticketResponse.json()
const opened = await fetch(origin + '/open?ticket=' + ticket, { redirect: 'manual' })
assert.equal(opened.status, 303)
const cookie = opened.headers.get('set-cookie')!.split(';')[0]
let id = ''
async function call(path: string, body?: unknown) {
 const response = await fetch(origin + path, { method: body === undefined ? 'GET' : 'POST', headers: { Origin: origin, Cookie: cookie, 'Content-Type': 'application/json', 'X-Connection-Id': id }, body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(70000) })
 const data = await response.json(); if (!response.ok) throw new Error(data.error); return data
}
const status = await call('/desktop/status', {})
assert.equal(status.running, true)
id = (await call('/connect', {})).connectionId
let polling = false
const heartbeat = setInterval(async () => { if (polling) return; polling = true; try { await call('/events') } catch { /* main request reports errors */ } finally { polling = false } }, 500)
const client: ExecutionClient = {
 mode: 'local', describeTopic: name => call('/topics/describe', { name }), listTopics: () => call('/topics/list', {}),
 clusterInfo: () => call('/cluster/info', {}), createTopic: topic => call('/topics/create', topic),
 increasePartitions: (name, count) => call('/topics/increase', { name, count }), startSession: session => call('/sessions/start', session),
 stopSession: id => call('/sessions/stop', { id }), sendInput: (id, text) => call('/sessions/input', { id, text }),
 describeGroup: group => call('/groups/describe', { group }), resetOffsets: (group, topic) => call('/groups/reset', { group, topic }),
 subscribe: () => () => {}, dispose: () => {},
}
try {
 const invalid = makeCase('invalid', Date.now().toString(36))
 await client.createTopic(invalid.topics[0])
 for (const check of ['duplicate', 'decrease', 'replicas', 'stopped'] as const) {
   const result = await probe(client, check, invalid)
   console.log(check, result.outcome, result.message.slice(0, 500))
   assert.equal(result.outcome, 'expected')
 }
 const keyed = makeCase('key', Date.now().toString(36))
 await client.createTopic(keyed.topics[0]); await client.startSession(keyed.sessions[0])
 assert.equal((await probe(client, 'bad-key', keyed)).outcome, 'expected')
 console.log('PASS: 실제 Kafka / Studio 사전 검증의 오류 검사 5종', invalid.topics[0].name, keyed.topics[0].name)
} finally { clearInterval(heartbeat); await call('/disconnect', {}) }
