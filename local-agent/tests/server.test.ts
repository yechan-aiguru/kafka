import { test } from 'node:test'
import assert from 'node:assert/strict'
import { createAgent } from '../src/server.ts'
import type { AddressInfo } from 'node:net'

test('Origin/토큰 인증과 연결별 세션 격리, 만료', async () => {
  const agent = createAgent({ token: 'test-token', origins: ['http://localhost:5173'], bootstrap: 'localhost:9092', runner: { mode: 'local', kafkaBin: '/nonexistent', container: '', image: '' }, leaseMs: 10 })
  await new Promise<void>(resolve => agent.server.listen(0, '127.0.0.1', resolve))
  const base = `http://127.0.0.1:${(agent.server.address() as AddressInfo).port}`
  const headers = { Origin: 'http://localhost:5173', Authorization: 'Bearer test-token', 'Content-Type': 'application/json' }
  try {
    assert.equal((await fetch(base + '/connect', { method: 'POST', headers: { ...headers, Origin: 'https://evil.example' } })).status, 403)
    assert.equal((await fetch(base + '/connect', { method: 'POST', headers: { ...headers, Authorization: 'Bearer bad' } })).status, 401)
    const response = await fetch(base + '/connect', { method: 'POST', headers })
    const { connectionId } = await response.json() as { connectionId: string }
    assert.ok(connectionId)
    assert.equal((await fetch(base + '/events', { headers: { ...headers, 'X-Connection-Id': 'wrong' } })).status, 410)
    assert.equal((await fetch(base + '/events', { headers: { ...headers, 'X-Connection-Id': connectionId } })).status, 200)
    await new Promise(resolve => setTimeout(resolve, 1100))
    assert.equal((await fetch(base + '/events', { headers: { ...headers, 'X-Connection-Id': connectionId } })).status, 410)
  } finally { await agent.shutdown() }
})
