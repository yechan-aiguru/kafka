import { test } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, writeFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import type { AddressInfo } from 'node:net'
import { createAgent } from '../src/server.ts'
import { createDesktopServer } from '../src/desktopServer.ts'
import { Environments } from '../src/environments.ts'
import { DesktopBroker, type Command } from '../src/desktopBroker.ts'

test('desktop: one-use launch, cookie+Origin auth, static serving, proxy and stop cleanup', async () => {
  const root = await mkdtemp(join(tmpdir(), 'kts-desktop-'))
  await writeFile(join(root, 'index.html'), '<html><head></head><body>Studio</body></html>')
  const origins: string[] = []
  const agent = createAgent({ token: 'agent-secret', origins, bootstrap: 'localhost:9092', runner: { mode: 'local', container: '', image: '', kafkaBin: root } })
  await new Promise<void>(r => agent.server.listen(0, '127.0.0.1', r))
  const commands: string[][] = []
  const run: Command = async (file, args) => { commands.push([file, ...args]); return args[0] === 'inspect' ? '{"Running":true,"Status":"running"}' : 'ok' }
  const environments = new Environments(join(root, 'environments.json'), 'test-kafka', run)
  let changes = 0
  const options = { environments, targetChanged: async () => { changes++; await agent.setTarget({ mode: 'local', container: environments.container, image: '', kafkaBin: root }, 'localhost:9092') }, origin: 'http://127.0.0.1:0', secret: 'launcher-secret', agentToken: 'agent-secret', agentUrl: `http://127.0.0.1:${(agent.server.address() as AddressInfo).port}`, webRoot: root, broker: new DesktopBroker('test-kafka', run), disconnectAll: () => agent.disconnectAll(), exit: () => {} }
  const server = createDesktopServer(options)
  await new Promise<void>(r => server.listen(0, '127.0.0.1', r))
  options.origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`; origins.push(options.origin)
  const url = options.origin
  try {
    assert.match(await (await fetch(url)).text(), /kts-local-studio/)
    assert.equal((await fetch(url + '/desktop/status', { method: 'POST', headers: { Origin: url } })).status, 403)
    assert.equal((await fetch(url + '/desktop/ticket', { method: 'POST', headers: { Origin: 'https://evil.example', Authorization: 'Bearer launcher-secret' } })).status, 403)
    const launch = await fetch(url + '/desktop/ticket', { method: 'POST', headers: { Origin: url, Authorization: 'Bearer launcher-secret' } })
    const { ticket } = await launch.json()
    const opened = await fetch(url + '/open?ticket=' + ticket, { redirect: 'manual' })
    assert.equal(opened.status, 303)
    const setCookie = opened.headers.get('set-cookie')!
    assert.match(setCookie, /HttpOnly/); assert.match(setCookie, /SameSite=Strict/)
    assert.equal((await fetch(url + '/open?ticket=' + ticket)).status, 403)
    const headers = { Cookie: setCookie.split(';')[0], Origin: url, 'Content-Type': 'application/json' }
    assert.equal((await fetch(url + '/desktop/status', { method: 'POST', headers: { ...headers, Origin: 'https://evil.example' } })).status, 403)
    assert.equal((await fetch(url + '/desktop/status', { method: 'POST', headers: { Cookie: headers.Cookie } })).status, 403)
    assert.equal((await fetch(url + '/%2e%2e%2fsecret')).status, 403)
    const connected = await (await fetch(url + '/connect', { method: 'POST', headers, body: '{}' })).json()
    assert.ok(connected.connectionId)
    const eventsHeaders = { Cookie: headers.Cookie, 'Sec-Fetch-Site': 'same-origin', 'X-Connection-Id': connected.connectionId }
    assert.equal((await fetch(url + '/events', { headers: eventsHeaders })).status, 200)
    const start = await fetch(url + '/desktop/kafka/start', { method: 'POST', headers, body: '{"container":"other"}' })
    assert.equal(start.status, 200)
    assert.ok(commands.some(c => c.join(' ') === 'docker start test-kafka'))
    const stop = await fetch(url + '/desktop/kafka/stop', { method: 'POST', headers, body: '{}' })
    assert.equal(stop.status, 200)
    assert.ok(commands.some(c => c.join(' ') === 'docker stop --time 15 test-kafka'))
    assert.equal((await fetch(url + '/events', { headers: eventsHeaders })).status, 410)
    assert.equal((await fetch(url + '/desktop/environments/create', { method: 'POST', headers: { Origin: url }, body: '{}' })).status, 403)
    const saved = await (await fetch(url + '/desktop/environments/create', { method: 'POST', headers, body: JSON.stringify({ name: 'test', brokers: 1, partitions: 3, replication: 1, minIsr: 1, retentionHours: 24, autoCreate: false, memoryMB: 1024 }) })).json()
    assert.equal(saved.profiles.length, 1)
    const second = await (await fetch(url + '/connect', { method: 'POST', headers, body: '{}' })).json()
    assert.equal((await fetch(url + '/desktop/environments/select', { method: 'POST', headers, body: JSON.stringify({ id: saved.profiles[0].id }) })).status, 200)
    assert.equal(changes, 1)
    assert.equal((await fetch(url + '/events', { headers: { ...eventsHeaders, 'X-Connection-Id': second.connectionId } })).status, 410)
    assert.equal((await fetch(url + '/desktop/environments/select', { method: 'POST', headers, body: '{"id":"other"}' })).status, 400)
    assert.ok(!JSON.stringify(await (await fetch(url + '/desktop/config', { method: 'POST', headers })).json()).includes('secret'))
  } finally { await new Promise<void>(r => server.close(() => r())); await agent.shutdown(); await rm(root, { recursive: true, force: true }) }
})

test('broker errors remain actionable and commands never accept browser shell input', async () => {
  assert.throws(() => new DesktopBroker('--privileged'))
  const down = new DesktopBroker('known', async () => { throw new Error('daemon unavailable') })
  assert.equal((await down.status()).docker, false)
  await assert.rejects(down.start(), /unavailable/)
  let called = ''
  const broker = new DesktopBroker('known', async (file, args) => { called = file + ' ' + args.join(' '); if (file === 'colima') throw new Error('colima missing'); return '' })
  await assert.rejects(broker.startDocker(), /missing/); assert.equal(called, 'colima start')
})
