import assert from 'node:assert/strict'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import { Environments } from '../src/environments.ts'
import { command } from '../src/desktopBroker.ts'
const root = await mkdtemp(join(tmpdir(), 'kts-env-real-'))
const env = new Environments(join(root, 'environments.json'), 'ksl-kafka')
let id = ''
try {
  await env.create({ name: 'integration only', brokers: 3, partitions: 6, replication: 3, minIsr: 2, retentionHours: 24, autoCreate: false, memoryMB: 1024 })
  id = env.list().profiles[0].id; await env.select(id)
  console.log('Starting isolated cluster:', id)
  await env.start()
  const cli = (script: string, args: string[]) => command('docker', ['exec', env.container, '/opt/kafka/bin/' + script, '--bootstrap-server', 'localhost:9092', ...args], 15000)
  let ready = false
  for (let n = 0; n < 15; n++) {
    try { const brokers = await cli('kafka-broker-api-versions.sh', []); if ([1, 2, 3].every(id => brokers.includes(`(id: ${id} `))) { ready = true; break } } catch { /* broker booting */ }
    await delay(2000)
  }
  assert.ok(ready, 'All three brokers must register')
  await cli('kafka-topics.sh', ['--create', '--topic', 'environment-validation', '--partitions', '6', '--replication-factor', '3'])
  const description = await cli('kafka-topics.sh', ['--describe', '--topic', 'environment-validation'])
  assert.match(description, /ReplicationFactor:\s*3/)
  assert.equal(description.split('\n').filter(line => /Isr:\s*[123],[123],[123]/.test(line)).length, 6)
  console.log('PASS: three brokers, six partitions, replication=3 and ISR=3')
  await env.stop(); assert.equal((await env.status()).running, false)
  const restored = new Environments(join(root, 'environments.json'), 'ksl-kafka'); await restored.load(); await restored.start()
  let persisted = false
  for (let n = 0; n < 10; n++) { try { if ((await cli('kafka-topics.sh', ['--list'])).includes('environment-validation')) { persisted = true; break } } catch { /* broker booting */ }; await delay(2000) }
  assert.ok(persisted); console.log('PASS: saved selection, stop/start, topic persistence')
} finally {
  if (id) {
    for (let i = 1; i <= 3; i++) {
      const name = `${id}-b${i}`
      try { const data = JSON.parse(await command('docker', ['container', 'inspect', name]))[0]; if (data.Config.Labels['io.kafka-terminal-studio.environment'] === id) await command('docker', ['rm', '-f', '-v', name], 30000) } catch { /* absent */ }
    }
    try { await command('docker', ['network', 'rm', id]) } catch { /* absent */ }
  }
  await rm(root, { recursive: true, force: true })
}
