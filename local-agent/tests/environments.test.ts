import { test } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm, readFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Environments, parseEnvironment } from '../src/environments.ts'
import type { Command } from '../src/desktopBroker.ts'
const settings = { name: '3 brokers', brokers: 3, partitions: 6, replication: 3, minIsr: 2, retentionHours: 24, autoCreate: false, memoryMB: 1024 }
test('environment validation rejects invalid replication, ISR, resource limits and shell-shaped input', () => {
  for (const change of [{ brokers: 2 }, { replication: 4 }, { minIsr: 4 }, { partitions: 0 }, { retentionHours: 721 }, { memoryMB: 512 }, { memoryMB: 1200 }, { autoCreate: 'false' }, { name: '' }, { brokers: '3;docker stop x' }]) assert.throws(() => parseEnvironment({ ...settings, ...change }))
})
test('saved isolated environments provision idempotently and never stop an unowned resource', async () => {
  const root = await mkdtemp(join(tmpdir(), 'kts-env-test-'))
  const resources = new Map<string, Record<string, unknown>>()
  const calls: string[][] = []
  const run: Command = async (_file, args) => {
    calls.push(args)
    if (args[0] === 'info') return args.includes('{{.MemTotal}}') ? String(8 * 1024 ** 3) : '28'
    if (args[0] === 'image') return '[]'
    if (args[1] === 'inspect') { const r = resources.get(args[2]); if (!r) throw new Error('No such ' + args[0]); return JSON.stringify([r]) }
    if (args[0] === 'network') { const id = args.at(-1)!; resources.set(id, { Labels: { 'io.kafka-terminal-studio.environment': id } }); return id }
    if (args[0] === 'create') { const name = args[args.indexOf('--name') + 1], id = args[args.indexOf('--network') + 1]; resources.set(name, { Config: { Labels: { 'io.kafka-terminal-studio.environment': id } }, State: { Status: 'created' } }); return name }
    if (args[0] === 'start' || args[0] === 'stop') { resources.get(args.at(-1)!)!.State = { Status: args[0] === 'start' ? 'running' : 'exited' }; return 'ok' }
    throw new Error('Unexpected command ' + args.join(' '))
  }
  try {
    const path = join(root, 'environments.json'), env = new Environments(path, 'ksl-kafka', run)
    await env.load(); assert.equal(env.container, 'ksl-kafka')
    await env.create(settings); await assert.rejects(env.create(settings), /이름/)
    const id = env.list().profiles[0].id
    await env.select(id); assert.equal(env.container, id + '-b1')
    await assert.rejects(env.select('other'), /등록/)
    assert.equal((await env.start()).running, true)
    await env.start(); assert.equal(calls.filter(c => c[0] === 'create').length, 3)
    const create = calls.find(c => c[0] === 'create')!
    assert.ok(create.includes('KAFKA_MIN_INSYNC_REPLICAS=2'))
    assert.ok(create.includes('KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://' + id + '-b1:9092'))
    assert.ok(!create.includes('-p')); assert.ok(!calls.some(c => c.includes('ksl-kafka')))
    const restored = new Environments(path, 'ksl-kafka', run); await restored.load(); assert.equal(restored.container, env.container)
    await restored.stop(); assert.equal((await restored.status()).running, false)
    resources.get(id + '-b3')!.Config = { Labels: {} }
    const stopCount = calls.filter(c => c[0] === 'stop').length
    await assert.rejects(env.stop(), /소유/); assert.equal(calls.filter(c => c[0] === 'stop').length, stopCount)
    await assert.rejects(env.start(), /소유/)
    assert.equal(JSON.parse(await readFile(path, 'utf8')).selected, id)
  } finally { await rm(root, { recursive: true, force: true }) }
})
