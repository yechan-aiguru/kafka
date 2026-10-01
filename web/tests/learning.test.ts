import { test } from 'node:test'
import assert from 'node:assert/strict'
import { MockExecutionClient } from '../src/execution/mock.ts'
import { defaultScenario } from '../src/features/scenarios/defaultScenario.ts'
import { sessionCommand } from '../src/execution/commands.ts'
import { keyPartition } from '../src/execution/partitioning.ts'

test('Round Robin 분배·리밸런싱·lag·정지 후 offset reset·replay', async () => {
  const c = new MockExecutionClient()
  await c.createTopic(defaultScenario.topics[0])
  const producer = { ...defaultScenario.sessions[0], partitioner: 'round-robin' as const }
  const consumer = defaultScenario.sessions[1]
  assert.equal(consumer.role, 'consumer'); if (consumer.role !== 'consumer') return
  await c.startSession(producer); await c.startSession(consumer)
  for (let i = 0; i < 6; i++) await c.sendInput(producer.id, 'message')
  const before = await c.describeGroup(consumer.group)
  assert.deepEqual(before.partitions.map(p => p.logEndOffset), [2, 2, 2])
  assert.deepEqual(before.partitions.map(p => p.lag), [0, 0, 0])
  await c.startSession({ ...consumer, id: 'consumer-2' })
  assert.equal(new Set((await c.describeGroup(consumer.group)).partitions.map(p => p.consumerId)).size, 2)
  await assert.rejects(c.resetOffsets(consumer.group, consumer.topic))
  await c.stopSession(consumer.id); await c.stopSession('consumer-2')
  await c.sendInput(producer.id, 'backlog')
  assert.equal((await c.describeGroup(consumer.group)).partitions[0].lag, 1)
  await c.resetOffsets(consumer.group, consumer.topic)
  assert.deepEqual((await c.describeGroup(consumer.group)).partitions.map(p => p.currentOffset), [0, 0, 0])
  await c.startSession(consumer)
  assert.ok((await c.describeGroup(consumer.group)).partitions.every(p => p.lag === 0))
  c.dispose()
})
test('Kafka keyed Murmur2 golden vectors and command strategy options', () => {
  assert.equal(keyPartition('', 2147483647), 275646681)
  assert.equal(keyPartition('hello', 2147483647), 2132663229)
  assert.equal(keyPartition('customer-1', 3), keyPartition('customer-1', 3))
  assert.ok(sessionCommand({ ...defaultScenario.sessions[0], role: 'producer', acks: 'all', parseKey: true, partitioner: 'round-robin' }).args.includes('partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner'))
  assert.ok(sessionCommand(defaultScenario.sessions[1]).args.includes('group.protocol=classic'))
})
