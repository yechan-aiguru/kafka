import { test } from 'node:test'
import assert from 'node:assert/strict'
import { MockExecutionClient } from '../src/execution/mock.ts'
import { createLocalExecutionClient } from '../src/execution/local.ts'
import { defaultScenario } from '../src/features/scenarios/defaultScenario.ts'
import type { ExecutionEvent } from '../src/execution/types.ts'

test('토픽 입력 검증과 파티션 증가만 허용', async () => {
  const client = new MockExecutionClient()
  await assert.rejects(client.createTopic({ name: '../invalid', partitions: 1, replicationFactor: 1 }))
  await client.createTopic(defaultScenario.topics[0])
  await assert.rejects(client.createTopic(defaultScenario.topics[0]))
  await assert.rejects(client.increasePartitions('orders', 2))
  await client.increasePartitions('orders', 4)
  const topics = await client.listTopics()
  assert.equal(topics[0].partitions, 4)
  topics[0].partitions = 99
  assert.equal((await client.listTopics())[0].partitions, 4)
})

test('실행한 Consumer만 수신하고 중지 후 입력은 거부', async () => {
  const client = new MockExecutionClient()
  const events: ExecutionEvent[] = []
  client.subscribe(event => events.push(event))
  await client.createTopic(defaultScenario.topics[0])
  for (const session of defaultScenario.sessions) await client.startSession(session)
  await client.sendInput('producer-1', 'hello')
  assert.ok(events.some(event => event.type === 'output' && event.sessionId === 'consumer-1' && event.text === 'hello'))
  await client.stopSession('consumer-1')
  events.length = 0
  await client.sendInput('producer-1', 'after stop')
  assert.equal(events.filter(event => event.type === 'output' && event.stream === 'stdout').length, 0)
  await client.stopSession('producer-1')
  await assert.rejects(client.sendInput('producer-1', 'invalid'))
})

test('출력 구독 해제와 dispose 후 세션 정리', async () => {
  const client = new MockExecutionClient()
  let count = 0
  const unsubscribe = client.subscribe(() => count++)
  unsubscribe()
  await client.createTopic(defaultScenario.topics[0])
  await client.startSession(defaultScenario.sessions[0])
  assert.equal(count, 0)
  client.dispose()
  assert.deepEqual(await client.listTopics(), [])
  await assert.rejects(client.sendInput('producer-1', 'invalid'))
})

test('로컬 연결 토큰 없이는 연결을 생성하지 않음', () => {
  assert.throws(() => createLocalExecutionClient(), /토큰/)
})

test('실행 설정은 복사되며 같은 그룹은 한 Consumer만 수신', async () => {
  const client = new MockExecutionClient()
  const events: ExecutionEvent[] = []
  client.subscribe(e => events.push(e))
  await client.createTopic(defaultScenario.topics[0])
  const producer = { ...defaultScenario.sessions[0] }
  await client.startSession(producer)
  producer.topic = 'changed-after-start'
  await client.startSession(defaultScenario.sessions[1])
  await client.startSession({ ...defaultScenario.sessions[1], id: 'consumer-2' })
  await client.sendInput(producer.id, 'hello')
  assert.equal(events.filter(e => e.type === 'output' && e.stream === 'stdout').length, 1)
  await client.stopSession('consumer-1')
  events.length = 0
  await client.sendInput(producer.id, 'next')
  assert.ok(events.some(e => e.type === 'output' && e.sessionId === 'consumer-2' && e.text === 'next'))
})

test('모의 토픽 상세와 증가 전후 파티션·관리 로그', async () => {
  const client = new MockExecutionClient()
  const events: ExecutionEvent[] = []
  client.subscribe(e => events.push(e))
  await client.createTopic({ name: 'topic.lab', partitions: 1, replicationFactor: 1 })
  const before = await client.describeTopic('topic.lab')
  await client.increasePartitions('topic.lab', 3)
  const after = await client.describeTopic('topic.lab')
  assert.equal(before.partitionDetails.length, 1)
  assert.equal(after.partitionDetails.length, 3)
  assert.deepEqual(after.partitionDetails[2], { id: 2, leader: 1, replicas: [1], isr: [1] })
  after.partitionDetails[0].replicas.push(99)
  assert.deepEqual((await client.describeTopic('topic.lab')).partitionDetails[0].replicas, [1])
  await assert.rejects(client.increasePartitions('topic.lab', 2))
  await assert.rejects(client.createTopic({ name: 'too-many', partitions: 1, replicationFactor: 2 }))
  assert.ok(events.some(e => e.type === 'output' && e.sessionId === 'admin' && e.text.includes('--alter')))
})
