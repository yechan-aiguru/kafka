import { test } from 'node:test'
import assert from 'node:assert/strict'
import { cases, makeCase, probe, expectRejection } from '../src/features/experiments/cases.ts'
import { MockExecutionClient } from '../src/execution/mock.ts'
import { parseScenario } from '../src/storage/scenarios.ts'

test('모든 예외 템플릿은 저장 가능하고 서로 다른 토픽/그룹을 사용', () => {
  for (const exercise of cases) {
    const a = makeCase(exercise.id, 'run-a'), b = makeCase(exercise.id, 'run-b')
    assert.equal(parseScenario(a).name, exercise.title)
    assert.notEqual(a.topics[0].name, b.topics[0].name)
    assert.ok(a.sessions.every(s => s.topic === a.topics[0].name))
  }
})
test('파티션보다 많은 Consumer는 일부만 할당되고 중지 후 재할당', async () => {
  const c = new MockExecutionClient(), s = makeCase('idle', 'idle-test')
  await c.createTopic(s.topics[0])
  for (const session of s.sessions) await c.startSession(session)
  const consumer = s.sessions[1]; if (consumer.role !== 'consumer') throw new Error()
  const first = await c.describeGroup(consumer.group)
  assert.equal(first.partitions.length, 1)
  const owner = first.partitions[0].consumerId
  await c.stopSession(owner)
  assert.notEqual((await c.describeGroup(consumer.group)).partitions[0].consumerId, owner)
  c.dispose()
})
test('관리 오류 검사 결과와 상태 보존', async () => {
  const c = new MockExecutionClient(), s = makeCase('invalid', 'errors-test')
  await c.createTopic(s.topics[0])
  for (const id of ['duplicate', 'decrease', 'replicas', 'stopped'] as const) assert.equal((await probe(c, id, s)).outcome, 'expected')
  assert.deepEqual(await c.listTopics(), s.topics)
  assert.equal((await c.describeTopic(s.topics[0].name)).partitions, 2)
  c.dispose()
})
test('잘못된 key 검사에는 실행 중 key Producer가 필요', async () => {
  const c = new MockExecutionClient(), s = makeCase('key', 'key-test')
  await c.createTopic(s.topics[0])
  assert.equal((await probe(c, 'bad-key', s)).outcome, 'unconfirmed')
  await c.startSession(s.sessions[0])
  assert.equal((await probe(c, 'bad-key', s)).outcome, 'expected')
  c.dispose()
})
test('연결 실패와 예상하지 않은 성공을 기대한 오류로 오판하지 않음', async () => {
  assert.equal((await expectRejection(async () => { throw new Error('Failed to fetch') }, /이미 존재/)).outcome, 'unconfirmed')
  assert.equal((await expectRejection(async () => {}, /이미 존재/)).outcome, 'unexpected')
})
