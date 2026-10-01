import { test } from 'node:test'
import assert from 'node:assert/strict'
import { parseSession, parseTopic, inputText } from '../src/validation.ts'
import { defaultScenario } from '../../web/src/features/scenarios/defaultScenario.ts'
import { launch } from '../src/runner.ts'

test('외부 명령과 잘못된 토픽/복제수 입력을 거부한다', () => {
  assert.throws(() => parseTopic({ name: 'orders;rm', partitions: 1, replicationFactor: 1 }))
  assert.throws(() => parseTopic({ name: 'orders', partitions: 1.1, replicationFactor: 1 }))
  assert.throws(() => parseTopic({ name: 'orders', partitions: 1, replicationFactor: 0 }))
  assert.throws(() => launch({ mode: 'local', kafkaBin: '/tmp', container: '', image: '' }, 'sh', []))
})
test('서버 설정 외 대상 및 역할/옵션 주입을 거부한다', () => {
  assert.throws(() => parseSession({ ...defaultScenario.sessions[0], bootstrapServer: 'external:9092' }, 'localhost:9092'))
  assert.throws(() => parseSession({ ...defaultScenario.sessions[0], role: 'shell' }, 'localhost:9092'))
  assert.throws(() => parseSession({ ...defaultScenario.sessions[0], acks: 'all --help' }, 'localhost:9092'))
  assert.throws(() => parseSession({ ...defaultScenario.sessions[0], id: 'admin' }, 'localhost:9092'))
  assert.equal(parseSession(defaultScenario.sessions[0], 'localhost:9092').role, 'producer')
})
test('Producer stdin은 16KB 이하 한 줄만 허용한다', () => {
  assert.equal(inputText('id:한글'), 'id:한글')
  assert.throws(() => inputText('a\nb'))
  assert.throws(() => inputText('x'.repeat(16385)))
})

test('분배 옵션은 허용된 문자열만 받는다', () => {
  const base = { id: 'test', name: 'test', topic: 'orders', bootstrapServer: 'localhost:9092' }
  for (const partitioner of [['round-robin'], 'unknown', 1]) assert.throws(() => parseSession({ ...base, role: 'producer', acks: 'all', parseKey: false, partitioner }, base.bootstrapServer))
  for (const assignor of [['range'], 'unknown', 1]) assert.throws(() => parseSession({ ...base, role: 'consumer', group: 'g', fromBeginning: true, assignor }, base.bootstrapServer))
})
