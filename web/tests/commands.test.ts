import { test } from 'node:test'
import assert from 'node:assert/strict'
import { sessionCommand, shellQuote } from '../src/execution/commands.ts'
import { changeRole, newSession } from '../src/features/sessions/sessionModel.ts'

test('역할 변경은 공통 설정을 유지하고 역할 전용 인자를 교체', () => {
  const consumer = newSession('id', 3)
  const producer = changeRole(consumer, 'producer')
  assert.equal(producer.id, consumer.id)
  assert.equal(producer.topic, consumer.topic)
  assert.equal('group' in producer, false)
  assert.ok(sessionCommand(producer).args.includes('acks=all'))
  const restored = changeRole(producer, 'consumer')
  assert.equal('acks' in restored, false)
  assert.ok(sessionCommand(restored).args.includes('--from-beginning'))
})

test('명령 인자는 문자열 결합 없이 유지하고 표시 문자열은 shell quote', () => {
  const session = { ...newSession('id', 1), topic: "a'$(echo test)" }
  const command = sessionCommand(session)
  assert.equal(command.args[3], session.topic)
  assert.equal(shellQuote("a'b"), "'a'\\''b'")
  assert.ok(command.display.includes(shellQuote(session.topic)))
})
