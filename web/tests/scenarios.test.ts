import { test } from 'node:test'
import assert from 'node:assert/strict'
import { parseScenario, readScenarios, writeScenarios } from '../src/storage/scenarios.ts'
import { defaultScenario } from '../src/features/scenarios/defaultScenario.ts'
test('저장 JSON round trip 및 비밀 필드 제거', () => {
  let value = ''
  const storage = { getItem: () => value, setItem: (_: string, v: string) => { value = v } }
  const scenario = { ...defaultScenario, token: 'secret', sessions: defaultScenario.sessions.map(s => ({ ...s, password: 'secret' })) }
  writeScenarios(storage, [scenario])
  assert.equal(value.includes('secret'), false)
  assert.deepEqual(readScenarios(storage), [parseScenario(scenario)])
})
test('손상·미지원 버전·중복 ID·옵션 거부 및 저장 실패 전달', () => {
  for (const value of [null, { ...defaultScenario, schemaVersion: 2 }, { ...defaultScenario, sessions: [defaultScenario.sessions[0], defaultScenario.sessions[0]] }, { ...defaultScenario, topics: [{ name: 'x', partitions: -1, replicationFactor: 1 }] }, { ...defaultScenario, sessions: [{ ...defaultScenario.sessions[0], partitioner: 'shell' }] }]) assert.throws(() => parseScenario(value))
  assert.throws(() => readScenarios({ getItem: () => '{bad' }))
  assert.throws(() => writeScenarios({ setItem: () => { throw new Error('quota') } }, [defaultScenario]), /quota/)
})
