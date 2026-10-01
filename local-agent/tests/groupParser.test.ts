import { test } from 'node:test'
import assert from 'node:assert/strict'
import { parseGroup, inactiveGroup } from '../src/groupParser.ts'
test('Kafka group output includes missing commits without coercing to zero', () => {
  const raw = 'GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID\ng orders 0 4 7 3 member /127.0.0.1 consumer-1\ng orders 1 - 7 - - - -'
  const g = parseGroup('g', raw)
  assert.equal(g.partitions.length, 2)
  assert.equal(g.partitions[0].lag, 3)
  assert.equal(g.partitions[1].currentOffset, null)
})
test('reset only for verified inactive group', () => {
  assert.equal(inactiveGroup('g', 'g localhost:9092 (1) range Empty 0'), true)
  assert.equal(inactiveGroup('g', 'g localhost:9092 (1) range Stable 2'), false)
  assert.equal(inactiveGroup('g', 'other localhost:9092 (1) range Empty 0'), false)
  assert.equal(inactiveGroup('g', ''), false)
})
