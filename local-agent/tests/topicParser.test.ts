import { test } from 'node:test'
import assert from 'node:assert/strict'
import { parseTopicDescriptions, parseBrokerIds } from '../src/topicParser.ts'

test('Kafka 4.x 상세: TopicId, ELR, 여러 replica, 리더 없음과 빈 ISR', () => {
  const result = parseTopicDescriptions('Topic: orders.v1\tTopicId: abc\tPartitionCount: 2\tReplicationFactor: 2\tConfigs:\n\tTopic: orders.v1\tPartition: 0\tLeader: 1\tReplicas: 1,2\tIsr: 1,2\tElr: \tLastKnownElr:\n\tTopic: orders.v1\tPartition: 1\tLeader: -1\tReplicas: 2,1\tIsr: \tElr: 2\tLastKnownElr:\n')
  assert.equal(result.length, 1)
  assert.deepEqual(result[0].partitionDetails, [
    { id: 0, leader: 1, replicas: [1, 2], isr: [1, 2] },
    { id: 1, leader: -1, replicas: [2, 1], isr: [] },
  ])
})
test('구버전 상세 출력과 브로커 메타데이터 중복 제거', () => {
  assert.deepEqual(parseTopicDescriptions('Topic: x PartitionCount: 1 ReplicationFactor: 1 Configs:\n Topic: x Partition: 0 Leader: 0 Replicas: 0 Isr: 0\n')[0].partitionDetails, [{ id: 0, leader: 0, replicas: [0], isr: [0] }])
  assert.deepEqual(parseBrokerIds('a:9092 (id: 2 rack: null) -> {\na:9092 (id: 2 rack: null) -> {}\nb:9092 (id: 1 rack: zone) -> {}'), [1, 2])
})
