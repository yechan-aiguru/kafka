import type { Scenario } from '../../domain/types.ts'
export const defaultScenario: Scenario = {
  schemaVersion: 1, id: 'first-message', name: '첫 번째 메시지 보내기',
  topics: [{ name: 'orders', partitions: 3, replicationFactor: 1 }],
  sessions: [
    { id: 'producer-1', name: 'Producer 01', role: 'producer', topic: 'orders', bootstrapServer: 'localhost:9092', acks: 'all', parseKey: false },
    { id: 'consumer-1', name: 'Consumer 01', role: 'consumer', topic: 'orders', bootstrapServer: 'localhost:9092', group: 'demo-group', fromBeginning: true },
  ],
}
