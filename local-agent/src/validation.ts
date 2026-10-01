import type { Session, Topic } from '../../web/src/domain/types.ts'
export function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('객체가 필요합니다.')
  return value as Record<string, unknown>
}
export function text(value: unknown, name: string, max = 249): string {
  if (typeof value !== 'string' || !value.trim() || value.length > max || /[\x00-\x1f\x7f]/.test(value)) throw new Error(`${name} 값이 올바르지 않습니다.`)
  return value
}
export function topicName(value: unknown) {
  const name = text(value, 'Topic')
  if (!/^[a-zA-Z0-9._-]+$/.test(name) || ['.', '..'].includes(name)) throw new Error('Topic 이름이 올바르지 않습니다.')
  return name
}
export function integer(value: unknown, name: string, max: number) {
  if (typeof value !== 'number' || !Number.isInteger(value) || value < 1 || value > max) throw new Error(`${name}: 1~${max} 정수가 필요합니다.`)
  return value
}
export function parseTopic(value: unknown): Topic {
  const topic = object(value)
  return { name: topicName(topic.name), partitions: integer(topic.partitions, 'Partitions', 100), replicationFactor: integer(topic.replicationFactor, 'Replication', 10) }
}
export function parseSession(value: unknown, bootstrapServer: string): Session {
  const session = object(value)
  const base = { id: text(session.id, 'ID', 80), name: text(session.name, 'Name', 80), topic: topicName(session.topic), bootstrapServer: text(session.bootstrapServer, 'Bootstrap server') }
  if (base.id === 'admin') throw new Error('예약된 세션 ID입니다.')
  if (base.bootstrapServer !== bootstrapServer) throw new Error(`연결 서버에 설정된 ${bootstrapServer}만 사용할 수 있습니다.`)
  if (session.role === 'producer') {
    if (!['all', '1', '0'].includes(String(session.acks)) || typeof session.acks !== 'string' || typeof session.parseKey !== 'boolean') throw new Error('Producer 인자가 올바르지 않습니다.')
    if (session.partitioner !== undefined && (typeof session.partitioner !== 'string' || !['default', 'round-robin'].includes(session.partitioner))) throw new Error('지원하지 않는 Partitioner입니다.')
    return { ...base, ...(session.partitioner ? { partitioner: session.partitioner as 'default' | 'round-robin' } : {}), role: 'producer', acks: session.acks as 'all' | '1' | '0', parseKey: session.parseKey }
  }
  if (session.role === 'consumer') {
    if (typeof session.fromBeginning !== 'boolean') throw new Error('fromBeginning은 boolean이어야 합니다.')
    if (session.assignor !== undefined && (typeof session.assignor !== 'string' || !['range', 'round-robin'].includes(session.assignor))) throw new Error('지원하지 않는 Assignor입니다.')
    return { ...base, ...(session.assignor ? { assignor: session.assignor as 'range' | 'round-robin' } : {}), role: 'consumer', group: text(session.group, 'Group'), fromBeginning: session.fromBeginning }
  }
  throw new Error('지원하지 않는 세션 역할입니다.')
}
export function inputText(value: unknown): string {
  if (typeof value !== 'string' || !value.trim() || Buffer.byteLength(value) > 16384 || /[\r\n\x00]/.test(value)) throw new Error('메시지는 16KB 이하의 한 줄이어야 합니다.')
  return value
}
