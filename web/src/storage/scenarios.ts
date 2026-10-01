import type { Scenario, Session } from '../domain/types.ts'
const KEY = 'kafka-terminal-studio.scenarios.v1'
export const LAST = 'kafka-terminal-studio.last.v1'
function obj(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('설정 객체가 필요합니다.')
  return value as Record<string, unknown>
}
function str(v: unknown, max = 249): string {
  // Reject imported control characters before generating commands.
  // eslint-disable-next-line no-control-regex
  if (typeof v !== 'string' || !v.trim() || v.length > max || /[\x00-\x1f\x7f]/.test(v)) throw new Error('설정 문자열이 올바르지 않습니다.')
  return v
}
function topic(v: unknown) { const s = str(v); if (!/^[\w.-]+$/.test(s) || ['.', '..'].includes(s)) throw new Error('토픽 이름 오류'); return s }
function bool(v: unknown) { if (typeof v !== 'boolean') throw new Error('옵션 값 오류'); return v }
function count(v: unknown, max: number) { if (typeof v !== 'number' || !Number.isInteger(v) || v < 1 || v > max) throw new Error('개수 범위 오류'); return v }
// Whitelist serialization also strips imported secrets and unrelated fields.
export function parseScenario(value: unknown): Scenario {
  const s = obj(value)
  if (s.schemaVersion !== 1) throw new Error('지원하는 schemaVersion은 1입니다.')
  if (!Array.isArray(s.topics) || s.topics.length > 100 || !Array.isArray(s.sessions) || s.sessions.length > 8) throw new Error('토픽 100개, 세션 8개까지 지원합니다.')
  const topics = s.topics.map(value => { const t = obj(value); return { name: topic(t.name), partitions: count(t.partitions, 100), replicationFactor: count(t.replicationFactor, 10) } })
  const sessions: Session[] = s.sessions.map(value => {
    const t = obj(value)
    const base = { id: str(t.id, 80), name: str(t.name, 80), topic: topic(t.topic), bootstrapServer: str(t.bootstrapServer) }
    if (base.id === 'admin' || !/^[a-zA-Z0-9.:[\],_-]+$/.test(base.bootstrapServer)) throw new Error('세션 ID 또는 Bootstrap server 오류')
    if (t.role === 'producer') {
      if (!['all', '1', '0'].includes(t.acks as string) || (t.partitioner !== undefined && !['default', 'round-robin'].includes(t.partitioner as string))) throw new Error('Producer 설정 오류')
      return { ...base, role: 'producer', acks: t.acks as 'all' | '1' | '0', parseKey: bool(t.parseKey), partitioner: (t.partitioner ?? 'default') as 'default' | 'round-robin' }
    }
    if (t.role !== 'consumer' || (t.assignor !== undefined && !['range', 'round-robin'].includes(t.assignor as string))) throw new Error('Consumer 설정 오류')
    return { ...base, role: 'consumer', group: str(t.group), fromBeginning: bool(t.fromBeginning), assignor: (t.assignor ?? 'range') as 'range' | 'round-robin' }
  })
  if (new Set(topics.map(t => t.name)).size !== topics.length || new Set(sessions.map(t => t.id)).size !== sessions.length) throw new Error('중복된 토픽 또는 세션 ID입니다.')
  return { schemaVersion: 1, id: str(s.id, 80), name: str(s.name, 80), topics, sessions }
}
export function readScenarios(storage: Pick<Storage, 'getItem'>): Scenario[] {
  const raw = JSON.parse(storage.getItem(KEY) ?? '[]') as unknown
  if (!Array.isArray(raw) || raw.length > 100) throw new Error('저장 목록이 손상되었습니다. JSON 백업을 확인하세요.')
  return raw.map(parseScenario)
}
export function writeScenarios(storage: Pick<Storage, 'setItem'>, scenarios: Scenario[]) {
  if (scenarios.length > 100) throw new Error('최대 100개까지 저장할 수 있습니다.')
  storage.setItem(KEY, JSON.stringify(scenarios.map(parseScenario)))
}
