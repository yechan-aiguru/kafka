import type { Scenario, Consumer, Producer } from '../../domain/types.ts'
import type { ExecutionClient } from '../../execution/types.ts'
export const cases = [
  { id: 'idle', title: 'Consumer가 파티션보다 많으면?', partitions: 1, consumers: 3, keyed: false,
    steps: ['Producer와 Consumer 3개를 모두 시작합니다.', '메시지를 여러 건 보낸 뒤 아래 그룹 패널에서 할당을 조회합니다.', '파티션을 가진 Consumer를 중지하고 다시 조회합니다.'],
    expected: '할당이 안정되면 파티션 1개는 그룹 멤버 1개가 담당합니다. 나머지 Consumer가 메시지를 받지 않는 것은 오류가 아닙니다. 담당자를 중지하면 다른 멤버로 재할당됩니다.' },
  { id: 'lag', title: 'Consumer 중지 → Lag 증가 → 복구', partitions: 3, consumers: 1, keyed: false,
    steps: ['두 세션을 시작하고 메시지 3건을 보냅니다. 그룹을 조회해 Committed 값이 생길 때까지 기다립니다.', 'Consumer만 중지한 뒤 Producer에서 메시지를 10건 더 보냅니다.', '그룹을 조회해 Lag를 확인하고, 같은 Consumer를 재시작한 뒤 다시 조회합니다.'],
    expected: '정지 중 Log end가 늘고 Committed는 유지됩니다. 재시작하면 밀린 메시지를 읽습니다. 실제 Kafka의 commit 주기 때문에 Lag 반영은 늦을 수 있습니다.' },
  { id: 'replay', title: '--from-beginning인데 다시 읽지 않는 이유', partitions: 3, consumers: 1, keyed: false,
    steps: ['두 세션을 시작하고 메시지를 보낸 뒤 그룹 조회에서 Committed와 Lag 0을 확인합니다.', 'Consumer를 중지하고 같은 그룹·from-beginning 상태로 다시 시작합니다.', 'Consumer를 중지한 뒤 그룹 이름을 새 이름으로 바꾸고 다시 시작합니다.'],
    expected: '기존 그룹은 저장된 offset에서 이어 읽습니다. 새 그룹은 보관된 메시지를 처음부터 읽습니다. 기존 그룹의 earliest 초기화는 모든 Consumer를 중지한 후 별도로 실행합니다.' },
  { id: 'key', title: '같은 key 편중 · 잘못된 key 입력', partitions: 3, consumers: 2, keyed: true,
    steps: ['모든 세션을 시작합니다. Producer 반복 전송 템플릿을 customer-1:message-{n}으로 설정합니다.', '10건을 보낸 뒤 Consumer 출력의 Partition을 비교합니다.', '아래 잘못된 key 입력 검사로 구분자 없는 메시지의 거부 결과를 확인합니다.'],
    expected: '기본 key 해시 전략에서는 같은 key가 한 파티션으로 갑니다. Consumer를 늘려도 그 key의 부하는 나뉘지 않습니다. 구분자 없는 입력은 Studio가 Kafka CLI에 보내기 전에 차단합니다.' },
  { id: 'growth', title: '파티션 증가 후 같은 key의 목적지', partitions: 2, consumers: 1, keyed: true,
    steps: ['두 세션을 시작하고 customer-1:before, customer-2:before를 보냅니다.', '토픽 관리에서 이 실습 토픽의 파티션 수를 2에서 3으로 늘립니다.', '메타데이터가 갱신되도록 Producer를 중지·재시작하고 같은 key로 after 메시지를 보냅니다.'],
    expected: '일부 key의 새 메시지는 다른 파티션으로 갈 수 있습니다. 기존 메시지는 옮겨지지 않습니다. 파티션을 가로지르는 같은 key의 전체 순서는 보장되지 않으며, 모든 key가 반드시 이동하지는 않습니다.' },
  { id: 'invalid', title: '잘못된 관리 요청과 정지 세션 전송', partitions: 2, consumers: 1, keyed: false,
    steps: ['실습 토픽 생성 버튼으로 전용 토픽을 준비합니다.', '아래 검사 버튼을 하나씩 누르고 거부 지점·원본 오류를 확인합니다.', '예상과 다른 오류는 통과로 표시하지 않습니다. 연결 오류이면 연결 상태를 먼저 확인합니다.'],
    expected: '중복 토픽 생성은 실제 모드에서 Kafka가 거부합니다. 파티션 감소·복제 수 초과·정지 Producer 입력은 Studio가 사전 검증으로 차단합니다.' },
] as const
export type CaseId = typeof cases[number]['id']
export function makeCase(id: CaseId, suffix: string, bootstrapServer = 'localhost:9092'): Scenario {
  if (!/^[a-zA-Z0-9-]{1,40}$/.test(suffix)) throw new Error('실습 식별자가 올바르지 않습니다.')
  const selected = cases.find(c => c.id === id)!
  const topic = `kts-case-${id}-${suffix}`
  const producer: Producer = { id: `${suffix}-p`, name: '실습 Producer', role: 'producer', topic, bootstrapServer, acks: 'all', parseKey: selected.keyed, partitioner: 'default' }
  const consumers: Consumer[] = Array.from({ length: selected.consumers }, (_, i) => ({ id: `${suffix}-c${i + 1}`, name: `실습 Consumer ${i + 1}`, role: 'consumer', topic, bootstrapServer, group: `${topic}-group`, fromBeginning: true, assignor: 'range' }))
  return { schemaVersion: 1, id: topic, name: selected.title, topics: [{ name: topic, partitions: selected.partitions, replicationFactor: 1 }], sessions: [producer, ...consumers] }
}
export type ProbeId = 'duplicate' | 'decrease' | 'replicas' | 'stopped' | 'bad-key'
export interface ProbeResult { outcome: 'expected' | 'unexpected' | 'unconfirmed'; message: string }
export async function expectRejection(action: () => Promise<void>, pattern: RegExp): Promise<ProbeResult> {
  try { await action(); return { outcome: 'unexpected', message: '요청이 성공했습니다. 예상했던 거부가 발생하지 않았으므로 결과와 토픽 상태를 확인하세요.' } }
  catch (error) { const message = error instanceof Error ? error.message : String(error); return { outcome: pattern.test(message) ? 'expected' : 'unconfirmed', message } }
}
export async function probe(client: ExecutionClient, id: ProbeId, scenario: Scenario): Promise<ProbeResult> {
  const topic = scenario.topics[0], producer = scenario.sessions.find(s => s.role === 'producer')!
  if (!topic.name.startsWith('kts-case-')) throw new Error('전용 실습 토픽에서만 검사합니다.')
  // Preconditions fail normally; a connection failure is never an expected rejection.
  const current = await client.describeTopic(topic.name)
  if (id === 'duplicate') return expectRejection(() => client.createTopic(topic), /TopicExistsException|already exists|이미 존재/i)
  if (id === 'decrease') {
    if (current.partitions < 2) throw new Error('감소 검사는 파티션이 2개 이상이어야 합니다.')
    return expectRejection(() => client.increasePartitions(topic.name, current.partitions - 1), /현재보다 큰|현재.*큰/)
  }
  if (id === 'replicas') {
    const cluster = await client.clusterInfo()
    if (!cluster.brokerIds.length || cluster.brokerIds.length >= 10) throw new Error('현재 브로커 수는 이 복제 수 초과 검사의 범위를 벗어납니다.')
    return expectRejection(() => client.createTopic({ ...topic, name: topic.name + '-rf-check', replicationFactor: cluster.brokerIds.length + 1 }), /브로커.*초과|모의 환경은 브로커/)
  }
  if (id === 'stopped') {
    await client.stopSession(producer.id)
    return expectRejection(() => client.sendInput(producer.id, 'after-stop'), /실행 중인 Producer가 필요/)
  }
  if (producer.role !== 'producer' || !producer.parseKey) throw new Error('key:value 실습 설정이 필요합니다.')
  return expectRejection(() => client.sendInput(producer.id, 'missing-key-separator'), /key:value 형식/)
}
