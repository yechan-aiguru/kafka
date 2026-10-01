import { useState } from 'react'
import type { Scenario, Session, SessionStatus, Topic } from '../../domain/types'
import type { ExecutionClient } from '../../execution/types'
import { cases, makeCase, probe, type CaseId, type ProbeId, type ProbeResult } from './cases'
interface Props {
  mode: 'mock' | 'local'; disabled: boolean; sessions: Session[]; statuses: Record<string, SessionStatus>
  run: (fn: (c: ExecutionClient) => Promise<void>) => Promise<boolean>
  load: (s: Scenario) => Promise<boolean>; onTopics: (topics: Topic[]) => void
}
const probes: { id: ProbeId; label: string; layer: string }[] = [
  { id: 'duplicate', label: '중복 토픽 생성 거부', layer: '실제 모드: Kafka / 모의 모드: 학습 모델' },
  { id: 'decrease', label: '파티션 감소 거부', layer: 'Studio 사전 검증' },
  { id: 'replicas', label: '브로커 수 초과 복제 거부', layer: 'Studio 사전 검증 · 별도 -rf-check 토픽 이름 사용' },
  { id: 'stopped', label: 'Producer 중지 후 전송 거부', layer: 'Studio 세션 검증 · 이 실습 Producer를 중지합니다' },
  { id: 'bad-key', label: '잘못된 key 입력 거부', layer: 'Studio 입력 검증 · key 실습 Producer가 실행 중이어야 합니다' },
]
export function ExceptionLab({ mode, disabled, sessions, statuses, run, load, onTopics }: Props) {
  const [selected, setSelected] = useState<CaseId>('idle')
  const [applied, setApplied] = useState<{ id: CaseId; scenario: Scenario } | null>(null)
  const [results, setResults] = useState<Partial<Record<ProbeId, ProbeResult>>>({})
  const [notice, setNotice] = useState('')
  const exercise = cases.find(c => c.id === selected)!
  const active = applied?.id === selected && JSON.stringify(sessions) === JSON.stringify(applied.scenario.sessions)
  const producer = applied?.scenario.sessions.find(s => s.role === 'producer')
  return <details className="exception-lab"><summary>예외 상황 실습 · 재현 방법과 예상 결과</summary>
    <p className="muted">{mode === 'local' ? '실제 Kafka를 사용합니다. 실습마다 새로운 토픽·그룹을 사용하며 결과는 자동 삭제하지 않습니다.' : '모의 학습 모델입니다. 실제 Kafka의 장애·타이밍을 재현한 결과는 아닙니다.'}</p>
    <label>실습 선택<select value={selected} disabled={disabled} onChange={e => { setSelected(e.target.value as CaseId); setNotice(''); setResults({}) }}>{cases.map(c => <option key={c.id} value={c.id}>{c.title}</option>)}</select></label>
    <p><strong>예상 결과</strong> · {exercise.expected}</p>
    <ol>{exercise.steps.map(step => <li key={step}>{step}</li>)}</ol>
    <div className="actions"><button disabled={disabled} onClick={async () => {
      const scenario = makeCase(selected, crypto.randomUUID().slice(0, 8), sessions[0]?.bootstrapServer ?? 'localhost:9092')
      if (await load(scenario)) { setApplied({ id: selected, scenario }); setResults({}); setNotice('현재 세션을 중지하고 설정을 적용했습니다. 다음으로 전용 토픽을 생성하세요.') }
    }}>1. 실습 설정 적용 · 현재 세션 중지</button>
    <button disabled={disabled || !active} onClick={() => void run(async c => {
      const topic = applied!.scenario.topics[0], existing = await c.listTopics()
      const found = existing.find(t => t.name === topic.name)
      if (!found) await c.createTopic(topic)
      else if (found.partitions !== topic.partitions || found.replicationFactor !== topic.replicationFactor) throw new Error('토픽 정의가 달라졌습니다. 새 실습 설정을 적용하거나 현재 토픽 상태를 확인하세요.')
      onTopics(await c.listTopics()); setNotice('토픽을 준비했습니다. 아래 세션의 시작 버튼으로 실습을 진행하세요.')
    })}>2. 전용 실습 토픽 생성</button></div>
    {applied && <p className="muted">적용된 Topic: <code>{applied.scenario.topics[0].name}</code><br />Group: <code>{applied.scenario.sessions.find(s => s.role === 'consumer')?.group}</code></p>}
    {applied && !active && <p className="muted">선택한 실습 또는 세션 설정이 달라졌습니다. 자동 오류 검사에는 실습 설정을 다시 적용하세요. 수동 실습은 계속할 수 있습니다.</p>}
    {(selected === 'invalid' || selected === 'key') && <div className="probe-list">{probes.filter(p => selected === 'key' ? p.id === 'bad-key' : p.id !== 'bad-key').map(p => <div key={p.id}>
      <button disabled={disabled || !active || (p.id === 'bad-key' && (!producer || statuses[producer.id] !== 'running'))} onClick={() => void run(async c => { setResults(v => ({ ...v, [p.id]: undefined })); const result = await probe(c, p.id, applied!.scenario); setResults(v => ({ ...v, [p.id]: result })); onTopics(await c.listTopics()) })}>{p.label}</button>
      <p className="muted">{p.layer}</p>{results[p.id] && <pre role="status" className={`probe-${results[p.id]!.outcome}`}>{results[p.id]!.outcome === 'expected' ? '예상한 거부 확인' : results[p.id]!.outcome === 'unexpected' ? '예상과 다른 성공' : '판정 보류 · 다른 오류'}{'\n'}{results[p.id]!.message}</pre>}
    </div>)}</div>}
    {notice && <p role="status">{notice}</p>}
    <p className="muted">모든 실패가 Kafka 장애는 아닙니다. 검사 결과에 거부 지점을 구분합니다. 개별 브로커 중단·ISR 축소 같은 장애 주입은 아직 자동 실습하지 않습니다. 복제 비교는 로컬 환경 설정의 3개 브로커 프리셋을 이용하세요.</p>
  </details>
}
