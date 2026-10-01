import { useState } from 'react'
import type { GroupDetails, Session, SessionStatus } from '../../domain/types'
import type { ExecutionClient } from '../../execution/types'
export function GroupInspector({ sessions, statuses, disabled, run }: { sessions: Session[]; statuses: Record<string, SessionStatus>; disabled: boolean; run: (fn: (c: ExecutionClient) => Promise<void>) => Promise<boolean> }) {
  const [group, setGroup] = useState(sessions.find(s => s.role === 'consumer')?.group ?? 'demo-group')
  const [topic, setTopic] = useState(sessions.find(s => s.role === 'consumer')?.topic ?? 'orders')
  const [current, setCurrent] = useState<GroupDetails | null>(null)
  const [before, setBefore] = useState<GroupDetails | null>(null)
  const [confirm, setConfirm] = useState('')
  const [notice, setNotice] = useState('')
  const groups = [...new Set(sessions.flatMap(s => s.role === 'consumer' ? [s.group] : []))]
  const inspect = () => run(async c => { const next = await c.describeGroup(group); setBefore(current?.group === group ? current : null); setCurrent(next); setNotice('조회 완료 · ' + new Date().toLocaleTimeString()) })
  return <details className="group-inspector"><summary>Consumer Group · 할당 / 리밸런싱 / Offset / Lag</summary>
    <p className="muted">조회 → Consumer 추가·중지 → 다시 조회하면 이전 할당과 비교합니다. 실제 Kafka의 commit은 지연될 수 있습니다. 단일 토픽·동일 구독에서는 Range와 Round Robin의 분배량이 같을 수 있습니다.</p>
    <div className="actions"><label>Group<input list="consumer-groups" value={group} onChange={e => { setGroup(e.target.value); setCurrent(null); setBefore(null); setConfirm('') }} /></label><datalist id="consumer-groups">{groups.map(g => <option key={g} value={g} />)}</datalist><button disabled={disabled || !group.trim()} onClick={() => void inspect()}>할당·Offset·Lag 조회</button></div>
    {current && <><div className="member-summary"><h3>이 화면의 Consumer별 조회 결과</h3>{sessions.filter(s => s.role === 'consumer' && s.group === current.group).map(s => { const assigned = current.partitions.filter(p => p.consumerId === s.id || p.consumerId.startsWith(s.id + ' · ')); return <p key={s.id}><strong>{s.name}</strong> · {statuses[s.id] === 'running' ? '프로세스 실행 중' : '정지'} · {assigned.length ? assigned.map(p => `${p.topic}/P${p.partition}`).join(', ') : '조회된 할당 없음 (가입 대기·유휴·정지 가능)'}</p> })}<p className="muted">마지막 조회 시점의 할당입니다. 실행 중 표시는 그룹 가입 완료를 뜻하지 않습니다. 리밸런싱 후 다시 조회하세요.</p></div><div className="table-scroll"><table><thead><tr><th>Topic / Partition</th><th>이전 Consumer</th><th>현재 Consumer</th><th>Committed</th><th>Log end</th><th>Lag</th></tr></thead><tbody>{current.partitions.map(p => <tr key={p.topic + '/' + p.partition}><td>{p.topic} / P{p.partition}</td><td>{before?.partitions.find(b => b.topic === p.topic && b.partition === p.partition)?.consumerId ?? '—'}</td><td>{p.consumerId}</td><td>{p.currentOffset ?? '미커밋'}</td><td>{p.logEndOffset ?? '—'}</td><td>{p.lag ?? '—'}</td></tr>)}</tbody></table></div><details><summary>Kafka 조회 원본</summary><pre>{current.raw}</pre></details></>}
    <p className="muted">처음부터 다시 읽으려면 정지한 Consumer의 그룹을 새 이름으로 바꾸고 --from-beginning을 선택하세요. 기존 그룹은 저장된 offset이 우선입니다.</p>
    <details><summary>기존 그룹 offset 초기화 실험</summary><p>그룹의 모든 Consumer를 중지한 후, 지정 토픽의 offset을 earliest로 변경합니다. 기존 처리 위치가 변경됩니다.</p><div className="actions"><label>대상 Topic<input value={topic} onChange={e => { setTopic(e.target.value); setConfirm('') }} /></label><label>확인: 그룹 이름을 입력<input value={confirm} onChange={e => setConfirm(e.target.value)} placeholder={group} /></label><button disabled={disabled || !group.trim() || confirm !== group || !topic.trim()} onClick={() => void run(async c => { await c.resetOffsets(group, topic); setConfirm(''); setBefore(null); setCurrent(await c.describeGroup(group)); setNotice('Offset을 earliest로 초기화했습니다.') })}>Offset 초기화 실행</button></div></details>
    {notice && <p role="status">{notice}</p>}
  </details>
}
