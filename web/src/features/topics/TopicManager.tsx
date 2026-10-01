import { useState } from 'react'
import type { ClusterInfo, Topic, TopicDetails } from '../../domain/types'
import type { ExecutionClient, ExecutionEvent } from '../../execution/types'
import { shellQuote } from '../../execution/commands'

interface Props {
  topics: Topic[]
  disabled: boolean
  mode: 'mock' | 'local'
  events: ExecutionEvent[]
  run: (action: (client: ExecutionClient) => Promise<void>) => Promise<boolean>
  onTopics: (topics: Topic[]) => void
}
export function TopicManager({ topics, disabled, mode, events, run, onTopics }: Props) {
  const [name, setName] = useState('')
  const [partitions, setPartitions] = useState('3')
  const [replicas, setReplicas] = useState('1')
  const [cluster, setCluster] = useState<ClusterInfo | null>(null)
  const [selected, setSelected] = useState('')
  const [detail, setDetail] = useState<TopicDetails | null>(null)
  const [target, setTarget] = useState('')
  const [change, setChange] = useState<{ before: TopicDetails; after: TopicDetails } | null>(null)
  const [notice, setNotice] = useState('')
  const [validation, setValidation] = useState('')
  const bootstrap = cluster?.bootstrapServer ?? 'localhost:9092'
  const output = events.filter(e => e.type === 'output' && e.sessionId === 'admin')
  async function inspect(topic: string) {
    setSelected(topic); setDetail(null); setChange(null); setValidation(''); setNotice('')
    if (!topic) return
    await run(async client => {
      const result = await client.describeTopic(topic)
      setDetail(result); setTarget(String(result.partitions + 1))
    })
  }
  async function refresh() {
    setValidation('')
    await run(async client => {
      const result = await client.listTopics()
      onTopics(result)
      if (selected && !result.some(topic => topic.name === selected)) { setSelected(''); setDetail(null); setChange(null) }
      else if (selected) { const current = await client.describeTopic(selected); setDetail(current); setTarget(String(current.partitions + 1)); setChange(null) }
    })
  }
  async function create() {
    setValidation(''); setNotice('')
    const topic = { name: name.trim(), partitions: Number(partitions), replicationFactor: Number(replicas) }
    if (!/^[a-zA-Z0-9._-]{1,249}$/.test(topic.name) || ['.', '..'].includes(topic.name)) { setValidation('토픽 이름은 영문·숫자·점·밑줄·하이픈으로 입력하세요.'); return }
    if (topics.some(item => item.name === topic.name)) { setValidation('이미 존재하는 토픽입니다. 새 이름을 입력하세요.'); return }
    if (!Number.isInteger(topic.partitions) || topic.partitions < 1 || topic.partitions > 100) { setValidation('파티션 수는 1~100 사이의 정수여야 합니다.'); return }
    if (!Number.isInteger(topic.replicationFactor) || topic.replicationFactor < 1 || topic.replicationFactor > Math.min(cluster?.brokerIds.length ?? 10, 10)) { setValidation('Replication factor가 유효하지 않습니다. 브로커 수를 확인하세요.'); return }
    await run(async client => {
      await client.createTopic(topic)
      setNotice(`${topic.name} 생성 완료. 상세 정보를 조회합니다.`)
      setSelected(topic.name); setDetail(null); setChange(null); setName('')
      onTopics(await client.listTopics())
      const result = await client.describeTopic(topic.name)
      setDetail(result); setTarget(String(result.partitions + 1))
    })
  }
  async function increase() {
    if (!detail) return
    setValidation(''); setNotice('')
    const count = Number(target)
    if (!Number.isInteger(count) || count <= detail.partitions || count > 100) { setValidation(`현재 ${detail.partitions}개보다 큰 정수(최대 100)를 입력하세요. 감소는 지원하지 않습니다.`); return }
    await run(async client => {
      const before = await client.describeTopic(detail.name)
      if (count <= before.partitions) { setDetail(before); throw new Error('다른 작업에서 파티션 수가 변경됐습니다. 최신 수보다 크게 입력하세요.') }
      await client.increasePartitions(before.name, count)
      setNotice(`${before.name}: 파티션 증가 요청 완료. 실제 상태를 다시 조회합니다.`)
      // 조회 실패 시 기존 정보를 현재 상태로 오인하지 않도록 비운다.
      setDetail(null); setChange(null)
      onTopics(await client.listTopics())
      const after = await client.describeTopic(before.name)
      setDetail(after); setTarget(String(after.partitions + 1)); setChange({ before, after })
      setNotice(`파티션 변경 확인: ${before.partitions} → ${after.partitions}`)
    })
  }
  return <section className="topic-manager" aria-label="토픽 관리">
    <div className="topic-manager-header"><div><span className="eyebrow">TOPIC LAB</span><h2>토픽과 파티션</h2></div><div className="actions"><button disabled={disabled} onClick={() => void refresh()}>목록 새로고침</button><button disabled={disabled} onClick={() => void run(async client => setCluster(await client.clusterInfo()))}>브로커 조회</button></div></div>
    <p className="muted">{cluster ? `브로커 ${cluster.brokerIds.length}개 · ID ${cluster.brokerIds.join(', ')} · ${cluster.bootstrapServer}` : '브로커 수를 조회하면 생성 가능한 복제 수를 확인할 수 있습니다.'}{mode === 'mock' && ' · 모의 데이터'}</p>
    <div className="topic-forms">
      <form onSubmit={e => { e.preventDefault(); void create() }} aria-label="토픽 생성">
        <h3>새 토픽 생성</h3><fieldset disabled={disabled}>
          <label>토픽 이름<input value={name} onChange={e => setName(e.target.value)} placeholder="예: orders-lab" required maxLength={249} /></label>
          <div className="topic-pair"><label>파티션 수<input type="number" min="1" max="100" step="1" required value={partitions} onChange={e => setPartitions(e.target.value)} /></label><label>Replication factor<input type="number" min="1" max={Math.min(cluster?.brokerIds.length ?? 10, 10)} step="1" required value={replicas} onChange={e => setReplicas(e.target.value)} /></label></div>
          <button type="submit">토픽 생성</button>
        </fieldset>
        <code className="topic-command">bin/kafka-topics.sh --bootstrap-server {shellQuote(bootstrap)} --create --topic {shellQuote(name.trim())} --partitions {partitions} --replication-factor {replicas}</code>
      </form>
      <div><h3>기존 토픽 조회·변경</h3><label className="topic-select">조회할 토픽<select value={selected} disabled={disabled} onChange={e => void inspect(e.target.value)}><option value="">토픽 선택</option>{topics.map(topic => <option key={topic.name} value={topic.name}>{topic.name} ({topic.partitions})</option>)}</select></label>
        <button className="detail-refresh" disabled={disabled || !selected} onClick={() => void inspect(selected)}>상세 다시 조회</button>
        {detail && <form onSubmit={e => { e.preventDefault(); void increase() }} aria-label="파티션 증가"><p>현재 <strong>{detail.partitions}개</strong> · 복제 수 {detail.replicationFactor}</p><label className="topic-select">변경할 총 파티션 수<input type="number" min={detail.partitions + 1} max="100" step="1" required value={target} disabled={disabled || selected.startsWith('__')} onChange={e => setTarget(e.target.value)} /></label><button disabled={disabled || selected.startsWith('__') || detail.partitions >= 100}>파티션 증가 적용</button><p className="muted">{selected.startsWith('__') ? '내부 토픽은 조회만 지원합니다.' : '감소는 불가합니다. 기존 메시지는 재분배되지 않으며 key의 목적 파티션이 달라질 수 있습니다.'}</p><code className="topic-command">bin/kafka-topics.sh --bootstrap-server {shellQuote(bootstrap)} --alter --topic {shellQuote(detail.name)} --partitions {target}</code></form>}
      </div>
    </div>
    {validation && <p className="error" role="alert">{validation}</p>}{notice && <p className="topic-notice" role="status">{notice}</p>}
    {detail && <div className="topic-details"><h3>{detail.name} <span className="muted">{change ? `${change.before.partitions} → ${change.after.partitions} partitions` : `${detail.partitions} partitions`}</span></h3><div className="topic-table-wrap"><table><thead><tr><th>파티션</th><th>Leader</th><th>Replicas</th><th>ISR</th><th>변경</th></tr></thead><tbody>{detail.partitionDetails.map(partition => <tr key={partition.id}><td>P{partition.id}</td><td>{partition.leader === -1 ? '없음 (-1)' : partition.leader}</td><td>{partition.replicas.join(', ') || '없음'}</td><td>{partition.isr.join(', ') || '없음'}</td><td>{change ? change.before.partitionDetails.some(p => p.id === partition.id) ? '기존' : '추가됨' : '—'}</td></tr>)}</tbody></table></div></div>}
    <details className="terminal admin-terminal" open><summary className="terminal-heading">Admin · 실행한 명령과 결과 {mode === 'mock' ? '(모의)' : '(실제)'}</summary><pre className="admin-output" tabIndex={0} aria-label="관리 명령 출력">{output.map((event, index) => event.type === 'output' && <span key={index} className={event.stream}>{event.text}</span>)}</pre></details>
  </section>
}
