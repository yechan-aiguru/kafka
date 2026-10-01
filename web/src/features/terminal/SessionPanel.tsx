import { useEffect, useRef, useState } from 'react'
import type { Session } from '../../domain/types'
import type { ExecutionEvent } from '../../execution/types'
import { sessionCommand } from '../../execution/commands'
import { changeRole } from '../sessions/sessionModel'

interface Props {
  mode: 'mock' | 'local'
  session: Session
  running: boolean
  disabled: boolean
  events: ExecutionEvent[]
  update: (session: Session) => void
  remove: () => void
  toggle: () => void
  send: (text: string) => Promise<boolean>
}
export function SessionPanel({ mode, session, running, disabled, events, update, remove, toggle, send }: Props) {
  const [message, setMessage] = useState('{"orderId":1001}')
  const [batchCount, setBatchCount] = useState(10)
  const [interval, setIntervalMs] = useState(250)
  const [batchStatus, setBatchStatus] = useState('')
  const [batching, setBatching] = useState(false)
  const batch = useRef<AbortController | null>(null)
  useEffect(() => { if (!running) batch.current?.abort(); return () => batch.current?.abort() }, [running])
  async function sendBatch() {
    if (batch.current || !Number.isInteger(batchCount) || batchCount < 1 || batchCount > 100 || !Number.isInteger(interval) || interval < 0 || interval > 5000) return
    const controller = new AbortController(); batch.current = controller; setBatching(true)
    let sent = 0
    try {
      for (let i = 1; i <= batchCount && !controller.signal.aborted; i++) {
        if (!await send(message.replaceAll('{n}', String(i)))) break
        setBatchStatus(`${++sent}/${batchCount}건 전송 요청 완료`)
        if (i < batchCount && !controller.signal.aborted) await new Promise<void>(resolve => { const done = () => { clearTimeout(timer); controller.signal.removeEventListener('abort', done); resolve() }; const timer = setTimeout(done, interval); controller.signal.addEventListener('abort', done, { once: true }) })
      }
    } finally { batch.current = null; setBatching(false); setBatchStatus(`${sent}건 전송 요청 완료${controller.signal.aborted ? ' · 취소됨' : ''}`) }
  }
  const [follow, setFollow] = useState(true)
  const body = useRef<HTMLDivElement>(null)
  const output = events.filter(event => event.type === 'output' && event.sessionId === session.id)
  useEffect(() => {
    if (follow && body.current) body.current.scrollTop = body.current.scrollHeight
  }, [events, follow])
  return <section className="terminal" aria-label={session.name} onKeyDown={e => { if (e.ctrlKey && e.key.toLowerCase() === 'c' && running && !disabled) { e.preventDefault(); toggle() } }}>
    <div className="terminal-heading"><span>{session.name} · {session.role}</span><span className={running ? 'running' : 'stopped'}>{running ? '● 실행 중' : '○ 정지'}</span></div>
    <div className="terminal-toolbar">
      <label>역할 <select aria-label="세션 역할" value={session.role} disabled={running || disabled} onChange={e => update(changeRole(session, e.target.value as Session['role']))}><option value="producer">Producer</option><option value="consumer">Consumer</option></select></label>
      <div className="actions"><button disabled={disabled} onClick={toggle}>{running ? '■ 중지' : '▶ 시작'}</button><button disabled={disabled} onClick={remove}>삭제</button></div>
    </div>
    <details className="session-settings"><summary>인자 설정 · {session.topic || '토픽 미설정'}</summary>
      <fieldset disabled={running || disabled}>
        <label>세션 이름<input value={session.name} onChange={e => update({ ...session, name: e.target.value })} /></label>
        <label>Bootstrap server<input value={session.bootstrapServer} onChange={e => update({ ...session, bootstrapServer: e.target.value })} /></label>
        <label>Topic<input value={session.topic} onChange={e => update({ ...session, topic: e.target.value })} /></label>
        {session.role === 'producer' ? <><label>Producer 파티션 선택<select value={session.partitioner ?? 'default'} onChange={e => update({ ...session, partitioner: e.target.value as 'default' | 'round-robin' })}><option value="default">기본 · key 있으면 해시</option><option value="round-robin">Round Robin · key 무시</option></select></label><label>acks<select value={session.acks} onChange={e => update({ ...session, acks: e.target.value as 'all' | '1' | '0' })}><option>all</option><option>1</option><option>0</option></select></label><label className="check"><input type="checkbox" checked={session.parseKey} onChange={e => update({ ...session, parseKey: e.target.checked })} />key:value 입력</label></> : <><label>Consumer 할당 전략 · classic<select value={session.assignor ?? 'range'} onChange={e => update({ ...session, assignor: e.target.value as 'range' | 'round-robin' })}><option value="range">Range</option><option value="round-robin">Round Robin</option></select></label><label>Consumer group<input value={session.group} onChange={e => update({ ...session, group: e.target.value })} /></label><label className="check"><input type="checkbox" checked={session.fromBeginning} onChange={e => update({ ...session, fromBeginning: e.target.checked })} />--from-beginning</label></>}
      </fieldset>
      <p className="system">{session.role === 'producer' ? 'key:value를 켜면 기본 전략은 key를 해시합니다. 파티션 수가 달라지면 같은 key의 목적지도 달라질 수 있습니다. 모의 기본 무키 전략은 5건 단위 sticky로 단순화합니다.' : 'Kafka 4.0 classic 프로토콜 기준. 같은 그룹의 모든 Consumer는 같은 전략을 선택하세요.'}</p>
      <p className="system">{running ? '실행 중에는 설정이 잠깁니다. 중지 후 변경하세요.' : '변경한 인자는 다음 시작에 적용됩니다.'}</p>
    </details>
    <details className="command-preview" open><summary>실행할 명령어</summary><code>{sessionCommand(session).display}</code></details>
    <div className="terminal-body" ref={body} role="log" aria-label={`${session.name} 출력`} tabIndex={0}>
      <div className="system">{mode === 'mock' ? '# 모의 실행 · 실제 쉘은 실행하지 않습니다.' : '# 실제 Kafka CLI · stdin/stdout/stderr 연결'}</div>
      {output.map((event, index) => event.type === 'output' && <span key={index} className={event.stream}>{event.stream === 'stdin' ? '> ' : ''}{event.text}</span>)}
    </div>
    <label className="follow"><input type="checkbox" checked={follow} onChange={e => setFollow(e.target.checked)} />새 출력 따라가기 · 최근 200개 이벤트</label>
    {session.role === 'producer' && <form className="terminal-input" onSubmit={e => { e.preventDefault(); void send(message) }}>
      <span aria-hidden="true">❯</span><input aria-label="전송 메시지" value={message} disabled={!running || disabled || batching} onChange={e => setMessage(e.target.value)} />
      <button disabled={!running || disabled || batching || !message.trim()}>전송 ↵</button>
    </form>}
    {session.role === 'producer' && <details className="batch-settings"><summary>반복 전송 실험</summary><p className="muted">입력의 {'{n}'}을 순번으로 바꿉니다. 같은 key 비교: customer-1:message-{'{n}'}</p><div className="actions"><button disabled={batching || disabled} onClick={() => setMessage(session.parseKey ? 'customer-1:message-{n}' : 'message-{n}')}>메시지 템플릿</button><label>건수<input type="number" min="1" max="100" value={batchCount} disabled={batching} onChange={e => setBatchCount(Number(e.target.value))} /></label><label>간격(ms)<input type="number" min="0" max="5000" value={interval} disabled={batching} onChange={e => setIntervalMs(Number(e.target.value))} /></label><button disabled={!running || disabled || batching || !message.trim()} onClick={() => void sendBatch()}>반복 전송</button><button disabled={!batching} onClick={() => batch.current?.abort()}>전송 취소</button></div><p role="status">{batchStatus}</p></details>}
  </section>
}
