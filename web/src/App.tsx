import { ExceptionLab } from './features/experiments/ExceptionLab'
import { LocalStudio } from './features/local/LocalStudio'
import { useMemo, useState } from 'react'
import { ScenarioLibrary } from './features/scenarios/ScenarioLibrary'
import { GroupInspector } from './features/groups/GroupInspector'
import { defaultScenario } from './features/scenarios/defaultScenario'
import { useWorkspace } from './features/sessions/useWorkspace'
import { TopicManager } from './features/topics/TopicManager'
import { SessionPanel } from './features/terminal/SessionPanel'
import { MockExecutionClient } from './execution/mock'
import { createLocalExecutionClient, type LocalOptions } from './execution/local'
import type { ExecutionClient } from './execution/types'
import './App.css'

const createMockClient = () => new MockExecutionClient()
function App() {
  const localStudio = document.querySelector('meta[name="kts-local-studio"]') !== null
  const [url, setUrl] = useState('http://127.0.0.1:4317')
  const [token, setToken] = useState('')
  const [connection, setConnection] = useState<LocalOptions | null>(null)
  const [attempt, setAttempt] = useState(0)
  const [error, setError] = useState('')
  const createClient = useMemo(() => connection ? () => createLocalExecutionClient(connection) : createMockClient, [connection])
  return <>
    {localStudio ? <LocalStudio connect={() => { setConnection({ url: location.origin, token: 'local-studio-cookie' }); setAttempt(n => n + 1) }} disconnect={() => setConnection(null)} /> : <form className="connection-bar" onSubmit={e => {
      e.preventDefault()
      try {
        const parsed = new URL(url)
        if (parsed.protocol !== 'http:' || !['127.0.0.1', 'localhost'].includes(parsed.hostname) || parsed.username || parsed.password) throw new Error('http://127.0.0.1 또는 localhost 주소를 사용하세요.')
        if (!token.trim()) throw new Error('로컬 연결 서버의 토큰을 입력하세요.')
        setError(''); setConnection({ url: parsed.origin, token }); setToken(''); setAttempt(n => n + 1)
      } catch (reason) { setError(reason instanceof Error ? reason.message : '연결 설정 오류') }
    }}>
      <label>연결 서버<input aria-label="연결 서버 주소" value={url} onChange={e => setUrl(e.target.value)} /></label>
      <label>연결 토큰<input type="password" autoComplete="off" value={token} onChange={e => setToken(e.target.value)} placeholder="local-agent/.local/token 파일의 값" /></label>
      <button type="submit">실제 Kafka 연결</button>
      {connection && <button type="button" onClick={() => { setConnection(null); setToken('') }}>연결 해제 · 모의 모드</button>}
      <span>전환·새로고침 시 기존 세션을 중지합니다. 토큰은 저장하지 않습니다.</span>
      {location.protocol === 'https:' && <p>배포 화면의 실제 연결은 브라우저의 로컬 네트워크 권한과 연결 서버의 허용 Origin 설정이 필요합니다. 연결이 제한되면 로컬 웹 화면을 사용하세요.</p>}
      {error && <p role="alert">{error}</p>}
    </form>}
    <Workspace key={attempt + (connection ? '-local' : '-mock')} createClient={createClient} mode={connection ? 'local' : 'mock'} />
  </>
}
function Workspace({ createClient, mode }: { createClient: () => ExecutionClient; mode: 'local' | 'mock' }) {
  const workspace = useWorkspace(createClient, mode)
  const { sessions, topics, statuses, events, ready, busy, error, run } = workspace
  const [name, setName] = useState(workspace.initialScenario?.name ?? defaultScenario.name)
  const [plannedTopics, setPlannedTopics] = useState(workspace.initialScenario?.topics ?? defaultScenario.topics.map(t => mode === 'local' ? { ...t, name: 'kts-practice' } : t))
  const [stacked, setStacked] = useState(false)
  const [admin, setAdmin] = useState(false)
  
  return <div className="studio">
    <header className="topbar"><a className="brand" href="/">⌘ <span>Kafka Terminal Studio</span></a><span className="mode">{mode === 'mock' ? '모의 모드 · 실제 Kafka 미연결' : ready ? '실제 Kafka · 로컬 연결됨' : '실제 Kafka · 연결 확인 필요'}</span></header>
    <div className="layout">
      <aside><span className="eyebrow">WORKSPACE</span><h2>실습 공간</h2><label className="scenario-name">실습 이름<input value={name} onChange={e => setName(e.target.value)} /></label>
        <ScenarioLibrary name={name} sessions={sessions} topics={plannedTopics.map(t => topics.find(actual => actual.name === t.name) ?? t).concat(topics.filter(t => !t.name.startsWith('__') && !plannedTopics.some(p => p.name === t.name)))} disabled={!ready || busy} load={async s => { if (!await workspace.loadScenario(s)) return false; setName(s.name); setPlannedTopics(s.topics); return true }} />
        <details><summary>불러온 토픽 정의</summary>{plannedTopics.map(t => <p key={t.name}>{t.name} · {t.partitions} partitions · 복제 {t.replicationFactor}</p>)}</details>
        <button disabled={!ready || busy} onClick={() => void run(async executor => { const existing = await executor.listTopics(); for (const t of plannedTopics) if (!existing.some(x => x.name === t.name)) await executor.createTopic(t); workspace.setTopics(await executor.listTopics()) })}>불러온 토픽 중 없는 토픽 생성</button><p className="muted">기존 토픽의 파티션·복제 수는 자동 변경하지 않습니다.</p>
        <div className="aside-section"><span className="eyebrow">TOPICS</span><div className="topic-list">{topics.map(topic => <div key={topic.name} className="topic-line"><span>{topic.name}</span><span>{topic.partitions} partitions</span></div>)}</div><button className="admin-button" aria-expanded={admin} onClick={() => setAdmin(!admin)}>토픽 관리 {admin ? '닫기' : '열기'}</button><p className="muted">토픽 관리에서 생성·상세 조회·파티션 증가를 진행할 수 있습니다.</p></div>
        <div className="aside-section"><span className="eyebrow">실행 안내</span><p>{mode === 'mock' ? '모의 클러스터 · 브로커 1개' : '실제 Kafka CLI · 프로세스 실행'}</p><p className="muted">{mode === 'mock' ? '단일 토픽의 할당·즉시 commit을 단순화한 학습 모델입니다. 실제 Kafka 결과와는 다를 수 있습니다.' : '실제 브로커의 할당·offset에 따라 수신합니다. 웹 연결이 끊기면 약 20초 안에 세션 정리를 시작합니다.'}</p></div>
      </aside>
      <main><div className="page-heading"><div><span className="eyebrow">EXPERIMENT / SESSIONS</span><h1>{name || '이름 없는 실습'}</h1><p className="muted">역할과 인자를 바꾸고, 터미널 출력을 나란히 비교하세요.</p></div><div className="actions"><button onClick={() => setStacked(!stacked)} aria-pressed={stacked}>{stacked ? '나란히 보기' : '세로 보기'}</button><button onClick={workspace.add} disabled={!ready || busy}>＋ 세션 추가</button></div></div>
        <div className="route"><span>Producer × {sessions.filter(s => s.role === 'producer').length}</span><span aria-hidden="true">→</span><strong>{mode === 'mock' ? '모의 클러스터' : '실제 Kafka'}</strong><span aria-hidden="true">→</span><span>Consumer × {sessions.filter(s => s.role === 'consumer').length}</span><span>{Object.values(statuses).filter(s => s === 'running').length}개 실행 중</span></div>
        {error && <p className="error" role="alert">{error}</p>}
        {admin && <TopicManager key={workspace.connectionRevision} topics={topics} disabled={!ready || busy} mode={mode} events={events} run={run} onTopics={workspace.setTopics} />}
        <ExceptionLab mode={mode} disabled={!ready || busy} sessions={sessions} statuses={statuses} run={run} onTopics={workspace.setTopics} load={async s => { if (!await workspace.loadScenario(s)) return false; setName(s.name); setPlannedTopics(s.topics); return true }} />
        <GroupInspector statuses={statuses} key={workspace.connectionRevision} sessions={sessions} disabled={!ready || busy} run={run} />
        <div className={`terminals ${stacked ? 'stacked' : ''}`}>{sessions.map(session => <SessionPanel mode={mode} key={session.id} session={session} running={statuses[session.id] === 'running'} disabled={!ready || busy} events={events} update={workspace.update} remove={() => void workspace.remove(session.id)} toggle={() => void run(executor => statuses[session.id] === 'running' ? executor.stopSession(session.id) : executor.startSession(session))} send={text => run(executor => executor.sendInput(session.id, text))} />)}</div>
        {!sessions.length && <div className="empty-state">세션이 없습니다. ‘세션 추가’로 실습을 시작하세요.</div>}
        <p className="workspace-note">{ready ? (mode === 'mock' ? '모의 실행 준비 완료' : '실제 Kafka 연결 준비 완료') : '연결 초기화 중 또는 오류 · 위 안내를 확인하세요'} · 세션별 최근 이벤트 200개 보관 · 새로고침 시 초기화</p>
        <p className="simulation-note">실행 중에는 인자 변경이 잠깁니다. 실행 중인 세션을 삭제하면 먼저 중지합니다. 저장된 실습을 불러오면 현재 세션을 중지하고 설정만 복원합니다.</p>
      </main>
    </div>
  </div>
}
export default App
