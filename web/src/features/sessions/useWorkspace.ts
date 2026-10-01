import { useEffect, useRef, useState } from 'react'
import type { ExecutionClient, ExecutionEvent } from '../../execution/types.ts'
import { LAST, parseScenario } from '../../storage/scenarios.ts'
import type { Scenario, Session, SessionStatus, Topic } from '../../domain/types.ts'
import { defaultScenario } from '../scenarios/defaultScenario.ts'
import { newSession } from './sessionModel.ts'

export function useWorkspace(createClient: () => ExecutionClient, mode: 'mock' | 'local' = 'mock') {
  const [initial] = useState(() => { try { const value = localStorage.getItem(LAST); return { scenario: value ? parseScenario(JSON.parse(value)) : null, error: '' } } catch (e) { return { scenario: null, error: '저장 설정 복원 실패: ' + String(e) } } })
  const client = useRef<ExecutionClient | null>(null)
  const lock = useRef(false)
  const number = useRef(2)
  const [sessions, setSessions] = useState<Session[]>(() => initial.scenario?.sessions ?? structuredClone(defaultScenario.sessions).map(session => mode === 'local' ? { ...session, topic: 'kts-practice', ...(session.role === 'consumer' ? { group: 'kts-demo-group' } : {}) } : session))
  const [connectionRevision, setConnectionRevision] = useState(0)
  const [topics, setTopics] = useState<Topic[]>([])
  const [statuses, setStatuses] = useState<Record<string, SessionStatus>>({})
  const [events, setEvents] = useState<ExecutionEvent[]>([])
  const [ready, setReady] = useState(false)
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)

  useEffect(() => {
    const executor = createClient()
    client.current = executor
    let active = true
    // 개발 중 HMR로 실행 어댑터가 다시 생성돼도 이전 실행 상태를 남기지 않는다.
    queueMicrotask(() => {
      if (!active) return
      setConnectionRevision(previous => previous + 1)
      setStatuses({}); setEvents([]); setReady(false); setError(initial.error); setBusy(false); lock.current = false
    })
    const unsubscribe = executor.subscribe(event => {
      if (!active) return
      if (event.type === 'connection') {
        if (!event.connected) { setReady(false); setError(event.message); setStatuses({}) }
        return
      }
      if (event.type === 'status') setStatuses(previous => ({ ...previous, [event.sessionId]: event.status }))
      // 세션별 상한을 적용해 다른 세션의 대량 출력이 기존 로그를 밀어내지 않게 한다.
      setEvents(previous => {
        const next = [...previous, event]
        const excess = next.filter(item => item.type !== 'connection' && item.sessionId === event.sessionId).length - 200
        let count = 0
        return next.filter(item => item.type === 'connection' || item.sessionId !== event.sessionId || ++count > excess)
      })
    })
    void (executor.mode === 'mock' ? Promise.all(defaultScenario.topics.map(topic => executor.createTopic(topic))) : Promise.resolve())
      .then(() => executor.listTopics())
      .then(result => { if (active) { setTopics(result); setReady(true) } })
      .catch((reason: unknown) => { if (active) setError(reason instanceof Error ? reason.message : '초기화 실패') })
    return () => { active = false; unsubscribe(); executor.dispose(); client.current = null }
  }, [createClient, initial.error])

  async function run(action: (executor: ExecutionClient) => Promise<void>): Promise<boolean> {
    if (!client.current || !ready || lock.current) return false
    lock.current = true
    setBusy(true)
    setError('')
    try { await action(client.current); return true }
    catch (reason) { setError(reason instanceof Error ? reason.message : '실행 실패'); return false }
    finally { lock.current = false; setBusy(false) }
  }
  function add() {
    if (lock.current || sessions.length >= 8) return
    const session = newSession(crypto.randomUUID(), ++number.current)
    if (mode === 'local') session.topic = 'kts-practice'
    setSessions(previous => [...previous, session])
  }
  function update(session: Session) {
    if (lock.current || statuses[session.id] === 'running') return
    setSessions(previous => previous.map(item => item.id === session.id ? session : item))
  }
  async function remove(id: string) {
    const success = await run(executor => executor.stopSession(id))
    if (!success) return
    setSessions(previous => previous.filter(item => item.id !== id))
    setEvents(previous => previous.filter(event => event.type === 'connection' || event.sessionId !== id))
    setStatuses(previous => { const next = { ...previous }; delete next[id]; return next })
  }
  async function refreshTopics() {
    return run(async executor => setTopics(await executor.listTopics()))
  }

  async function loadScenario(s: Scenario) {
    return run(async executor => {
      for (const session of sessions) await executor.stopSession(session.id)
      setConnectionRevision(v => v + 1); setSessions(structuredClone(s.sessions)); setStatuses({}); setEvents([]); number.current = s.sessions.length
    })
  }

  return { initialScenario: initial.scenario, loadScenario, connectionRevision, setTopics, refreshTopics, sessions, topics, statuses, events, ready, error, busy, run, add, update, remove }
}
