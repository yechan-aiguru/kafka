import { EnvironmentPanel } from './EnvironmentPanel'
import { useEffect, useRef, useState } from 'react'
interface Status { container: string; docker: boolean; exists: boolean; running: boolean; message: string }
export function LocalStudio({ connect, disconnect }: { connect: () => void; disconnect: () => void }) {
  const [status, setStatus] = useState<Status | null>(null)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const [confirmation, setConfirmation] = useState(false)
  const [quit, setQuit] = useState(false)
  const lock = useRef(false)
  const firstConnect = useRef(connect)
  async function request<T>(path: string, body: unknown = {}): Promise<T> {
    const response = await fetch(path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body), signal: AbortSignal.timeout(200000) })
    const data = await response.json()
    if (!response.ok) throw new Error(data.error ?? '로컬 실행 요청 실패')
    return data as T
  }
  useEffect(() => {
    let active = true
    void request<Status>('/desktop/status').then(value => { if (active) { setStatus(value); if (value.running) firstConnect.current() } }).catch(e => { if (active) setError(String(e)) })
    return () => { active = false }
  }, [])
  async function action(path: string, body: unknown = {}): Promise<boolean> {
    if (lock.current) return false
    lock.current = true; setBusy(true); setError('')
    try {
      if (path.endsWith('/stop') || path.endsWith('/quit') || path.endsWith('/select')) disconnect()
      if (path.endsWith('/quit')) { await request(path); setQuit(true); return true }
      const result = await request<Status>(path, body)
      const value = path.includes('/environments/') ? await request<Status>('/desktop/status') : result; setStatus(value); setConfirmation(false)
      if (path === '/desktop/kafka/start' && value.running) connect()
      return true
    } catch (e) { setError(String(e)); return false }
    finally { lock.current = false; setBusy(false) }
  }
  return <section className="local-studio" aria-label="로컬 실행 관리">
    <div><strong>Kafka Studio · 로컬 전용</strong><p>아이콘 하나로 실행 · 토큰 입력 없이 연결</p></div>
    {quit ? <p role="status">앱을 종료했습니다. 다시 사용하려면 Kafka Studio 아이콘을 여세요. Kafka 컨테이너는 그대로 유지됩니다.</p> : <>
      <p role="status">{status ? `${status.container} · ${status.running ? '컨테이너 실행 중' : status.docker ? 'Kafka 시작 필요' : 'Docker 시작 필요'}` : '상태 확인 중'}</p>
      <div className="actions">
        <button disabled={busy} onClick={() => void action('/desktop/status')}>상태 새로고침</button>
        {status && !status.docker && <button disabled={busy} onClick={() => void action('/desktop/docker/start')}>Docker 환경 시작 · Colima</button>}
        <button disabled={busy || !status?.docker || !status.exists || status.running} onClick={() => void action('/desktop/kafka/start')}>Kafka 시작</button>
        <button disabled={busy || !status?.running} onClick={connect}>Kafka 연결 · 다시 연결</button>
        <button disabled={busy} onClick={disconnect}>모의 모드</button>
      </div>
      <EnvironmentPanel busy={busy} action={action} />
      <details><summary>중지 및 종료</summary><p>Kafka 중지는 이 컨테이너를 사용하는 다른 앱에도 영향을 줍니다. 저장하지 않은 설정은 먼저 저장하세요.</p><label className="check"><input type="checkbox" checked={confirmation} disabled={busy} onChange={e => setConfirmation(e.target.checked)} />{status?.container ?? '대상 Kafka'}를 중지하겠습니다.</label><div className="actions"><button disabled={busy || !status?.running || !confirmation} onClick={() => void action('/desktop/kafka/stop')}>Kafka 중지</button><button disabled={busy} onClick={() => void action('/desktop/quit')}>Studio 종료 · Kafka 유지</button></div></details>
      <p className="muted">{busy ? '처리 중입니다. Docker 시작은 수 분 걸릴 수 있습니다.' : status?.message} {status?.running && '브로커가 준비되기 전 연결에 실패하면 잠시 후 다시 연결하세요.'}</p>
      {status?.docker && !status.exists && <p>설정된 컨테이너가 없습니다. 기존 Kafka 설치·컨테이너 이름을 확인하세요.</p>}
      {!status?.docker && <p className="muted">Docker Desktop을 사용하는 경우 해당 앱을 먼저 실행하세요. Colima 버튼은 설치된 Colima를 시작합니다.</p>}
    </>}
    {error && <p role="alert" className="error">{error}</p>}
  </section>
}
