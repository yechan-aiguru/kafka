import { useEffect, useState } from 'react'
interface Settings { name: string; brokers: number; partitions: number; replication: number; minIsr: number; retentionHours: number; autoCreate: boolean; memoryMB: number }
interface Library { selected: string; existing: string; profiles: (Settings & { id: string })[] }
const single: Settings = { name: '단일 브로커 실습', brokers: 1, partitions: 3, replication: 1, minIsr: 1, retentionHours: 24, autoCreate: false, memoryMB: 1024 }
const triple: Settings = { ...single, name: '복제와 내구성 실습', brokers: 3, replication: 3, minIsr: 2 }
export function EnvironmentPanel({ busy, action }: { busy: boolean; action: (path: string, body?: unknown) => Promise<boolean> }) {
  const [library, setLibrary] = useState<Library | null>(null)
  const [form, setForm] = useState(single)
  const [error, setError] = useState('')
  const [notice, setNotice] = useState('')
  async function readLibrary(): Promise<Library> {
    const response = await fetch('/desktop/environments/list', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' })
    const data = await response.json()
    if (!response.ok) throw new Error(data.error ?? '환경 목록 조회 실패')
    return data
  }
  async function refresh() { setLibrary(await readLibrary()) }
  useEffect(() => {
    let active = true
    void readLibrary().then(data => { if (active) setLibrary(data) }).catch(e => { if (active) setError(String(e)) })
    return () => { active = false }
  }, [])
  async function perform(path: string, body: unknown) {
    setError(''); setNotice('')
    if (await action('/desktop/environments/' + path, body)) {
      try { await refresh(); setNotice(path === 'create' ? '환경을 저장했습니다. 목록에서 선택한 뒤 Kafka 시작을 누르세요.' : '환경을 전환했습니다. Kafka 시작 또는 다시 연결을 누르세요.') } catch (e) { setError(String(e)) }
    }
  }
  function count(brokers: number) { setForm(f => ({ ...f, brokers, replication: Math.min(f.replication, brokers), minIsr: Math.min(f.minIsr, brokers) })) }
  const active = library?.profiles.find(p => p.id === library.selected)
  return <details className="environment-panel"><summary>실습 환경 설정 · {active?.name ?? '기존 Kafka'}</summary>
    <p>독립된 Kafka 4.0.0 환경을 저장하고 선택합니다. 환경 전환 시 모든 Studio 탭의 실행 세션이 종료됩니다. 실습 설정은 먼저 저장하세요. 이전 환경의 Kafka는 계속 실행됩니다.</p>
    <div className="actions"><label>현재 환경<select aria-label="현재 실습 환경" disabled={busy || !library} value={library?.selected ?? 'existing'} onChange={e => void perform('select', { id: e.target.value })}><option value="existing">기존 Kafka · {library?.existing}</option>{library?.profiles.map(p => <option key={p.id} value={p.id}>{p.name} · 브로커 {p.brokers}개</option>)}</select></label><button disabled={busy} onClick={() => void refresh().catch(e => setError(String(e)))}>환경 목록 새로고침</button></div>
    {active && <p className="environment-summary">브로커 {active.brokers} · 기본 파티션 {active.partitions} · 기본 복제 수 {active.replication} · 최소 ISR {active.minIsr} · 보존 {active.retentionHours}시간 · 자동 토픽 생성 {active.autoCreate ? '켜짐' : '꺼짐'} · 메모리 한도 {active.memoryMB / 1024} GiB/브로커</p>}
    <form onSubmit={e => { e.preventDefault(); void perform('create', form) }}>
      <fieldset disabled={busy}><legend>새 환경 만들기</legend>
        <div className="actions"><button type="button" onClick={() => setForm(single)}>단일 브로커 프리셋</button><button type="button" onClick={() => setForm(triple)}>3개 브로커 · 복제 프리셋</button></div>
        <div className="environment-fields">
          <label>환경 이름<input required maxLength={60} value={form.name} onChange={e => setForm({ ...form, name: e.target.value })} /></label>
          <label>브로커 수<select value={form.brokers} onChange={e => count(Number(e.target.value))}><option value={1}>1개</option><option value={3}>3개</option></select></label>
          <label>기본 파티션 수<input type="number" required min={1} max={100} value={form.partitions} onChange={e => setForm({ ...form, partitions: Number(e.target.value) })} /></label>
          <label>기본 복제 수<input type="number" required min={1} max={form.brokers} value={form.replication} onChange={e => { const replication = Number(e.target.value); setForm({ ...form, replication, minIsr: Math.min(form.minIsr, replication) }) }} /></label>
          <label>최소 ISR<input type="number" required min={1} max={form.replication} value={form.minIsr} onChange={e => setForm({ ...form, minIsr: Number(e.target.value) })} /></label>
          <label>보존 시간<input type="number" required min={1} max={720} value={form.retentionHours} onChange={e => setForm({ ...form, retentionHours: Number(e.target.value) })} /></label>
          <label>브로커당 메모리 한도<select value={form.memoryMB} onChange={e => setForm({ ...form, memoryMB: Number(e.target.value) })}><option value={1024}>1 GiB</option><option value={2048}>2 GiB</option><option value={4096}>4 GiB</option></select></label>
          <label className="check"><input type="checkbox" checked={form.autoCreate} onChange={e => setForm({ ...form, autoCreate: e.target.checked })} />자동 토픽 생성</label>
        </div>
        <p className="muted">메모리 한도 합계 {form.brokers * form.memoryMB / 1024} GiB. 다른 실행 환경도 메모리를 사용합니다. 사용하지 않는 환경은 선택 후 중지하세요.</p>
        <button type="submit">새 환경으로 저장</button>
      </fieldset>
    </form>
    <p className="muted">설정은 새 환경에 적용됩니다. 수정하려면 값을 바꿔 다른 이름으로 저장하세요. 기본 파티션·복제 수는 자동 생성 토픽에 적용되며, 토픽 생성 화면에서 지정한 값이 우선합니다. 최소 ISR은 Producer의 acks=all과 함께 실습하세요. 보존 시간은 즉시 삭제를 보장하지 않습니다.</p>
    <p className="muted">중지·앱 종료 후에도 환경과 데이터는 유지됩니다. Docker 컨테이너를 직접 삭제하면 데이터가 사라집니다. 외부 포트를 열지 않는 로컬 학습용 구성입니다.</p>
    {notice && <p role="status" className="topic-notice">{notice}</p>}{error && <p role="alert" className="error">{error}</p>}
  </details>
}
