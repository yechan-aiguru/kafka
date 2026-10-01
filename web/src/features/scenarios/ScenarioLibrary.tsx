import { useState } from 'react'
import type { Scenario, Session, Topic } from '../../domain/types'
import { LAST, parseScenario, readScenarios, writeScenarios } from '../../storage/scenarios'
import { defaultScenario } from './defaultScenario'

export function ScenarioLibrary({ name, sessions, topics, disabled, load }: { name: string; sessions: Session[]; topics: Topic[]; disabled: boolean; load: (s: Scenario) => Promise<boolean> }) {
  const [initial] = useState(() => { try { return { items: readScenarios(localStorage), error: '' } } catch (e) { return { items: [] as Scenario[], error: String(e) } } })
  const [items, setItems] = useState<Scenario[]>(initial.items)
  const [selected, setSelected] = useState('')
  const [notice, setNotice] = useState(initial.error)
  const [overwrite, setOverwrite] = useState(false)
  const current = () => parseScenario({ schemaVersion: 1, id: crypto.randomUUID(), name, sessions, topics })
  function safely(action: () => void) { try { action() } catch (e) { setNotice('저장/불러오기 실패: ' + String(e)) } }
  async function apply(s: Scenario) {
    if (await load(s)) { try { localStorage.setItem(LAST, JSON.stringify(s)); setNotice('설정을 불러왔습니다. 토픽 준비와 세션 시작은 직접 진행하세요.') } catch (e) { setNotice('설정은 복원했지만 브라우저 저장은 실패했습니다: ' + String(e)) } }
  }
  return <div className="aside-section scenario-library"><span className="eyebrow">SCENARIOS</span>
    <label className="check"><input type="checkbox" checked={overwrite} onChange={e => setOverwrite(e.target.checked)} />같은 이름 덮어쓰기 허용</label>
    <button disabled={disabled} onClick={() => safely(() => {
      const s = current(), existing = items.find(i => i.name === s.name)
      if (existing && !overwrite) throw new Error('같은 이름이 있습니다. 덮어쓰기 옵션을 선택하세요.')
      const next = [...items.filter(i => i.name !== s.name), { ...s, id: existing?.id ?? s.id }]
      writeScenarios(localStorage, next); setItems(next); localStorage.setItem(LAST, JSON.stringify(s)); setNotice('저장했습니다. 새로고침하면 마지막 저장/불러온 설정이 복원됩니다.')
    })}>현재 실습 저장</button>
    <select aria-label="저장된 실습" value={selected} onChange={e => setSelected(e.target.value)}><option value="">저장된 실습 선택</option>{items.map(i => <option key={i.id} value={i.id}>{i.name}</option>)}</select>
    <div className="actions"><button disabled={disabled || !selected} onClick={() => { const s = items.find(i => i.id === selected); if (s) void apply(s) }}>불러오기</button><button disabled={!selected} onClick={() => safely(() => { const next = items.filter(i => i.id !== selected); writeScenarios(localStorage, next); if (localStorage.getItem(LAST) && parseScenario(JSON.parse(localStorage.getItem(LAST)!)).name === items.find(i => i.id === selected)?.name) localStorage.removeItem(LAST); setItems(next); setSelected(''); setNotice('삭제했습니다.') })}>저장 삭제</button></div>
    <button onClick={() => safely(() => { const blob = new Blob([JSON.stringify(current(), null, 2)], { type: 'application/json' }); const url = URL.createObjectURL(blob); const a = document.createElement('a'); a.href = url; a.download = 'kafka-scenario.json'; a.click(); setTimeout(() => URL.revokeObjectURL(url), 1000) })}>JSON 내보내기</button>
    <label>JSON 가져오기<input type="file" accept=".json,application/json" disabled={disabled} onChange={async e => { const f = e.target.files?.[0]; e.target.value = ''; if (!f) return; try { if (f.size > 262144) throw new Error('파일은 256KB 이하여야 합니다.'); await apply(parseScenario(JSON.parse(await f.text()))) } catch (e) { setNotice(String(e)) } }} /></label>
    <label>기본 템플릿<select defaultValue="" disabled={disabled} onChange={e => { const kind = e.target.value; e.target.value = ''; if (!kind) return; const s = structuredClone(defaultScenario); s.id = crypto.randomUUID(); s.name = kind === 'single' ? '단일 Producer / Consumer' : kind === 'same' ? '같은 그룹 · 분담' : '다른 그룹 · 각각 수신'; if (kind !== 'single') { const c = s.sessions[1]; if (c.role === 'consumer') s.sessions.push({ ...c, id: 'consumer-2', name: 'Consumer 2', group: kind === 'same' ? c.group : 'another-group' }) } void apply(s) }}><option value="">템플릿 선택</option><option value="single">단일 Producer / Consumer</option><option value="same">같은 그룹 Consumer 2개</option><option value="different">다른 그룹 Consumer 2개</option></select></label>
    <p className="muted">설정만 저장합니다. 토큰·메시지·프로세스는 제외합니다. 주소마다 저장 공간이 다르므로 배포 주소로 이동할 때 JSON을 사용하세요.</p>
    {notice && <p role="status">{notice}</p>}
  </div>
}
