import type { Scenario } from '../domain/types.ts'
// 동기 localStorage 구현과 검증 함수는 scenarios.ts에 있다. 비동기 저장소 확장용 계약.
export interface ScenarioRepository {
  list(): Promise<Scenario[]>
  save(scenario: Scenario): Promise<void>
  remove(id: string): Promise<void>
}
