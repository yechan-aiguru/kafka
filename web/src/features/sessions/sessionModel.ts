import type { Session } from '../../domain/types.ts'
export function changeRole(session: Session, role: Session['role']): Session {
  if (session.role === role) return session
  const base = { id: session.id, name: session.name, topic: session.topic, bootstrapServer: session.bootstrapServer }
  return role === 'producer'
    ? { ...base, role, acks: 'all', parseKey: false }
    : { ...base, role, group: 'demo-group', fromBeginning: true }
}
export function newSession(id: string, number: number): Session {
  return { id, name: `Session ${String(number).padStart(2, '0')}`, role: 'consumer', topic: 'orders', bootstrapServer: 'localhost:9092', group: 'demo-group', fromBeginning: true }
}
