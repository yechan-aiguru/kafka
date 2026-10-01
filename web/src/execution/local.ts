import type { Session, Topic, TopicDetails, ClusterInfo, GroupDetails } from '../domain/types.ts'
import type { ExecutionClient, ExecutionEvent } from './types.ts'

export interface LocalOptions { url: string; token: string }
export function createLocalExecutionClient(options?: LocalOptions): ExecutionClient {
  if (!options?.token.trim()) throw new Error('로컬 연결 토큰이 필요합니다.')
  const url = new URL(options.url)
  if (url.protocol !== 'http:' || !['localhost', '127.0.0.1'].includes(url.hostname) || url.username || url.password) throw new Error('로컬 HTTP 주소만 사용할 수 있습니다.')
  const base = url.origin
  const token = options.token.trim()
  const listeners = new Set<(event: ExecutionEvent) => void>()
  let connectionId = '', cursor = 0, disposed = false, failed = false
  let timer: ReturnType<typeof setTimeout> | undefined
  const emit = (event: ExecutionEvent) => listeners.forEach(listener => listener(event))
  async function request<T>(path: string, data?: unknown, keepalive = false): Promise<T> {
    const requestOptions: RequestInit & { targetAddressSpace: string } = {
      method: data === undefined ? 'GET' : 'POST',
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json', ...(connectionId ? { 'X-Connection-Id': connectionId } : {}) },
      body: data === undefined ? undefined : JSON.stringify(data),
      signal: keepalive ? undefined : AbortSignal.timeout(70000), keepalive,
      targetAddressSpace: 'loopback',
    }
    const response = await fetch(base + path, requestOptions).catch((error: unknown) => { throw new Error('로컬 서버에 연결할 수 없습니다. 실행 여부, 허용 Origin과 브라우저의 로컬 네트워크 권한을 확인하세요. ' + (error instanceof Error ? error.message : '')) })
    const result = await response.json()
    if (!response.ok) throw new Error(result.error || '로컬 연결 요청 실패')
    return result as T
  }
  async function disconnect() {
    if (connectionId) await request('/disconnect', {}, true).catch(() => {})
  }
  function fail(error: unknown) {
    if (disposed || failed) return
    failed = true
    emit({ type: 'connection', connected: false, message: error instanceof Error ? error.message : '로컬 연결 실패' })
    void disconnect()
  }
  async function poll() {
    if (disposed || failed) return
    try {
      const result = await request<{ events: { sequence: number; event: ExecutionEvent }[]; cursor: number; truncated: boolean }>(`/events?after=${cursor}`)
      if (disposed) return
      if (result.truncated) emit({ type: 'output', sessionId: 'admin', stream: 'system', text: '[연결 안내] 수신 지연으로 일부 과거 로그가 생략되었습니다.' })
      for (const item of result.events) emit(item.event)
      cursor = result.cursor
      timer = setTimeout(() => void poll(), 400)
    } catch (error) { fail(error) }
  }
  const connected = request<{ connectionId: string; bootstrapServer: string }>('/connect', {}).then(async result => {
    connectionId = result.connectionId
    if (disposed) { await disconnect(); return }
    emit({ type: 'connection', connected: true, message: `로컬 연결 완료 · ${result.bootstrapServer}` })
    void poll()
  })
  // Attach immediately so a quick connection failure cannot become unhandled.
  void connected.catch(fail)
  async function call<T>(path: string, data: unknown): Promise<T> {
    await connected
    if (disposed || failed) throw new Error('로컬 연결이 종료되었습니다. 다시 연결하세요.')
    return request<T>(path, data)
  }
  function dispose() {
    if (disposed) return
    disposed = true
    clearTimeout(timer)
    if (typeof window !== 'undefined') window.removeEventListener('pagehide', dispose)
    void disconnect()
    listeners.clear()
  }
  if (typeof window !== 'undefined') window.addEventListener('pagehide', dispose)
  return {
    mode: 'local',
    describeGroup: group => call<GroupDetails>('/groups/describe', { group }),
    resetOffsets: (group, topic) => call<void>('/groups/reset', { group, topic }),
    describeTopic: name => call<TopicDetails>('/topics/describe', { name }),
    clusterInfo: () => call<ClusterInfo>('/cluster/info', {}),
    listTopics: () => call<Topic[]>('/topics/list', {}),
    createTopic: topic => call<void>('/topics/create', topic),
    increasePartitions: (name, count) => call<void>('/topics/increase', { name, count }),
    startSession: (session: Session) => call<void>('/sessions/start', session),
    stopSession: id => call<void>('/sessions/stop', { id }),
    sendInput: (id, text) => call<void>('/sessions/input', { id, text }),
    subscribe: listener => { listeners.add(listener); return () => { listeners.delete(listener) } },
    dispose,
  }
}
