import { createServer, type IncomingMessage, type ServerResponse } from 'node:http'
import { randomBytes, timingSafeEqual } from 'node:crypto'
import { Engine } from './engine.ts'
import type { RunnerConfig } from './runner.ts'
import type { ExecutionEvent } from '../../web/src/execution/types.ts'
import { object, parseSession, parseTopic, text, inputText, topicName, integer } from './validation.ts'

interface Config { token: string; origins: string[]; runner: RunnerConfig; bootstrap: string; leaseMs?: number }
interface Connection { engine: Engine; touched: number; sequence: number; events: { sequence: number; event: ExecutionEvent }[]; busy: boolean }
function authorized(header: string | undefined, token: string) {
  const expected = Buffer.from(`Bearer ${token}`), actual = Buffer.from(header ?? '')
  return expected.length === actual.length && timingSafeEqual(expected, actual)
}
async function body(req: IncomingMessage) {
  if (!req.headers['content-type']?.startsWith('application/json')) throw new Error('JSON 요청만 지원합니다.')
  let value = ''
  for await (const chunk of req) {
    value += chunk.toString()
    if (Buffer.byteLength(value) > 32768) throw new Error('요청 크기를 초과했습니다.')
  }
  return object(JSON.parse(value || '{}'))
}
export function createAgent(config: Config) {
  const connections = new Map<string, Connection>()
  const lease = config.leaseMs ?? 20000
  async function close(id: string) {
    const connection = connections.get(id)
    connections.delete(id)
    await connection?.engine.dispose()
  }
  const timer = setInterval(() => {
    for (const [id, connection] of connections) if (Date.now() - connection.touched > lease) void close(id)
  }, 1000)
  timer.unref()
  const server = createServer(async (req: IncomingMessage, res: ServerResponse) => {
    const origin = req.headers.origin
    const json = (status: number, value: unknown) => {
      if (res.destroyed || res.writableEnded) return
      res.writeHead(status, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' })
      res.end(JSON.stringify(value))
    }
    // Host check defends loopback binding against DNS rebinding.
    if (!/^(127\.0\.0\.1|localhost):\d+$/.test(req.headers.host ?? '') || !origin || !config.origins.includes(origin)) {
      json(403, { error: '허용되지 않은 웹 Origin 또는 Host입니다.' }); return
    }
    res.setHeader('Access-Control-Allow-Origin', origin)
    res.setHeader('Vary', 'Origin')
    res.setHeader('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
    res.setHeader('Access-Control-Allow-Headers', 'Authorization, Content-Type, X-Connection-Id')
    if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return }
    if (!authorized(req.headers.authorization, config.token)) { json(401, { error: '연결 토큰이 올바르지 않습니다.' }); return }
    const path = new URL(req.url ?? '/', 'http://localhost').pathname
    try {
      if (path === '/connect' && req.method === 'POST') {
        if (connections.size >= 8) { json(429, { error: '연결 수 제한에 도달했습니다.' }); return }
        const id = randomBytes(24).toString('hex')
        const connection: Connection = { engine: null as unknown as Engine, touched: Date.now(), sequence: 0, events: [], busy: false }
        connection.engine = new Engine(config.runner, config.bootstrap, event => {
          // Bounded stream, including output chunks. Sequence numbers reveal dropped entries.
          if (event.type === 'output' && event.text.length > 8192) event = { ...event, text: event.text.slice(0, 8192) + '\n[출력 일부 생략]' }
          connection.events.push({ sequence: ++connection.sequence, event })
          if (connection.events.length > 500) connection.events.shift()
        })
        connections.set(id, connection)
        json(200, { connectionId: id, bootstrapServer: config.bootstrap, mode: config.runner.mode }); return
      }
      const id = req.headers['x-connection-id']
      const connection = typeof id === 'string' ? connections.get(id) : undefined
      if (!connection || typeof id !== 'string') { json(410, { error: '로컬 연결이 종료되었습니다. 다시 연결하세요.' }); return }
      connection.touched = Date.now()
      if (path === '/events' && req.method === 'GET') {
        const after = Number(new URL(req.url!, 'http://localhost').searchParams.get('after') || 0)
        if (!Number.isSafeInteger(after) || after < 0) throw new Error('잘못된 이벤트 위치입니다.')
        json(200, { events: connection.events.filter(item => item.sequence > after), cursor: connection.sequence, truncated: connection.events.length > 0 && after < connection.events[0].sequence - 1 }); return
      }
      if (path === '/disconnect' && req.method === 'POST') { await close(id); json(200, {}); return }
      if (req.method !== 'POST') { json(405, { error: 'POST 요청이 필요합니다.' }); return }
      if (connection.busy) { json(409, { error: '이전 명령을 처리 중입니다.' }); return }
      connection.busy = true
      try {
        const data = await body(req)
        let result: unknown = {}
        switch (path) {
          case '/groups/describe': result = await connection.engine.describeGroup(text(data.group, 'Group')); break
          case '/groups/reset': await connection.engine.resetOffsets(text(data.group, 'Group'), topicName(data.topic)); break
          case '/cluster/info': result = await connection.engine.clusterInfo(); break
          case '/topics/describe': result = await connection.engine.describeTopic(topicName(data.name)); break
          case '/topics/list': result = await connection.engine.listTopics(); break
          case '/topics/create': await connection.engine.createTopic(parseTopic(data)); break
          case '/topics/increase': await connection.engine.increasePartitions(topicName(data.name), integer(data.count, 'Partitions', 100)); break
          case '/sessions/start': await connection.engine.startSession(parseSession(data, config.bootstrap)); break
          case '/sessions/stop': await connection.engine.stopSession(text(data.id, 'ID', 80)); break
          case '/sessions/input': await connection.engine.sendInput(text(data.id, 'ID', 80), inputText(data.text)); break
          default: json(404, { error: '지원하지 않는 명령입니다.' }); return
        }
        json(200, result)
      } finally { connection.busy = false }
    } catch (error) { json(400, { error: error instanceof Error ? error.message : '실행 실패' }) }
  })
  server.requestTimeout = 10000
  server.headersTimeout = 10000
  return {
    server,
    async setTarget(runner: Config['runner'], bootstrap: string) {
      await Promise.allSettled([...connections.keys()].map(close))
      config.runner = runner; config.bootstrap = bootstrap
    },
    async disconnectAll() { await Promise.allSettled([...connections.keys()].map(close)) },
    async shutdown() {
      clearInterval(timer)
      await Promise.allSettled([...connections.keys()].map(close))
      await new Promise<void>(resolve => server.close(() => resolve()))
    },
  }
}
