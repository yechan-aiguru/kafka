import { createServer, type IncomingMessage } from 'node:http'
import { randomBytes, timingSafeEqual, createHash } from 'node:crypto'
import { readFile } from 'node:fs/promises'
import { resolve, sep, extname } from 'node:path'
import type { Environments } from './environments.ts'
import type { DesktopBroker } from './desktopBroker.ts'

interface Options {
  origin: string; secret: string; agentUrl: string; agentToken: string; webRoot: string
  environments?: Environments; targetChanged?: () => Promise<void>
  broker: DesktopBroker; disconnectAll: () => Promise<void>; exit: () => void
}
function equal(a: string, b: string) { const aa = Buffer.from(a), bb = Buffer.from(b); return aa.length === bb.length && timingSafeEqual(aa, bb) }
export function createDesktopServer(options: Options) {
  const cookie = randomBytes(32).toString('hex'), tickets = new Map<string, number>()
  const cookieName = 'kts_local_' + new URL(options.origin).port
  let changing = false
  const apiPaths = new Set(['/connect', '/disconnect', '/events', '/cluster/info', '/topics/list', '/topics/describe', '/topics/create', '/topics/increase', '/sessions/start', '/sessions/stop', '/sessions/input', '/groups/describe', '/groups/reset'])
  const hasCookie = (req: IncomingMessage) => (req.headers.cookie ?? '').split(';').some(v => equal(v.trim(), cookieName + '=' + cookie))
  return createServer(async (req, res) => {
    res.setHeader('Cache-Control', 'no-store'); res.setHeader('Referrer-Policy', 'no-referrer')
    res.setHeader('X-Content-Type-Options', 'nosniff'); res.setHeader('X-Frame-Options', 'DENY')
    const json = (status: number, value: unknown) => { if (!res.destroyed) { res.writeHead(status, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(value)) } }
    if (req.headers.host !== new URL(options.origin).host) { json(403, { error: '허용되지 않은 Host입니다.' }); return }
    const url = new URL(req.url ?? '/', options.origin)
    const sameOrigin = req.headers.origin === options.origin || (!req.headers.origin && req.headers['sec-fetch-site'] === 'same-origin' && req.method === 'GET')
    try {
      if (url.pathname === '/desktop/identity' && req.method === 'GET') { json(200, { identity: createHash('sha256').update(options.secret).digest('hex') }); return }
      if (url.pathname === '/desktop/ticket' && req.method === 'POST') {
        if (req.headers.origin !== options.origin || !equal(req.headers.authorization ?? '', 'Bearer ' + options.secret)) { json(403, { error: '실행기 인증 실패' }); return }
        for (const [key, expiry] of tickets) if (expiry < Date.now()) tickets.delete(key)
        if (tickets.size > 20) { json(429, { error: '잠시 후 다시 실행하세요.' }); return }
        const ticket = randomBytes(32).toString('hex'); tickets.set(ticket, Date.now() + 30000)
        json(200, { ticket }); return
      }
      if (url.pathname === '/open' && req.method === 'GET') {
        const ticket = url.searchParams.get('ticket') ?? '', expires = tickets.get(ticket)
        tickets.delete(ticket)
        if (!expires || expires < Date.now()) { json(403, { error: '실행 링크가 만료되었습니다. Kafka Studio 아이콘을 다시 여세요.' }); return }
        res.setHeader('Set-Cookie', `${cookieName}=${cookie}; HttpOnly; SameSite=Strict; Path=/`)
        res.writeHead(303, { Location: '/' }); res.end(); return
      }
      if (url.pathname.startsWith('/desktop/') || apiPaths.has(url.pathname)) {
        if (!sameOrigin || !hasCookie(req)) { json(403, { error: 'Kafka Studio 실행 아이콘으로 이 화면을 다시 여세요.' }); return }
        if (url.pathname.startsWith('/desktop/')) {
          if (req.method !== 'POST') { json(405, { error: 'POST 요청이 필요합니다.' }); return }
          if (url.pathname === '/desktop/config') { json(200, { localStudio: true, container: options.broker.container }); return }
          if (changing) { json(409, { error: '이전 상태 변경이 진행 중입니다.' }); return }
          if (url.pathname === '/desktop/status') { json(200, await options.broker.status()); return }
          if (url.pathname === '/desktop/environments/list' && options.environments) { json(200, options.environments.list()); return }
          changing = true
          try {
            if (url.pathname.startsWith('/desktop/environments/') && options.environments) {
              let content = ''
              for await (const chunk of req) { content += chunk.toString(); if (Buffer.byteLength(content) > 8192) throw new Error('요청 크기 초과') }
              const data = JSON.parse(content || '{}')
              if (url.pathname.endsWith('/create')) json(200, await options.environments.create(data))
              else if (url.pathname.endsWith('/select')) {
                await options.disconnectAll()
                const result = await options.environments.select(data.id)
                await options.targetChanged?.()
                json(200, result)
              } else json(404, { error: '지원하지 않는 환경 동작입니다.' })
            }
            else if (url.pathname === '/desktop/kafka/start') json(200, await options.broker.start())
            else if (url.pathname === '/desktop/kafka/stop') { await options.disconnectAll(); json(200, await options.broker.stop()) }
            else if (url.pathname === '/desktop/docker/start') json(200, await options.broker.startDocker())
            else if (url.pathname === '/desktop/quit') { await options.disconnectAll(); json(200, { stopped: true }); setTimeout(options.exit, 100) }
            else json(404, { error: '지원하지 않는 동작입니다.' })
          } finally { changing = false }
          return
        }
        if (changing) { json(409, { error: 'Kafka 상태를 변경하고 있습니다. 완료 후 다시 연결하세요.' }); return }
        if (!['GET', 'POST'].includes(req.method ?? '')) { json(405, { error: '지원하지 않는 요청입니다.' }); return }
        const chunks: Buffer[] = []; let size = 0
        for await (const chunk of req) { size += chunk.length; if (size > 32768) { json(413, { error: '요청 크기 초과' }); return }; chunks.push(chunk) }
        const upstream = await fetch(options.agentUrl + url.pathname + url.search, {
          method: req.method,
          headers: { Origin: options.origin, Authorization: 'Bearer ' + options.agentToken, 'Content-Type': 'application/json', 'X-Connection-Id': String(req.headers['x-connection-id'] ?? '') },
          body: req.method === 'POST' ? Buffer.concat(chunks) : undefined, signal: AbortSignal.timeout(70000),
        })
        res.writeHead(upstream.status, { 'Content-Type': 'application/json' }); res.end(await upstream.text()); return
      }
      if (req.method !== 'GET') { json(405, { error: '지원하지 않는 요청입니다.' }); return }
      if (req.headers['sec-fetch-site'] === 'cross-site') { json(403, { error: '실행 아이콘으로 여세요.' }); return }
      const pathname = decodeURIComponent(url.pathname)
      const file = pathname === '/' ? resolve(options.webRoot, 'index.html') : resolve(options.webRoot, '.' + pathname)
      if (!file.startsWith(resolve(options.webRoot) + sep)) { json(403, { error: '허용되지 않은 경로입니다.' }); return }
      let data = await readFile(file)
      if (pathname === '/') data = Buffer.from(data.toString().replace('</head>', '<meta name="kts-local-studio" content="true"></head>'))
      const mime: Record<string, string> = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.svg': 'image/svg+xml', '.png': 'image/png', '.ico': 'image/x-icon' }
      res.writeHead(200, { 'Content-Type': mime[extname(file)] ?? 'application/octet-stream' }); res.end(data)
    } catch (e) { json(400, { error: e instanceof Error ? e.message : '실행 실패' }) }
  })
}
