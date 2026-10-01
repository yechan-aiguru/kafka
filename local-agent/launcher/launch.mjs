import { spawn } from 'node:child_process'
import { mkdir, readFile, writeFile, open, rm, stat } from 'node:fs/promises'
import { fileURLToPath } from 'node:url'
import { join } from 'node:path'
const root = fileURLToPath(new URL('../../', import.meta.url))
const local = join(root, 'local-agent/.local'), statePath = join(local, 'desktop.json')
await mkdir(local, { recursive: true, mode: 0o700 })
const log = await open(join(local, 'launcher.log'), 'a', 0o600)
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms))
const run = (file, args, cwd = root) => new Promise((resolve, reject) => {
  const child = spawn(file, args, { cwd, stdio: ['ignore', log.fd, log.fd] })
  child.once('error', reject); child.once('exit', code => code === 0 ? resolve() : reject(new Error(`${file} 실행 실패 (${code}). launcher.log를 확인하세요.`)))
})
async function existing() {
  try {
    const state = JSON.parse(await readFile(statePath, 'utf8'))
    if (state.origin !== 'http://127.0.0.1:4318' || typeof state.secret !== 'string') return null
    const response = await fetch(state.origin + '/desktop/identity', { signal: AbortSignal.timeout(1000) })
    const identity = await response.json()
    return response.ok && identity.identity === state.identity ? state : null
  } catch { return null }
}
async function openStudio(state) {
  const response = await fetch(state.origin + '/desktop/ticket', { method: 'POST', headers: { Origin: state.origin, Authorization: 'Bearer ' + state.secret }, signal: AbortSignal.timeout(5000) })
  const data = await response.json()
  if (!response.ok) throw new Error(data.error ?? '실행 인증 실패')
  if (process.env.KTS_NO_OPEN !== '1') await run('/usr/bin/open', [state.origin + '/open?ticket=' + data.ticket])
  console.log('Kafka Studio: ' + state.origin)
}
const lock = join(local, 'launcher.lock')
let locked = false
try {
  const [major, minor] = process.versions.node.split('.').map(Number)
  if (major < 22 || (major === 22 && minor < 18)) throw new Error('Node.js 22.18 이상이 필요합니다.')
  let state = await existing()
  if (!state) {
    try { await mkdir(lock); locked = true; await writeFile(join(lock, 'pid'), String(process.pid)) }
    catch (error) {
      if (error.code !== 'EEXIST') throw error
      // Never kill a process by a stale PID. Only reclaim the launcher lock.
      const age = Date.now() - (await stat(lock)).mtimeMs
      let alive = true
      try { const pid = Number(await readFile(join(lock, 'pid'), 'utf8')); if (!Number.isInteger(pid) || pid <= 0) throw new Error(); process.kill(pid, 0) } catch { alive = false }
      if (!alive && age > 3000) { await rm(lock, { recursive: true }); await mkdir(lock); locked = true; await writeFile(join(lock, 'pid'), String(process.pid)) }
      else { for (let i = 0; i < 180 && !state; i++) { await sleep(1000); state = await existing() }; if (!state) throw new Error('다른 실행기가 준비 중입니다. 잠시 후 다시 실행하세요.') }
    }
    if (locked) {
      await log.write('\nKafka Studio 준비: ' + new Date().toISOString() + '\n')
      try { await stat(join(root, 'web/node_modules/.bin/vite')) } catch { await run('npm', ['ci'], join(root, 'web')) }
      await run('npm', ['run', 'build'], join(root, 'web'))
      const child = spawn(process.execPath, ['--experimental-strip-types', join(root, 'local-agent/src/desktop.ts')], { cwd: root, detached: true, stdio: ['ignore', log.fd, log.fd] })
      let spawnError
      child.once('error', e => { spawnError = e }); child.unref()
      for (let i = 0; i < 100 && !state; i++) { if (spawnError) throw spawnError; await sleep(100); state = await existing() }
      if (!state) throw new Error('로컬 서버를 시작하지 못했습니다. 4318 포트와 launcher.log를 확인하세요.')
    }
  }
  await openStudio(state)
} catch (e) {
  const message = 'Kafka Studio 실행 실패\n\n' + String(e) + '\n\n최초 실행에는 Node.js 22.18+, Docker(또는 Colima), 기존 ksl-kafka 컨테이너가 필요합니다.\n로그: ' + join(local, 'launcher.log')
  await writeFile(join(local, '실행 오류.txt'), message)
  console.error(message)
  if (process.env.KTS_NO_OPEN !== '1') await run('/usr/bin/open', [join(local, '실행 오류.txt')]).catch(() => {})
  process.exitCode = 1
} finally { if (locked) await rm(lock, { recursive: true, force: true }); await log.close() }
