import { test } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, writeFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Engine } from '../src/engine.ts'
import type { ExecutionEvent } from '../../web/src/execution/types.ts'

test('실제 자식 프로세스 stdin/stdout, dispose와 프로세스 그룹 종료', async () => {
  const dir = await mkdtemp(join(tmpdir(), 'kts-engine-'))
  await writeFile(join(dir, 'kafka-console-producer.sh'), '#!/bin/sh\nprintf "pid:%s\\n" "$$"\nprintf "stderr-test\\n" >&2\nexec /bin/cat\n', { mode: 0o700 })
  const events: ExecutionEvent[] = []
  const engine = new Engine({ mode: 'local', kafkaBin: dir, image: '', container: '' }, 'localhost:9092', e => events.push(e))
  async function until(check: () => boolean) {
    const deadline = Date.now() + 3000
    while (!check() && Date.now() < deadline) await new Promise(resolve => setTimeout(resolve, 10))
    assert.ok(check())
  }
  try {
    await engine.startSession({ id: 'p', name: 'producer', role: 'producer', topic: 'test', bootstrapServer: 'localhost:9092', acks: 'all', parseKey: false })
    await engine.sendInput('p', '한글 입력')
    await until(() => events.some(e => e.type === 'output' && e.stream === 'stdout' && e.text.includes('한글 입력')))
    assert.ok(events.some(e => e.type === 'output' && e.stream === 'stderr' && e.text.includes('stderr-test')))
    const out = events.filter(e => e.type === 'output' && e.stream === 'stdout').map(e => e.type === 'output' ? e.text : '').join('')
    const pid = Number(out.match(/pid:(\d+)/)![1])
    await engine.dispose()
    await until(() => events.some(e => e.type === 'status' && e.status === 'stopped'))
    assert.throws(() => process.kill(pid, 0))
    await assert.rejects(engine.sendInput('p', 'after stop'))
    await assert.rejects(engine.startSession({ id: 'p', name: 'producer', role: 'producer', topic: 'test', bootstrapServer: 'localhost:9092', acks: 'all', parseKey: false }))
  } finally { await engine.dispose(); await rm(dir, { recursive: true, force: true }) }
})
