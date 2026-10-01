import { randomBytes } from 'node:crypto'
import { mkdir, readFile, writeFile, rename } from 'node:fs/promises'
import { dirname } from 'node:path'
import { DesktopBroker, command, type Command } from './desktopBroker.ts'

export interface EnvironmentSettings {
  name: string; brokers: number; partitions: number; replication: number; minIsr: number
  retentionHours: number; autoCreate: boolean; memoryMB: number
}
interface Profile extends EnvironmentSettings { id: string; clusterId: string }
const label = 'io.kafka-terminal-studio.environment'
export function parseEnvironment(value: unknown): EnvironmentSettings {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('환경 설정이 필요합니다.')
  const v = value as Record<string, unknown>
  const number = (key: string, max: number, min = 1) => {
    const n = v[key]; if (typeof n !== 'number' || !Number.isInteger(n) || n < min || n > max) throw new Error(`${key}: ${min}~${max} 정수가 필요합니다.`)
    return n
  }
  if (typeof v.name !== 'string' || !v.name.trim() || v.name.trim().length > 60) throw new Error('환경 이름은 1~60자입니다.')
  const brokers = number('brokers', 3)
  if (![1, 3].includes(brokers)) throw new Error('브로커는 1개 또는 3개를 선택하세요.')
  const replication = number('replication', brokers), minIsr = number('minIsr', replication)
  const memoryMB = number('memoryMB', 4096, 1024)
  if (![1024, 2048, 4096].includes(memoryMB) || typeof v.autoCreate !== 'boolean') throw new Error('메모리 또는 자동 생성 설정이 올바르지 않습니다.')
  return { name: v.name.trim(), brokers, partitions: number('partitions', 100), replication, minIsr, retentionHours: number('retentionHours', 720), memoryMB, autoCreate: v.autoCreate }
}
export class Environments extends DesktopBroker {
  private profiles: Profile[] = []
  private selected = 'existing'
  private path: string
  private execute: Command
  private existing: string
  constructor(path: string, existing: string, execute: Command = command) {
    super(existing, execute); this.path = path; this.existing = existing; this.execute = execute
  }
  async load() {
    let content: string
    try { content = await readFile(this.path, 'utf8') } catch (e) { if ((e as NodeJS.ErrnoException).code === 'ENOENT') return; throw e }
    const data = JSON.parse(content)
    if (data.version !== 1 || !Array.isArray(data.profiles) || data.profiles.length > 10) throw new Error('저장된 환경 파일이 올바르지 않습니다.')
    this.profiles = data.profiles.map((p: Profile) => {
      if (!/^kts-env-[a-f0-9]{16}$/.test(p.id) || !/^[A-Za-z0-9_-]{22}$/.test(p.clusterId)) throw new Error('환경 ID가 올바르지 않습니다.')
      return { ...parseEnvironment(p), id: p.id, clusterId: p.clusterId }
    })
    if (new Set(this.profiles.map(p => p.id)).size !== this.profiles.length) throw new Error('중복된 환경 ID입니다.')
    if (data.selected !== 'existing' && !this.profiles.some(p => p.id === data.selected)) throw new Error('선택된 환경을 찾을 수 없습니다.')
    this.selected = data.selected
  }
  private async save() {
    await mkdir(dirname(this.path), { recursive: true, mode: 0o700 })
    await writeFile(this.path + '.tmp', JSON.stringify({ version: 1, selected: this.selected, profiles: this.profiles }, null, 2), { mode: 0o600 })
    await rename(this.path + '.tmp', this.path)
  }
  private profile() { return this.profiles.find(p => p.id === this.selected) }
  private names(p: Profile) { return Array.from({ length: p.brokers }, (_, i) => `${p.id}-b${i + 1}`) }
  override get container() { const p = this.profile(); return p ? this.names(p)[0] : this.existing }
  list() { return { selected: this.selected, existing: this.existing, profiles: this.profiles.map(({ clusterId: _clusterId, ...p }) => p) } }
  async create(value: unknown) {
    const settings = parseEnvironment(value)
    if (this.profiles.length >= 10) throw new Error('환경은 최대 10개까지 저장할 수 있습니다.')
    if (this.profiles.some(p => p.name === settings.name)) throw new Error('이미 사용 중인 환경 이름입니다.')
    const p = { ...settings, id: 'kts-env-' + randomBytes(8).toString('hex'), clusterId: randomBytes(16).toString('base64url') }
    this.profiles.push(p)
    try { await this.save() } catch (e) { this.profiles.pop(); throw e }
    return this.list()
  }
  async select(id: unknown) {
    if (id !== 'existing' && !this.profiles.some(p => p.id === id)) throw new Error('등록된 환경만 선택할 수 있습니다.')
    const previous = this.selected; this.selected = id as string
    try { await this.save() } catch (e) { this.selected = previous; throw e }
    return this.list()
  }
  private async inspect(kind: 'container' | 'network', name: string, id: string) {
    let out: string
    try { out = await this.execute('docker', [kind, 'inspect', name]) }
    catch (e) { if (/No such (object|container|network)|network .* not found/i.test(String(e))) return null; throw e }
    const item = JSON.parse(out)[0]
    const labels = kind === 'container' ? item.Config?.Labels : item.Labels
    if (labels?.[label] !== id) throw new Error(`${name}: Studio 소유가 아닌 리소스는 변경할 수 없습니다.`)
    return item
  }
  override async status() {
    const p = this.profile()
    if (!p) return super.status()
    try { await this.execute('docker', ['info', '--format', '{{.ServerVersion}}']) }
    catch (e) { return { container: this.container, docker: false, exists: true, running: false, message: 'Docker 연결 필요: ' + String(e) } }
    const states = await Promise.all(this.names(p).map(async name => ({ name, state: (await this.inspect('container', name, p.id))?.State?.Status ?? '미생성' })))
    return { container: this.container, docker: true, exists: true, running: states.every(s => s.state === 'running'), message: states.map(s => `${s.name}: ${s.state}`).join(' / ') }
  }
  override async start() {
    const p = this.profile(); if (!p) return super.start()
    const names = this.names(p), network = p.id
    await this.execute('docker', ['image', 'inspect', 'apache/kafka:4.0.0'])
    const total = Number(await this.execute('docker', ['info', '--format', '{{.MemTotal}}']))
    if (total > 0 && total < p.brokers * p.memoryMB * 1024 * 1024) throw new Error(`Docker 메모리가 부족합니다. 이 환경의 한도 합계는 ${p.brokers * p.memoryMB / 1024} GiB입니다. Colima/Docker 메모리를 늘리세요.`)
    if (!await this.inspect('network', network, p.id)) await this.execute('docker', ['network', 'create', '--label', `${label}=${p.id}`, network])
    for (const [i, name] of names.entries()) {
      if (await this.inspect('container', name, p.id)) continue
      const env = {
        CLUSTER_ID: p.clusterId, KAFKA_NODE_ID: String(i + 1), KAFKA_PROCESS_ROLES: 'broker,controller',
        KAFKA_LISTENERS: 'PLAINTEXT://:9092,CONTROLLER://:9093', KAFKA_ADVERTISED_LISTENERS: `PLAINTEXT://${name}:9092`,
        KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT', KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER',
        KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT', KAFKA_CONTROLLER_QUORUM_VOTERS: names.map((n, j) => `${j + 1}@${n}:9093`).join(','),
        KAFKA_NUM_PARTITIONS: String(p.partitions), KAFKA_DEFAULT_REPLICATION_FACTOR: String(p.replication), KAFKA_MIN_INSYNC_REPLICAS: String(p.minIsr),
        KAFKA_LOG_RETENTION_HOURS: String(p.retentionHours), KAFKA_AUTO_CREATE_TOPICS_ENABLE: String(p.autoCreate),
        KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: String(p.brokers), KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: String(p.brokers),
        KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: String(Math.min(2, p.brokers)), KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: '0',
        KAFKA_LOG_DIRS: '/tmp/kraft-combined-logs', KAFKA_HEAP_OPTS: `-Xms${p.memoryMB / 2}m -Xmx${p.memoryMB / 2}m`,
      }
      await this.execute('docker', ['create', '--pull=never', '--name', name, '--hostname', name, '--network', network, '--label', `${label}=${p.id}`, '--memory', `${p.memoryMB}m`, ...Object.entries(env).flatMap(([key, value]) => ['-e', `${key}=${value}`]), 'apache/kafka:4.0.0'], 30000)
    }
    for (const name of names) await this.execute('docker', ['start', name], 30000)
    return this.status()
  }
  override async stop() {
    const p = this.profile(); if (!p) return super.stop()
    // Check every resource before stopping any of them.
    const states = await Promise.all(this.names(p).map(name => this.inspect('container', name, p.id)))
    const results = await Promise.allSettled(this.names(p).map((name, i) => states[i] ? this.execute('docker', ['stop', '--time', '15', name], 30000) : Promise.resolve()))
    const failed = results.filter(r => r.status === 'rejected')
    if (failed.length) throw new Error(failed.map(r => String(r.reason)).join('\n'))
    return this.status()
  }
}
