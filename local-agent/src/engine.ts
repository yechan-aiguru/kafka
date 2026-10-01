import { parseGroup, inactiveGroup } from './groupParser.ts'
import { parseTopicDescriptions, parseBrokerIds } from './topicParser.ts'
import { StringDecoder } from 'node:string_decoder'
import type { Session, Topic } from '../../web/src/domain/types.ts'
import type { ExecutionEvent } from '../../web/src/execution/types.ts'
import { sessionCommand } from '../../web/src/execution/commands.ts'
import { launch, type RunnerConfig, type RunningCommand } from './runner.ts'

export class Engine {
  config: RunnerConfig
  bootstrap: string
  emit: (event: ExecutionEvent) => void
  private sessions = new Map<string, { command: RunningCommand; session: Session; stopping: boolean }>()
  private commands = new Set<RunningCommand>()
  private disposed = false
  constructor(config: RunnerConfig, bootstrap: string, emit: (event: ExecutionEvent) => void) {
    this.config = config; this.bootstrap = bootstrap; this.emit = emit
  }
  private output(id: string, stream: 'stdout' | 'stderr' | 'system' | 'stdin', text: string) {
    this.emit({ type: 'output', sessionId: id, stream, text })
  }
  private async admin(args: string[], script = 'kafka-topics.sh') {
    if (this.disposed) throw new Error('연결이 종료되었습니다.')
    this.output('admin', 'system', '$ bin/' + script + ' ' + args.join(' '))
    const command = launch(this.config, script, args)
    this.commands.add(command)
    const result = await new Promise<string>((resolve, reject) => {
      let out = '', err = '', size = 0, failed = false
      const timer = setTimeout(() => { failed = true; void command.stop(); reject(new Error('Kafka 명령 실행 시간이 30초를 초과했습니다.')) }, 30000)
      for (const [stream, name] of [[command.child.stdout, 'stdout'], [command.child.stderr, 'stderr']] as const) {
        stream.setEncoding('utf8')
        stream.on('data', (chunk: string) => {
          size += Buffer.byteLength(chunk)
          if (size > 1024 * 1024) { failed = true; void command.stop(); clearTimeout(timer); reject(new Error('명령 출력이 1MB를 초과했습니다.')); return }
          this.output('admin', name, chunk)
          if (name === 'stdout') out += chunk; else err += chunk
        })
      }
      command.child.once('error', error => { clearTimeout(timer); reject(error) })
      command.child.once('close', (code, signal) => {
        clearTimeout(timer)
        this.output('admin', 'system', `[종료] code=${code} signal=${signal ?? '-'} `)
        if (!failed) code === 0 ? resolve(out) : reject(new Error((err || `Kafka 종료 코드: ${code}`).slice(-2000)))
      })
    }).finally(() => this.commands.delete(command))
    return result
  }
  async describeGroup(group: string) {
    return parseGroup(group, await this.admin(['--bootstrap-server', this.bootstrap, '--describe', '--group', group], 'kafka-consumer-groups.sh'))
  }
  async resetOffsets(group: string, topic: string) {
    if (topic.startsWith('__')) throw new Error('내부 토픽의 offset은 변경할 수 없습니다.')
    const state = await this.admin(['--bootstrap-server', this.bootstrap, '--describe', '--group', group, '--state'], 'kafka-consumer-groups.sh')
    if (!inactiveGroup(group, state)) throw new Error('모든 Consumer를 중지하고 그룹 상태가 Empty 또는 Dead가 된 후 다시 시도하세요.')
    const out = await this.admin(['--bootstrap-server', this.bootstrap, '--group', group, '--topic', topic, '--reset-offsets', '--to-earliest', '--execute'], 'kafka-consumer-groups.sh')
    // Some CLI validation failures return exit code 0, so require the result row.
    if (!out.split('\n').some(line => { const c = line.trim().split(/\s+/); return c[0] === group && c[1] === topic && /^\d+$/.test(c[2]) && /^\d+$/.test(c[3]) })) throw new Error('Offset 초기화를 확인하지 못했습니다. 관리 터미널을 확인하세요.')
  }
  async listTopics(): Promise<Topic[]> {
    const out = await this.admin(['--bootstrap-server', this.bootstrap, '--describe'])
    return parseTopicDescriptions(out).map(({ name, partitions, replicationFactor }) => ({ name, partitions, replicationFactor }))
  }
  async describeTopic(name: string) {
    // Kafka CLI treats --topic as a regex; escape metacharacters for an exact topic.
    const exact = name.replaceAll('.', '\\.')
    const out = await this.admin(['--bootstrap-server', this.bootstrap, '--describe', '--topic', '^' + exact + '$'])
    const topic = parseTopicDescriptions(out).find(topic => topic.name === name)
    if (!topic) throw new Error('토픽을 찾을 수 없습니다. 목록을 새로고침하세요.')
    if (topic.partitionDetails.length !== topic.partitions) throw new Error('파티션 상세 결과를 해석하지 못했습니다. 관리 터미널의 원본 출력을 확인하세요.')
    return topic
  }
  async clusterInfo() {
    const out = await this.admin(['--bootstrap-server', this.bootstrap], 'kafka-broker-api-versions.sh')
    const brokerIds = parseBrokerIds(out)
    if (!brokerIds.length) throw new Error('브로커 수를 확인하지 못했습니다. 관리 터미널을 확인하세요.')
    return { brokerIds, bootstrapServer: this.bootstrap }
  }

  async createTopic(topic: Topic) {
    const { brokerIds } = await this.clusterInfo()
    if (topic.replicationFactor > brokerIds.length) throw new Error(`Replication factor는 조회된 브로커 수(${brokerIds.length})를 초과할 수 없습니다.`)
    await this.admin(['--bootstrap-server', this.bootstrap, '--create', '--topic', topic.name, '--partitions', String(topic.partitions), '--replication-factor', String(topic.replicationFactor)])
  }
  async increasePartitions(name: string, count: number) {
    if (name.startsWith('__')) throw new Error('내부 토픽은 조회만 지원합니다.')
    const topic = await this.describeTopic(name)
    if (!topic || count <= topic.partitions) throw new Error('현재보다 큰 파티션 수가 필요합니다.')
    await this.admin(['--bootstrap-server', this.bootstrap, '--alter', '--topic', name, '--partitions', String(count)])
  }
  async startSession(session: Session) {
    if (this.disposed) throw new Error('연결이 종료되었습니다.')
    if (this.sessions.has(session.id)) throw new Error('이미 실행 중인 세션입니다.')
    if (this.sessions.size >= 8) throw new Error('동시 실행 세션은 8개까지 가능합니다.')
    const cmd = sessionCommand(session)
    const command = launch(this.config, `kafka-console-${session.role}.sh`, cmd.args)
    const record = { command, session: { ...session }, stopping: false }
    this.sessions.set(session.id, record)
    this.output(session.id, 'system', '$ ' + cmd.display)
    this.emit({ type: 'status', sessionId: session.id, status: 'running' })
    for (const [stream, name] of [[command.child.stdout, 'stdout'], [command.child.stderr, 'stderr']] as const) {
      const decoder = new StringDecoder('utf8')
      stream.on('data', (chunk: Buffer) => { this.output(session.id, name, decoder.write(chunk)) })
      stream.on('end', () => { const tail = decoder.end(); if (tail) this.output(session.id, name, tail) })
    }
    command.child.once('error', error => { this.output(session.id, 'stderr', error.message) })
    command.child.once('close', (code, signal) => {
      if (this.sessions.get(session.id) === record) this.sessions.delete(session.id)
      this.output(session.id, 'system', `[종료] code=${code} signal=${signal ?? '-'}${record.stopping ? ' (사용자 중지)' : ''}`)
      this.emit({ type: 'status', sessionId: session.id, status: 'stopped' })
    })
  }
  async stopSession(id: string) {
    const record = this.sessions.get(id)
    if (!record) return
    record.stopping = true
    await record.command.stop()
    if (this.sessions.get(id) === record) {
      this.sessions.delete(id)
      this.emit({ type: 'status', sessionId: id, status: 'stopped' })
    }
  }
  async sendInput(id: string, text: string) {
    const record = this.sessions.get(id)
    if (!record || record.session.role !== 'producer' || record.stopping) throw new Error('실행 중인 Producer가 필요합니다.')
    if (record.session.parseKey && !text.includes(':')) throw new Error('key:value 형식으로 입력하세요.')
    await new Promise<void>((resolve, reject) => record.command.child.stdin.write(text + '\n', error => error ? reject(error) : resolve()))
    this.output(id, 'stdin', text)
  }
  async dispose() {
    this.disposed = true
    await Promise.allSettled([...Array.from(this.sessions.keys(), id => this.stopSession(id)), ...Array.from(this.commands, command => command.stop())])
  }
}
