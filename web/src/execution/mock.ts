import { keyPartition } from './partitioning.ts'
import { sessionCommand } from './commands.ts'
import type { Consumer, GroupDetails, Session, Topic, TopicDetails } from '../domain/types.ts'
import type { ExecutionClient, ExecutionEvent } from './types.ts'

// 단일 토픽 구독 학습 모델. 즉시 commit 및 5건 단위 sticky를 사용한다.
export class MockExecutionClient implements ExecutionClient {
  readonly mode = 'mock' as const
  private topics = new Map<string, Topic>()
  private sessions = new Map<string, Session>()
  private logs = new Map<string, string[][]>()
  private offsets = new Map<string, Map<string, number[]>>()
  private counters = new Map<string, number>()
  private listeners = new Set<(event: ExecutionEvent) => void>()

  private consumers(group: string, topic: string): Consumer[] {
    return [...this.sessions.values()].filter((s): s is Consumer => s.role === 'consumer' && s.group === group && s.topic === topic).sort((a, b) => a.id.localeCompare(b.id))
  }
  private owner(group: string, topic: string, partition: number) {
    const consumers = this.consumers(group, topic), count = this.topics.get(topic)!.partitions
    if (!consumers.length) return undefined
    if (consumers[0].assignor === 'round-robin') return consumers[partition % consumers.length]
    let start = 0
    for (let i = 0; i < consumers.length; i++) { const end = start + Math.floor(count / consumers.length) + (i < count % consumers.length ? 1 : 0); if (partition < end) return consumers[i]; start = end }
  }
  private drain() {
    for (const [group, topics] of this.offsets) for (const [topic, offsets] of topics) {
      this.logs.get(topic)?.forEach((records, partition) => {
        const consumer = this.owner(group, topic, partition)
        if (!consumer) return
        offsets[partition] ??= consumer.fromBeginning ? 0 : records.length
        while (offsets[partition] < records.length) {
          const offset = offsets[partition]++
          this.emit({ type: 'output', sessionId: consumer.id, stream: 'system', text: `[모의] Partition:${partition} Offset:${offset}` })
          this.emit({ type: 'output', sessionId: consumer.id, stream: 'stdout', text: records[offset] })
        }
      })
    }
  }
  async describeGroup(group: string): Promise<GroupDetails> {
    const topics = this.offsets.get(group)
    const partitions = [...(topics?.entries() ?? [])].flatMap(([topic, offsets]) => this.logs.get(topic)!.map((records, partition) => ({ topic, partition, currentOffset: offsets[partition] ?? null, logEndOffset: records.length, lag: offsets[partition] === undefined ? null : records.length - offsets[partition], consumerId: this.owner(group, topic, partition)?.id ?? '-' })))
    const raw = '[모의 · 즉시 commit]\n' + JSON.stringify(partitions, null, 2)
    this.admin(raw)
    return { group, partitions, raw }
  }
  async resetOffsets(group: string, topic: string) {
    if (topic.startsWith('__')) throw new Error('내부 토픽은 초기화할 수 없습니다.')
    if ([...this.sessions.values()].some(s => s.role === 'consumer' && s.group === group)) throw new Error('그룹의 모든 Consumer를 중지하세요.')
    const offsets = this.offsets.get(group)
    if (!offsets?.has(topic)) throw new Error('그룹 또는 토픽의 offset이 없습니다.')
    offsets.set(topic, this.logs.get(topic)!.map(() => 0)); this.admin('Offset earliest 초기화: ' + group + ' / ' + topic)
  }
  private emit(event: ExecutionEvent) { this.listeners.forEach(listener => listener(event)) }

  private admin(text: string) { this.emit({ type: 'output', sessionId: 'admin', stream: 'system', text: '[모의] ' + text + '\n' }) }
  async listTopics() {
    this.admin('$ bin/kafka-topics.sh --bootstrap-server localhost:9092 --list\n' + [...this.topics.keys()].join('\n'))
    return Array.from(this.topics.values(), topic => ({ ...topic }))
  }
  async clusterInfo() { this.admin('브로커 조회: 1개 (ID 1)'); return { brokerIds: [1], bootstrapServer: 'localhost:9092' } }
  async describeTopic(name: string): Promise<TopicDetails> {
    const topic = this.topics.get(name)
    if (!topic) throw new Error('토픽이 없습니다.')
    this.admin('$ bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic ' + name + '\nPartitionCount: ' + topic.partitions)
    return { ...topic, partitionDetails: Array.from({ length: topic.partitions }, (_, id) => ({ id, leader: 1, replicas: [1], isr: [1] })) }
  }

  async createTopic(topic: Topic) {
    if (!/^[a-zA-Z0-9._-]{1,249}$/.test(topic.name) || ['.', '..'].includes(topic.name)) throw new Error('올바른 토픽 이름을 입력하세요.')
    if (this.topics.has(topic.name)) throw new Error('이미 존재하는 토픽입니다.')
    if (!Number.isInteger(topic.partitions) || topic.partitions < 1 || topic.partitions > 100) throw new Error('파티션 수는 1 이상의 정수여야 합니다.')
    if (topic.replicationFactor !== 1) throw new Error('모의 환경은 브로커 1개를 사용합니다.')
    this.topics.set(topic.name, { ...topic }); this.logs.set(topic.name, Array.from({ length: topic.partitions }, () => []))
    this.admin(`$ bin/kafka-topics.sh --create --topic ${topic.name} --partitions ${topic.partitions} --replication-factor ${topic.replicationFactor}\nCreated topic ${topic.name}.`)
  }

  async increasePartitions(name: string, count: number) {
    if (name.startsWith('__')) throw new Error('내부 토픽은 조회만 지원합니다.')
    const topic = this.topics.get(name)
    if (!topic) throw new Error('토픽이 없습니다.')
    if (!Number.isInteger(count) || count <= topic.partitions || count > 100) throw new Error('파티션 수는 현재보다 큰 정수로만 변경할 수 있습니다.')
    const logs = this.logs.get(name)!; while (logs.length < count) logs.push([])
    this.topics.set(name, { ...topic, partitions: count }); this.drain()
    this.admin(`$ bin/kafka-topics.sh --alter --topic ${name} --partitions ${count}\n파티션 변경: ${topic.partitions} → ${count}`)
  }

  async startSession(session: Session) {
    if (this.sessions.has(session.id)) throw new Error('이미 실행 중인 세션입니다.')
    if (!session.bootstrapServer.trim()) throw new Error('Bootstrap server가 필요합니다.')
    if (!session.name.trim()) throw new Error('세션 이름이 필요합니다.')
    if (!this.topics.has(session.topic)) throw new Error('토픽을 먼저 생성하세요.')
    if (session.role === 'consumer' && !session.group.trim()) throw new Error('그룹 이름이 필요합니다.')
    if (this.sessions.size >= 8) throw new Error('세션은 8개까지 실행할 수 있습니다.')
    if (session.role === 'consumer' && [...this.sessions.values()].some(s => s.role === 'consumer' && s.group === session.group && (s.assignor ?? 'range') !== (session.assignor ?? 'range'))) throw new Error('같은 그룹은 동일한 할당 전략을 사용하세요.')
    this.sessions.set(session.id, { ...session })
    if (session.role === 'consumer') {
      const group = this.offsets.get(session.group) ?? new Map<string, number[]>(); this.offsets.set(session.group, group)
      if (!group.has(session.topic)) group.set(session.topic, this.logs.get(session.topic)!.map(records => session.fromBeginning ? 0 : records.length))
    }
    this.emit({ type: 'status', sessionId: session.id, status: 'running' })
    this.emit({ type: 'output', sessionId: session.id, stream: 'system', text: '$ ' + sessionCommand(session).display }); this.drain()
  }

  async stopSession(id: string) {
    if (!this.sessions.delete(id)) return
    this.emit({ type: 'status', sessionId: id, status: 'stopped' })
    this.emit({ type: 'output', sessionId: id, stream: 'system', text: '^C\n[모의 실행] 세션을 중지했습니다.' }); this.drain()
  }

  async sendInput(id: string, text: string) {
    const producer = this.sessions.get(id)
    if (!producer || producer.role !== 'producer') throw new Error('실행 중인 Producer가 필요합니다.')
    if (!text.trim() || new TextEncoder().encode(text).length > 16384 || ['\r', '\n', '\0'].some(c => text.includes(c))) throw new Error('메시지는 16KB 이하의 한 줄이어야 합니다.')
    if (producer.parseKey && !text.includes(':')) throw new Error('key:value 형식으로 입력하세요.')
    this.emit({ type: 'output', sessionId: id, stream: 'stdin', text })
    const logs = this.logs.get(producer.topic)!
    if (logs.reduce((sum, p) => sum + p.length, 0) >= 10000) throw new Error('모의 토픽은 10,000건까지 보관합니다. 페이지를 새로고침하세요.')
    const counter = this.counters.get(id) ?? 0; this.counters.set(id, counter + 1)
    const key = producer.parseKey ? text.slice(0, text.indexOf(':')) : null
    const partition = producer.partitioner === 'round-robin' ? counter % logs.length : key !== null ? keyPartition(key, logs.length) : Math.floor(counter / 5) % logs.length
    logs[partition].push(producer.parseKey ? text.slice(text.indexOf(':') + 1) : text)
    this.emit({ type: 'output', sessionId: id, stream: 'system', text: `[모의 전송] key=${key ?? 'null'} → P${partition} / offset ${logs[partition].length - 1}` })
    this.drain()
  }

  subscribe(listener: (event: ExecutionEvent) => void) {
    this.listeners.add(listener)
    return () => { this.listeners.delete(listener) }
  }

  dispose() { this.sessions.clear(); this.topics.clear(); this.logs.clear(); this.offsets.clear(); this.counters.clear(); this.listeners.clear() }
}
