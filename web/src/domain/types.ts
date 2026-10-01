export interface Topic { name: string; partitions: number; replicationFactor: number }
interface SessionBase { id: string; name: string; topic: string; bootstrapServer: string }
export interface Producer extends SessionBase { role: 'producer'; acks: 'all' | '1' | '0'; parseKey: boolean; partitioner?: 'default' | 'round-robin' }
export interface Consumer extends SessionBase { role: 'consumer'; group: string; fromBeginning: boolean; assignor?: 'range' | 'round-robin' }
export type Session = Producer | Consumer
export type SessionStatus = 'stopped' | 'running'
// 설정만 저장한다. 프로세스, 메시지, 인증 토큰은 포함하지 않는다.
export interface Scenario { schemaVersion: 1; id: string; name: string; topics: Topic[]; sessions: Session[] }
export interface PartitionInfo { id: number; leader: number; replicas: number[]; isr: number[] }
export interface TopicDetails extends Topic { partitionDetails: PartitionInfo[] }
export interface GroupPartition { topic: string; partition: number; currentOffset: number | null; logEndOffset: number | null; lag: number | null; consumerId: string }
export interface GroupDetails { group: string; partitions: GroupPartition[]; raw: string }
export interface ClusterInfo { brokerIds: number[]; bootstrapServer: string }
