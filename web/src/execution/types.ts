import type { Session, SessionStatus, Topic, TopicDetails, ClusterInfo, GroupDetails } from '../domain/types.ts'
export type ExecutionEvent =
  | { type: 'connection'; connected: boolean; message: string }
  | { type: 'output'; sessionId: string; stream: 'stdin' | 'stdout' | 'stderr' | 'system'; text: string }
  | { type: 'status'; sessionId: string; status: SessionStatus }
export interface ExecutionClient {
  readonly mode: 'mock' | 'local'
  describeGroup(group: string): Promise<GroupDetails>
  resetOffsets(group: string, topic: string): Promise<void>
  describeTopic(name: string): Promise<TopicDetails>
  clusterInfo(): Promise<ClusterInfo>
  listTopics(): Promise<Topic[]>
  createTopic(topic: Topic): Promise<void>
  increasePartitions(name: string, count: number): Promise<void>
  startSession(session: Session): Promise<void>
  stopSession(id: string): Promise<void>
  sendInput(id: string, text: string): Promise<void>
  subscribe(listener: (event: ExecutionEvent) => void): () => void
  dispose(): void
}
