import type { TopicDetails } from '../../web/src/domain/types.ts'
export function parseTopicDescriptions(output: string): TopicDetails[] {
  const topics = new Map<string, TopicDetails>()
  for (const line of output.split('\n')) {
    const summary = line.match(/Topic:\s*(\S+).*PartitionCount:\s*(\d+).*ReplicationFactor:\s*(\d+)/)
    if (summary) {
      topics.set(summary[1], { name: summary[1], partitions: Number(summary[2]), replicationFactor: Number(summary[3]), partitionDetails: [] })
      continue
    }
    const partition = line.match(/Topic:\s*(\S+)\s+Partition:\s*(\d+)\s+Leader:\s*(-?\d+)\s+Replicas:\s*([\d,\s]*?)\s+Isr:\s*([\d,\s]*?)(?=\s+[A-Za-z]+:|$)/)
    if (!partition) continue
    const numbers = (value: string) => value.split(',').map(s => s.trim()).filter(Boolean).map(Number)
    topics.get(partition[1])?.partitionDetails.push({ id: Number(partition[2]), leader: Number(partition[3]), replicas: numbers(partition[4]), isr: numbers(partition[5]) })
  }
  return [...topics.values()].map(topic => ({ ...topic, partitionDetails: topic.partitionDetails.sort((a, b) => a.id - b.id) }))
}
export function parseBrokerIds(output: string): number[] {
  return [...new Set([...output.matchAll(/\(id:\s*(\d+)\s+rack:/g)].map(match => Number(match[1])))].sort((a, b) => a - b)
}
