import type { Session } from '../domain/types.ts'

// 표시용 셸 quoting. 실제 프로세스 실행에서는 인자 배열을 직접 전달한다.
export const shellQuote = (value: string) => "'" + value.replace(/'/g, "'\\''") + "'"
export function sessionCommand(session: Session) {
  const args = ['--bootstrap-server', session.bootstrapServer, '--topic', session.topic]
  if (session.role === 'producer') {
    args.push('--producer-property', `acks=${session.acks}`)
    if (session.partitioner === 'round-robin') args.push('--producer-property', 'partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner')
    if (session.parseKey) args.push('--property', 'parse.key=true', '--property', 'key.separator=:')
  } else {
    args.push('--group', session.group, '--consumer-property', 'group.protocol=classic', '--consumer-property', 'client.id=' + session.id, '--consumer-property', 'partition.assignment.strategy=org.apache.kafka.clients.consumer.' + (session.assignor === 'round-robin' ? 'RoundRobinAssignor' : 'RangeAssignor'), '--property', 'print.partition=true', '--property', 'print.offset=true', '--property', 'print.key=true')
    if (session.fromBeginning) args.push('--from-beginning')
  }
  return {
    executable: `bin/kafka-console-${session.role}.sh`,
    args,
    display: `bin/kafka-console-${session.role}.sh ` + args.map(value => value.startsWith('--') ? value : shellQuote(value)).join(' '),
  }
}
