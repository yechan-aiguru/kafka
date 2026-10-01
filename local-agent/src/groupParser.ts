import type { GroupDetails, GroupPartition } from '../../web/src/domain/types.ts'
export function parseGroup(group: string, raw: string): GroupDetails {
  const partitions: GroupPartition[] = []
  for (const line of raw.split('\n')) {
    const cols = line.trim().split(/\s+/)
    if (cols[0] !== group || cols.length < 9 || !/^\d+$/.test(cols[2])) continue
    const number = (s: string) => s === '-' ? null : /^\d+$/.test(s) && Number.isSafeInteger(Number(s)) ? Number(s) : null
    partitions.push({ topic: cols[1], partition: Number(cols[2]), currentOffset: number(cols[3]), logEndOffset: number(cols[4]), lag: number(cols[5]), consumerId: cols[6] === '-' ? '-' : cols[8] + ' · ' + cols[6] })
  }
  return { group, partitions, raw }
}
export function inactiveGroup(group: string, raw: string) {
  return raw.split('\n').some(line => { const c = line.trim().split(/\s+/); return c[0] === group && ['Empty', 'Dead'].includes(c[c.length - 2]) && c[c.length - 1] === '0' })
}
