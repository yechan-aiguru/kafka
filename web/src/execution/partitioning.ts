// Kafka's default keyed partitioning uses positive Murmur2 over UTF-8 bytes.
export function keyPartition(key: string, partitions: number): number {
  const bytes = new TextEncoder().encode(key), m = 0x5bd1e995
  let h = (0x9747b28c ^ bytes.length) | 0, i = 0
  while (i + 4 <= bytes.length) {
    let k = bytes[i] | bytes[i + 1] << 8 | bytes[i + 2] << 16 | bytes[i + 3] << 24
    k = Math.imul(k, m); k ^= k >>> 24; k = Math.imul(k, m)
    h = Math.imul(h, m) ^ k; i += 4
  }
  const remaining = bytes.length - i
  if (remaining >= 3) h ^= bytes[i + 2] << 16
  if (remaining >= 2) h ^= bytes[i + 1] << 8
  if (remaining >= 1) { h ^= bytes[i]; h = Math.imul(h, m) }
  h ^= h >>> 13; h = Math.imul(h, m); h ^= h >>> 15
  return (h & 0x7fffffff) % partitions
}
