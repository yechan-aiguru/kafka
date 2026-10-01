import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { join } from 'node:path'

export interface RunnerConfig {
  mode: 'docker' | 'local'
  container: string
  image: string
  kafkaBin: string
}
export interface RunningCommand { child: ChildProcessWithoutNullStreams; stop(): Promise<void> }
const scripts = new Set(['kafka-consumer-groups.sh', 'kafka-broker-api-versions.sh', 'kafka-topics.sh', 'kafka-console-producer.sh', 'kafka-console-consumer.sh'])
export function launch(config: RunnerConfig, script: string, args: string[]): RunningCommand {
  if (!scripts.has(script)) throw new Error('허용되지 않은 명령입니다.')
  const name = `kts-cli-${randomUUID()}`
  const docker = config.mode === 'docker'
  const child = docker
    ? spawn('docker', ['run', '--rm', '--pull=never', '-i', '--name', name, '--network', `container:${config.container}`, '--entrypoint', `/opt/kafka/bin/${script}`, config.image, ...args], { stdio: 'pipe' })
    : spawn(join(config.kafkaBin, script), args, { stdio: 'pipe', detached: true })
  let stopping: Promise<void> | undefined
  // stdin errors are surfaced by process exit; never let EPIPE crash the agent.
  child.stdin.on('error', () => {})
  async function cleanup() {
    if (docker) {
      // Only this invocation's random, agent-owned CLI container is removed.
      const remove = () => new Promise<void>(resolve => {
        const process = spawn('docker', ['rm', '-f', name], { stdio: 'ignore' })
        const timer = setTimeout(() => process.kill('SIGKILL'), 5000)
        process.once('error', () => { clearTimeout(timer); resolve() })
        process.once('close', () => { clearTimeout(timer); resolve() })
      })
      await new Promise<void>(resolve => {
        const signal = spawn('docker', ['kill', '--signal=SIGINT', name], { stdio: 'ignore' })
        const timer = setTimeout(() => signal.kill('SIGKILL'), 3000)
        signal.once('error', () => { clearTimeout(timer); resolve() })
        signal.once('close', () => { clearTimeout(timer); resolve() })
      })
      await new Promise(resolve => setTimeout(resolve, 700))
      await remove()
      if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL')
      // A docker run interrupted during container creation can race the first rm.
      await new Promise(resolve => setTimeout(resolve, 200))
      await remove()
    } else if (child.pid) {
      try { process.kill(-child.pid, 'SIGINT') } catch { /* already exited */ }
      await new Promise(resolve => setTimeout(resolve, 300))
      try { process.kill(-child.pid, 'SIGKILL') } catch { /* already exited */ }
    }
  }
  return { child, stop: () => stopping ??= cleanup() }
}
