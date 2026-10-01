import { execFile } from 'node:child_process'
export type Command = (file: string, args: string[], timeout?: number) => Promise<string>
export const command: Command = (file, args, timeout = 10000) => new Promise((resolve, reject) => {
  execFile(file, args, { timeout, maxBuffer: 1024 * 1024 }, (error, stdout, stderr) => {
    if (error) reject(new Error((stderr || error.message).slice(-1500)))
    else resolve(stdout)
  })
})
export class DesktopBroker {
  private target: string
  private run: Command
  constructor(container: string, run: Command = command) {
    if (!/^[a-zA-Z0-9][a-zA-Z0-9_.-]*$/.test(container)) throw new Error('Kafka 컨테이너 이름이 올바르지 않습니다.')
    this.target = container; this.run = run
  }
  get container() { return this.target }
  async status() {
    try { await this.run('docker', ['info', '--format', '{{.ServerVersion}}']) }
    catch (e) { return { container: this.container, docker: false, exists: false, running: false, message: 'Docker 연결 필요: ' + String(e) } }
    try {
      const out = await this.run('docker', ['inspect', '--type', 'container', '--format', '{{json .State}}', this.container])
      const state = JSON.parse(out)
      return { container: this.container, docker: true, exists: true, running: state.Running === true, message: '컨테이너 상태: ' + String(state.Status) }
    } catch (e) { return { container: this.container, docker: true, exists: false, running: false, message: '컨테이너 조회 실패: ' + String(e) } }
  }
  async start() { await this.run('docker', ['start', this.container], 30000); return this.status() }
  async stop() { await this.run('docker', ['stop', '--time', '15', this.container], 30000); return this.status() }
  async startDocker() { await this.run('colima', ['start'], 180000); return this.status() }
}
