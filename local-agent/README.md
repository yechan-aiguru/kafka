# 로컬 Kafka 연결 서버

Node.js 22.18 이상. 추가 npm 의존성 없이 내장 HTTP·프로세스 API를 사용한다.

더블클릭으로 쓰려면 저장소 루트의 **Kafka Studio.app**을 연다. 통합 모드에서는 웹과 agent가 함께 실행되고 토큰 입력이 필요 없다. [로컬 실행기 안내](../docs/local-studio.md). 아래는 기존 수동 개발 방식이다.

## 현재 컴퓨터에서 실행

기존 `ksl-kafka` 컨테이너와 로컬에 있는 `apache/kafka:4.0.0` 이미지를 사용한다. 브로커 설정이나 기존 컨테이너를 변경하지 않는다.

```sh
cd local-agent
npm run dev
```

다른 터미널에서 `web/`의 `npm run dev`를 실행한다. 웹 상단의 연결 서버는 `http://127.0.0.1:4317`이다. 아래 명령으로 읽은 토큰을 웹의 **연결 토큰** 필드에 입력하고 **실제 Kafka 연결**을 누른다.

```sh
cat .local/token
```

토큰은 서버를 시작할 때마다 새로 생성된다. 파일은 소유자 읽기/쓰기만 허용하고 Git에서 제외한다. 웹은 토큰을 메모리에서만 보관한다. URL·실습 JSON·localStorage에는 저장하지 않는다.

연결은 토픽 조회만 수행한다. `토픽 관리 열기`에서 이름과 파티션·복제 수를 지정해 토픽을 생성한다. Producer·Consumer 기본 대상은 이 토픽이며, Consumer Group은 `kts-demo-group`이다. 세션별 인자는 중지 상태에서 변경할 수 있다.

## 실행 방식

- Docker 모드: Kafka CLI마다 임시 `kts-cli-<random UUID>` 컨테이너를 만들고 기존 브로커의 네트워크를 공유한다. `--pull=never`이므로 지정 이미지가 로컬에 있어야 한다.
- 로컬 설치 모드: 지정한 Kafka `bin/`의 허용된 스크립트를 직접 실행한다. Java는 별도 설치가 필요하다.
- 셸 문자열을 실행하지 않고 허용된 실행 파일과 검증된 인자 배열을 사용한다.
- stdin/stdout/stderr 파이프를 연결한다. Kafka 입력·출력용이며 임의 bash, 전체 TTY/ANSI 터미널 에뮬레이션은 제공하지 않는다.
- 실행 상태는 CLI 프로세스 상태다. Consumer 그룹 할당 완료나 broker 연결 완료는 출력과 실제 수신으로 확인한다.
- 중지/Ctrl+C는 SIGINT를 전달한 뒤 남은 실행 프로세스나 agent 소유 CLI 컨테이너를 정리한다.

## 설정

| 환경 변수 | 기본값 | 설명 |
|---|---|---|
| `PORT` | `4317` | loopback 포트 |
| `KAFKA_MODE` | `docker` | `docker` 또는 `local` |
| `KAFKA_CONTAINER` | `ksl-kafka` | 네트워크를 공유할 기존 브로커 |
| `KAFKA_CLI_IMAGE` | `apache/kafka:4.0.0` | 로컬에 준비된 공식 CLI 이미지 |
| `KAFKA_BOOTSTRAP_SERVER` | `localhost:9092` | **CLI 실행 환경에서** 접근할 주소 |
| `KAFKA_BIN` | 없음 | 로컬 설치 모드의 Kafka bin 절대 경로 |
| `ALLOWED_ORIGINS` | `http://127.0.0.1:5173,http://localhost:5173` | 정확히 일치하는 웹 Origin 목록 |

```sh
KAFKA_MODE=local KAFKA_BIN=/absolute/path/kafka/bin npm run dev
```

서버는 127.0.0.1에만 바인딩한다. 요청의 Origin, Host, Bearer 토큰을 모두 확인한다. 웹 세션의 bootstrap server는 서버 설정과 일치해야 한다. 실습용 인증 범위는 연결된 로컬 Kafka이며 다중 사용자 서비스용 권한 모델은 아니다.

## 연결과 종료 정책

- 브라우저마다 별도의 connection ID와 CLI 세션을 갖는다. 다른 브라우저 세션을 중지하지 않는다.
- 출력은 약 400ms 간격으로 수신한다. WebSocket 대신 HTTP polling을 사용한다.
- 연결 해제/새로고침/모드 전환 시 해당 연결의 세션을 정리한다. 재연결은 새 세션이며 기존 실행을 복원하지 않는다.
- 비정상 탭 종료, 절전, 네트워크 중단으로 20초 동안 요청이 없으면 정리를 시작한다. 브라우저가 백그라운드 작업을 오래 멈춘 경우에도 재연결이 필요하다.
- agent의 Ctrl+C/SIGTERM 종료는 모든 agent 세션을 정리한다. OS 강제 종료/SIGKILL은 정리 코드를 실행할 수 없으므로 `docker ps --filter name=kts-cli-`로 잔여 컨테이너를 확인해야 한다.
- 기존 Kafka 브로커는 종료하지 않는다.
- 동시 연결 최대 8개, 연결당 실행 세션 최대 8개. 관리 명령은 30초, 출력 1MB로 제한한다.
- 서버 출력 버퍼는 연결당 최근 500개 이벤트, 이벤트당 최대 8KB다. 브라우저는 세션별 최근 200개를 보관한다. 버퍼 손실은 안내 로그로 표시한다.

## 검증

```sh
npm test
# 서버가 켜져 있을 때: kts-practice를 준비하고 테스트 메시지를 남긴다.
node --experimental-strip-types scripts/integration.ts
```

현재 Kafka 4.0.0 Docker 환경에서 브로커 수 조회, 토픽 상세(Leader/Replicas/ISR), 파티션 증가·감소 차단, 토픽 조회·생성, key 입력, Consumer 한글 수신, 시작/중지, 중지 후 입력 차단 및 임시 컨테이너 정리를 검증했다. 로컬 설치 모드는 자식 프로세스 입출력·종료 테스트를 통과했으며 실제 Java/Kafka 설치 환경에서는 별도 검증이 필요하다.

Vercel HTTPS → 로컬 HTTP 연결은 아직 배포 검증하지 않았다. 이 단계는 localhost 개발 환경을 대상으로 한다.

## 토픽 API 검증

```sh
node --experimental-strip-types scripts/topics-integration.ts
```

실행 시 전용 `kts-partitions.<timestamp>` 토픽을 생성하고 1→3 파티션 증가를 검증한다. 이 토픽은 결과 확인을 위해 남긴다. 복제 수 초과·파티션 감소·중복 생성도 검증한다.

실제 생성 요청은 `kafka-broker-api-versions.sh`로 메타데이터의 브로커 ID를 조회한 뒤 복제 수를 검사하며, 최종 생성 가능 여부는 Kafka 응답으로 확인한다. CLI의 상세 출력 형식은 Kafka 4.0.0에서 검증했다. 파싱할 수 없는 상세 출력은 원본 로그 확인을 안내하며 가짜 결과로 대체하지 않는다.

## 분배·그룹 API 검증

```sh
node --experimental-strip-types scripts/learning-integration.ts
```

전용 `kts-learning-<timestamp>` 토픽과 그룹을 만들고, RoundRobin Producer·classic RoundRobin Consumer 송수신, Consumer 1→2 리밸런싱, 실행 중 초기화 거부, 정지 후 earliest 초기화, Lag·replay를 검증한다. 결과 확인을 위해 토픽과 메시지는 남긴다. 테스트 종료 시 해당 CLI 프로세스는 정리한다.

그룹 조회/초기화에는 허용된 `kafka-consumer-groups.sh`만 사용한다. 초기화는 그룹의 broker 상태가 Empty/Dead이고 멤버가 0인지 먼저 확인하며 실제 reset 결과 행도 검증한다. 화면 밖의 Consumer가 실행 중이면 초기화가 거부된다. 웹의 기본 assignor는 Range, 선택 옵션은 RoundRobin이고 protocol은 classic이다. 다른 Kafka 버전은 별도 호환 검증이 필요하다.

[실습 가이드](../docs/learning.md) · [배포 및 Origin 설정](../docs/deployment.md)
