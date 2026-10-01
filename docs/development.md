# 폴더 구조와 작업 방법

## 하나의 저장소 안에서 역할별로 분리

현재 프로젝트 폴더를 저장소 루트로 사용한다. 처음에는 복잡한 모노레포 도구 없이 `web`과 `local-agent`가 각각 `package.json`을 갖는 형태로 시작한다.

현재 파일 구조:

```text
kafka-terminal-studio/
├── README.md / TODO.md
├── docs/                        # development, learning, deployment
├── web/                         # Vercel 배포 대상
│   ├── vercel.json
│   ├── tests/
│   └── src/
│       ├── App.tsx / App.css
│       ├── domain/types.ts
│       ├── features/
│       │   ├── sessions/         # 설정·실행 상태
│       │   ├── terminal/         # 출력·입력·배치
│       │   ├── topics/           # 토픽 관리
│       │   ├── groups/           # 할당·Offset·Lag
│       │   └── scenarios/        # 저장·템플릿
│       ├── execution/           # commands, local, mock, partitioning
│       └── storage/             # JSON 검증·localStorage
└── local-agent/
    ├── src/                     # index, server, engine, runner, validation, parsers
    ├── tests/
    └── scripts/                 # 실제 Kafka 통합 검증
```


## 첫 시작

Vite 앱은 이미 `web/`에 생성되어 있다. Node.js 22.18.0 이상을 사용한다.

저장소 루트에서:

```sh
cd web
npm install
npm run dev
```

먼저 `web/src/`에서 화면과 모의 실행을 구현한다. 버튼 클릭이 직접 브라우저의 가짜 로그를 수정하게 만들기보다는 실행 인터페이스를 호출하도록 구성한다. 이후 같은 호출을 `execution/local.ts`로 연결하면 화면을 크게 바꾸지 않아도 된다.

모의 모드는 실제 Kafka와 동일한 구현이라고 표시하지 않는다. 특히 partitioner, offset commit, 리밸런싱은 단순화한 시뮬레이션과 실제 관찰 결과를 구분한다.

## 실제 Kafka 연결 구성

1. `local-agent/`에 Node.js + TypeScript 앱과 `npm run dev` 스크립트를 작성한다.
2. Kafka 설치 방식과 버전을 확정하고 CLI 실행 경로를 설정한다.
3. 읽기 기능인 토픽 목록 조회로 연결을 먼저 검증한다.
4. 토픽 생성 → Consumer 실행 → Producer 실행 → 메시지 전송 → 중지 순서로 연결한다.
5. UI 옵션을 검증된 인자 배열로 바꾸어 프로세스를 실행한다. 임의의 셸 명령을 받는 API는 만들지 않는다.

실제 실행 개발 중에는 개발용 터미널을 다음처럼 사용한다.

| 터미널 | 작업 위치 | 역할 |
|---|---|---|
| A | `web/` | Vite 개발 서버 실행 |
| B | `local-agent/` | 로컬 연결 서버 실행 |
| C | `infra/` 또는 기존 Kafka 환경 | Kafka 시작/종료·환경 점검 |

개발 서버를 켜기 위한 터미널은 필요하지만, 실습마다 열던 Producer·Consumer 터미널은 웹 화면에서 관리하게 된다. 동작이 안정된 후에만 통합 실행 스크립트를 추가한다.

## 기능별로 수정할 위치

| 하고 싶은 작업 | 수정할 곳 |
|---|---|
| Producer 인자 추가 | `features/sessions/` + `execution/` + 서버 인자 검증 |
| 터미널 모양·입력 개선 | `features/terminal/` |
| 토픽·파티션 기능 추가 | `features/topics/` + 서버 명령 처리 |
| 실습 저장 형식 변경 | `features/scenarios/` + `storage/` |
| 실제 CLI 실행/중지 수정 | `local-agent/src/` |
| 로컬 브로커 구성 변경 | `infra/` |

## 저장과 배포

- 브라우저 저장에는 설정과 세션 구성만 보관한다. 실제 프로세스나 Kafka의 메시지 데이터가 저장되는 것은 아니다.
- `localhost`와 Vercel 주소의 localStorage는 서로 다르다. 설정 이전은 JSON 내보내기/불러오기로 해결한다.
- Vercel 프로젝트의 Root Directory는 `web`, Build Command는 `npm run build`, Output Directory는 기본 Vite 기준 `dist`로 둔다.
- 먼저 모의 모드의 정적 배포를 확인하고 실제 로컬 연결을 따로 검증한다.
- 배포된 HTTPS 화면에서 사용자 PC로 연결할 때는 브라우저의 로컬 네트워크 접근 정책과 보안 연결 요건을 확인해야 한다. 단순히 주소를 `localhost`로 지정하는 것만으로 지원 완료로 보지 않는다.
- Next.js로 바꾸더라도 브라우저와 로컬 쉘 사이의 연결 프로그램 필요성은 동일하다.

## 작게 완성하는 단위

첫 목표는 **토픽 하나 생성 → Producer 하나와 Consumer 하나 실행 → 메시지 한 건 송수신 → 설정 저장**이다. 이 흐름이 실제 Kafka에서 동작하면 세션 수, 분배 전략, 리밸런싱 순서로 확장한다.

## 현재 검증 명령

`web/`에서 `npm run build`, `npm run lint`, `npm test`를 실행한다. 테스트는 Node 내장 테스트 러너를 사용한다. 모의 모델과 실제 Kafka의 차이는 [실습 가이드](./learning.md)를 확인한다. 저장 및 분배·그룹 테스트도 함께 실행된다.

## 구현된 로컬 연결

`local-agent/README.md`에 현재 환경의 실행 방법을 정리했다. 에이전트는 Node 내장 API로 HTTP polling과 파이프 입출력을 연결하며, 임의 셸 API를 제공하지 않는다. Docker CLI 컨테이너는 브로커와 별도로 생성·정리한다. 웹 빌드와 별도로 `web/node_modules/.bin/tsc -p local-agent/tsconfig.json`으로 서버 타입을 검사할 수 있다.
