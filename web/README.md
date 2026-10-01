# Kafka Terminal Studio — Web

Vite + React + TypeScript 기반 실습 화면. 실행 방법과 현재 범위는 [상위 README](../README.md), 다음 작업은 [TODO](../TODO.md)를 확인한다.

- `npm run dev`: 개발 서버
- `npm run build`: TypeScript 검사 및 배포 빌드
- `npm run lint`: 린트
- `npm test`: 실행 계층 검증

`src/execution/`은 실행 방식, `src/features/`는 UI 상태, `src/storage/`는 저장 계약을 담당한다. 로컬 어댑터는 인증된 HTTP 요청과 출력 polling을 구현했다. 저장소는 버전 검증·비밀 필드 제외·localStorage 저장·JSON 이전을 구현했다. 실제 실행 설정은 `../local-agent/README.md`를 참고한다.
