# Vercel 배포

웹 화면만 정적으로 배포한다. Kafka와 연결 서버는 각 사용자의 PC에서 실행한다. 연결하지 않아도 모의 모드와 실습 저장을 사용할 수 있다.

## 현재 상태

로컬 구현·빌드·실제 Kafka 연결 검증 완료. Vercel CLI/프로젝트 연결 정보 및 기본 CLI 인증 파일이 없는 상태라 공개 배포는 실행하지 않았다. 배포 도메인에서 실제 연결의 브라우저 검증도 아직 남아 있다.

## Git 연동

1. 이 저장소를 본인의 Git 원격 저장소에 올린다.
2. Vercel에서 해당 저장소를 Import한다.
3. Root Directory: `web`, Framework: Vite, Node.js: 22.x.
4. Install: `npm ci`, Build: `npm run build`, Output: `dist`.
5. 환경 변수에 로컬 연결 토큰을 넣지 않는다. Deploy 후 모의 송수신·저장·새로고침을 확인한다.

`web/vercel.json`에 빌드 설정을 포함했다. CLI를 선호하면 프로젝트의 `web` 폴더에서 다음을 실행한다. CLI는 공식 npm 패키지를 사용한다.

```sh
npx vercel login
npx vercel
npx vercel --prod
```

CLI를 `web`에서 시작했다면 Vercel Root Directory는 `.`이다. Git 연동 시의 `web` 설정과 중복 적용하지 않는다.

## 배포 주소에서 내 Kafka 연결

아래 예시의 도메인을 실제 배포 Origin으로 바꾸고 `local-agent` 폴더에서 실행한다. Origin에는 경로나 끝의 `/`를 넣지 않는다.

```sh
ALLOWED_ORIGINS='http://127.0.0.1:5173,http://localhost:5173,https://YOUR-PROJECT.vercel.app' npm run dev
```

서버 주소 `http://127.0.0.1:4317`과 `.local/token`의 값을 웹에 입력한다. 브라우저가 해당 사이트의 로컬 네트워크 접근을 물으면 본인의 실습 사이트인지 확인하고 선택한다. 토큰은 메모리에만 두며 Vercel로 전송하지 않는다. 서버는 정확히 등록한 Origin과 토큰을 검사한다. Preview 배포의 주소가 바뀌면 허용 Origin도 갱신해야 한다.

HTTPS 페이지에서 HTTP loopback 접근은 브라우저/버전/권한에 따라 달라진다. 클라이언트는 `targetAddressSpace: 'loopback'`을 명시한다. 현재 연결 서버는 HTTP만 지원하며, 모든 브라우저 지원이나 배포 환경의 실제 연결 성공을 보장하지 않는다. 제한되는 브라우저에서는 웹도 로컬에서 실행한다. 서버의 외부 바인딩이나 보안 경고 우회로 해결하지 않는다.

배포 후 확인할 항목:

- HTTPS 주소에서 모의 송수신과 설정 저장·JSON 이전.
- 실제 주소를 Origin에 등록한 후 연결 및 실제 메시지 수신.
- 페이지 종료 후 CLI 세션 정리.
- Chromium과 사용하려는 다른 브라우저에서 로컬 접근 권한/차단 여부 기록.

## 로컬 프로덕션 미리보기

`web` 폴더에서:

```sh
npm ci
npm run build
npm run preview -- --host 127.0.0.1
```

기본 주소는 `http://127.0.0.1:4173`. 이 주소에서 실제 Kafka에 연결하려면 서버의 ALLOWED_ORIGINS에 같은 Origin을 추가한다.

참고: [Vite on Vercel](https://vercel.com/docs/frameworks/frontend/vite), [Vercel 빌드 설정](https://vercel.com/docs/builds/configure-a-build), [Chrome 로컬 네트워크 접근](https://developer.chrome.com/blog/local-network-access).
