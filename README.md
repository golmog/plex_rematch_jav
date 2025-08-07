# Plex JAV Rematch (plex_rematch_jav.py)

Plex에 등록된 JAV 라이브러리의 메타데이터를 효율적으로 수정하고 재매칭하기 위한 파이썬 스크립트입니다. 파일명을 기반으로 정확한 품번을 추출하여 Plex의 검색 기능을 통해 올바른 메타데이터를 찾고, 잘못 매칭되었거나 정보가 부족한 항목들을 자동으로 또는 인터랙티브하게 수정합니다.

## 주요 기능

-   **자동 재매칭**: 지정된 라이브러리에서 메타데이터가 불완전한 항목들을 찾아 파일명 기반으로 자동 재매칭을 시도합니다.
-   **인터랙티브 모드**:
    -   **품번 수정 (`--fix-labels`)**: 파일명과 Plex 제목의 품번이 일치하지 않거나, 미매칭된 항목들을 찾아 목록을 보여주고, 사용자가 직접 올바른 메타데이터를 선택하여 수정할 수 있습니다.
    -   **포스터 없는 항목 처리 (`--no-poster`)**: 포스터가 없거나 임시 썸네일로 지정된 항목들을 찾아 재매칭을 유도합니다.
    -   **검색 및 재매칭 (`--search`)**: 특정 키워드로 라이브러리를 검색하여 나온 결과를 대상으로 재매칭을 진행합니다.
-   **Plex Dance 자동화**: 메타데이터가 심하게 꼬인 항목에 대해 라이브러리에서 아이템을 완전히 제거했다가 다시 추가하는 "Plex Dance" 과정을 자동화하여 문제를 해결합니다.
-   **분할 스캔 (`--scan-full`)**: 대용량 라이브러리 스캔 시 Plex 서버에 과부하가 걸리는 것을 방지하기 위해, 라이브러리 경로를 지정된 깊이(depth)의 하위 폴더 단위로 나누어 순차적으로 스캔합니다.
-   **설정 파일 지원**: `YAML` 형식의 설정 파일을 통해 Plex 서버 정보, 작업 옵션 등을 쉽게 관리할 수 있습니다.
-   **Dry Run 지원**: `--dry-run` 옵션을 통해 실제 변경 없이 어떤 작업이 수행될지 미리 확인할 수 있습니다.

## 요구사항

-   Python 3.8 이상
-   라이브러리:
    -   `requests`
    -   `psutil`
    -   `PyYAML` (YAML 설정 파일 사용 시)
    -   `plexapi` (일부 고급 기능 및 스캔 기능 사용 시)

## 설치

1.  **스크립트 다운로드**:
    `plex_rematch_jav.py` 파일을 다운로드하거나 Git을 통해 저장소를 복제합니다.

    ```bash
    git clone [저장소 URL]
    cd [저장소 디렉터리]
    ```

2.  **필요한 라이브러리 설치**:

    ```bash
    pip install requests psutil pyyaml plexapi
    ```

## 설정

스크립트를 처음 실행하기 전에 설정 파일을 준비해야 합니다.

1.  **설정 파일 생성**:
    스크립트와 같은 디렉터리에 `plex_rematch_jav.yaml` 파일을 생성하고 아래 내용을 복사하여 붙여넣습니다.

    ```yaml
    # Plex 서버 정보 (필수)
    PLEX_URL: "http://127.0.0.1:32400"  # 실제 Plex 서버 주소로 변경
    PLEX_TOKEN: "YOUR_PLEX_TOKEN"      # 실제 Plex 토큰으로 변경
    PLEX_DB: "/path/to/your/Plex Media Server/Plug-in Support/Databases/com.plexapp.plugins.library.db" # Plex DB 파일 경로

    # 완료된 작업을 기록할 DB 파일 경로
    COMPLETION_DB: "plex_rematch_jav.db"

    # 작업자 및 시간 설정
    WORKERS: 2                # 자동 재매칭 시 동시에 실행할 작업자 수
    MATCH_INTERVAL: 2         # 각 아이템 처리 후 대기 시간 (초)
    REQUESTS_TIMEOUT_GET: 30  # GET 요청 타임아웃 (초)
    REQUESTS_TIMEOUT_PUT: 60  # PUT 요청 타임아웃 (초)

    # 매칭 관련 설정
    SCORE_MIN: 95             # 매칭 후보로 인정할 최소 점수 (0-100)
    MATCH_LIMIT: 0            # 처리할 최대 아이템 수 (0이면 무제한)
    NUMERIC_PADDING_LENGTH: 7 # 품번 숫자 부분의 0-패딩 목표 길이

    # 스캔 관련 설정
    SCAN_DEPTH: 2             # 분할 스캔 시 탐색할 하위 디렉터리 깊이
    SCAN_PATH_MAPPING_ENABLED: false # 경로 변환 기능 활성화 여부
    SCAN_PATH_MAP: "/host/path:/container/path" # 스크립트 실행 경로 -> Plex 컨테이너 경로
    # 예시: /mnt/media:/data
    ```

2.  **필수 항목 수정**:
    -   `PLEX_URL`: 사용자의 Plex 서버 주소로 변경하세요.
    -   `PLEX_TOKEN`: [Finding your Plex authentication token](https://support.plex.tv/articles/204059436-finding-an-authentication-token-x-plex-token/)을 참고하여 Plex 토큰을 찾아 입력하세요.
    -   `PLEX_DB`: 사용자의 Plex 데이터베이스 파일 경로를 정확하게 입력하세요. 경로는 운영체제 및 설치 방식에 따라 다릅니다.

## 사용법

모든 명령어는 터미널에서 실행합니다.

### 1. 라이브러리 ID 확인

작업을 수행할 라이브러리의 섹션 ID를 먼저 확인해야 합니다.

```bash
python3 plex_rematch_jav.py --check-id
```

### 2. 자동 재매칭

지정한 라이브러리에서 조건에 맞는 아이템들을 자동으로 재매칭합니다. (가장 기본적인 사용법)

```bash
# 섹션 ID 2번 라이브러리에 대해 자동 재매칭 실행
python3 plex_rematch_jav.py --section-id 2
```

-   `--include-completed`: 이미 처리 완료된 항목도 다시 작업에 포함합니다.
-   `--force-rematch`: 현재 매칭 상태와 상관없이 강제로 언매치 후 재매칭을 시도합니다.

### 3. 인터랙티브 모드

#### 품번 수정 (`--fix-labels`)

품번이 파일명과 다르거나 미매칭된 아이템을 찾아 수정합니다.

```bash
python3 plex_rematch_jav.py --section-id 2 --fix-labels
```
실행하면 문제 항목 목록이 나타나며, 처리할 항목 번호와 처리 방식(`자동`, `수동`, `Plex Dance`)을 선택할 수 있습니다.

#### 포스터 없는 항목 처리 (`--no-poster`)

포스터가 없거나 비정상적인 항목을 찾아 처리합니다.

```bash
python3 plex_rematch_jav.py --section-id 2 --no-poster
```

#### 검색 후 재매칭 (`--search`)

제목에 "ABCD-123"이 포함된 항목을 검색하여 재매칭을 진행합니다.

```bash
python3 plex_rematch_jav.py --section-id 2 --search "ABCD-123"
```
-   `--search-field`: 검색할 필드를 `title`(기본값), `label`, `site` 중에서 선택할 수 있습니다.

### 4. 라이브러리 스캔

#### 분할 스캔 (`--scan-full`)

라이브러리를 설정된 `SCAN_DEPTH` 기준으로 나누어 순차적으로 스캔하여 서버 부하를 줄입니다.

```bash
# 섹션 ID 2번 라이브러리 분할 스캔
python3 plex_rematch_jav.py --section-id 2 --scan-full
```

#### 특정 경로 스캔 (`--scan-path`)

지정한 특정 폴더만 스캔합니다.

```bash
python3 plex_rematch_jav.py --section-id 2 --scan-path "/mnt/media/new_folder"
```

### 일반 옵션

-   `--dry-run`: 실제 변경 작업을 수행하지 않고 로그만 출력하여 테스트합니다.
-   `-v`, `-vv`: 로그 출력 수준을 높입니다. (디버깅 시 `-vv` 사용)
-   `--config`: 기본 `plex_rematch_jav.yaml` 외에 다른 설정 파일을 사용합니다.

## 기여

버그를 발견하거나 새로운 기능을 제안하고 싶다면 언제든지 GitHub 이슈를 열어주세요. Pull Request도 환영합니다.

## 면책 조항

이 스크립트는 Plex 데이터베이스와 상호작용합니다. 사용하기 전에 **반드시 Plex 데이터베이스 파일을 백업**하세요. 스크립트 사용으로 인해 발생하는 데이터 손실이나 문제에 대해 제작자는 책임을 지지 않습니다. `--dry-run` 옵션을 통해 충분히 테스트한 후 사용하시기 바랍니다.
