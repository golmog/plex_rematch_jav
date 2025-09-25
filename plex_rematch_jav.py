#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# plex_rematch_jav.py

import sqlite3
import functools
import traceback
import time
import asyncio
import urllib.parse
import psutil
import requests
import argparse
import sys
import os
import re
import logging
import signal
import shutil
from datetime import datetime
from typing import List, Optional, Tuple, Union, Any

try:
    import yaml
    PYYAML_AVAILABLE = True
except ImportError:
    PYYAML_AVAILABLE = False
    print("경고: PyYAML 라이브러리를 찾을 수 없습니다. YAML 설정 파일을 사용하려면 설치해야 합니다.")
    print("라이브러리를 설치하려면 'pip install pyyaml'를 실행하세요.")

try:
    from plexapi.server import PlexServer
    from plexapi.exceptions import NotFound as PlexApiNotFound
    PLEXAPI_AVAILABLE = True
except ImportError:
    PLEXAPI_AVAILABLE = False
    # print 문은 setup_logging 이후 logger로 대체 가능하므로 여기서는 일단 유지

logger = logging.getLogger(__name__)

# YAML 설정 파일 기본 경로 (스크립트와 같은 디렉터리)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_YAML_CONFIG_PATH = os.path.join(SCRIPT_DIR, "plex_rematch_jav.yaml")

# 스크립트 내 최소한의 기본값 (YAML 로드 실패 시 또는 YAML에 없는 항목 대비)
FALLBACK_DEFAULT_CONFIG = {
    "COMPLETION_DB": os.path.join(SCRIPT_DIR, "plex_rematch_jav.db"), # YAML 로드 실패 시 현재 디렉터리에 생성
    "PLEX_DB": None, # YAML 또는 CLI에서 필수로 받아야 함
    "PLEX_URL": "http://127.0.0.1:32400",
    "PLEX_TOKEN": None, # YAML 또는 CLI에서 필수로 받아야 함

    "PLEX_DANCE_TEMP_PATH": None,
    "PLEX_DANCE_REAPPEAR_TIMEOUT": 600,
    "PLEX_DANCE_REAPPEAR_INTERVAL": 10,
    "PLEX_DANCE_EMPTY_TRASH_TIMEOUT": 300,
    "PLEX_DANCE_EMPTY_TRASH_INTERVAL": 5,

    "WORKERS": 2,
    "CHECK_INTERVAL": 3,
    "CHECK_COUNT": 10,
    "SCORE_MIN": 95,
    "REQUESTS_TIMEOUT_GET": 30,
    "REQUESTS_TIMEOUT_PUT": 60,
    "NUMERIC_PADDING_LENGTH": 5,
    "MATCH_LIMIT": 0,
    "MATCH_INTERVAL": 2,

    "SCAN_DEPTH": 2,
    "SCAN_PATH_MAPPING_ENABLED": False,
    "SCAN_PATH_MAP": "/path/on/script_host:/path/on/plex_host",

    "SJVA_AGENT_GUID_PREFIX": "com.plexapp.agents.sjva_agent://",
    "SITE_CODE_MAP": {
        "dmm": ["CD", "YMCD"],
        "mgs": ["CM", "YMCM"],
        "jav321": ["CT", "YMCT"],
        "javbus": ["CB", "YMCB"],
        "javdb": ["CJ", "YMCJ"],
    },

    "JAV_PARSING_RULES": {
        'generic_rules': [
            r'.*?\d*([a-z]+)-(\d+)(?=[^\d]|\b) => {0}|{1}',
            r'.*?\b([0-9a-z]+)-(\d+)(?=[^\d]|\b) => {0}|{1}',
            r'.*?\d*([a-z]+)-?(\d+)(?=[^\d]|\b) => {0}|{1}',
            r'.*?\b([0-9a-z]*[a-z]+)-?(\d+)(?=[^\d]|\b) => {0}|{1}'
        ],
        'censored_special_rules': [
            r'.*?(3dsvr)[-_]?(\d+)(?=[^\d]|\b) => {0}|{1}',
            r'.*?(?<![a-z0-9])(741[a-z]\d{3})[-_]?(g\d{2,})(?=[^\d]|\b) => {0}|{1}',
            r'.*?(?<![a-z])(t)[-_]?(28|38)[-_]?(\d+)(?=[^\d]|\b) => {0}{1}|{2}',
            r'.*?(\d{2})?(id)[-_]?([1-9]\d|\d[1-9])(\d{3})(?=[^\d]|\b) => {2}{1}|{3}',
            r'.*?(?<![a-z])(cpz\d{2})[-_]?([a-z]\d+)(?=[^\d]|\b) => {0}|{1}',
            r'.*?(?<![a-z])(g)[-_](area)[-_]?(\d+)(?=[^\d]|\b) => {0}{1}|{2}',
            r'.*?(?<![a-z])(s)[-_](cute)[-_]?(\d+)(?=[^\d]|\b) => {0}{1}|{2}',
            r'.*?(?<![a-z])(mar|mbr|mmr)[-_]([a-z]{2})[-_]?(\d+)(?=[^\d]|\b) => {0}{1}|{2}',
            r'.*?(?<![a-z])(tokyo)247[-_]?(\d+)(?=[^\d]|\b) => {0}|{1}',
            r'.*?(?<![a-z])(wvr[1-9])[-_]?([a-z]?\d{3,})\b => {0}|{1}',
            r'.*?((?:[0-9]{3})?ypp)[-_]?(\d+)(?=[^\d]|\b) => {0}|{1}',
            r'.*?(\d{3})(ap|good|san|ten)[-_]?(\d+)(?=[^\d]|\b) => {0}{1}|{2}',
            r'.*?(\d{2})(ap|id|ntrd|san|sora|sw|ten)[-_]?(\d{3,4})(?=[^\d]|\b) => {0}{1}|{2}'
            ]
    },

    "ACTORS_DB_PATH": None, # 배우 DB 경로

    # 수정 금지 (스크립트 내부 또는 옵션으로 제어) - YAML에 넣지 않음
    "DRY_RUN": False,
    "FORCE_REMATCH": False,
    "INCLUDE_COMPLETED": False,
    "FIX_LABELS": False,
    "MANUAL_SEARCH": False,
    "MOVE_NO_META_PATH": None,
    "MOVE_NO_META": False,
    "SCAN_NO_WAIT": False,
}

CONFIG = {}
PLEX_SERVER_INSTANCE = None
SHUTDOWN_REQUESTED = False
ACTORS_DB_CACHE = None

PLEX_MEDIA_TYPE_VALUES = {1: 'movie', 13: 'photoalbum'}
PLEX_SECTION_TYPE_VALUES = {1: "영화", 13: "사진"}
MATCH_CANDIDATE_EXCLUDED_GUID_PREFIXES = ['collection://']

def setup_logging(verbosity: int):
    log_level = logging.INFO # 기본값은 INFO
    if verbosity == 1: # -v 하나면 INFO 유지 (이미 기본값)
        log_level = logging.INFO
    elif verbosity >= 2: # -vv 이상이면 DEBUG
        log_level = logging.DEBUG

    logging.basicConfig(
        level=log_level,
        format='%(asctime)s %(levelname)-6s: %(message)s',
        datefmt='%Y/%m/%d %H:%M:%S',
        stream=sys.stdout
    )
    for lib_name in ["requests", "urllib3", "plexapi"]:
        logging.getLogger(lib_name).setLevel(logging.WARNING)

def load_yaml_config(config_path: str) -> dict:
    if not PYYAML_AVAILABLE:
        logger.warning("PyYAML 라이브러리가 없어 YAML 설정을 로드할 수 없습니다. 스크립트 내 기본값을 사용합니다.")
        return {}

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            yaml_config = yaml.safe_load(f)
            if yaml_config is None: # 파일이 비어있는 경우
                logger.warning(f"'{config_path}' 파일이 비어있거나 유효한 YAML 내용이 없습니다.")
                return {}
            logger.info(f"'{config_path}' 파일에서 설정을 로드했습니다.")
            return yaml_config
    except FileNotFoundError:
        logger.warning(f"'{config_path}' 설정 파일을 찾을 수 없습니다. 스크립트 내 기본값을 사용하거나, 명령행 인자로 모든 설정을 제공해야 합니다.")
        return {}
    except yaml.YAMLError as e:
        logger.error(f"'{config_path}' 파일 파싱 중 오류 발생: {e}")
        return {}
    except Exception as e:
        logger.error(f"'{config_path}' 파일 로드 중 예기치 않은 오류 발생: {e}")
        return {}

def handle_signal(sig, frame):
    global SHUTDOWN_REQUESTED
    if not SHUTDOWN_REQUESTED:
        msg = f"\n종료 신호 ({signal.Signals(sig).name}) 수신. 현재 진행 중인 작업을 완료 후 종료합니다..."
        if logger.hasHandlers() and logger.isEnabledFor(logging.WARNING):
            logger.warning(msg)
        else:
            print(msg)
        SHUTDOWN_REQUESTED = True
    else:
        msg = "이미 종료 요청됨. 즉시 종료합니다."
        if logger.hasHandlers() and logger.isEnabledFor(logging.CRITICAL):
            logger.critical(msg)
        else:
            print(msg)
        sys.exit(1)

def natural_sort_key(s: str) -> List[Union[str, int]]:
    """
    자연 정렬(Natural Sort)을 위한 정렬 키를 생성합니다.
    예: "abp-101" -> ['abp-', 101, '']
    """
    if not isinstance(s, str):
        return [s]
    return [int(text) if text.isdigit() else text.lower() for text in re.split('([0-9]+)', s)]

def mem_usage() -> int: return psutil.Process().memory_info().rss / (1024 * 1024)

def require_cursor(db_path_key: str, read_only: bool = False):
    def decorator(f) -> callable:
        @functools.wraps(f)
        def wrap(*args, **kwds) -> any:
            db_path = None
            args_ns_from_kwds = kwds.get('args_namespace')

            if 'cs' in kwds and kwds['cs'] is not None: return f(*args, **kwds)

            if CONFIG and CONFIG.get(db_path_key): db_path = CONFIG.get(db_path_key)
            elif args_ns_from_kwds and hasattr(args_ns_from_kwds, db_path_key.lower()):
                db_path = getattr(args_ns_from_kwds, db_path_key.lower())
            elif DEFAULT_CONFIG.get(db_path_key):
                db_path = DEFAULT_CONFIG.get(db_path_key)

            if not db_path:
                print(f"CRITICAL: DB 경로 미설정 for key '{db_path_key}'");
                raise ValueError(f"DB path for key '{db_path_key}' not determined.")
            if 'args_namespace' in kwds: del kwds['args_namespace']

            connect_path = f"file:{db_path}?mode=ro" if read_only else db_path
            try:
                with sqlite3.connect(connect_path, uri=read_only) as con:
                    con.row_factory = sqlite3.Row
                    return f(*args, cs=con.cursor(), **kwds)
            except sqlite3.OperationalError as e:
                if read_only and ("uris not supported" in str(e).lower() or "invalid uri" in str(e).lower()):
                    with sqlite3.connect(db_path) as con_fallback:
                        con_fallback.row_factory = sqlite3.Row
                        return f(*args, cs=con_fallback.cursor(), **kwds)
                log_func = logger.error if logger.hasHandlers() else print
                log_func(f"ERROR: DB 연결 실패 ({db_path}): {e}"); raise
        return wrap
    return decorator

@require_cursor("PLEX_DB", read_only=True)
def list_library_sections_from_db(cs: sqlite3.Cursor, args_namespace=None) -> List[sqlite3.Row]:
    query = "SELECT id, name, section_type, agent, scanner, language FROM library_sections ORDER BY name ASC"
    return cs.execute(query).fetchall()

@require_cursor("PLEX_DB", read_only=True)
def fetch_all(query_template: str, params: tuple = None, cs: sqlite3.Cursor = None) -> List[dict]:
    logger.debug(f"DB Query (all): {query_template[:100]}... | Params: {params}")
    rows = cs.execute(query_template, params or ()).fetchall()
    return [dict(row) for row in rows] if rows else []

@require_cursor("PLEX_DB", read_only=True)
def fetch_one(query_template: str, params: tuple = None, cs: sqlite3.Cursor = None) -> Optional[dict]:
    logger.debug(f"DB Query (one): {query_template[:100]}... | Params: {params}")
    row = cs.execute(query_template, params or ()).fetchone()
    return dict(row) if row else None

@require_cursor("COMPLETION_DB")
def create_table_if_not_exists(cs: sqlite3.Cursor) -> None:
    cs.execute("CREATE TABLE IF NOT EXISTS completed_tasks (metadata_id INTEGER PRIMARY KEY, status TEXT, matched_guid TEXT, completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")

@require_cursor("COMPLETION_DB")
def record_completed_task(metadata_id: int, status: str = "COMPLETED", matched_guid: str = None, cs: sqlite3.Cursor = None) -> None:
    cs.execute("INSERT OR REPLACE INTO completed_tasks (metadata_id, status, matched_guid, completed_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
               (metadata_id, status, matched_guid))

@require_cursor("COMPLETION_DB", read_only=True)
def is_task_completed(metadata_id: int, cs: sqlite3.Cursor = None) -> bool:
    return cs.execute("SELECT 1 FROM completed_tasks WHERE metadata_id = ? AND status LIKE 'COMPLETED%'", (metadata_id,)).fetchone() is not None

@require_cursor("COMPLETION_DB", read_only=True)
def get_completed_task_info(metadata_id: int, cs: sqlite3.Cursor = None) -> Optional[sqlite3.Row]:
    return cs.execute("SELECT metadata_id, status, matched_guid, completed_at FROM completed_tasks WHERE metadata_id = ?", (metadata_id,)).fetchone()

@require_cursor("PLEX_DB", read_only=True)
def get_media_file_path(metadata_id: int, cs: sqlite3.Cursor = None) -> Optional[str]:
    query = "SELECT mp.file FROM media_parts mp JOIN media_items mi ON mp.media_item_id = mi.id WHERE mi.metadata_item_id = ? ORDER BY mi.id, mp.id LIMIT 1;"
    row = cs.execute(query, (metadata_id,)).fetchone()
    return row['file'] if row and 'file' in row.keys() and row['file'] else None

def pad_numeric_part(num_str: str, target_length: int) -> str:
    """숫자 문자열의 숫자 부분을 대상 길이로 0-패딩합니다. 뒤에 붙는 알파벳은 유지합니다."""
    if not num_str: return ""
    match = re.match(r"(\d+)([A-Za-z]*)$", num_str)
    if match:
        numeric_digits, trailing_alpha = match.group(1), match.group(2)
        return numeric_digits.zfill(target_length) + trailing_alpha
    if num_str.isdigit():
        return num_str.zfill(target_length)
    logger.debug(f"pad_numeric_part: '{num_str}'는 숫자/후행알파벳 패턴이 아님. 원본 반환.")
    return num_str


@require_cursor("PLEX_DB", read_only=True)
def check_items_exist_by_paths(file_paths: List[str], cs: sqlite3.Cursor = None) -> bool:
    """주어진 파일 경로 목록이 모두 유효한 metadata_item에 연결되었는지 DB에서 확인합니다."""
    if not file_paths:
        return False

    query = """
    SELECT COUNT(mi.id)
    FROM media_parts mp
    JOIN media_items mi ON mp.media_item_id = mi.id
    WHERE mp.file = ? AND mi.metadata_item_id IS NOT NULL
    """

    for path in file_paths:
        row = cs.execute(query, (path,)).fetchone()
        if not row or row[0] == 0:
            logger.debug(f"DB 확인: 경로 '{path}'가 아직 라이브러리에 추가되지 않았습니다.")
            return False # 하나라도 없으면 즉시 False 반환

    # 모든 경로가 존재하면 True 반환
    return True


@require_cursor("PLEX_DB", read_only=True)
def check_item_deleted_by_id(metadata_id: int, cs: sqlite3.Cursor = None) -> bool:
    """주어진 metadata_id가 DB에서 완전히 삭제되었는지 확인합니다."""
    query = "SELECT 1 FROM metadata_items WHERE id = ?"
    row = cs.execute(query, (metadata_id,)).fetchone()
    # row가 없으면 (None) 삭제된 것이므로 True를 반환
    return row is None


# 배우 DB 조회를 위한 전용 커서 데코레이터
def require_actors_db_cursor(f):
    @functools.wraps(f)
    def wrap(*args, **kwds):
        db_path = CONFIG.get("ACTORS_DB_PATH")
        if not db_path:
            logger.error("배우 DB 경로(ACTORS_DB_PATH)가 설정되지 않았습니다.")
            return None # 또는 적절한 예외 처리

        if not os.path.exists(db_path):
            logger.error(f"배우 DB 파일을 찾을 수 없습니다: {db_path}")
            return None

        try:
            # 읽기 전용으로 연결
            with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as con:
                con.row_factory = sqlite3.Row
                return f(*args, cs=con.cursor(), **kwds)
        except sqlite3.OperationalError: # uri=True 미지원 환경 대비
            with sqlite3.connect(db_path) as con:
                con.row_factory = sqlite3.Row
                return f(*args, cs=con.cursor(), **kwds)
    return wrap


def parse_jp_names_from_onm(onm_string: str) -> List[str]:
    """'한국어(일본어), 다른한국어(다른일본어)' 형태의 문자열에서 모든 일본어 이름을 추출합니다."""
    if not onm_string:
        return []
    return re.findall(r'\(([^)]+)\)', onm_string)


@require_actors_db_cursor
def initialize_actors_cache(cs: sqlite3.Cursor = None) -> bool:
    """배우 DB를 읽어와 메모리에 캐시를 생성합니다."""
    global ACTORS_DB_CACHE
    if ACTORS_DB_CACHE is not None:
        logger.debug("배우 DB 캐시가 이미 초기화되었습니다.")
        return True

    logger.info("배우 DB를 읽어와 메모리에 캐시를 생성합니다...")

    # 캐시 구조: {'일본어/원어이름': {'kr_name': '한국어이름', 'cn_name': '대표원어', 'onm': '별명들'}}
    temp_cache = {}

    query = "SELECT inner_name_kr, inner_name_cn, actor_onm FROM actors"
    all_actors = cs.execute(query).fetchall()

    for actor_record in all_actors:
        kr_name = actor_record['inner_name_kr'] if 'inner_name_kr' in actor_record.keys() else None
        cn_name = actor_record['inner_name_cn'] if 'inner_name_cn' in actor_record.keys() else None
        onm_str = actor_record['actor_onm'] if 'actor_onm' in actor_record.keys() else None

        # 이 배우의 모든 원어 이름을 담을 집합
        all_jp_names = set()

        if cn_name:
            all_jp_names.add(cn_name)
        if onm_str:
            jp_names_in_onm = parse_jp_names_from_onm(onm_str)
            all_jp_names.update(jp_names_in_onm)

        # 각 원어 이름을 키로 하여 캐시에 저장
        for jp_name in all_jp_names:
            if jp_name not in temp_cache:
                temp_cache[jp_name] = {
                    'kr_name': kr_name,
                    'cn_name': cn_name,
                    'onm': onm_str
                }

    ACTORS_DB_CACHE = temp_cache
    logger.info(f"배우 DB 캐시 생성 완료. 총 {len(all_actors)}명의 배우, {len(temp_cache)}개의 원어 이름 인덱싱됨.")
    return True


def get_actor_info_from_jp_name(jp_name: str) -> Optional[dict]:
    """메모리에 캐시된 배우 정보에서 주어진 원어 이름과 일치하는 정보를 찾습니다."""
    global ACTORS_DB_CACHE
    if ACTORS_DB_CACHE is None:
        logger.error("배우 DB 캐시가 초기화되지 않았습니다. get_actor_info_from_jp_name을 호출할 수 없습니다.")
        return None

    if not jp_name:
        return None

    # 캐시에서 직접 조회 (매우 빠름)
    actor_info = ACTORS_DB_CACHE.get(jp_name)

    if actor_info:
        # sqlite3.Row와 유사한 인터페이스를 제공하기 위해 딕셔너리를 반환
        return {
            'inner_name_kr': actor_info['kr_name'],
            'inner_name_cn': actor_info['cn_name'],
            'actor_onm': actor_info['onm']
        }

    return None


# Plex DB에서 특정 아이템의 배우 정보를 가져오는 함수
@require_cursor("PLEX_DB", read_only=True)
def get_actors_for_item_from_plex_db(item_id: int, cs: sqlite3.Cursor = None) -> List[str]:
    """Plex DB에서 특정 metadata_item_id에 연결된 배우(tag) 목록을 가져옵니다."""
    query = """
    SELECT
        t.tag AS actor_name_kr -- 배우 이름 (한국어 또는 원어)
    FROM tags t
    JOIN taggings tg ON t.id = tg.tag_id
    WHERE tg.metadata_item_id = ? AND t.tag_type = 6
    """
    rows = cs.execute(query, (item_id,)).fetchall()
    # 문자열 리스트를 반환
    return [row['actor_name_kr'] for row in rows if row and row['actor_name_kr']]


def format_numeric_for_search(num_str: str, min_digits: int = 3) -> str:
    """숫자 문자열을 검색에 적합한 형태로 포맷합니다 (최소 3자리, 그 이상은 원본 유지, 후행 알파벳 보존)."""
    if not num_str:
        return ""

    num_part = ""
    trailing_alpha = ""

    match_num_alpha = re.match(r"(\d+)([A-Za-z]*)$", num_str)
    if match_num_alpha:
        num_part = match_num_alpha.group(1)
        trailing_alpha = match_num_alpha.group(2)
    elif num_str.isdigit():
        num_part = num_str
    else:
        logger.debug(f"format_numeric_for_search: '{num_str}'는 숫자 형태로 변환 불가. 원본 반환.")
        return num_str

    try:
        num_as_int = int(num_part)
        num_unpadded_str = str(num_as_int)
    except ValueError:
        logger.debug(f"format_numeric_for_search: '{num_part}'를 int로 변환 실패. 원본 숫자 부분 사용.")
        num_unpadded_str = num_part

    if len(num_unpadded_str) < min_digits:
        formatted_num = num_unpadded_str.zfill(min_digits)
    else:
        formatted_num = num_unpadded_str

    return formatted_num + trailing_alpha


COMPILED_PARSING_RULES = {}

def compile_parsing_rules():
    """
    CONFIG에 있는 raw string 규칙들을 파싱하여
    실행 가능한 {'pattern', 'label_format', 'num_format'} 딕셔너리 리스트로 컴파일합니다.
    """
    global COMPILED_PARSING_RULES
    if COMPILED_PARSING_RULES: # 이미 컴파일되었으면 건너뜀
        return

    logger.debug("품번 파싱 규칙을 컴파일합니다...")
    raw_rules = CONFIG.get("JAV_PARSING_RULES", {})
    compiled = {'special': [], 'generic': []}

    rule_parser = re.compile(r'(.*?)\s*=>\s*(.*)')

    # 특수 규칙 컴파일
    for rule_str in raw_rules.get('censored_special_rules', []):
        match = rule_parser.match(rule_str)
        if not match: continue

        pattern, format_body = match.groups()
        if '|' not in format_body: continue

        label_format, num_format = format_body.split('|', 1)[:2]
        compiled['special'].append({
            'pattern': pattern,
            'label_format': label_format.strip(),
            'num_format': num_format.strip()
        })

    # 범용 규칙 컴파일
    for rule_str in raw_rules.get('generic_rules', []):
        match = rule_parser.match(rule_str)
        if not match: continue

        pattern, format_body = match.groups()
        if '|' not in format_body: continue

        label_format, num_format = format_body.split('|', 1)[:2]
        compiled['generic'].append({
            'pattern': pattern,
            'label_format': label_format.strip(),
            'num_format': num_format.strip()
        })

    COMPILED_PARSING_RULES = compiled
    logger.debug(f"규칙 컴파일 완료: 특수 규칙 {len(compiled['special'])}개, 범용 규칙 {len(compiled['generic'])}개")


def parse_product_id_with_rules(text: str) -> Optional[str]:
    """
    컴파일된 규칙을 사용하여 문자열에서 품번을 추출하고 정규화합니다.
    """
    if not text: return None

    # 규칙이 컴파일되지 않았다면 컴파일 실행
    if not COMPILED_PARSING_RULES:
        compile_parsing_rules()

    normalized_text = text.lower()
    padding_len = CONFIG.get("NUMERIC_PADDING_LENGTH", 7)

    # 특수 규칙과 범용 규칙을 순서대로 순회
    for rule_type in ['special', 'generic']:
        for rule in COMPILED_PARSING_RULES.get(rule_type, []):
            try:
                match = re.match(rule['pattern'], normalized_text)
                if match:
                    groups = match.groups('') # 매치 안된 그룹은 빈 문자열로

                    # 포맷 문자열을 사용하여 레이블과 숫자 파트 생성
                    label_part = rule['label_format'].format(*groups)
                    num_part = rule['num_format'].format(*groups)

                    if label_part and num_part:
                        padded_num = pad_numeric_part(num_part, padding_len)
                        pid = f"{label_part}-{padded_num}"
                        logger.debug(f"규칙 '{rule['pattern']}' 매치 성공 ('{rule_type}'): '{text}' -> '{pid}'")
                        return pid.lower()
            except (re.error, IndexError) as e:
                logger.error(f"규칙 처리 중 오류: {rule['pattern']} on '{text}'. 오류: {e}")

    logger.debug(f"모든 품번 추출 규칙에 매치 실패: '{text}'")
    return None


def extract_product_id_from_filename(filename: str) -> Optional[str]:
    if not filename: return None
    base_name = os.path.splitext(filename)[0]
    return parse_product_id_with_rules(base_name)


def extract_product_id_from_title(title: str, original_file_pid_parts: Optional[Tuple[str, str]] = None) -> Optional[str]:
    return parse_product_id_with_rules(title)


def normalize_pid_for_comparison(pid_alpha_hyphen_padded_num: str) -> Optional[str]:
    if not pid_alpha_hyphen_padded_num:
        return None

    try:
        label_part_raw, num_part_raw = pid_alpha_hyphen_padded_num.split('-', 1)
    except ValueError:
        logger.warning(f"normalize_pid_for_comparison: 입력 '{pid_alpha_hyphen_padded_num}'에 하이픈이 없어 분리 불가. 정규화 실패.")
        return None

    # 1. 레이블 부분 정규화
    normalized_label = label_part_raw.lower()
    # '741'로 시작하지 않는 경우, 레이블 앞부분의 연속된 숫자만 제거
    if not normalized_label.startswith("741"):
        stripped_from_front = normalized_label.lstrip('0123456789')
        # lstrip 후에도 문자열이 남아있고, 원래 레이블이 순수 숫자가 아니라면 적용
        if stripped_from_front and not normalized_label.isdigit():
            normalized_label = stripped_from_front

    # 2. 숫자 부분 표준화 (Standardization)
    num_part_lower = num_part_raw.lower()
    padding_len = CONFIG.get("NUMERIC_PADDING_LENGTH", 7)

    # 숫자와 후행 알파벳 분리 (예: '012a' -> '012', 'a')
    match = re.match(r"(\d+)([a-z]*)?$", num_part_lower)
    if not match:
        logger.warning(f"normalize_pid_for_comparison: 숫자 부분 '{num_part_lower}'에서 숫자/알파벳 패턴 파싱 실패. 원본 사용 시도.")
        # 파싱 실패 시, 비교 일관성을 위해 원본 숫자 부분을 합쳐서 반환
        return normalized_label + num_part_lower

    numeric_digits_str = match.group(1)
    trailing_alpha = match.group(2) or ""

    try:
        standardized_num_part = str(int(numeric_digits_str)).zfill(padding_len)
    except (ValueError, TypeError):
        logger.warning(f"normalize_pid_for_comparison: 정규식 통과 후에도 '{numeric_digits_str}'를 정수로 변환 실패. zfill만 적용.")
        standardized_num_part = numeric_digits_str.zfill(padding_len)

    # 3. 최종 비교 문자열 생성
    # 정규화된 레이블 + 표준화된 숫자 부분 + 후행 알파벳
    final_comparison_pid = normalized_label + standardized_num_part + trailing_alpha

    logger.debug(f"normalize_pid_for_comparison: '{pid_alpha_hyphen_padded_num}' (L:'{label_part_raw}', N:'{num_part_raw}') -> '{final_comparison_pid}'")
    return final_comparison_pid


async def await_sync(func: callable, *args, **kwds) -> any:
    if SHUTDOWN_REQUESTED: raise asyncio.CancelledError("Shutdown requested during await_sync preparation")
    return await asyncio.get_running_loop().run_in_executor(None, functools.partial(func, *args, **kwds))

async def get_query_count(base_query_template_for_select: str, params: tuple = None) -> int:
    lower_template = base_query_template_for_select.lower()
    idx_select = lower_template.find("select ")
    idx_from = lower_template.find(" from ", idx_select)
    idx_order_by = lower_template.rfind(" order by ")
    if idx_select == -1 or idx_from == -1:
        logger.error(f"COUNT 쿼리 생성 실패: {base_query_template_for_select}"); return -1

    from_where_part_end_idx = idx_order_by if idx_order_by > idx_from else len(base_query_template_for_select)
    from_where_part = base_query_template_for_select[idx_from:from_where_part_end_idx]

    count_query = f"SELECT COUNT(*) AS count {from_where_part}"
    row = await await_sync(fetch_one, count_query, params)

    if row:
        return row.get('count', row.get('COUNT(*)'))
    return -1

async def get_metadata_by_id(metadata_id: int) -> sqlite3.Row:
    query = "SELECT id, title, guid, library_section_id, metadata_type, year, original_title, title_sort, refreshed_at FROM metadata_items WHERE id=?"
    return await await_sync(fetch_one, query, (metadata_id,))

async def get_section_by_id(section_id: int) -> sqlite3.Row:
    query = "SELECT id, name, section_type, agent FROM library_sections WHERE id=?"
    return await await_sync(fetch_one, query, (section_id,))

def response_json(f) -> callable:
    @functools.wraps(f)
    async def wrap(*args, **kwds) -> dict:
        if SHUTDOWN_REQUESTED: raise asyncio.CancelledError("Shutdown requested before API call")
        response = await f(*args, **kwds)
        try: return response.json()
        except requests.exceptions.JSONDecodeError: logger.warning(f"JSON 디코딩 오류. 응답: {response.text[:200]}..."); return {}
        except Exception as e: logger.error(f"응답 처리 중 예외: {e}", exc_info=True); return {}
    return wrap

@response_json
async def _get_matches_api(metadata_id: int, title: str = None, year: int = None, agent: str = None) -> dict:
    query_params = {'X-Plex-Token': CONFIG["PLEX_TOKEN"], 'language': 'ko'}
    plex_url_base = CONFIG.get('PLEX_URL', '')

    is_fix_labels_mode = CONFIG.get("FIX_LABELS", False)
    use_manual_one = is_fix_labels_mode or CONFIG.get("MANUAL_SEARCH", False)
    logger.debug(f"_get_matches_api: metadata_id={metadata_id}, FIX_LABELS={is_fix_labels_mode}, MANUAL_SEARCH={CONFIG.get('MANUAL_SEARCH', False)}, use_manual_one={use_manual_one}")

    query_params['manual'] = '1' if use_manual_one else '0'
    manual_mode_for_log = query_params['manual']
    if is_fix_labels_mode: manual_mode_for_log += " (fix-labels)"
    elif CONFIG.get("MANUAL_SEARCH"): manual_mode_for_log += " (manual-search CLI)"
    else: manual_mode_for_log += " (auto-prioritized)"

    if title: query_params['title'] = title
    if year: query_params['year'] = str(year)
    if agent: query_params['agent'] = agent

    final_query_params = {k: v for k, v in query_params.items() if v is not None}
    url = f"{plex_url_base}/library/metadata/{metadata_id}/matches?{urllib.parse.urlencode(final_query_params)}"

    log_url_display = f"{plex_url_base}/library/metadata/{metadata_id}/matches?title={title or ''}&year={year or ''}&manual={manual_mode_for_log}"
    logger.debug(f"매치 검색 API 호출: {log_url_display} (에이전트: {agent or '기본'})")

    try:
        response = await asyncio.get_running_loop().run_in_executor(
            None, functools.partial(requests.get, url, headers={'Accept': 'application/json'}, timeout=CONFIG.get("REQUESTS_TIMEOUT_GET", 30))
        )
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        logger.warning(f"매치 검색 API 실패 (ID: {metadata_id}, Title: {title}, Manual: {manual_mode_for_log}): {e}")

        empty_resp = requests.Response()
        empty_resp.status_code = 500
        empty_resp._content = b'{}'
        empty_resp.headers['Content-Type'] = 'application/json'
        return empty_resp

async def get_plex_matches(item_row: sqlite3.Row) -> dict:
    item_id = item_row['id']
    logger.debug(f"get_plex_matches 호출: ID={item_id}")

    section_id = item_row['library_section_id'] if 'library_section_id' in item_row.keys() else None
    if section_id is None:
        logger.error(f"ID {item_id}: library_section_id 누락. 매칭 검색 불가.")
        return {'MediaContainer': {'size': 0, 'SearchResult': []}}

    section = await get_section_by_id(section_id)
    agent_to_use = section['agent'] if section and 'agent' in section.keys() else None
    item_year_val = item_row['year'] if 'year' in item_row.keys() else None

    media_path = await await_sync(get_media_file_path, item_id)
    if not media_path:
        logger.warning(f"ID {item_id}: 미디어 파일 경로 없음. 검색어 생성 불가.")
        return {'MediaContainer': {'size': 0, 'SearchResult': []}}

    base_filename = os.path.basename(media_path)
    logger.debug(f"  ID {item_id}: 파일명 기반 검색어 추출 시작 (파일명: '{base_filename}')")

    filename_pid_padded = extract_product_id_from_filename(base_filename)

    if not filename_pid_padded:
        logger.warning(f"  ID {item_id}: 파일명 '{base_filename}'에서 품번 추출 실패.")
        return {'MediaContainer': {'size': 0, 'SearchResult': []}}

    try:
        label_part, num_part_padded = filename_pid_padded.split('-', 1)
        num_part_for_search = format_numeric_for_search(num_part_padded)
        search_term = f"{label_part}-{num_part_for_search}"
    except ValueError:
        logger.error(f"  ID {item_id}: 추출된 품번 '{filename_pid_padded}' 분리 실패. 원본 사용.")
        search_term = filename_pid_padded

    logger.info(f"  ID {item_id}: 파일명 기반 단일 검색어 사용 - 검색어='{search_term}', 연도={item_year_val}, 에이전트='{agent_to_use or '기본'}'")

    if SHUTDOWN_REQUESTED:
        logger.warning(f"ID {item_id}: API 호출 전 종료 요청 (get_plex_matches)")
        raise asyncio.CancelledError("Shutdown requested in get_plex_matches")

    matches_result = await _get_matches_api(item_id, search_term, item_year_val, agent_to_use)

    if matches_result and isinstance(matches_result.get('MediaContainer'), dict):
        size = matches_result['MediaContainer'].get('size', 0)
        log_msg_suffix = f"(ID {item_id})"
        if size > 0:
            logger.info(f"  > 검색어 '{search_term}'로 {size}개 결과 찾음 {log_msg_suffix}")
        else:
            logger.info(f"  > 검색어 '{search_term}'로 결과 없음 {log_msg_suffix}")
    else:
        logger.warning(f"  > 검색어 '{search_term}' API 응답 유효하지 않음 (ID {item_id})")
        if not matches_result:
            matches_result = {'MediaContainer': {'size': 0, 'SearchResult': []}}

    return matches_result


async def check_plex_update(metadata_id: int, start_timestamp: int) -> bool:
    """Plex 아이템의 메타데이터 업데이트 여부 및 내용의 완전성을 확인합니다."""
    if SHUTDOWN_REQUESTED: raise asyncio.CancelledError("Shutdown requested during check_plex_update")

    row = await get_metadata_by_id(metadata_id)
    if not row:
        logger.warning(f"  업데이트 확인 중 아이템 (ID={metadata_id})이 DB에서 삭제됨."); return True

    title = row.get('title') or f"ID {metadata_id}"
    refreshed_at = row.get('refreshed_at')
    if refreshed_at is None:
        refreshed_at = 0

    # 1. 타임스탬프 확인
    is_timestamp_updated = (refreshed_at >= start_timestamp)

    # 2. 제목 형식의 완전성 확인
    is_title_complete = False
    if title:
        if title.strip().startswith('[') or title.strip().startswith('【'):
            is_title_complete = True

    # 최종 판단
    if is_timestamp_updated and is_title_complete:
        refreshed_dt = datetime.fromtimestamp(refreshed_at)
        logger.info(f"  업데이트 확인 성공: ID={metadata_id}, Title='{title}', RefreshedAt={refreshed_dt}")
        return True

    if is_timestamp_updated and not is_title_complete:
        logger.debug(f"  업데이트 미완료 (내용): ID={metadata_id}, Title='{title}' (형식 불완전, 대기 계속)")
    else:
        logger.debug(f"  업데이트 미반영 (시간): ID={metadata_id}, RefreshedAt={refreshed_at} < StartTS={start_timestamp}")

    return False


async def unmatch_plex_item(metadata_id: int) -> bool:
    """로컬 JSON 정리 및 Plex API를 통한 아이템 언매치를 수행합니다."""
    if SHUTDOWN_REQUESTED: raise asyncio.CancelledError("Shutdown requested before unmatch_plex_item")
    if not PLEXAPI_AVAILABLE:
        logger.error("plexapi 라이브러리가 없어 unmatch_plex_item을 실행할 수 없습니다.")
        return False

    item_info_row = await get_metadata_by_id(metadata_id)
    item_title_for_log = item_info_row['title'] if item_info_row and 'title' in item_info_row.keys() and item_info_row['title'] else f"ID {metadata_id}"
    logger.info(f"  ID {metadata_id} ('{item_title_for_log}'): 언매칭...")

    # 로컬 JSON 파일 삭제 로직
    if not CONFIG.get("DRY_RUN"):
        try:
            video_file_path = await await_sync(get_media_file_path, metadata_id)
            if video_file_path:
                video_dir, base_filename = os.path.dirname(video_file_path), os.path.basename(video_file_path)
                # JSON 파일명 규칙에 따라 품번 추출 (파일명 시작부분)
                pid_match_for_json = re.match(r"^([a-zA-Z0-9\-]+)", base_filename)
                if pid_match_for_json:
                    json_filename = f"{pid_match_for_json.group(1).lower()}.json"
                    json_file_path = os.path.join(video_dir, json_filename)
                    if os.path.exists(json_file_path):
                        os.remove(json_file_path)
                        logger.info(f"    ID {metadata_id}: 성공 - JSON 파일 삭제: {json_file_path}")
                    else:
                        logger.debug(f"    ID {metadata_id}: 정보 - 해당 경로에 JSON 파일 없음: {json_file_path}")
                else:
                    logger.warning(f"    ID {metadata_id}: 파일명 '{base_filename}'에서 JSON 파일명 생성을 위한 품번 추출 실패.")
            else:
                logger.warning(f"    ID {metadata_id}: 동영상 파일 경로를 찾을 수 없어 JSON 파일 정리 불가.")
        except Exception as e_json_del:
            logger.error(f"    ID {metadata_id}: 로컬 JSON 파일 처리 중 오류 발생: {e_json_del}", exc_info=True)
    else:
        logger.info(f"[DRY RUN] ID {metadata_id}: 로컬 JSON 파일 삭제/정리 건너뜀.")

    # Plex API를 통한 언매치
    plex_url, plex_token = CONFIG.get("PLEX_URL"), CONFIG.get("PLEX_TOKEN")
    if not (plex_url and plex_token):
        logger.error("Plex URL 또는 토큰이 설정되지 않았습니다 (plexapi unmatch)."); return False

    # logger.info(f"  ID {metadata_id} ('{item_title_for_log}'): plexapi로 매치 해제 시도...")
    try:
        def _unmatch_sync_call():
            global PLEX_SERVER_INSTANCE
            server_instance = PLEX_SERVER_INSTANCE
            try:
                if server_instance is None:
                    server_instance = PlexServer(plex_url, plex_token, timeout=CONFIG.get("REQUESTS_TIMEOUT_PUT"))
                    PLEX_SERVER_INSTANCE = server_instance

                plex_item_obj = server_instance.fetchItem(int(metadata_id)) # ratingKey로 아이템 가져오기
                plex_item_obj.unmatch() # 아이템 언매치
                logger.info(f"    ID {metadata_id}: plexapi를 통해 매치 해제 완료.")
                return True
            except PlexApiNotFound:
                logger.info(f"    ID {metadata_id}: plexapi - 아이템을 찾을 수 없음 (이미 unmatch 상태일 수 있음).")
                return True # 찾을 수 없는 것도 성공으로 간주 (목표 달성)
            except requests.exceptions.ConnectionError as conn_err_plex:
                logger.error(f"    ID {metadata_id}: plexapi - Plex 서버 연결 실패: {conn_err_plex}")
                PLEX_SERVER_INSTANCE = None # 연결 실패 시 인스턴스 리셋
                return False
            except Exception as sync_call_err:
                logger.error(f"    ID {metadata_id}: plexapi - 매치 해제 중 내부 오류 발생: {sync_call_err}", exc_info=True)
                return False

        return await await_sync(_unmatch_sync_call)
    except asyncio.CancelledError:
        logger.warning(f"  ID {metadata_id}: plexapi 매치 해제 작업 중 명시적으로 취소됨."); raise
    except Exception as async_call_err:
        logger.error(f"  ID {metadata_id}: plexapi 매치 해제 비동기 호출 설정 중 오류 발생: {async_call_err}", exc_info=True)
        return False


async def refresh_plex_item_metadata(metadata_id: int) -> bool:
    """Plex API를 통해 아이템의 메타데이터를 새로고침합니다."""
    if SHUTDOWN_REQUESTED: raise asyncio.CancelledError("Shutdown requested before refresh_plex_item_metadata")
    if not PLEXAPI_AVAILABLE:
        logger.error("plexapi 라이브러리가 없어 refresh_plex_item_metadata를 실행할 수 없습니다.")
        return False

    item_info_row = await get_metadata_by_id(metadata_id)
    item_title_for_log = item_info_row['title'] if item_info_row and 'title' in item_info_row.keys() and item_info_row['title'] else f"ID {metadata_id}"
    logger.info(f"  ID {metadata_id} ('{item_title_for_log}'): 메타데이터 새로고침 시도...")

    if CONFIG.get("DRY_RUN"):
        logger.info(f"[DRY RUN] ID {metadata_id}: 메타데이터 새로고침 건너뜀.")
        return True

    plex_url, plex_token = CONFIG.get("PLEX_URL"), CONFIG.get("PLEX_TOKEN")
    if not (plex_url and plex_token):
        logger.error("Plex URL 또는 토큰이 설정되지 않았습니다 (plexapi refresh)."); return False

    try:
        def _refresh_sync_call():
            global PLEX_SERVER_INSTANCE
            server_instance = PLEX_SERVER_INSTANCE
            try:
                if server_instance is None:
                    server_instance = PlexServer(plex_url, plex_token, timeout=CONFIG.get("REQUESTS_TIMEOUT_PUT"))
                    PLEX_SERVER_INSTANCE = server_instance

                plex_item_obj = server_instance.fetchItem(int(metadata_id))
                plex_item_obj.refresh()
                logger.info(f"    ID {metadata_id}: plexapi를 통해 메타데이터 새로고침 요청 완료.")
                return True
            except PlexApiNotFound:
                logger.warning(f"    ID {metadata_id}: plexapi - 새로고침 대상 아이템을 찾을 수 없음.")
                return False
            except requests.exceptions.ConnectionError as conn_err_plex:
                logger.error(f"    ID {metadata_id}: plexapi - Plex 서버 연결 실패 (새로고침): {conn_err_plex}")
                PLEX_SERVER_INSTANCE = None # 연결 실패 시 인스턴스 리셋
                return False
            except Exception as sync_call_err:
                logger.error(f"    ID {metadata_id}: plexapi - 새로고침 중 내부 오류 발생: {sync_call_err}", exc_info=True)
                return False

        return await await_sync(_refresh_sync_call)
    except asyncio.CancelledError:
        logger.warning(f"  ID {metadata_id}: plexapi 새로고침 작업 중 명시적으로 취소됨."); raise
    except Exception as async_call_err:
        logger.error(f"  ID {metadata_id}: plexapi 새로고침 비동기 호출 설정 중 오류 발생: {async_call_err}", exc_info=True)
        return False


async def rematch_plex_item(metadata_id: int, guid: str, name_for_match: str = None, year_for_match: int = None) -> bool:
    """Plex API를 통해 아이템을 주어진 GUID로 재매칭하고, 업데이트를 확인합니다."""
    if SHUTDOWN_REQUESTED: raise asyncio.CancelledError("Shutdown requested before rematch_plex_item")

    current_item_info = await get_metadata_by_id(metadata_id)
    if not current_item_info:
        logger.warning(f"재매칭 시도 전 아이템 (ID={metadata_id})이 DB에서 삭제됨."); return False

    current_title_log = current_item_info.get('title') or f"ID {metadata_id}"
    logger.info(f"재매칭 실행: ID={metadata_id}, Title='{current_title_log}' -> 새 GUID: {guid}")
    if name_for_match: logger.debug(f"  매칭 시 사용할 이름 (API 전달용): {name_for_match}")

    if CONFIG.get("DRY_RUN"):
        logger.info(f"[DRY RUN] ID {metadata_id}: 재매칭 건너뜀 (새 GUID: {guid})"); return True

    start_time_ts_for_rematch_check = int(datetime.now().timestamp())
    query_params_dict = {'guid': guid, 'X-Plex-Token': CONFIG["PLEX_TOKEN"]}
    if name_for_match: query_params_dict['name'] = name_for_match
    if year_for_match: query_params_dict['year'] = str(year_for_match)

    plex_url_base = CONFIG.get('PLEX_URL', '')
    url_to_put = f"{plex_url_base}/library/metadata/{metadata_id}/match?{urllib.parse.urlencode(query_params_dict)}"

    try:
        response_put = await await_sync(requests.put, url_to_put, headers={'Accept': 'application/json'}, timeout=CONFIG.get("REQUESTS_TIMEOUT_PUT", 60))
        response_put.raise_for_status()
        logger.info(f"  ID {metadata_id}: 재매칭 요청 성공 (Status: {response_put.status_code})")
    except asyncio.CancelledError:
        logger.warning(f"  ID {metadata_id}: 재매칭 API 호출 중 명시적으로 취소됨."); raise
    except requests.exceptions.RequestException as req_err:
        logger.error(f"  ID {metadata_id}: 재매칭 API 실패. Error: {req_err}"); return False

    # 1. 재매칭 후 업데이트 확인
    rematch_update_confirmed = False
    for i_check in range(CONFIG.get("CHECK_COUNT", 10)):
        if SHUTDOWN_REQUESTED: break
        logger.debug(f"  ID {metadata_id}: 재매칭 후 메타 업데이트 확인 중... ({i_check+1}/{CONFIG.get('CHECK_COUNT',10)})")
        if await check_plex_update(metadata_id, start_time_ts_for_rematch_check):
            rematch_update_confirmed = True
            break
        await asyncio.sleep(CONFIG.get("CHECK_INTERVAL", 4))

    if rematch_update_confirmed:
        return True # 재매칭 후 바로 업데이트 확인 성공 시, 즉시 종료

    if SHUTDOWN_REQUESTED:
        logger.warning(f"  ID {metadata_id}: 재매칭 확인 중 종료 요청으로 중단됨."); return False

    # --- 2. 새로고침 및 확인 재시도 루프 ---
    max_refresh_retries = 3
    for attempt in range(max_refresh_retries):
        if SHUTDOWN_REQUESTED: break

        log_prefix = f"  ID {metadata_id}:"
        if attempt > 0:
            log_prefix += f" [새로고침 재시도 {attempt}/{max_refresh_retries-1}]"

        logger.warning(f"{log_prefix} 재매칭 후 업데이트 미반영. 아이템 새로고침 시도...")
        if not await refresh_plex_item_metadata(metadata_id):
            logger.error(f"{log_prefix} 아이템 새로고침 요청 실패.")
            # 새로고침 요청 자체가 실패하면 잠시 후 재시도
            if attempt < max_refresh_retries - 1:
                await asyncio.sleep(5)
            continue # 다음 재시도로

        # 새로고침 요청 후 다시 업데이트 확인
        logger.info(f"{log_prefix} 새로고침 요청 후 메타데이터 업데이트 재확인 시작...")
        start_time_ts_for_refresh_check = int(datetime.now().timestamp())

        refresh_check_count = max(3, CONFIG.get("CHECK_COUNT", 10) // 2)
        refresh_check_interval = max(2, CONFIG.get("CHECK_INTERVAL", 4))

        is_update_confirmed_after_refresh = False
        for i_refresh_check in range(refresh_check_count):
            if SHUTDOWN_REQUESTED: break
            logger.debug(f"{log_prefix} 새로고침 후 확인 중... ({i_refresh_check+1}/{refresh_check_count})")
            if await check_plex_update(metadata_id, start_time_ts_for_refresh_check): 
                is_update_confirmed_after_refresh = True
                break
            await asyncio.sleep(refresh_check_interval)

        if is_update_confirmed_after_refresh:
            return True # 새로고침 후 업데이트 확인 성공 시, 즉시 종료

        # 이번 재시도에서도 확인 실패 시, 루프 마지막이 아니면 잠시 대기
        if attempt < max_refresh_retries - 1 and not SHUTDOWN_REQUESTED:
            logger.warning(f"{log_prefix} 새로고침 후에도 업데이트 미확인. 5초 후 재시도합니다.")
            await asyncio.sleep(5)

    # 모든 재시도가 실패한 경우
    logger.warning(f"  ID {metadata_id}: 재매칭 및 모든 새로고침 시도 후에도 최종 업데이트 확인 실패."); 
    return False


def get_plex_library_paths(section_id: int) -> list[str]:
    """DB에서 주어진 섹션 ID에 등록된 모든 경로를 가져옵니다."""
    query = "SELECT root_path FROM section_locations WHERE library_section_id = ?"
    # fetch_all이 dict 리스트를 반환하므로 수정
    rows = fetch_all(query, (section_id,))
    return [row['root_path'] for row in rows] if rows else []

def map_scan_path(path: str) -> str:
    """설정에 따라 스캔 경로를 Plex가 인식하는 경로로 변환합니다."""
    if not CONFIG.get("SCAN_PATH_MAPPING_ENABLED"):
        return path

    try:
        # 설정 파일의 '소스:타겟' 쌍들을 세미콜론으로 구분하여 여러 개 처리 가능하도록 수정
        mapping_rules = CONFIG.get("SCAN_PATH_MAP", "").split(';')
        for rule in mapping_rules:
            if ':' not in rule: continue
            source_prefix, dest_prefix = rule.split(':', 1)
            source_prefix = source_prefix.strip()
            dest_prefix = dest_prefix.strip()
            if path.startswith(source_prefix):
                mapped_path = path.replace(source_prefix, dest_prefix, 1)
                logger.debug(f"경로 변환 (규칙: {rule}): '{path}' -> '{mapped_path}'")
                return mapped_path
    except Exception as e:
        logger.error(f"SCAN_PATH_MAP 설정 처리 중 오류: {e}")

    logger.debug(f"일치하는 경로 변환 규칙이 없어 원본 경로 반환: '{path}'")
    return path


async def get_plex_library_activities(library_section: Any) -> List[Any]: # library_section 인자 추가
    """Plex 서버의 현재 라이브러리 관련 활동 목록을 가져옵니다."""
    if not PLEXAPI_AVAILABLE:
        logger.error("plexapi 라이브러리가 없어 Plex 활동 상태를 확인할 수 없습니다.")
        return []

    # library_section 객체가 유효하지 않으면 빈 리스트 반환
    if not library_section:
        logger.warning("get_plex_library_activities 호출 시 library_section 객체가 없습니다.")
        return []

    try:
        # --- _get_activities_sync 내부 로직 수정 ---
        def _get_activities_sync() -> List[Any]:
            all_activities = []

            # 1. 전역 활동 확인 (기존 로직)
            # PLEX_SERVER_INSTANCE는 이미 외부에서 생성되었으므로 그대로 사용
            global PLEX_SERVER_INSTANCE
            if PLEX_SERVER_INSTANCE:
                server_activities = PLEX_SERVER_INSTANCE.activities
                for activity in server_activities:
                    activity_type_str = getattr(activity, 'type', '')
                    if 'library' in activity_type_str or 'media' in activity_type_str:
                        all_activities.append(activity)

            # 2. 특정 라이브러리 섹션의 활동 확인 (새로 추가된 로직)
            # library.reload()를 통해 최신 상태를 가져옵니다.
            library_section.reload()
            library_activity = getattr(library_section, 'activity', None)
            if library_activity:
                # 중복을 피하기 위해 이미 리스트에 없는 경우에만 추가
                # 보통 activity 객체는 유일하므로 id 등으로 비교할 수 있으나, 간단히 객체 자체로 비교
                if library_activity not in all_activities:
                    all_activities.append(library_activity)

            return all_activities

        return await await_sync(_get_activities_sync)

    except Exception as e:
        logger.error(f"Plex 활동 상태 확인 중 오류: {e}", exc_info=False)
        return []


async def run_scan_mode(args):
    """라이브러리 스캔 기능을 실행하는 메인 함수입니다."""
    global CONFIG, SHUTDOWN_REQUESTED, PLEX_SERVER_INSTANCE

    section_id = CONFIG.get("ID")
    if not section_id:
        logger.error("스캔 모드에는 --section-id가 필수입니다.")
        return

    try:
        if PLEX_SERVER_INSTANCE is None:
            PLEX_SERVER_INSTANCE = PlexServer(CONFIG["PLEX_URL"], CONFIG["PLEX_TOKEN"])

        library = await await_sync(PLEX_SERVER_INSTANCE.library.sectionByID, section_id)
    except PlexApiNotFound:
        logger.error(f"Plex에서 섹션 ID {section_id}를 찾을 수 없습니다.")
        return
    except Exception as e:
        logger.error(f"Plex 서버 연결 또는 섹션 조회 중 오류 발생: {e}")
        return

    # --scan-path 옵션 처리 (특정 경로만 스캔)
    scan_path_specific = CONFIG.get("SCAN_PATH")
    if scan_path_specific:
        mapped_path = map_scan_path(scan_path_specific)

        # --- '--scan-no-wait' 옵션에 따른 분기 처리 ---
        if CONFIG.get("SCAN_NO_WAIT"):
            # 즉시 스캔 요청 후 종료 (Fire-and-Forget)
            logger.info(f"즉시 스캔 요청 (--scan-no-wait): '{mapped_path}'")
            try:
                await await_sync(library.update, path=mapped_path)
                logger.info(f"경로 '{mapped_path}'에 대한 스캔 요청을 성공적으로 보냈습니다. 스크립트를 종료합니다.")
            except Exception as e:
                logger.error(f"즉시 스캔 요청 중 오류 발생: {e}")
            return # 즉시 종료
        else:
            # 유휴 상태 확인 후, 스캔 완료까지 대기
            logger.info(f"지정된 단일 경로 스캔 시작 (완료까지 대기): '{mapped_path}' (원본: '{scan_path_specific}')")

            # 1. 스캔 전 유휴 상태 확인
            logger.info("스캔 시작 전 Plex 유휴 상태를 확인합니다...")
            while True:
                if SHUTDOWN_REQUESTED: break
                active_tasks = await get_plex_library_activities(library)
                if not active_tasks:
                    logger.debug("Plex가 유휴 상태입니다. 스캔을 진행합니다."); break
                for task in active_tasks:
                    progress_val = getattr(task, 'progress', 0)
                    details = [d for d in (getattr(task, 'subtitle', None), getattr(task, 'context', None)) if d]
                    details_str = f" - {', '.join(details)}" if details else ""
                    logger.info(f"Plex 활동 대기 중: [{task.type}] {task.title}{details_str} ({progress_val:.0f}%)")
                logger.info("...Plex가 이전 작업을 완료할 때까지 15초 대기합니다...")
                try: await asyncio.sleep(15)
                except asyncio.CancelledError: SHUTDOWN_REQUESTED = True; break

            if SHUTDOWN_REQUESTED: return

            # 2. 스캔 요청
            try:
                await await_sync(library.update, path=mapped_path)
                logger.info(f"경로 스캔 요청 완료: '{mapped_path}'. Plex가 백그라운드에서 처리합니다.")
                logger.debug("스캔 요청 후 5초 대기 (Plex 활동 시작 대기)...")
                await asyncio.sleep(5)
            except Exception as e:
                logger.error(f"경로 '{mapped_path}' 스캔 요청 중 오류 발생: {e}")
                return

            # 3. 스캔 완료 대기
            logger.info(f"'{mapped_path}' 스캔이 완료될 때까지 대기합니다...")
            while True:
                if SHUTDOWN_REQUESTED: break
                active_tasks = await get_plex_library_activities(library)
                if not active_tasks: break
                for task in active_tasks:
                    progress_val = getattr(task, 'progress', 0)
                    details = [d for d in (getattr(task, 'subtitle', None), getattr(task, 'context', None)) if d]
                    details_str = f" - {', '.join(details)}" if details else ""
                    logger.info(f"...스캔 진행 중: [{task.type}] {task.title}{details_str} ({progress_val:.0f}%)")
                try: await asyncio.sleep(15)
                except asyncio.CancelledError: SHUTDOWN_REQUESTED = True; break

            if not SHUTDOWN_REQUESTED:
                logger.info(f"경로 '{mapped_path}' 스캔이 완료되었습니다.")
        return

    # --scan-full 옵션 처리 (분할 스캔)
    if CONFIG.get("SCAN_FULL"):
        logger.info(f"섹션 '{library.title}' (ID: {section_id}) 전체 분할 스캔을 시작합니다.")

        root_paths = await await_sync(get_plex_library_paths, section_id)
        if not root_paths:
            logger.error(f"DB에서 섹션 ID {section_id}에 등록된 경로를 찾을 수 없습니다.")
            return

        scan_depth = CONFIG.get("SCAN_DEPTH", 2)
        dirs_to_scan = set()

        logger.info(f"등록된 경로들을 depth {scan_depth} 기준으로 분석합니다...")
        for root_path in root_paths:
            if not os.path.isdir(root_path):
                logger.warning(f"등록된 경로 '{root_path}'를 찾을 수 없거나 디렉터리가 아닙니다. 건너뜀.")
                continue

            if scan_depth == 0:
                dirs_to_scan.add(root_path)
                continue

            for dirpath, dirnames, _ in os.walk(root_path, topdown=True):
                relative_path = os.path.relpath(dirpath, root_path)
                current_depth = 0 if relative_path == '.' else len(relative_path.split(os.sep))

                if current_depth >= scan_depth:
                    del dirnames[:]
                    continue

                if current_depth == scan_depth - 1:
                    for dirname in dirnames:
                        dirs_to_scan.add(os.path.join(dirpath, dirname))
                    del dirnames[:]

        if not dirs_to_scan:
            logger.warning(f"Depth {scan_depth}에서 스캔할 하위 디렉터리를 찾지 못했습니다. 루트 경로를 직접 스캔합니다.")
            dirs_to_scan.update(root_paths)

        sorted_scan_list = sorted(list(dirs_to_scan), key=natural_sort_key)
        total_dirs = len(sorted_scan_list)
        logger.info(f"총 {total_dirs}개의 디렉터리를 순차적으로 스캔합니다.")

        for i, path_to_scan in enumerate(sorted_scan_list):
            if SHUTDOWN_REQUESTED:
                logger.warning("사용자 요청으로 스캔이 중단되었습니다.")
                break

            # 1. 다음 스캔 '전'에 Plex가 유휴 상태인지 확인
            logger.debug(f"[{i+1}/{total_dirs}] 다음 스캔 전 Plex 상태 확인...")
            while True:
                if SHUTDOWN_REQUESTED: break

                active_tasks = await get_plex_library_activities(library)
                if not active_tasks:
                    logger.debug("Plex가 유휴 상태입니다. 다음 스캔을 진행합니다.")
                    break

                # 활동이 있을 경우, 상세 내용을 로그로 남깁니다.
                for task in active_tasks:
                    progress_val = getattr(task, 'progress', 0)
                    # 상세 정보를 담을 리스트
                    details = []
                    # subtitle 속성이 존재하고 비어있지 않으면 추가
                    subtitle = getattr(task, 'subtitle', None)
                    if subtitle:
                        details.append(subtitle)

                    # context 속성이 존재하고 비어있지 않으면 추가
                    context = getattr(task, 'context', None)
                    if context:
                        details.append(context)

                    # details 리스트에 내용이 있으면 " - " 와 함께 문자열로 만듦
                    details_str = f" - {', '.join(details)}" if details else ""

                    logger.info(f"...스캔 진행 중: [{task.type}] {task.title}{details_str} ({progress_val:.0f}%)")

                logger.info("...Plex가 이전 작업을 완료할 때까지 15초 대기합니다...")
                try:
                    await asyncio.sleep(15)
                except asyncio.CancelledError:
                    SHUTDOWN_REQUESTED = True
                    break

            if SHUTDOWN_REQUESTED: break

            # 2. 스캔 요청 보내기
            mapped_path_to_scan = map_scan_path(path_to_scan)
            logger.info(f"[{i+1}/{total_dirs}] 경로 스캔 요청: '{mapped_path_to_scan}'")
            try:
                await await_sync(library.update, path=mapped_path_to_scan)
                logger.debug("스캔 요청 후 5초 대기 (Plex 활동 시작 대기)...")
                await asyncio.sleep(5) 
            except Exception as e:
                logger.error(f"'{mapped_path_to_scan}' 스캔 요청 중 오류 발생: {e}")
                continue

            # 3. 방금 보낸 스캔 요청이 '끝날 때까지' 대기
            logger.info(f"'{mapped_path_to_scan}' 스캔이 완료될 때까지 대기합니다...")
            retries = 0

            while True:
                if SHUTDOWN_REQUESTED: break

                active_tasks = await get_plex_library_activities(library)
                if not active_tasks:
                    break

                for task in active_tasks:
                    progress_val = getattr(task, 'progress', 0)
                    subtitle_val = getattr(task, 'subtitle', '')
                    progress_str = f" ({progress_val:.0f}%)"
                    subtitle_str = f" - {subtitle_val}" if subtitle_val else ""
                    logger.info(f"...스캔 진행 중: [{task.type}] {task.title}{subtitle_str}{progress_str}")

                try:
                    await asyncio.sleep(15)
                except asyncio.CancelledError:
                    SHUTDOWN_REQUESTED = True
                    break

                retries += 1
                if retries > 120: # 30분 이상 걸리면 경고
                    logger.warning(f"경로 '{mapped_path_to_scan}' 스캔이 30분 이상 소요되고 있습니다. 계속 대기합니다...")

            if SHUTDOWN_REQUESTED: break

            logger.info(f"[{i+1}/{total_dirs}] 경로 '{mapped_path_to_scan}' 스캔 요청 완료.")

        if not SHUTDOWN_REQUESTED:
            logger.info("모든 분할 스캔 요청 및 처리가 완료되었습니다.")


async def process_single_item_for_auto_rematch(item_row: sqlite3.Row, worker_name_for_log: str) -> bool:
    item_id = item_row['id']; start_time = time.monotonic(); api_changed_flag = False
    current_task_status_val = "ATTEMPTED_NO_CHANGE"

    item_title_log = item_row['title'] if 'title' in item_row.keys() and item_row['title'] else f"ID {item_id}"
    logger.info(f"[{worker_name_for_log}] 처리 시작: ID={item_id}, Title='{item_title_log}'")

    if SHUTDOWN_REQUESTED:
        logger.info(f"[{worker_name_for_log}] ID {item_id}: 종료 요청으로 작업 시작 안 함.")
        current_task_status_val = "SKIPPED_SHUTDOWN"
        if not CONFIG.get("DRY_RUN"): await await_sync(record_completed_task, item_id, status=current_task_status_val)
        return False

    original_filename_pid, original_filename_pid_parts = None, None
    media_file_path = await await_sync(get_media_file_path, item_id)
    if media_file_path:
        extracted_pid_from_fn = extract_product_id_from_filename(os.path.basename(media_file_path))
        if extracted_pid_from_fn:
            original_filename_pid = extracted_pid_from_fn # 패딩된 형태 (예: "alpha-00001")
            pid_alpha_num_parts = extracted_pid_from_fn.upper().split('-',1)
            if len(pid_alpha_num_parts) == 2 and pid_alpha_num_parts[0] and pid_alpha_num_parts[1]:
                original_filename_pid_parts = (pid_alpha_num_parts[0], pid_alpha_num_parts[1]) # (ALPHA, 00001)
            logger.debug(f"  ID {item_id}: 파일명 원본 품번(패딩됨): '{original_filename_pid}' (파싱된 부분: {original_filename_pid_parts})")
    else: logger.warning(f"  ID {item_id}: 미디어 파일 경로를 찾을 수 없어 파일명 기반 품번 추출 불가.")

    normalized_original_file_pid_for_comp = normalize_pid_for_comparison(original_filename_pid) # 비교용 (예: "alpha00001")
    if normalized_original_file_pid_for_comp:
        logger.debug(f"  ID {item_id}: 정규화된 원본 품번 (비교용): '{normalized_original_file_pid_for_comp}'")

    selected_candidate_obj_for_db = None # DB 기록용 최종 선택 후보 객체
    final_selected_candidate_obj = None # 실제 재매칭에 사용될 최종 선택 후보 객체

    try:
        if CONFIG.get("FORCE_REMATCH"):
            logger.info(f"  ID {item_id}: --force-rematch 옵션으로 언매치 및 JSON 삭제 실행.")
            if not await unmatch_plex_item(item_id):
                current_task_status_val = "FAILED_UNMATCH_FORCE_REMATCH"
                raise Exception(f"ID {item_id}: --force-rematch 중 언매치 실패.")
            if not CONFIG.get("DRY_RUN"): api_changed_flag = True

        if SHUTDOWN_REQUESTED: raise asyncio.CancelledError("Shutdown request before get_plex_matches")

        matches_data_result = await get_plex_matches(item_row)
        valid_search_results = [
            candidate for candidate in matches_data_result.get('MediaContainer',{}).get('SearchResult',[]) 
            if not any(candidate.get('guid','').startswith(excluded_prefix) for excluded_prefix in MATCH_CANDIDATE_EXCLUDED_GUID_PREFIXES)
        ]

        if not valid_search_results:
            logger.info(f"  ID {item_id}: (필터링 후) 매칭 후보 없음.")
            current_task_status_val = "ATTEMPTED_NO_CANDIDATE"
        else:
            current_best_eval_score = -1
            logger.debug(f"  ID {item_id}: 후보 {len(valid_search_results)}개 선택 (기준 품번: {normalized_original_file_pid_for_comp or '없음'})")

            for candidate_item_s in valid_search_results:
                if SHUTDOWN_REQUESTED: raise asyncio.CancelledError("Shutdown request during candidate selection loop")

                plex_score = candidate_item_s.get('score',0)
                candidate_name = candidate_item_s.get('name','N/A')
                logger.debug(f"    후보 검토: Name='{candidate_name[:40]}...', PlexScore={plex_score}, GUID='{candidate_item_s.get('guid', '')}'")

                # 후보 제목에서 품번 추출 (패딩 적용된 형태 alpha-xxxxx)
                candidate_title_pid_padded = extract_product_id_from_title(candidate_name, original_filename_pid_parts)
                # 비교용 정규화 (alphaXXXXX)
                normalized_candidate_title_pid_for_comp = normalize_pid_for_comparison(candidate_title_pid_padded)

                # 현재 후보의 종합 평가 점수 (Plex 점수 + 품번 일치 가점)
                evaluation_score_for_this_candidate = plex_score

                if normalized_original_file_pid_for_comp and normalized_candidate_title_pid_for_comp:
                    logger.debug(f"      후보 품번(패딩): Raw='{candidate_title_pid_padded}', Norm(비교용)='{normalized_candidate_title_pid_for_comp}'")
                    if normalized_original_file_pid_for_comp == normalized_candidate_title_pid_for_comp:
                        logger.debug(f"      >> 품번 정확히 일치! (가점 +1000)")
                        evaluation_score_for_this_candidate += 1000 # 품번 일치 시 큰 가점
                elif normalized_original_file_pid_for_comp: # 원본 품번은 있으나 후보 품번 추출 실패
                    logger.debug(f"      후보 '{candidate_name[:30]}...' 제목에서 품번 추출 실패 (또는 원본 품번 기준 없음).")

                # 최적 후보 선택 로직 (Plex 최소 점수 만족 + 종합 평가 점수 비교)
                if plex_score >= CONFIG.get("SCORE_MIN",95):
                    if final_selected_candidate_obj is None or evaluation_score_for_this_candidate > current_best_eval_score:
                        final_selected_candidate_obj, current_best_eval_score = candidate_item_s, evaluation_score_for_this_candidate
                        logger.debug(f"      >> 현재 최적 후보로 선택 (종합점수: {current_best_eval_score})")
                    elif evaluation_score_for_this_candidate == current_best_eval_score and plex_score > final_selected_candidate_obj.get('score',0):
                        final_selected_candidate_obj = candidate_item_s # 종합점수 같으면 Plex 점수 높은 것
                        logger.debug(f"      >> 현재 최적 후보로 선택 (종합점수 동일, Plex 점수 우위)")

            if not final_selected_candidate_obj: # 모든 후보 검토 후에도 최적 후보 없음
                logger.info(f"  ID {item_id}: 조건에 맞는 최적 후보 없음 (모든 후보 점수/품번 불일치).")
                # 파일명 품번이 있었는지 여부에 따라 상태 구체화
                if normalized_original_file_pid_for_comp:
                    current_task_status_val = f"ATTEMPTED_NO_OPTIMAL_CANDIDATE_FOR_{normalized_original_file_pid_for_comp}"
                else:
                    current_task_status_val = "ATTEMPTED_NO_OPTIMAL_CANDIDATE_NO_SRC_PID"
            else: # 최적 후보 선택됨
                selected_candidate_obj_for_db = final_selected_candidate_obj # DB 기록용으로 확정

                sel_cand_name = final_selected_candidate_obj.get('name')
                sel_cand_guid = final_selected_candidate_obj.get('guid')
                sel_cand_year = final_selected_candidate_obj.get('year')
                sel_cand_score = final_selected_candidate_obj.get('score',0)

                # 최종 선택된 후보의 품번 정보 로깅 (패딩된 형태 기준)
                final_sel_cand_pid_padded = extract_product_id_from_title(sel_cand_name, original_filename_pid_parts)
                final_sel_cand_pid_norm_for_comp = normalize_pid_for_comparison(final_sel_cand_pid_padded)

                pid_match_log_info = "N/A"
                if normalized_original_file_pid_for_comp and final_sel_cand_pid_norm_for_comp:
                    pid_match_log_info = "일치" if normalized_original_file_pid_for_comp == final_sel_cand_pid_norm_for_comp else \
                                        f"불일치 (원본:{normalized_original_file_pid_for_comp} vs 후보:{final_sel_cand_pid_norm_for_comp})"

                logger.info(f"  ID {item_id}: 최종 선택 후보: '{sel_cand_name[:40]}...', PlexScore={sel_cand_score}, 품번비교: {pid_match_log_info}, GUID: {sel_cand_guid}")

                # 재매칭 실행 여부 결정
                needs_to_rematch = False
                is_final_pid_match = normalized_original_file_pid_for_comp and final_sel_cand_pid_norm_for_comp and \
                                     normalized_original_file_pid_for_comp == final_sel_cand_pid_norm_for_comp

                # 1. --force-rematch 옵션이 켜져있다면 (이미 언매치됨), 선택된 후보로 무조건 재매칭
                if CONFIG.get("FORCE_REMATCH"):
                    needs_to_rematch = True
                    logger.debug("    조건: --force-rematch 활성 -> 재매칭 (선택된 후보 존재 시)")

                # 2. 기본 매칭: 품번이 일치하고 Plex 점수가 충분하면, 현재 GUID와 같더라도 재매칭
                #    (get_matches를 이미 호출했으므로, 상태 보정 및 메타데이터 재적용 목적)
                elif is_final_pid_match and sel_cand_score >= CONFIG.get("SCORE_MIN", 95) :
                    needs_to_rematch = True
                    current_db_guid = item_row['guid'] if 'guid' in item_row.keys() else None
                    log_detail_guid_compare = "(현재 GUID와 동일하지만, get_matches 호출했으므로 재매칭)" if current_db_guid == sel_cand_guid else ""
                    logger.debug(f"    조건: 품번 일치 & 점수 만족 {log_detail_guid_compare} -> 재매칭")

                # 3. (선택적 확장) 품번 불일치 시에도 특정 조건 만족 시 재매칭 (예: 점수 매우 높음)
                # elif not is_final_pid_match and sel_cand_score == 100 and \
                #      (item_row.get('guid') != sel_cand_guid): # 현재 GUID와 다를 때
                #     needs_to_rematch = True
                #     logger.info("    조건: 품번 불일치 & Plex점수 100 & GUID 다름 -> 재매칭 시도")

                else: # 위의 어떤 재매칭 조건도 만족하지 못한 경우
                    current_task_status_val = f"ATTEMPTED_SKIPPED_REMATCH_NO_COND ({sel_cand_guid if sel_cand_guid else 'N/A'})"
                    logger.info(f"    조건: 위 재매칭 조건 불충족 -> 재매칭 불필요/건너뜀 (현재 GUID: {item_row.get('guid')}, 후보 GUID: {sel_cand_guid})")

                # 실제 재매칭 실행
                if needs_to_rematch:
                    is_rematch_successful_call = False
                    if CONFIG.get("DRY_RUN"):
                        logger.info(f"  [DRY RUN] ID {item_id}: 재매칭 시뮬레이션 (새 GUID: {sel_cand_guid})")
                        current_task_status_val = f"COMPLETED_REMATCHED_DRYRUN ({sel_cand_guid})"
                        is_rematch_successful_call = True
                    else:
                        if SHUTDOWN_REQUESTED: raise asyncio.CancelledError("Shutdown request before actual rematch_plex_item call")
                        if await rematch_plex_item(item_id, sel_cand_guid, sel_cand_name, sel_cand_year):
                            current_task_status_val = f"COMPLETED_REMATCHED ({sel_cand_guid})"
                            is_rematch_successful_call = True
                        else:
                            current_task_status_val = f"FAILED_REMATCH ({sel_cand_guid})"

                    if not CONFIG.get("DRY_RUN") and is_rematch_successful_call:
                        api_changed_flag = True

        # DB 기록: DryRun이 아닐 때만 실제 기록
        if not CONFIG.get("DRY_RUN"):
            guid_for_db_record = None
            if "COMPLETED_REMATCHED" in current_task_status_val and selected_candidate_obj_for_db: # DRYRUN 포함
                guid_for_db_record = selected_candidate_obj_for_db.get('guid')
            elif "ATTEMPTED_SKIPPED_REMATCH_NO_COND" in current_task_status_val and final_selected_candidate_obj:
                guid_for_db_record = final_selected_candidate_obj.get('guid') # 매칭은 안 했지만 유력했던 후보 GUID
            elif "FAILED_REMATCH" in current_task_status_val and final_selected_candidate_obj:
                guid_for_db_record = final_selected_candidate_obj.get('guid') # 실패했지만 시도했던 GUID
            # 그 외 ATTEMPTED_NO_CANDIDATE, ATTEMPTED_NO_OPTIMAL_CANDIDATE 등은 guid_for_db_record = None 유지

            await await_sync(record_completed_task, item_id, status=current_task_status_val, matched_guid=guid_for_db_record)

        log_prefix_dry_run = "[DRY RUN] " if CONFIG.get("DRY_RUN") else ""
        logger.info(f"  ID {item_id}: {log_prefix_dry_run}상태 기록: {current_task_status_val}")

    except asyncio.CancelledError:
        logger.warning(f"[{worker_name_for_log}] ID {item_id} 작업 명시적 취소됨 (종료 요청).")
        # 취소된 작업은 별도 상태 기록 안 함 (SHUTDOWN_REQUESTED 플래그로 루프 탈출)
        raise # worker_task_loop에서 잡아서 처리하도록 다시 raise
    except Exception as e_item_processing:
        logger.error(f"!!! [{worker_name_for_log}] ID {item_id} 처리 중 예외 발생: {e_item_processing}", exc_info=True)
        current_task_status_val = "FAILED_EXCEPTION"
        if not CONFIG.get("DRY_RUN"):
            # 예외 발생 시에도 상태 기록 시도
            await await_sync(record_completed_task, item_id, status=current_task_status_val)
    finally:
        total_item_processing_time = time.monotonic() - start_time
        logger.info(f"[{worker_name_for_log}] ID {item_id} 처리 완료. 소요 시간: {total_item_processing_time:.2f}초.")

    return api_changed_flag # API 변경 여부 반환


async def run_manual_interactive_rematch_for_item(
    item_data: dict,
    calling_mode_tag: str = "MANUAL"
) -> Tuple[bool, Optional[str]]:

    global SHUTDOWN_REQUESTED, CONFIG

    item_id = item_data['id']
    item_title = item_data.get('title', f"ID {item_id}")
    original_row_for_matches = item_data.get('original_row')

    file_path = item_data.get('file_path') or item_data.get('original_row', {}).get('file_path')
    display_filename = os.path.basename(file_path) if file_path else "파일 경로를 찾을 수 없음"
    print(f"\n──────────────────────────────────────────────────────────────────")
    print(f"  현재 작업 대상 파일: {display_filename}")
    print(f"──────────────────────────────────────────────────────────────────")

    original_file_pid_raw_for_hint = item_data.get('file_pid_raw')
    original_file_pid_parts_for_hint = None
    if original_file_pid_raw_for_hint:
        parts = original_file_pid_raw_for_hint.upper().split('-', 1)
        if len(parts) == 2 and parts[0] and parts[1]:
            original_file_pid_parts_for_hint = (parts[0], parts[1])

    current_norm_file_pid = item_data.get('norm_file_pid')
    current_file_pid_raw = item_data.get('file_pid_raw')
    current_normalized_file_pid_for_comparison = item_data.get('norm_file_pid')

    logger.debug(f"\n>>> [{calling_mode_tag}] ID {item_id} ('{item_title}') 수동 매칭 시작...")
    
    if SHUTDOWN_REQUESTED: raise asyncio.CancelledError("Shutdown requested")

    matches_data = await get_plex_matches(original_row_for_matches)
    valid_candidates = [
        c for c in matches_data.get('MediaContainer', {}).get('SearchResult', [])
        if not any(c.get('guid', '').startswith(p) for p in MATCH_CANDIDATE_EXCLUDED_GUID_PREFIXES)
    ]

    final_selected_candidate = None

    # --- 사용자 입력 및 후보 선택/검색 루프 시작 ---
    while True:
        if SHUTDOWN_REQUESTED:
            return False, "GLOBAL_QUIT_REQUESTED_BY_USER"

        if not valid_candidates:
            logger.info(f"  ID {item_id}: [{calling_mode_tag}] 현재 표시할 매칭 후보가 없습니다.")
        else:
            print(f"\n  ID {item_id}: 다음 매칭 후보 중에서 선택하세요 ({len(valid_candidates)}개):")
            for i_cand, cand_obj in enumerate(valid_candidates):
                candidate_name_from_api = cand_obj.get('name', '이름없음')
                candidate_title_pid_padded_raw = extract_product_id_from_title(candidate_name_from_api, original_file_pid_parts_for_hint)
                normalized_candidate_pid_for_comparison = normalize_pid_for_comparison(candidate_title_pid_padded_raw)

                candidate_pid_display_search_form = "추출실패"
                if candidate_title_pid_padded_raw:
                    parts_cand_display = candidate_title_pid_padded_raw.split('-', 1)
                    if len(parts_cand_display) == 2:
                        cand_num_for_display = format_numeric_for_search(parts_cand_display[1], 3)
                        candidate_pid_display_search_form = f"{parts_cand_display[0].lower()}-{cand_num_for_display.lower()}"
                    else:
                        candidate_pid_display_search_form = candidate_title_pid_padded_raw.lower()

                pid_match_display_string = ""
                if current_normalized_file_pid_for_comparison and normalized_candidate_pid_for_comparison:
                    is_actually_match = (current_normalized_file_pid_for_comparison == normalized_candidate_pid_for_comparison)
                    if is_actually_match:
                        pid_match_display_string = " [품번일치]"
                    else:
                        original_file_pid_display_search_form = "원본N/A"
                        if original_file_pid_raw_for_hint:
                            orig_label_disp, orig_num_disp = original_file_pid_raw_for_hint.split('-', 1)
                            orig_num_disp_formatted = format_numeric_for_search(orig_num_disp, 3)
                            original_file_pid_display_search_form = f"{orig_label_disp.lower()}-{orig_num_disp_formatted.lower()}"
                        pid_match_display_string = f" [품번다름! 원본:{original_file_pid_display_search_form}]"
                elif current_norm_file_pid:
                    original_file_pid_display_search_form_f = "원본N/A"
                    if current_file_pid_raw:
                        parts_orig_f_disp = current_file_pid_raw.split('-',1)
                        if len(parts_orig_f_disp) == 2: original_file_pid_display_search_form_f = f"{parts_orig_f_disp[0].lower()}-{format_numeric_for_search(parts_orig_f_disp[1], 3).lower()}"
                    pid_match_display_string = f" [후보품번추출실패! 원본:{original_file_pid_display_search_form_f}]"

                print(f"    {i_cand+1}. {candidate_name_from_api[:50]}... "
                    f"(품번: {candidate_pid_display_search_form}{pid_match_display_string}) "
                    f"Score: {cand_obj.get('score',0)}, GUID: {cand_obj.get('guid')}")

        try:
            prompt_msg = f"  ID {item_id} - 매칭할 번호 (없으면 N, 수동 검색 S, 종료 Q): "
            user_choice_input_str = (await asyncio.get_running_loop().run_in_executor(None, input, prompt_msg)).strip().lower()
        except (EOFError, KeyboardInterrupt):
            SHUTDOWN_REQUESTED = True
            return False, "GLOBAL_QUIT_REQUESTED_BY_USER"

        if user_choice_input_str == 'q':
            SHUTDOWN_REQUESTED = True
            return False, "GLOBAL_QUIT_REQUESTED_BY_USER"

        elif user_choice_input_str in ['n', '']:
            logger.info(f"  ID {item_id}: 사용자가 후보를 선택하지 않아 건너뜁니다.")
            if not CONFIG.get("DRY_RUN"):
                await await_sync(record_completed_task, item_id, status=f"SKIPPED_USER_NO_CHOICE_{calling_mode_tag.upper()}")
            return False, "USER_SKIPPED_CHOICE"

        elif user_choice_input_str == 's':
            try:
                custom_search_term = (await asyncio.get_running_loop().run_in_executor(None, input, "  사용할 검색어를 입력하세요: ")).strip()
            except (EOFError, KeyboardInterrupt):
                SHUTDOWN_REQUESTED = True; continue

            if not custom_search_term:
                logger.warning("검색어가 입력되지 않았습니다. 다시 시도하세요.")
                continue

            logger.info(f"  사용자 검색어 '{custom_search_term}'로 다시 검색합니다...")
            section = await get_section_by_id(original_row_for_matches['library_section_id'])
            agent_to_use = section.get('agent') if section else None
            year_to_use = original_row_for_matches.get('year')

            original_manual_config = CONFIG.get("MANUAL_SEARCH", False)
            CONFIG["MANUAL_SEARCH"] = True
            new_matches_data = await _get_matches_api(item_id, custom_search_term, year_to_use, agent_to_use)
            CONFIG["MANUAL_SEARCH"] = original_manual_config

            valid_candidates = [
                c for c in new_matches_data.get('MediaContainer', {}).get('SearchResult', [])
                if not any(c.get('guid', '').startswith(p) for p in MATCH_CANDIDATE_EXCLUDED_GUID_PREFIXES)
            ]

            if not valid_candidates:
                logger.warning(f"  검색어 '{custom_search_term}'에 대한 결과를 찾을 수 없습니다.")
            continue

        elif user_choice_input_str.isdigit() and 0 < int(user_choice_input_str) <= len(valid_candidates):
            final_selected_candidate = valid_candidates[int(user_choice_input_str) - 1]
            break

        else:
            logger.warning("잘못된 입력입니다. 목록 내 번호, N, S, 또는 Q를 입력하세요.")
    # --- 루프 끝 ---

    if not final_selected_candidate:
        return False, "NO_CANDIDATE_SELECTED"

    sel_name = final_selected_candidate.get('name')
    sel_guid = final_selected_candidate.get('guid')
    sel_year = final_selected_candidate.get('year')
    logger.info(f"  ID {item_id}: [{calling_mode_tag}] 후보 '{sel_name}' ({sel_guid}) 선택됨. 재매칭 실행...")

    rematch_ok = await rematch_plex_item(item_id, sel_guid, sel_name, sel_year)

    db_status_str = ""
    final_op_status_msg = ""
    if rematch_ok:
        db_status_str = f"COMPLETED_{calling_mode_tag.upper()}_REMATCH ({sel_guid})"
        final_op_status_msg = sel_guid
    else:
        if SHUTDOWN_REQUESTED:
            final_op_status_msg = "GLOBAL_QUIT_REQUESTED_DURING_REMATCH_API"
            db_status_str = f"ABORTED_REMATCH_SHUTDOWN_{calling_mode_tag.upper()} ({sel_guid})"
        else:
            final_op_status_msg = f"REMATCH_API_FAILED_FOR_{sel_guid}"
            db_status_str = f"FAILED_REMATCH_{calling_mode_tag.upper()} ({sel_guid})"

    if not CONFIG.get("DRY_RUN"):
        await await_sync(record_completed_task, item_id, status=db_status_str,
                         matched_guid=sel_guid if rematch_ok else None)

    return rematch_ok, final_op_status_msg


async def worker_task_loop(p_queue: asyncio.Queue, worker_name_str: str):
    """워커가 큐에서 아이템을 가져와 처리하는 비동기 루프입니다."""
    global SHUTDOWN_REQUESTED
    logger.debug(f"[{worker_name_str}] 시작.")
    while not SHUTDOWN_REQUESTED:
        item_to_process_in_worker = None
        try:
            # 큐에서 아이템 가져오기 (타임아웃을 두어 주기적으로 SHUTDOWN_REQUESTED 확인)
            item_to_process_in_worker = await asyncio.wait_for(p_queue.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue # 타임아웃 시 다시 루프 시작 (SHUTDOWN_REQUESTED 확인)
        except asyncio.CancelledError:
            logger.info(f"[{worker_name_str}] 외부 요청(cancel)으로 정상 종료됨.")
            break # 워커 루프 종료

        if SHUTDOWN_REQUESTED: # 아이템을 가져왔더라도, 종료 요청이 있다면 처리 안 함
            logger.info(f"[{worker_name_str}] 종료 요청으로 새 작업 (ID: {item_to_process_in_worker['id'] if item_to_process_in_worker and 'id' in item_to_process_in_worker else 'N/A'}) 시작 안 함.")
            if item_to_process_in_worker: # 큐에서 꺼낸 아이템은 task_done() 처리
                p_queue.task_done()
            break # 워커 루프 종료

        try:
            # 실제 아이템 처리 함수 호출
            await process_single_item_for_auto_rematch(item_to_process_in_worker, worker_name_str)
        except asyncio.CancelledError:
            item_id_cancelled = item_to_process_in_worker['id'] if item_to_process_in_worker and 'id' in item_to_process_in_worker else "unknown_item"
            logger.warning(f"[{worker_name_str}] 작업 (ID: {item_id_cancelled}) 처리 중 명시적으로 취소됨.")
            SHUTDOWN_REQUESTED = True 
            break 
        except Exception: 
            item_id_exception = item_to_process_in_worker['id'] if item_to_process_in_worker and 'id' in item_to_process_in_worker else "unknown_item"
            logger.error(f"!!! [{worker_name_str}] 아이템 (ID: {item_id_exception}) 처리 중 심각한 예외 발생 (worker_task_loop).", exc_info=True)
            break
        finally:
            if item_to_process_in_worker:
                p_queue.task_done()

        # 아이템 처리 후, 다음 아이템을 가져오기 전 딜레이 (match-interval)
        # 큐에 다음 아이템이 있고, 종료 요청이 없을 때만 적용
        if not SHUTDOWN_REQUESTED and not p_queue.empty():
            match_interval_s = CONFIG.get("MATCH_INTERVAL", 1)
            if match_interval_s > 0:
                logger.debug(f"[{worker_name_str}] 다음 아이템 가져오기 전 {match_interval_s}초 대기 (match-interval)...")
                try:
                    await asyncio.wait_for(asyncio.sleep(match_interval_s), timeout=match_interval_s + 0.5)
                except asyncio.TimeoutError: pass
                except asyncio.CancelledError:
                    # sleep 중 취소되면 SHUTDOWN_REQUESTED 플래그 설정하고 루프 상단에서 처리
                    SHUTDOWN_REQUESTED = True
    logger.info(f"[{worker_name_str}] 정상적으로 루프 종료 (SHUTDOWN_REQUESTED: {SHUTDOWN_REQUESTED}).")


async def run_interactive_search_and_rematch_mode(args):
    global CONFIG, SHUTDOWN_REQUESTED
    user_has_quit_this_mode = False
    processed_items_count_this_mode = 0

    search_keyword_val = CONFIG.get("SEARCH")
    search_field_val = CONFIG.get("SEARCH_FIELD", "title")
    library_id_val = CONFIG.get("ID")
    site_code_map_from_config = CONFIG.get("SITE_CODE_MAP", {})

    if not search_keyword_val:
        logger.error("--search 옵션에는 검색할 키워드가 필요합니다. (예: --search ABCD-123)")
        return
    if not library_id_val:
        logger.error("--id 옵션은 검색 작업에 필수입니다. (검색할 라이브러리 ID 지정)")
        return

    print(f"\n인터랙티브 검색 및 재매칭 모드 시작: 키워드='{search_keyword_val}', 필드='{search_field_val}', 라이브러리 ID={library_id_val}")
    print("경고: 이 모드에서 재매칭 선택 시, 기본적으로 로컬 JSON 삭제 및 언매치 후 새로운 매칭을 시도합니다.")

    while not SHUTDOWN_REQUESTED:
        base_s_query = """
        SELECT
            mi.id, mi.title, mi.guid, mi.library_section_id, mi.metadata_type,
            mi.year, mi.original_title, mi.title_sort,
            MIN(mp.file) AS file_path
        FROM metadata_items mi
        LEFT JOIN media_items mpi ON mpi.metadata_item_id = mi.id
        LEFT JOIN media_parts mp ON mp.media_item_id = mpi.id
        WHERE mi.library_section_id = ? AND mi.metadata_type = 1
        """
        s_params_list = [str(library_id_val)]
        actual_search_keyword_for_query = search_keyword_val 

        if search_field_val == 'title':
            base_s_query += " AND mi.title LIKE ?"
            s_params_list.append(f'%{actual_search_keyword_for_query}%')
        elif search_field_val == 'label':
            base_s_query += " AND mi.title_sort LIKE ?"
            s_params_list.append(f'%{actual_search_keyword_for_query}%')
        elif search_field_val == 'site':
            site_key = actual_search_keyword_for_query.lower()
            target_prefixes = site_code_map_from_config.get(site_key)

            if target_prefixes:
                # 여러 OR 조건을 동적으로 생성
                or_conditions = " OR ".join(["mi.guid LIKE ?"] * len(target_prefixes))
                base_s_query += f" AND ({or_conditions})"
                # 각 조건에 맞는 파라미터 추가
                s_params_list.extend([f'{prefix}%' for prefix in target_prefixes])
            else:
                logger.error(f"알 수 없는 사이트 키워드: '{site_key}'. 사용 가능: {list(site_code_map_from_config.keys())}")
                return

        base_s_query += " GROUP BY mi.id"

        search_result_rows = await await_sync(fetch_all, base_s_query, tuple(s_params_list))

        if not search_result_rows:
            # return 대신 break를 사용하여 while 루프를 정상적으로 종료하도록 수정
            logger.info("검색 결과가 더 이상 없습니다. 모드를 종료합니다.")
            break

        if search_result_rows:
            logger.debug("검색 결과를 자연 정렬합니다...")
            try:
                search_result_rows.sort(key=lambda row: natural_sort_key(row['title_sort'] or row['title'] or ''))
            except Exception as e_sort:
                logger.warning(f"검색 결과 정렬 중 오류 발생: {e_sort}. 정렬 없이 진행합니다.")

        items_for_user_action = [] 
        print(f"\n--- 검색 결과 ({len(search_result_rows)}개) ---")
        # 헤더 형식 정의
        header_fmt_search = "{idx:<5s} {id:<7s} | {guid_display:<16s} | {site:<8s}"
        header_title_part_search = "제목 (DB)" # 너비는 유동적이므로 따로 관리
        # 여기서는 전체 너비를 대략적으로 맞추기 위해 print("-" * 110) 사용
        print(f"{header_fmt_search.format(idx='No.', id='ID', guid_display='GUID', site='SITE')} | {header_title_part_search}")
        print("-" * (len(header_fmt_search.format(idx="", id="", guid_display="", site="")) + 3 + 50 )) # 제목 너비 대략 50으로 가정

        for i_s_row, db_row_searched_item in enumerate(search_result_rows):
            item_s_id_val = db_row_searched_item['id']
            item_s_title_val = db_row_searched_item['title'] if 'title' in db_row_searched_item.keys() and db_row_searched_item['title'] is not None else "N/A"
            item_s_guid_val = db_row_searched_item['guid'] if 'guid' in db_row_searched_item.keys() and db_row_searched_item['guid'] is not None else "N/A"
            item_s_media_path_val = db_row_searched_item['file_path'] if 'file_path' in db_row_searched_item.keys() and db_row_searched_item['file_path'] is not None else None

            filename_pid_padded_search = None # 각 루프 시작 시 초기화
            if item_s_media_path_val:
                base_filename_for_pid = os.path.basename(item_s_media_path_val)
                extracted_fn_pid = extract_product_id_from_filename(base_filename_for_pid)
                if extracted_fn_pid:
                    filename_pid_padded_search = extracted_fn_pid
            display_site_code_search = "N/A"
            display_guid_to_print_search = "N/A"

            if item_s_guid_val and item_s_guid_val != "N/A":
                base_sjva_agent_prefix = CONFIG.get("SJVA_AGENT_GUID_PREFIX")
                is_sjva_guid = False

                for site_key, full_prefixes_list in site_code_map_from_config.items():
                    if any(item_s_guid_val.startswith(p) for p in full_prefixes_list):
                        # 일치하는 접두사를 찾으면 사이트 정보 설정하고 루프 탈출
                        display_site_code_search = site_key.upper()

                        # 어떤 접두사와 일치했는지 찾아서 GUID 내용 생성
                        matched_prefix = next(p for p in full_prefixes_list if item_s_guid_val.startswith(p))
                        site_code_in_map = matched_prefix.replace(base_sjva_agent_prefix, "")
                        content_id_part = item_s_guid_val.replace(matched_prefix, "", 1)
                        display_guid_to_print_search = site_code_in_map + content_id_part

                        is_sjva_guid = True
                        break # 바깥쪽 for 루프 탈출

                if not is_sjva_guid and item_s_guid_val.startswith(base_sjva_agent_prefix):
                    is_sjva_guid = True
                    content_id_part = item_s_guid_val.replace(base_sjva_agent_prefix, "")
                    display_guid_to_print_search = content_id_part
                    display_site_code_search = "SJVA_GEN"

                if not is_sjva_guid:
                    try:
                        uri_p = urllib.parse.urlparse(item_s_guid_val)
                        display_site_code_search = uri_p.scheme.upper() if uri_p.scheme else "UNKNOWN"
                        if uri_p.netloc:
                            display_site_code_search += f":{uri_p.netloc.split('.')[0][:8]}" if '.' in uri_p.netloc else f":{uri_p.netloc[:8]}"
                        path_parts = uri_p.path.strip('/').split('/')
                        current_display_guid_path = path_parts[0] if path_parts and path_parts[0] else uri_p.path
                        if len(path_parts) > 1 and path_parts[0]:
                            current_display_guid_path += f"/{path_parts[1][:10]}" if len(path_parts[1]) > 10 else f"/{path_parts[1]}"
                        display_guid_to_print_search = current_display_guid_path
                        if uri_p.query: # 쿼리 파라미터가 있다면 일단 포함 (나중에 ? 로 자름)
                            display_guid_to_print_search += f"?{uri_p.query}"
                    except Exception:
                        display_guid_to_print_search = item_s_guid_val
                        display_site_code_search = "OTHER_ERR"

                if '?' in display_guid_to_print_search:
                    display_guid_to_print_search = display_guid_to_print_search.split('?', 1)[0]

            elif not item_s_guid_val or item_s_guid_val == "N/A":
                display_guid_to_print_search = "No GUID"
                display_site_code_search = "N/A"

            guid_for_print_final = display_guid_to_print_search[:14] + ".." if len(display_guid_to_print_search) > 16 else display_guid_to_print_search
            site_for_print_final = display_site_code_search[:6] + ".." if len(display_site_code_search) > 8 else display_site_code_search

            items_for_user_action.append({
                'idx_display': str(i_s_row + 1),
                'id': item_s_id_val,
                'title': item_s_title_val,
                'guid': item_s_guid_val, 
                'site_display': display_site_code_search, 
                'guid_display_content': display_guid_to_print_search,
                'file_pid_raw': filename_pid_padded_search,
                'norm_file_pid': normalize_pid_for_comparison(filename_pid_padded_search),
                'original_row': db_row_searched_item
            })
            
            title_display_limited_search = item_s_title_val[:48] + ('..' if len(item_s_title_val) > 50 else '')
            print(f"{header_fmt_search.format(idx=str(i_s_row+1), id=str(item_s_id_val), guid_display=guid_for_print_final, site=site_for_print_final)} | {title_display_limited_search}")

        if SHUTDOWN_REQUESTED: logger.warning("목록 출력 후 종료 요청 수신."); return
        if not items_for_user_action: logger.info("사용자에게 제시할 최종 검색 결과가 없습니다."); return

        # --match-limit 가져오기
        current_match_limit = CONFIG.get("MATCH_LIMIT", 0)
        if current_match_limit > 0:
            logger.info(f"--match-limit: 이 세션에서 최대 {current_match_limit}개 아이템만 처리합니다.")

        if current_match_limit > 0 and processed_items_count_this_mode >= current_match_limit:
            logger.info(f"--match-limit ({current_match_limit}개) 도달. 인터랙티브 검색 모드 추가 처리 중단.")
            break

        selected_indices_from_user_input_actual = set()
        user_input_str_val = ""
        try:
            user_input_str_val = await asyncio.get_running_loop().run_in_executor( None, input, "\n재매칭할 항목의 번호(들) (쉼표/하이픈 범위, 예: 1,3,5-7), 'all'(전체), 또는 'q'(모드종료): ")
            user_input_str_val = user_input_str_val.lower().strip()
        except EOFError: logger.warning("EOFError: 사용자 입력을 받을 수 없습니다. 모드를 종료합니다."); user_has_quit_this_mode = True; break
        except Exception as e_input_search: logger.error(f"입력 처리 중 오류: {e_input_search}. 모드를 종료합니다.", exc_info=True); user_has_quit_this_mode = True; break

        if user_input_str_val == 'q': logger.info("사용자 요청('q')으로 현재 모드를 중단합니다."); user_has_quit_this_mode = True; break
        if not user_input_str_val: continue

        is_current_input_valid = True
        if user_input_str_val == 'all':
            selected_indices_from_user_input_actual.update(range(len(items_for_user_action)))
        else:
            input_parts_list = user_input_str_val.split(',')
            for part_item_str in input_parts_list:
                part_item_str = part_item_str.strip()
                if '-' in part_item_str:
                    try:
                        start_i_str, end_i_str = part_item_str.split('-')
                        start_i, end_i = int(start_i_str), int(end_i_str)
                        if not (0 < start_i <= end_i <= len(items_for_user_action)): raise ValueError("범위 초과")
                        selected_indices_from_user_input_actual.update(range(start_i - 1, end_i))
                    except ValueError as ve_range: logger.warning(f"잘못된 범위: '{part_item_str}'. 오류: {ve_range}"); is_current_input_valid = False; break
                elif part_item_str.isdigit():
                    try:
                        num_i = int(part_item_str)
                        if not (0 < num_i <= len(items_for_user_action)): raise ValueError("번호 초과")
                        selected_indices_from_user_input_actual.add(num_i - 1)
                    except ValueError as ve_num: logger.warning(f"잘못된 번호: '{part_item_str}'. 오류: {ve_num}"); is_current_input_valid = False; break
                else: logger.warning(f"잘못된 입력 형식: '{part_item_str}'."); is_current_input_valid = False; break
            if not is_current_input_valid: continue

        if not selected_indices_from_user_input_actual:
            logger.info("선택된 항목이 없습니다. 다시 입력하거나 'q'로 종료하세요."); continue

        items_to_process_this_round = [items_for_user_action[i] for i in sorted(list(selected_indices_from_user_input_actual))]

        match_mode_choice = ''
        while not match_mode_choice:
            if SHUTDOWN_REQUESTED or user_has_quit_this_mode: break
            try:
                prompt_message = (
                    f"\n선택된 {len(items_to_process_this_round)}개 항목에 대해 어떤 방식으로 매칭하시겠습니까?\n"
                    f"  (A) 자동 매칭 (Plex 점수 및 품번 일치 기반, 실패/불일치 시 수동 전환 가능)\n"
                    f"  (M) 수동 매칭 (각 항목별로 매칭 후보 직접 선택)\n"
                    f"  (D) Plex Dance (라이브러리에서 완전히 제거 후 재추가)\n"
                    f"  (C) 취소 (항목 선택으로 돌아가기)\n"
                    f"입력 (A/M/D/C): "
                )
                match_mode_input = await asyncio.get_running_loop().run_in_executor(None, input, prompt_message)
                match_mode_choice = match_mode_input.lower().strip()
                if match_mode_choice not in ['a', 'm', 'd', 'c']: # <-- 'd' 추가
                    logger.warning("잘못된 입력입니다. A, M, D, C 중 하나를 입력하세요.")
                    match_mode_choice = '' 
            except EOFError: user_has_quit_this_mode = True; break
            except Exception as e_mode_input: user_has_quit_this_mode = True; break

        if SHUTDOWN_REQUESTED or user_has_quit_this_mode: break
        if match_mode_choice == 'c':
            logger.info("항목 처리 취소. 다시 항목을 선택하세요.")
            continue

        if match_mode_choice == 'd':
            logger.info(f"\n--- [SEARCH] Plex Dance 모드로 {len(items_to_process_this_round)}개 항목 처리 시작 ---")
            for item_idx_in_selection, item_data in enumerate(items_to_process_this_round):
                if SHUTDOWN_REQUESTED or user_has_quit_this_mode: break
                if current_match_limit > 0 and processed_items_count_this_mode >= current_match_limit:
                    logger.info(f"  --match-limit ({current_match_limit}개) 도달. Plex Dance 중단.")
                    break

                success = await run_plex_dance_for_item(item_data['id'])
                if success:
                    processed_items_count_this_mode += 1

                is_last_item = (item_idx_in_selection == len(items_to_process_this_round) - 1)
                if not SHUTDOWN_REQUESTED and not user_has_quit_this_mode and not is_last_item:
                    match_interval_s = CONFIG.get("MATCH_INTERVAL", 1)
                    if match_interval_s > 0:
                        logger.debug(f"  다음 Plex Dance 실행 전 {match_interval_s}초 대기...")
                        try:
                            await asyncio.sleep(match_interval_s)
                        except asyncio.CancelledError:
                            user_has_quit_this_mode = True
                            SHUTDOWN_REQUESTED = True
                            break

        elif match_mode_choice == 'm':
            logger.info(f"\n--- [SEARCH] 수동 매칭 모드로 {len(items_to_process_this_round)}개 항목 처리 시작 ---")
            original_manual_search_config = CONFIG.get("MANUAL_SEARCH", False)
            CONFIG["MANUAL_SEARCH"] = True
            logger.debug(f"  임시 설정: CONFIG['MANUAL_SEARCH'] = True (SEARCH 모드 내 수동 선택)")

            try:
                for item_idx_in_selection, item_data_manual in enumerate(items_to_process_this_round):
                    if SHUTDOWN_REQUESTED or user_has_quit_this_mode: break

                    # --match-limit 확인 (루프 시작 시)
                    if current_match_limit > 0 and processed_items_count_this_mode >= current_match_limit:
                        logger.info(f"  --match-limit ({current_match_limit}개) 도달. 수동 매칭 중단.")
                        break

                    unmatch_call_ok = await unmatch_plex_item(item_data_manual['id'])
                    if not unmatch_call_ok:
                        logger.error(f"  ID {item_data_manual['id']}: 수동 매칭 전 언매치 실패.")
                        if not CONFIG.get("DRY_RUN"): await await_sync(record_completed_task, item_data_manual['id'], status="FAILED_UNMATCH_SEARCH_MANUAL")
                        continue 
                    if SHUTDOWN_REQUESTED or user_has_quit_this_mode: break

                    rematch_success, _ = await run_manual_interactive_rematch_for_item(
                        item_data_manual,
                        calling_mode_tag="SEARCH_MANUAL"
                    )
                    if rematch_success:
                        processed_items_count_this_mode += 1

                    if SHUTDOWN_REQUESTED or user_has_quit_this_mode: break

                    # --match-interval 적용 (다음 아이템 처리 전)
                    is_last_in_selection = (item_idx_in_selection == len(items_to_process_this_round) - 1)
                    if not SHUTDOWN_REQUESTED and not user_has_quit_this_mode and not is_last_in_selection:
                        # 그리고 전체 match-limit에도 아직 도달하지 않았을 때
                        if not (current_match_limit > 0 and processed_items_count_this_mode >= current_match_limit):
                            match_interval_s = CONFIG.get("MATCH_INTERVAL", 1)
                            if match_interval_s > 0:
                                logger.debug(f"  다음 항목 수동 처리 전 {match_interval_s}초 대기 (match-interval)...")
                                try:
                                    await asyncio.wait_for(asyncio.sleep(match_interval_s), timeout=match_interval_s + 0.5)
                                except asyncio.TimeoutError: pass
                                except asyncio.CancelledError: user_has_quit_this_mode = True; SHUTDOWN_REQUESTED = True; break
            finally:
                CONFIG["MANUAL_SEARCH"] = original_manual_search_config
                logger.debug(f"  설정 복원: CONFIG['MANUAL_SEARCH'] = {original_manual_search_config}")

                if SHUTDOWN_REQUESTED or user_has_quit_this_mode: break
                # await asyncio.sleep(0.1) # finally 블록이 끝나기 전에 종료될 수 있으므로, 여기서 sleep은 주의

        elif match_mode_choice == 'a':
            logger.info(f"\n--- [SEARCH] 자동 매칭 모드로 {len(items_to_process_this_round)}개 항목 처리 시작 ---")
            auto_match_results_info = [] 
            items_for_manual_fallback_data = []

            total_selected_for_auto = len(items_to_process_this_round) # 자동 매칭 대상으로 선택된 총 아이템 수
            processed_count_in_auto_round = 0 # 이번 자동 매칭 라운드에서 처리 시작된 아이템 수

            for item_idx_in_selection_auto, item_data_auto in enumerate(items_to_process_this_round):
                if SHUTDOWN_REQUESTED or user_has_quit_this_mode: break

                # --match-limit 확인 (루프 시작 시)
                if current_match_limit > 0 and processed_items_count_this_mode >= current_match_limit:
                    logger.info(f"  --match-limit ({current_match_limit}개) 도달. 자동 매칭 중단.")
                    break

                processed_count_in_auto_round += 1
                percentage_auto_round = (processed_count_in_auto_round / total_selected_for_auto) * 100 if total_selected_for_auto > 0 else 0
                logger.info(f"\033[36m[SEARCH-AUTO] 진행: {processed_count_in_auto_round}/{total_selected_for_auto} ({percentage_auto_round:.1f}%) - ID {item_data_auto['id']} 처리 시작...\033[0m")

                item_id_auto = item_data_auto['id']
                original_row_auto = item_data_auto['original_row']
                original_file_pid_normalized_auto = item_data_auto.get('norm_file_pid')
                original_file_pid_parts_for_extraction_auto = None
                if item_data_auto.get('file_pid_raw'):
                    fn_parts_raw_auto_s = item_data_auto['file_pid_raw'].upper().split('-',1)
                    if len(fn_parts_raw_auto_s) == 2 and fn_parts_raw_auto_s[0] and fn_parts_raw_auto_s[1]:
                        original_file_pid_parts_for_extraction_auto = (fn_parts_raw_auto_s[0], fn_parts_raw_auto_s[1])

                # logger.info(f"\n>>> [SEARCH-AUTO] ID {item_id_auto} ('{item_data_auto['title']}') 처리 시작...")

                unmatch_ok_auto = await unmatch_plex_item(item_id_auto)
                if not unmatch_ok_auto:
                    logger.error(f"  ID {item_id_auto}: 자동 매칭 전 언매치 실패. 건너뜀.")
                    auto_match_results_info.append({'id': item_id_auto, 'success': False, 'status': 'FAILED_UNMATCH_SEARCH_AUTO', 'matched_guid': None, 'matched_title_pid_norm': None, 'original_file_pid_norm': original_file_pid_normalized_auto})
                    continue
                if SHUTDOWN_REQUESTED or user_has_quit_this_mode: break

                try:
                    _ = await process_single_item_for_auto_rematch(original_row_auto, f"SearchMode-AutoWorker-{item_id_auto}")
                except asyncio.CancelledError:
                    logger.warning(f"ID {item_id_auto}: 자동 매칭 처리 중 명시적으로 취소되었습니다 (CancelledError 수신).")
                    if not CONFIG.get("DRY_RUN"):
                        await await_sync(record_completed_task, item_id_auto, status="CANCELLED_IN_SEARCH_AUTO")
                    # SHUTDOWN_REQUESTED는 process_single_item_for_auto_rematch 내부에서 설정될 수 있음
                    pass 

                if SHUTDOWN_REQUESTED or user_has_quit_this_mode: break

                # --match-interval 적용 (다음 아이템 자동 처리 전)
                is_last_in_auto_selection = (item_idx_in_selection_auto == len(items_to_process_this_round) - 1)
                if not SHUTDOWN_REQUESTED and not user_has_quit_this_mode and not is_last_in_auto_selection:
                    if not (current_match_limit > 0 and processed_items_count_this_mode >= current_match_limit):
                        match_interval_s = CONFIG.get("MATCH_INTERVAL", 1)
                        if match_interval_s > 0:
                            logger.debug(f"  다음 항목 자동 처리 전 {match_interval_s}초 대기 (match-interval)...")
                            try:
                                await asyncio.wait_for(asyncio.sleep(match_interval_s), timeout=match_interval_s + 0.5)
                            except asyncio.TimeoutError: pass
                            except asyncio.CancelledError: user_has_quit_this_mode = True; SHUTDOWN_REQUESTED = True; break

                # 자동 매칭 후 결과 확인 (DB 조회)
                current_status_db, matched_guid_db, matched_title_db = "UNKNOWN_AFTER_AUTO", None, None
                is_success_db = False

                completed_info = await await_sync(get_completed_task_info, item_id_auto)
                if completed_info:
                    current_status_db = completed_info['status']
                    matched_guid_db = completed_info['matched_guid']
                    if matched_guid_db and ("COMPLETED" in current_status_db or "DRYRUN" in current_status_db):
                        is_success_db = True
                        plex_db_row_auto = await get_metadata_by_id(item_id_auto)
                        if plex_db_row_auto: matched_title_db = plex_db_row_auto['title']

                matched_title_pid_norm_db = None
                if is_success_db and matched_title_db:
                    extracted_pid = extract_product_id_from_title(matched_title_db, original_file_pid_parts_for_extraction_auto)
                    matched_title_pid_norm_db = normalize_pid_for_comparison(extracted_pid)

                result_entry = {
                    'id': item_id_auto, 'success': is_success_db, 'status': current_status_db, 
                    'matched_guid': matched_guid_db, 
                    'original_file_pid_norm': original_file_pid_normalized_auto,
                    'matched_title_pid_norm': matched_title_pid_norm_db,
                    'original_item_data': item_data_auto 
                }
                auto_match_results_info.append(result_entry)

                pid_mismatch_auto = False
                if is_success_db:
                    if original_file_pid_normalized_auto and matched_title_pid_norm_db:
                        if original_file_pid_normalized_auto != matched_title_pid_norm_db: pid_mismatch_auto = True
                    elif original_file_pid_normalized_auto and not matched_title_pid_norm_db:
                        pid_mismatch_auto = True 
                
                if not is_success_db or pid_mismatch_auto:
                    items_for_manual_fallback_data.append(item_data_auto)
                elif is_success_db and not pid_mismatch_auto:
                    processed_items_count_this_mode +=1

            if total_selected_for_auto > 0 and not (SHUTDOWN_REQUESTED or user_has_quit_this_mode):
                logger.info(f"\033[36m[SEARCH-AUTO] 진행: {processed_count_in_auto_round}/{total_selected_for_auto} (100.0%) 처리 완료.\033[0m")

            if SHUTDOWN_REQUESTED or user_has_quit_this_mode: break

            success_count_final = sum(1 for r in auto_match_results_info if r['success'] and not (r.get('original_file_pid_norm') and r.get('matched_title_pid_norm') and r['original_file_pid_norm'] != r['matched_title_pid_norm'] or (r.get('original_file_pid_norm') and not r.get('matched_title_pid_norm'))))
            fail_count_final = sum(1 for r in auto_match_results_info if not r['success'])
            pid_mismatch_count_final = sum(1 for r in auto_match_results_info if r['success'] and ((r.get('original_file_pid_norm') and r.get('matched_title_pid_norm') and r['original_file_pid_norm'] != r['matched_title_pid_norm']) or (r.get('original_file_pid_norm') and not r.get('matched_title_pid_norm'))))
            
            logger.info("\n--- [SEARCH] 자동 매칭 시도 결과 요약 ---")
            logger.info(f"  - 총 시도: {len(auto_match_results_info)}건")
            logger.info(f"  - 성공 (품번 일치 또는 원본 품번 없음): {success_count_final}건")
            logger.info(f"  - 성공 (품번 불일치 또는 매칭결과 품번추출실패): {pid_mismatch_count_final}건 (수동 폴백 대상)")
            logger.info(f"  - 실패 (후보 없음 또는 오류): {fail_count_final}건 (수동 폴백 대상)")

            seen_fallback_ids = set()
            unique_items_for_manual_fallback = []
            for item_fb_data_orig in items_for_manual_fallback_data:
                if item_fb_data_orig['id'] not in seen_fallback_ids:
                    unique_items_for_manual_fallback.append(item_fb_data_orig)
                    seen_fallback_ids.add(item_fb_data_orig['id'])

            if unique_items_for_manual_fallback:
                logger.info(f"\n다음 {len(unique_items_for_manual_fallback)}개 항목에 대해 수동 폴백 매칭을 시도할 수 있습니다:")
                for i_fb, item_data_to_display_fb in enumerate(unique_items_for_manual_fallback):
                    reason_fb_str = "자동매칭실패"
                    fb_item_result_detail = next((r for r in auto_match_results_info if r['id'] == item_data_to_display_fb['id']), None)
                    if fb_item_result_detail:
                        if fb_item_result_detail['success']: 
                            reason_fb_str = (f"품번불일치/추출실패 (파일:{fb_item_result_detail.get('original_file_pid_norm','N/A')}, "
                                            f"매칭:{fb_item_result_detail.get('matched_title_pid_norm','N/A')})")
                        else: 
                            reason_fb_str = fb_item_result_detail.get('status', '자동매칭실패')
                    logger.info(f"  {i_fb+1}. ID: {item_data_to_display_fb['id']}, Title: '{item_data_to_display_fb['title'][:30]}...', 이유: {reason_fb_str}")

                fallback_choice_input = ''
                try:
                    fb_prompt_auto = f"위 {len(unique_items_for_manual_fallback)}개 항목에 대해 수동 매칭을 진행하시겠습니까? (Y/N): "
                    fallback_choice_input = (await asyncio.get_running_loop().run_in_executor(None, input, fb_prompt_auto)).lower().strip()
                except EOFError: user_has_quit_this_mode = True
                except Exception as e_fb_input: logger.error(f"폴백 선택 입력 오류: {e_fb_input}"); user_has_quit_this_mode = True
                
                if SHUTDOWN_REQUESTED or user_has_quit_this_mode: break

                if fallback_choice_input == 'y':
                    logger.info(f"\n--- [SEARCH] 자동 매칭 후 수동 폴백 ({len(unique_items_for_manual_fallback)}개 항목) ---")
                    original_manual_search_config_fb = CONFIG.get("MANUAL_SEARCH", False)
                    CONFIG["MANUAL_SEARCH"] = True # 수동 폴백 시 manual=1 검색
                    logger.debug(f"  임시 설정: CONFIG['MANUAL_SEARCH'] = True (SEARCH 모드 자동 후 폴백)")

                    total_selected_for_fallback = len(unique_items_for_manual_fallback)
                    processed_count_in_fallback_round = 0

                    try:
                        for item_data_fb_manual_run in unique_items_for_manual_fallback:
                            if SHUTDOWN_REQUESTED or user_has_quit_this_mode: break

                            processed_count_in_fallback_round += 1
                            percentage_fallback_round = (processed_count_in_fallback_round / total_selected_for_fallback) * 100 if total_selected_for_fallback > 0 else 0
                            logger.info(f"\033[36m[SEARCH-FALLBACK] 진행: {processed_count_in_fallback_round}/{total_selected_for_fallback} ({percentage_fallback_round:.1f}%) - ID {item_data_fb_manual_run['id']} 수동 폴백 시작...\033[0m")

                            unmatch_ok_fb_run = await unmatch_plex_item(item_data_fb_manual_run['id'])
                            if not unmatch_ok_fb_run:
                                logger.error(f"  ID {item_data_fb_manual_run['id']}: 수동 폴백 전 언매치 실패.")
                                if not CONFIG.get("DRY_RUN"): await await_sync(record_completed_task, item_data_fb_manual_run['id'], status="FAILED_UNMATCH_SEARCH_AUTO_FB")
                                continue
                            if SHUTDOWN_REQUESTED or user_has_quit_this_mode: break

                            try:
                                rematch_success_fb_run, _ = await run_manual_interactive_rematch_for_item(
                                    item_data_fb_manual_run,
                                    calling_mode_tag="SEARCH_AUTO_FALLBACK"
                                )
                                if rematch_success_fb_run:
                                    processed_items_count_this_mode += 1

                            except asyncio.CancelledError:
                                logger.warning(f"ID {item_data_fb_manual_run['id']}: 수동 폴백 처리 중 명시적으로 취소됨.")
                                if not CONFIG.get("DRY_RUN"):
                                    await await_sync(record_completed_task, item_data_fb_manual_run['id'], status="CANCELLED_IN_SEARCH_FALLBACK")
                                pass # SHUTDOWN_REQUESTED는 내부에서 설정될 수 있음
                            if SHUTDOWN_REQUESTED or user_has_quit_this_mode: break
                            await asyncio.sleep(0.1)
                    finally:
                        CONFIG["MANUAL_SEARCH"] = original_manual_search_config_fb
                        logger.debug(f"  설정 복원: CONFIG['MANUAL_SEARCH'] = {original_manual_search_config_fb}")
                else: 
                    logger.info("수동 폴백 매칭을 진행하지 않습니다.")
            else:
                logger.info("[SEARCH] 자동 매칭 후 추가 수동 폴백 대상이 없습니다.")

        if user_input_str_val == 'all' and (match_mode_choice in ['m', 'a', 'd']):
            logger.info("'all' 옵션에 대한 처리가 완료되었습니다. --search 모드를 종료합니다.")
            user_has_quit_this_mode = True 
        else:
            if match_mode_choice in ['a', 'm', 'd']:
                logger.info("선택한 항목에 대한 작업이 완료되었습니다. 다음 작업을 위해 프롬프트를 다시 표시합니다...")
                await asyncio.sleep(2)

    logger.info(f"--search (인터랙티브) 모드 완료/중단. 총 {processed_items_count_this_mode}개 아이템 재매칭 시도됨.")

async def run_auto_rematch_mode(args):
    """자동 재매칭 모드를 실행합니다."""
    global CONFIG, SHUTDOWN_REQUESTED
    lib_id_val = CONFIG.get("ID")
    logger.info(f"자동 재매칭 모드 시작. 대상 라이브러리 ID: {lib_id_val}")

    base_query_auto = "SELECT id, title, guid, library_section_id, metadata_type, year, original_title, title_sort FROM metadata_items WHERE library_section_id=?"
    query_params_list_auto = [str(lib_id_val)]

    # --include 적용 (OR 조건)
    include_keywords_list = CONFIG.get("INCLUDE", []) 
    if include_keywords_list:
        include_conditions_sql_parts = []
        for kw_include in include_keywords_list:
            include_conditions_sql_parts.append("(mi.title LIKE ? OR mi.title_sort LIKE ?)")
            query_params_list_auto.extend([f'%{kw_include}%', f'%{kw_include}%'])
        if include_conditions_sql_parts:
            base_query_auto += " AND (" + " OR ".join(include_conditions_sql_parts) + ")"

    # --exclude 적용 (OR 조건)
    exclude_keywords_list = CONFIG.get("EXCLUDE", [])
    if exclude_keywords_list:
        exclude_inner_conditions_sql_parts = []
        for kw_exclude in exclude_keywords_list:
            exclude_inner_conditions_sql_parts.append("(mi.title LIKE ? OR mi.title_sort LIKE ?)")
            query_params_list_auto.extend([f'%{kw_exclude}%', f'%{kw_exclude}%'])
        if exclude_inner_conditions_sql_parts:
            base_query_auto += " AND NOT (" + " OR ".join(exclude_inner_conditions_sql_parts) + ")"

    CONFIG["QUERY_TEMPLATE"] = base_query_auto
    CONFIG["QUERY_PARAMS"] = tuple(query_params_list_auto)

    logger.info("--- 스크립트 설정 (자동 재매칭 모드) ---")
    for k_conf, v_conf in CONFIG.items():
        if k_conf == "PLEX_TOKEN" and v_conf: logger.info(f"  - {k_conf}: ***") # 토큰 값은 숨김
        elif k_conf.startswith("QUERY_") or k_conf == "verbose": continue
        else: logger.info(f"  - {k_conf}: {v_conf}")
    if CONFIG.get("DRY_RUN"): logger.warning("*** DRY RUN 모드 활성화됨 ***")

    await await_sync(create_table_if_not_exists) # 완료 DB 테이블 생성
    total_items_from_db_query = await get_query_count(CONFIG["QUERY_TEMPLATE"], CONFIG["QUERY_PARAMS"])
    if total_items_from_db_query <= 0:
        logger.info(f"DB에서 작업 대상 아이템을 찾지 못했습니다 (쿼리 결과: {total_items_from_db_query})."); return

    logger.info(f"DB에서 총 {total_items_from_db_query}개 아이템 로드 예정. 필터링 및 큐 등록 시작...")
    all_items_from_db_rows = await await_sync(fetch_all, CONFIG["QUERY_TEMPLATE"], CONFIG["QUERY_PARAMS"])

    if all_items_from_db_rows:
        logger.debug("DB에서 로드한 아이템을 자연 정렬합니다...")
        try:
            all_items_from_db_rows.sort(key=lambda row: natural_sort_key(row['title_sort'] or row['title'] or ''))
        except Exception as e_sort:
            logger.warning(f"자동 재매칭 모드 정렬 중 오류 발생: {e_sort}. 정렬 없이 진행합니다.")

    if not all_items_from_db_rows:
        logger.error("DB에서 아이템을 가져오지 못했습니다 (fetch_all 결과 없음)."); return

    item_processing_q = asyncio.Queue()
    items_added_count, items_skipped_type_count, items_skipped_completed_count = 0,0,0
    should_include_completed_items = CONFIG.get("INCLUDE_COMPLETED", False)
    # 로그 간격은 전체 아이템 수에 따라 동적으로 조절 (너무 많거나 적지 않게)
    log_output_interval = max(1, len(all_items_from_db_rows) // 10 if len(all_items_from_db_rows) > 20 else 1)

    # --match-limit 가져오기
    current_match_limit = CONFIG.get("MATCH_LIMIT", 0)
    if current_match_limit > 0:
        logger.info(f"--match-limit: 최대 {current_match_limit}개 아이템만 큐에 추가합니다.")

    for current_idx, item_row_obj in enumerate(all_items_from_db_rows):
        if SHUTDOWN_REQUESTED: # 큐 추가 중 종료 요청 확인
            logger.warning("큐 추가 중 종료 요청으로 중단됨.")
            break

        should_log_this_item_progress = (current_idx < 3 or \
                                        (current_idx + 1) % log_output_interval == 0 or \
                                         current_idx == len(all_items_from_db_rows) -1)

        # --match-limit 적용
        if current_match_limit > 0 and items_added_count >= current_match_limit:
            if should_log_this_item_progress or (items_added_count == current_match_limit):
                logger.info(f"  --match-limit ({current_match_limit}개) 도달. 큐 추가 중단.")
            break

        item_id_log = item_row_obj['id']
        if item_row_obj['metadata_type'] != 1: # 영화(1) 타입만 처리
            if should_log_this_item_progress: logger.debug(f"  건너뜀 (타입 미지원 ID {item_id_log})")
            items_skipped_type_count +=1; continue

        if not should_include_completed_items and await await_sync(is_task_completed, item_id_log):
            if should_log_this_item_progress: logger.debug(f"  건너뜀 (이미 완료 ID {item_id_log})")
            items_skipped_completed_count += 1; continue
        elif should_include_completed_items and await await_sync(is_task_completed, item_id_log):
            if should_log_this_item_progress:
                logger.info(f"  * 포함 (완료된 항목, --include-completed 옵션 활성): ID {item_id_log}")

        item_processing_q.put_nowait(item_row_obj)
        items_added_count += 1
        if should_log_this_item_progress: logger.debug(f"  + 큐 추가됨: ID {item_id_log}")

    logger.info(f"큐 등록 완료: {items_added_count}개. "
                f"(타입미지원 {items_skipped_type_count}개, 완료건너뜀 {items_skipped_completed_count}개)")
    if items_added_count == 0: 
        logger.info("처리할 아이템이 큐에 없습니다."); return

    worker_async_tasks = []
    num_concurrent_workers = min(CONFIG.get("WORKERS",1), items_added_count)
    logger.info(f"{num_concurrent_workers}개의 워커를 시작합니다...")
    for i_worker in range(num_concurrent_workers):
        worker_task = asyncio.create_task(worker_task_loop(item_processing_q, f"Worker-{i_worker+1}"))
        worker_async_tasks.append(worker_task)

    # 진행 상황 표시 로직
    total_items_to_process_in_queue_initially = items_added_count # 큐에 처음 추가된 아이템 수
    last_logged_processed_count = -1
    
    # 큐가 비거나 종료 요청이 있을 때까지 대기 (메인 루프에서 주기적 확인)
    while not item_processing_q.empty() and not SHUTDOWN_REQUESTED:
        items_remaining_in_queue = item_processing_q.qsize()
        items_considered_processed = total_items_to_process_in_queue_initially - items_remaining_in_queue

        # 진행 상황이 변경되었을 때만 로그 출력 (너무 자주 출력 방지)
        # 또는 일정 시간 간격으로 출력하도록 조절 가능 (예: time.monotonic() 사용)
        if items_considered_processed != last_logged_processed_count:
            percentage_processed = (items_considered_processed / total_items_to_process_in_queue_initially) * 100 if total_items_to_process_in_queue_initially > 0 else 0
            logger.info(f"\033[36m진행: {items_considered_processed}/{total_items_to_process_in_queue_initially} ({percentage_processed:.1f}%) 처리 시작됨. (큐 잔여: {items_remaining_in_queue})\033[0m")
            last_logged_processed_count = items_considered_processed

        try:
            await asyncio.sleep(1) # 1초 간격으로 큐 상태 확인 및 로깅 (조절 가능)
        except asyncio.CancelledError:
            SHUTDOWN_REQUESTED = True # sleep 중 취소되면 플래그 설정
            break

        await asyncio.sleep(0.5) # 너무 짧으면 CPU 사용률 증가, 너무 길면 종료 지연

    if SHUTDOWN_REQUESTED:
        logger.warning("종료 요청으로 인해 새 작업 할당 중단. 현재 큐에 남은 아이템 수: " + str(item_processing_q.qsize()))
    elif item_processing_q.empty() and total_items_to_process_in_queue_initially > 0: # 큐가 비었고, 처리할 아이템이 있었던 경우
        # 마지막으로 100% 완료 로그를 찍어줌
        logger.info(f"진행: {total_items_to_process_in_queue_initially}/{total_items_to_process_in_queue_initially} (100.0%) 처리 시작됨. (큐 비었음)")

    logger.info("큐 소진 또는 종료 요청 수신. 워커 태스크들의 완료/취소를 기다립니다...")

    # 모든 아이템이 큐에서 처리될 때까지 기다림 (단, 종료 요청이 없다면)
    # 큐에 아이템이 있었던 경우에만 join 시도 (items_added_count > 0)
    if not SHUTDOWN_REQUESTED and items_added_count > 0 :
        try:
            # 타임아웃을 설정하여 무한 대기 방지
            await asyncio.wait_for(item_processing_q.join(), timeout=120) # 예: 2분
        except asyncio.TimeoutError:
            logger.error("큐 join 작업 타임아웃. 일부 작업이 완료되지 않았을 수 있습니다.")
        except Exception as e_queue_join:
            logger.error(f"큐 join 중 예외 발생: {e_queue_join}", exc_info=True)

    # 모든 워커 태스크가 종료될 때까지 기다리거나, 취소 신호 보내기
    for task_obj_worker in worker_async_tasks:
        if not task_obj_worker.done():
            task_obj_worker.cancel() # 아직 실행 중인 태스크는 취소

    # 태스크 결과 수집 (예외 포함)
    worker_results_list = await asyncio.gather(*worker_async_tasks, return_exceptions=True)
    for i_res, result_item in enumerate(worker_results_list):
        if isinstance(result_item, Exception) and not isinstance(result_item, asyncio.CancelledError):
            logger.error(f"Worker-{i_res+1}에서 처리되지 않은 예외가 발생했습니다: {result_item}", exc_info=result_item)
        elif isinstance(result_item, asyncio.CancelledError):
            logger.info(f"Worker-{i_res+1}가 정상적으로 취소되었습니다.")

    logger.info(f"모든 자동 재매칭 작업이 완료되었거나 중단되었습니다. 최종 메모리 사용량: {mem_usage():.2f} MB")


@require_cursor("PLEX_DB", read_only=True)
def get_all_media_file_paths(metadata_id: int, cs: sqlite3.Cursor = None) -> List[str]:
    """주어진 metadata_id에 연결된 모든 media_part의 파일 경로를 가져옵니다."""
    query = """
    SELECT mp.file FROM media_parts mp
    JOIN media_items mi ON mp.media_item_id = mi.id
    WHERE mi.metadata_item_id = ?
    """
    rows = cs.execute(query, (metadata_id,)).fetchall()
    return [row['file'] for row in rows if row and 'file' in row.keys() and row['file']]


async def run_plex_dance_for_item(item_id: int) -> bool:
    """Plex Dance를 수행합니다."""
    global PLEX_SERVER_INSTANCE, CONFIG

    if not PLEXAPI_AVAILABLE:
        logger.error("Plex Dance를 수행하려면 plexapi 라이브러리가 필요합니다.")
        return False

    temp_path = CONFIG.get("PLEX_DANCE_TEMP_PATH")
    if not temp_path:
        logger.error("Plex Dance를 위한 임시 경로(PLEX_DANCE_TEMP_PATH)가 설정되지 않았습니다.")
        return False

    if not os.path.isdir(temp_path):
        try:
            os.makedirs(temp_path)
            logger.info(f"임시 경로 '{temp_path}'를 생성했습니다.")
        except Exception as e:
            logger.error(f"임시 경로 '{temp_path}'를 생성할 수 없습니다: {e}")
            return False

    logger.info(f"ID {item_id}: Plex Dance 시작...")

    original_paths = []
    moved_files = {} # {"임시경로": "원본경로"}
    library = None

    try:
        # 0. Plex 서버 및 아이템 객체, 파일 경로 가져오기
        if PLEX_SERVER_INSTANCE is None:
            PLEX_SERVER_INSTANCE = PlexServer(CONFIG["PLEX_URL"], CONFIG["PLEX_TOKEN"])

        item_to_dance = await await_sync(PLEX_SERVER_INSTANCE.fetchItem, item_id)
        library = item_to_dance.section()
        item_title_log = item_to_dance.title

        original_paths = await await_sync(get_all_media_file_paths, item_id)
        if not original_paths:
            logger.warning(f"ID {item_id}: DB에서 미디어 파일 경로를 찾을 수 없습니다. Plex Dance를 진행할 수 없습니다.")
            return False

        scan_dirs = set(os.path.dirname(p) for p in original_paths)
        logger.debug(f"ID {item_id}: 스캔 대상 디렉터리: {scan_dirs}")

        if CONFIG.get("DRY_RUN"):
            logger.info(f"[DRY RUN] ID {item_id} ('{item_title_log}')에 대한 Plex Dance 시뮬레이션:")
            logger.info(f"  - 관련 .json 파일 삭제 (시뮬레이션)")
            for path in original_paths:
                logger.info(f"  - 파일 이동 (시뮬레이션): '{path}' -> '{temp_path}'")
            logger.info("  - 라이브러리 스캔, 휴지통 비우기, 파일 원위치, 재스캔 (건너뜀)")
            return True

        # 로컬 .json 파일 삭제
        logger.info(f"  1/7. ID {item_id}: 관련 로컬 .json 파일을 찾아 삭제합니다...")
        # 아이템에 여러 파일이 있을 수 있으므로, 각 파일에 대해 .json을 찾습니다.
        json_files_deleted = 0
        for media_path in original_paths:
            try:
                video_dir = os.path.dirname(media_path)
                base_filename = os.path.basename(media_path)

                # SJVA 에이전트의 .json 파일명 규칙 (파일명의 첫 단어(품번) + .json)
                pid_match_for_json = re.match(r"^([a-zA-Z0-9\-]+)", base_filename)
                if pid_match_for_json:
                    json_filename = f"{pid_match_for_json.group(1).lower()}.json"
                    json_file_path = os.path.join(video_dir, json_filename)
                    if os.path.exists(json_file_path):
                        await await_sync(os.remove, json_file_path)
                        logger.info(f"    - JSON 파일 삭제 완료: {json_file_path}")
                        json_files_deleted += 1
                    else:
                        logger.debug(f"    - 해당 경로에 JSON 파일 없음: {json_file_path}")
                else:
                    logger.warning(f"    - 파일명 '{base_filename}'에서 .json 파일명 생성을 위한 품번 추출 실패.")
            except Exception as e_json_del:
                logger.error(f"    - 로컬 JSON 파일 처리 중 오류 발생: {e_json_del}", exc_info=True)

        if json_files_deleted > 0:
            logger.info(f"  ...총 {json_files_deleted}개의 .json 파일을 삭제했습니다.")
        else:
            logger.info("  ...삭제할 관련 .json 파일을 찾지 못했습니다.")

        # 미디어 파일들을 임시 경로로 이동
        logger.info(f"  2/7. ID {item_id}: {len(original_paths)}개의 미디어 파일을 '{temp_path}'로 이동합니다...")
        for src_path in original_paths:
            if not os.path.exists(src_path):
                logger.warning(f"    - 원본 파일 없음, 건너뜀: {src_path}")
                continue

            filename = os.path.basename(src_path)
            dest_path = os.path.join(temp_path, filename)

            # 파일명 충돌 방지
            if os.path.exists(dest_path):
                name, ext = os.path.splitext(filename)
                timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                dest_path = os.path.join(temp_path, f"{name}_{timestamp}{ext}")

            await await_sync(shutil.move, src_path, dest_path)
            moved_files[dest_path] = src_path
            logger.info(f"    - 이동 완료: '{os.path.basename(src_path)}'")

        if not moved_files:
            logger.error(f"ID {item_id}: 이동할 유효한 파일이 없어 Plex Dance를 중단합니다.")
            return False

        # 라이브러리 스캔 (파일 제거 감지)
        logger.info(f"  3/7. '{library.title}' 라이브러리의 관련 경로들을 스캔하여 파일 제거를 감지합니다...")
        for scan_dir in scan_dirs:
            mapped_dir = map_scan_path(scan_dir)
            logger.info(f"    - 경로 스캔 중: {mapped_dir}")
            await await_sync(library.update, path=mapped_dir)

        # 스캔 완료 대기
        logger.info("  ...스캔 작업이 완료될 때까지 대기합니다...")
        while not SHUTDOWN_REQUESTED:
            # get_plex_library_activities는 라이브러리 객체를 받으므로, 라이브러리 단위로 확인
            active_tasks = await get_plex_library_activities(library)
            if not active_tasks: break
            logger.info(f"  ...스캔 진행 중: {active_tasks[0].title} ({getattr(active_tasks[0], 'progress', 0):.0f}%)")
            await asyncio.sleep(5)
        logger.info("  ...스캔 완료.")
        if SHUTDOWN_REQUESTED: raise asyncio.CancelledError("Plex Dance 중단 요청")

        # 휴지통 비우기
        logger.info("  4/7. 라이브러리 휴지통을 비우고 아이템이 완전히 삭제될 때까지 대기합니다...")
        await await_sync(library.emptyTrash)

        try:
            timeout_seconds = int(CONFIG.get("PLEX_DANCE_EMPTY_TRASH_TIMEOUT", 300))
            check_interval_seconds = int(CONFIG.get("PLEX_DANCE_EMPTY_TRASH_INTERVAL", 5))
        except (ValueError, TypeError):
            # YAML에 숫자가 아닌 값이 들어갔을 경우를 대비한 안전장치
            logger.warning("Plex Dance 대기 시간 설정값이 유효하지 않습니다. 기본값을 사용합니다.")
            timeout_seconds = 300
            check_interval_seconds = 5

        logger.info(f"  ...DB에서 ID {item_id}가 사라지는 것을 확인합니다 (최대 {timeout_seconds / 60:.1f}분)...")
        start_time = time.monotonic()
        item_is_deleted = False

        while time.monotonic() - start_time < timeout_seconds:
            if SHUTDOWN_REQUESTED:
                raise asyncio.CancelledError("휴지통 비우기 대기 중 중단 요청")

            if await await_sync(check_item_deleted_by_id, item_id):
                logger.info(f"  ...성공! ID {item_id}가 라이브러리 DB에서 완전히 제거되었습니다.")
                item_is_deleted = True
                break

            remaining_time = timeout_seconds - (time.monotonic() - start_time)
            logger.debug(f"  ...아직 삭제되지 않음. {check_interval_seconds}초 후 다시 확인합니다. (남은 시간: {remaining_time:.0f}초)")
            await asyncio.sleep(check_interval_seconds)

        if not item_is_deleted:
            logger.error(f"  ...실패! 제한 시간({timeout_seconds / 60:.1f}분) 내에 아이템이 DB에서 제거되지 않았습니다. Plex Dance를 중단합니다.")
            # 이 경우 파일 복원을 시도하고 실패로 종료해야 함
            raise Exception(f"ID {item_id}의 휴지통 비우기 확인 실패")

        # 번들 정리 실행
        logger.info("  5/7. 서버 전체의 미사용 번들을 정리합니다...")
        await await_sync(PLEX_SERVER_INSTANCE.library.cleanBundles)

        # 번들 정리는 백그라운드 작업이므로, 활동 상태를 모니터링하며 대기합니다.
        logger.info("  ...번들 정리 작업이 완료될 때까지 대기합니다...")
        while not SHUTDOWN_REQUESTED:
            # PLEX_SERVER_INSTANCE.activities는 전역 활동을 보여줍니다.
            active_tasks = await await_sync(lambda: PLEX_SERVER_INSTANCE.activities)
            bundle_cleaning_task = next((task for task in active_tasks if 'Cleaning bundles' in task.title), None)

            if not bundle_cleaning_task:
                # 작업이 목록에 없으면 완료된 것으로 간주
                break

            progress = getattr(bundle_cleaning_task, 'progress', 0)
            logger.info(f"  ...번들 정리 진행 중: {bundle_cleaning_task.title} ({progress:.0f}%)")
            await asyncio.sleep(15)
        logger.info("  ...번들 정리 완료.")
        if SHUTDOWN_REQUESTED: raise asyncio.CancelledError("Plex Dance 중단 요청")

        # 파일들을 원래 위치로 복원
        logger.info(f"  6/7. {len(moved_files)}개의 미디어 파일을 원래 위치로 복원합니다...")
        for dest_path, src_path in moved_files.items():
            await await_sync(shutil.move, dest_path, src_path)
            logger.info(f"    - 복원 완료: '{os.path.basename(src_path)}'")
        moved_files.clear() # 복원 후 목록 비우기

        # 라이브러리 다시 스캔 (새로운 아이템으로 추가)
        logger.info(f"  7/7. '{library.title}' 라이브러리의 관련 경로들을 다시 스캔하여 아이템 추가를 요청합니다...")
        for scan_dir in scan_dirs:
            mapped_dir = map_scan_path(scan_dir)
            logger.info(f"    - 경로 재스캔 요청: {mapped_dir}")
            await await_sync(library.update, path=mapped_dir)

        try:
            timeout_seconds = int(CONFIG.get("PLEX_DANCE_REAPPEAR_TIMEOUT", 600))
            check_interval_seconds = int(CONFIG.get("PLEX_DANCE_REAPPEAR_INTERVAL", 10))
        except (ValueError, TypeError):
            logger.warning("Plex Dance 복구 대기 시간 설정값이 유효하지 않습니다. 기본값을 사용합니다.")
            timeout_seconds = 600
            check_interval_seconds = 5

        logger.info(f"  ...아이템이 라이브러리에 다시 나타날 때까지 대기합니다 (최대 {timeout_seconds / 60:.1f}분)...")
        start_time = time.monotonic()
        item_reappeared = False

        while time.monotonic() - start_time < timeout_seconds:
            if SHUTDOWN_REQUESTED:
                raise asyncio.CancelledError("아이템 복구 대기 중 중단 요청")

            # DB를 직접 확인하여 모든 파일 경로가 다시 등록되었는지 검사
            if await await_sync(check_items_exist_by_paths, original_paths):
                logger.info("  ...성공! 모든 미디어 파일이 라이브러리에 다시 등록된 것을 확인했습니다.")
                item_reappeared = True
                break

            # 아직 등록되지 않았다면, 다음 확인까지 대기
            remaining_time = timeout_seconds - (time.monotonic() - start_time)
            logger.debug(f"  ...아직 복구되지 않음. {check_interval_seconds}초 후 다시 확인합니다. (남은 시간: {remaining_time:.0f}초)")
            await asyncio.sleep(check_interval_seconds)

        if not item_reappeared:
            logger.error(f"  ...실패! 제한 시간({timeout_seconds / 60:.1f}분) 내에 아이템이 라이브러리에 다시 나타나지 않았습니다.")
            return False

        logger.info(f"ID {item_id}: Plex Dance가 성공적으로 완료되었습니다.")
        return True

    except PlexApiNotFound:
        logger.warning(f"ID {item_id}: Plex Dance 중 아이템을 찾을 수 없습니다. 이미 삭제되었을 수 있습니다.")
        # 파일이 이동된 상태일 수 있으므로 복원 시도
        if moved_files:
            logger.warning("...하지만 일부 파일이 임시 폴더에 남아있을 수 있습니다. 복원을 시도합니다.")
            for dest_path, src_path in moved_files.items():
                try:
                    await await_sync(shutil.move, dest_path, src_path)
                    logger.info(f"    - 강제 복원 완료: '{os.path.basename(src_path)}'")
                except Exception as e_restore:
                    logger.error(f"    - 강제 복원 실패: {e_restore}")
        return True # 목표 달성으로 간주
    except Exception as e:
        logger.error(f"ID {item_id}: Plex Dance 중 심각한 오류 발생: {e}", exc_info=True)
        # 오류 발생 시에도 파일 복원 시도
        if moved_files:
            logger.error("...오류가 발생했지만 파일 복원을 시도합니다.")
            for dest_path, src_path in moved_files.items():
                try:
                    if os.path.exists(dest_path):
                        await await_sync(shutil.move, dest_path, src_path)
                        logger.info(f"    - 오류 후 복원 완료: '{os.path.basename(src_path)}'")
                except Exception as e_restore:
                    logger.error(f"    - 오류 후 복원 실패: {e_restore}")
        return False


async def run_fix_labels_mode(args):
    global CONFIG, SHUTDOWN_REQUESTED

    # --- 1. 루프 외부에서 초기 설정 수행 (한 번만 실행) ---
    lib_id_val = CONFIG.get("ID")
    logger.info(f"--fix-labels 모드 시작. 대상 라이브러리 ID: {lib_id_val}")
    logger.warning("이 모드는 각 아이템에 대해 사용자 확인을 거치며, 선택 시 기본적으로 강제 언매치 및 로컬 JSON 파일 삭제 후 재매칭을 시도합니다.")
    logger.info("다음 조건 중 하나 이상에 해당하는 아이템을 검토합니다:\n"
                "  1. GUID가 없거나, 'local://', 'com.plexapp.agents.none://' 등으로 시작하여 매칭이 완료되지 않은 아이템.\n"
                "  2. 제목 형식이 비정상적이어서 메타데이터가 완전히 적용되지 않은 것으로 추정되는 아이템 (예: '품번 / 정보 / 사이트코드' 형태).\n"
                f"  3. GUID가 SJVA 에이전트({CONFIG.get("SJVA_AGENT_GUID_PREFIX")})가 아닌 다른 에이전트로 매칭된 경우.\n"
                "  4. SJVA 에이전트로 매칭되었으나, 파일명 품번과 DB 제목(또는 정렬제목)에서 추출된 품번이 불일치하는 경우.\n"
                "  5. SJVA 에이전트로 매칭되었고 파일명 품번은 있으나, DB 제목에서 품번 추출이 안 되는 경우.")

    await await_sync(create_table_if_not_exists)

    # DB 쿼리와 파라미터 미리 준비
    query_params_list_fix = [str(lib_id_val)]
    base_query_fix = """
    SELECT 
        mi.id, mi.title, mi.guid, mi.library_section_id, mi.metadata_type,
        mi.year, mi.original_title, mi.title_sort, MIN(mp.file) AS file_path
    FROM metadata_items mi
    LEFT JOIN media_items mpi ON mpi.metadata_item_id = mi.id
    LEFT JOIN media_parts mp ON mp.media_item_id = mpi.id
    WHERE mi.library_section_id = ? AND mi.metadata_type = 1
    """

    include_keywords_list = CONFIG.get("INCLUDE", [])
    if include_keywords_list:
        include_conditions_sql = []
        for kw_include in include_keywords_list:
            include_conditions_sql.append("(mi.title LIKE ? OR mi.title_sort LIKE ?)")
            query_params_list_fix.extend([f'%{kw_include}%', f'%{kw_include}%'])
        if include_conditions_sql:
            base_query_fix += " AND (" + " OR ".join(include_conditions_sql) + ")"

    exclude_keywords_list = CONFIG.get("EXCLUDE", [])
    if exclude_keywords_list:
        for kw_exclude in exclude_keywords_list:
            base_query_fix += " AND (mi.title NOT LIKE ? AND mi.title_sort NOT LIKE ?)"
            query_params_list_fix.extend([f'%{kw_exclude}%', f'%{kw_exclude}%'])

    base_query_fix += " GROUP BY mi.id"
    db_query_template_fix = base_query_fix

    original_fix_labels_config = CONFIG.get("FIX_LABELS", False)
    original_manual_search_config = CONFIG.get("MANUAL_SEARCH", False)
    current_match_limit = CONFIG.get("MATCH_LIMIT", 0)
    processed_items_in_fix_mode = 0

    sjva_agent_guid_prefix_val = CONFIG.get("SJVA_AGENT_GUID_PREFIX")
    none_agent_guid_prefix_val = "com.plexapp.agents.none://"
    known_site_codes_for_incomplete_check = ["dmm", "mgs", "javbus", "javdb", "jav321", "1pondo", "heyzo", "caribbean", "10musume", "pacopacomama", "aventertainments", "fc2", "tokyo-hot"]

    # --- 2. 메인 인터랙션 루프 시작 ---
    while not SHUTDOWN_REQUESTED:
        if current_match_limit > 0 and processed_items_in_fix_mode >= current_match_limit:
            logger.info(f"--match-limit ({current_match_limit}개) 도달. 모드를 종료합니다.")
            break

        logger.info("\n검토 대상 아이템 목록을 새로고침합니다...")

        # --- 3. 매번 목록을 새로 생성하고 보여주기 ---
        all_items_to_check_rows = await await_sync(fetch_all, db_query_template_fix, tuple(query_params_list_fix))

        if not all_items_to_check_rows:
            logger.info("DB에서 검토할 아이템을 찾지 못했습니다. 모드를 종료합니다.")
            break

        items_for_user_review_data_fix = []
        for current_item_row_fix in all_items_to_check_rows:
            if SHUTDOWN_REQUESTED: break
            item_id_val_fix = current_item_row_fix.get('id')
            current_guid_val_fix = current_item_row_fix.get('guid', '')
            db_title_val_fix = current_item_row_fix.get('title', '')
            media_path_val_fix = current_item_row_fix.get('file_path')

            fn_pid_raw_val_fix, db_title_pid_raw_val_fix, fn_pid_parts_val_fix = None, None, None
            if media_path_val_fix:
                filename_pid_raw = extract_product_id_from_filename(os.path.basename(media_path_val_fix))
                if filename_pid_raw:
                    fn_pid_raw_val_fix = filename_pid_raw
                    pid_p_val = filename_pid_raw.upper().split('-', 1)
                    if len(pid_p_val) == 2 and pid_p_val[0] and pid_p_val[1]:
                        fn_pid_parts_val_fix = (pid_p_val[0], pid_p_val[1])

            db_title_pid_raw_val_fix = extract_product_id_from_title(db_title_val_fix, fn_pid_parts_val_fix)
            norm_filename_pid_val_fix = normalize_pid_for_comparison(fn_pid_raw_val_fix)
            norm_db_title_pid_val_fix = normalize_pid_for_comparison(db_title_pid_raw_val_fix)

            is_item_target_for_fix_review, reason_str_for_listing = False, "알 수 없는 사유"

            if not current_guid_val_fix:
                is_item_target_for_fix_review, reason_str_for_listing = True, "GUID 없음 (미매칭)"
            elif current_guid_val_fix.startswith('local://'):
                is_item_target_for_fix_review, reason_str_for_listing = True, "로컬 GUID"
            elif current_guid_val_fix.startswith(none_agent_guid_prefix_val):
                is_item_target_for_fix_review, reason_str_for_listing = True, "None 에이전트 (매치없음)"
            elif not current_guid_val_fix.startswith(sjva_agent_guid_prefix_val):
                is_item_target_for_fix_review, reason_str_for_listing = True, f"타 에이전트 매칭"
            elif current_guid_val_fix.startswith(sjva_agent_guid_prefix_val):
                is_incomplete_title_format = False
                if db_title_val_fix:
                    if not db_title_val_fix.startswith("[") and not db_title_val_fix.startswith("【"):
                        parts = db_title_val_fix.split('/')
                        if len(parts) >= 2:
                            last_part_trimmed = parts[-1].strip().lower()
                            first_part_trimmed = parts[0].strip()
                            is_valid_like_product_id_format = bool(re.match(r"^[A-Z0-9]{2,}(?:-?[A-Z0-9]+)*$", first_part_trimmed, re.IGNORECASE))
                            if is_valid_like_product_id_format and last_part_trimmed in known_site_codes_for_incomplete_check:
                                is_incomplete_title_format = True
                if is_incomplete_title_format:
                    is_item_target_for_fix_review, reason_str_for_listing = True, "제목 형식 비정상 (메타 미완료 추정)"
                elif norm_filename_pid_val_fix and norm_db_title_pid_val_fix and norm_filename_pid_val_fix != norm_db_title_pid_val_fix:
                    is_item_target_for_fix_review, reason_str_for_listing = True, "품번 불일치 (SJVA)"
                elif norm_filename_pid_val_fix and not norm_db_title_pid_val_fix:
                    is_item_target_for_fix_review, reason_str_for_listing = True, "DB 제목 품번 없음 (SJVA)"

            if is_item_target_for_fix_review:
                items_for_user_review_data_fix.append({
                    'id': item_id_val_fix,
                    'title': db_title_val_fix,
                    'guid': current_guid_val_fix,
                    'file_pid_raw': fn_pid_raw_val_fix,
                    'db_pid_raw': db_title_pid_raw_val_fix,
                    'norm_file_pid': norm_filename_pid_val_fix,
                    'norm_db_pid': norm_db_title_pid_val_fix,
                    'file_path': media_path_val_fix,
                    'reason': reason_str_for_listing,
                    'original_row': current_item_row_fix
                })

        if SHUTDOWN_REQUESTED: break

        if not items_for_user_review_data_fix:
            logger.info("조건에 맞는 검토 대상 아이템이 더 이상 없습니다. 모드를 종료합니다.")
            break

        logger.debug("--fix-labels: 'DB Label' 기준으로 목록을 자연 정렬합니다.")
        try:
            # 정렬 키: 1순위는 'norm_db_pid'(DB 품번), 2순위는 'norm_file_pid'(파일 품번), 3순위는 제목
            # 품번이 없는 경우(None)를 대비하여 빈 문자열('')로 처리
            items_for_user_review_data_fix.sort(key=lambda item: natural_sort_key(
                item.get('norm_db_pid') or item.get('norm_file_pid') or item.get('title') or ''
            ))
        except Exception as e_sort_fix:
            logger.error(f"--fix-labels: 목록 정렬 중 오류 발생: {e_sort_fix}. 정렬 없이 진행합니다.", exc_info=True)

        print(f"\n--- 품번 불일치 또는 미매칭 아이템 목록 ({len(items_for_user_review_data_fix)}개) ---")
        header_format_fix_new = "{idx:<5s} | {id:<7s} | {guid:<18s} | {dpl:<15s} | {fpl:<15s} | {title:<35s} | {reason:<25s}"
        print(header_format_fix_new.format(idx="No.", id="ID", guid="GUID", dpl="DB Label", fpl="File Label", title="Title", reason="Reason"))
        line_len_new = len(header_format_fix_new.format(idx="",id="",guid="",dpl="",fpl="",title="",reason="")) - 20
        print("-" * line_len_new)

        for idx, item_info_p in enumerate(items_for_user_review_data_fix):
            guid_to_display = item_info_p.get('guid', 'None')
            if guid_to_display.startswith(sjva_agent_guid_prefix_val): guid_to_display = guid_to_display.replace(sjva_agent_guid_prefix_val, "sjva:")
            elif guid_to_display.startswith(none_agent_guid_prefix_val): guid_to_display = guid_to_display.replace(none_agent_guid_prefix_val, "none:")
            if '?' in guid_to_display: guid_to_display = guid_to_display.split('?', 1)[0]

            title_val = item_info_p.get('title', '')
            reason_val = item_info_p.get('reason', '')

            print(header_format_fix_new.format(
                idx=str(idx + 1),
                id=str(item_info_p['id']),
                guid=guid_to_display[:16] + ".." if len(guid_to_display) > 18 else guid_to_display,
                dpl=(item_info_p.get('norm_db_pid') or 'N/A')[:13],
                fpl=(item_info_p.get('norm_file_pid') or 'N/A')[:13],
                title=(title_val[:33] + "..") if len(title_val) > 35 else title_val,
                reason=(reason_val[:23] + "..") if len(reason_val) > 25 else reason_val
            ))
        print("-" * line_len_new)

        # --- 4. 사용자 입력 및 처리 ---
        user_has_quit_interaction_fix = False
        selected_indices_to_process_fix_actual = set()
        user_input_raw_str_fix = ""
        try:
            user_input_raw_str_fix = await asyncio.get_running_loop().run_in_executor(
                None, input, "\n재매칭할 항목의 번호(들) (쉼표/하이픈 범위, 예: 1,3,5-7), 'all'(전체), 또는 'q'(모드종료): ")
            user_input_raw_str_fix = user_input_raw_str_fix.lower().strip()
        except (EOFError, KeyboardInterrupt):
            logger.warning("\n입력 중단. 모드를 종료합니다.")
            user_has_quit_interaction_fix = True
            break

        if user_input_raw_str_fix == 'q':
            user_has_quit_interaction_fix = True
            break
        if not user_input_raw_str_fix:
            continue

        is_input_valid_fix = True
        if user_input_raw_str_fix == 'all':
            selected_indices_to_process_fix_actual.update(range(len(items_for_user_review_data_fix)))
        else:
            input_parts_fix = user_input_raw_str_fix.split(',')
            for part_str_fix in input_parts_fix:
                part_str_fix = part_str_fix.strip()
                if '-' in part_str_fix:
                    try:
                        s_fix_str, e_fix_str = part_str_fix.split('-')
                        s_fix, e_fix = int(s_fix_str), int(e_fix_str)
                        if not (0 < s_fix <= e_fix <= len(items_for_user_review_data_fix)): raise ValueError("범위 초과")
                        selected_indices_to_process_fix_actual.update(range(s_fix - 1, e_fix))
                    except ValueError:
                        logger.warning(f"잘못된 범위: '{part_str_fix}'.")
                        is_input_valid_fix = False; break
                elif part_str_fix.isdigit():
                    try:
                        n_fix = int(part_str_fix)
                        if not (0 < n_fix <= len(items_for_user_review_data_fix)): raise ValueError("번호 초과")
                        selected_indices_to_process_fix_actual.add(n_fix - 1)
                    except ValueError:
                        logger.warning(f"잘못된 번호: '{part_str_fix}'.")
                        is_input_valid_fix = False; break
                else:
                    logger.warning(f"잘못된 입력 형식: '{part_str_fix}'.")
                    is_input_valid_fix = False; break

        if not is_input_valid_fix: continue
        if not selected_indices_to_process_fix_actual:
            print("선택된 항목이 없습니다. 다시 입력하거나 'q'로 종료하세요.")
            continue

        items_to_process_this_round_fix = [items_for_user_review_data_fix[i] for i in sorted(list(selected_indices_to_process_fix_actual))]

        match_mode_choice_fix = ''
        while not match_mode_choice_fix:
            if SHUTDOWN_REQUESTED or user_has_quit_interaction_fix: break
            try:
                prompt_msg_fix = (
                    f"\n선택된 {len(items_to_process_this_round_fix)}개 항목에 대해 어떤 방식으로 처리하시겠습니까?\n"
                    f"  (A) 자동 매칭 (언매치 후 재매칭)\n"
                    f"  (M) 수동 매칭 (언매치 후 재매칭)\n"
                    f"  (D) Plex Dance (라이브러리에서 완전히 제거 후 재추가)\n"
                    f"  (C) 취소 (항목 선택으로 돌아가기)\n"
                    f"입력 (A/M/D/C): "
                )
                match_mode_input_fix = await asyncio.get_running_loop().run_in_executor(None, input, prompt_msg_fix)
                match_mode_choice_fix = match_mode_input_fix.lower().strip()
                if match_mode_choice_fix not in ['a', 'm', 'd', 'c']:
                    logger.warning("잘못된 입력입니다. A, M, D, C 중 하나를 입력하세요.")
                    match_mode_choice_fix = ''
            except (EOFError, KeyboardInterrupt):
                user_has_quit_interaction_fix = True
                break

        if SHUTDOWN_REQUESTED or user_has_quit_interaction_fix: break
        if match_mode_choice_fix == 'c':
            logger.info("항목 처리 취소. 다시 항목을 선택하세요.")
            continue

        # --- D (Plex Dance) 옵션 처리 블록 추가 ---
        if match_mode_choice_fix == 'd':
            logger.info(f"--- [FIXLABELS] Plex Dance 모드로 {len(items_to_process_this_round_fix)}개 항목 처리 시작 ---")
            for item_data in items_to_process_this_round_fix:
                if SHUTDOWN_REQUESTED or user_has_quit_interaction_fix: break

                success = await run_plex_dance_for_item(item_data['id'])
                if success:
                    processed_items_in_fix_mode += 1

                # 다음 아이템 처리 전 딜레이
                if not SHUTDOWN_REQUESTED and not user_has_quit_interaction_fix:
                    match_interval_s = CONFIG.get("MATCH_INTERVAL", 1)
                    if match_interval_s > 0:
                        await asyncio.sleep(match_interval_s)

        # --- 수동 매칭 선택 (M) ---
        if match_mode_choice_fix == 'm':
            logger.info(f"--- [FIXLABELS] 수동 매칭 모드로 {len(items_to_process_this_round_fix)}개 항목 처리 시작 ---")
            CONFIG["MANUAL_SEARCH"] = True # 수동 매칭 시 manual=1 검색
            CONFIG["FIX_LABELS"] = True    # get_plex_matches가 manual=1로 검색하도록
            logger.debug(f"  임시 설정: CONFIG['MANUAL_SEARCH'] = True, CONFIG['FIX_LABELS'] = True (FIXLABELS 수동)")
            try:
                for item_data_fix_manual in items_to_process_this_round_fix:
                    if SHUTDOWN_REQUESTED or user_has_quit_interaction_fix: break

                    unmatch_call_ok_fix = await unmatch_plex_item(item_data_fix_manual['id'])
                    if not unmatch_call_ok_fix:
                        logger.error(f"  ID {item_data_fix_manual['id']}: 수동 매칭 전 언매치 실패.")
                        if not CONFIG.get("DRY_RUN"): await await_sync(record_completed_task, item_data_fix_manual['id'], status="FAILED_UNMATCH_FIXLABELS_MANUAL")
                        continue 
                    if SHUTDOWN_REQUESTED or user_has_quit_interaction_fix: break

                    rematch_success_fix, status_msg_fix = await run_manual_interactive_rematch_for_item(
                        item_data_fix_manual,
                        calling_mode_tag="FIXLABELS_MANUAL"
                    )
                    if status_msg_fix == "GLOBAL_QUIT_REQUESTED_BY_USER": user_has_quit_interaction_fix = True
                    if rematch_success_fix: processed_items_in_fix_mode += 1
                    if SHUTDOWN_REQUESTED or user_has_quit_interaction_fix: break
                    await asyncio.sleep(0.1)
            finally:
                CONFIG["MANUAL_SEARCH"] = original_manual_search_config # 원래대로 복원
                CONFIG["FIX_LABELS"] = original_fix_labels_config       # 원래대로 복원
                logger.debug(f"  설정 복원: CONFIG['MANUAL_SEARCH'] = {original_manual_search_config}, CONFIG['FIX_LABELS'] = {original_fix_labels_config}")

        # --- 자동 매칭 선택 (A) ---
        elif match_mode_choice_fix == 'a':
            logger.info(f"--- [FIXLABELS] 자동 매칭 모드로 {len(items_to_process_this_round_fix)}개 항목 처리 시작 ---")
            auto_match_results_info_fix = [] 
            items_for_manual_fallback_data_fix = []

            CONFIG["FIX_LABELS"] = False    # 자동 매칭 시 get_plex_matches가 manual=0으로 검색하도록
            CONFIG["MANUAL_SEARCH"] = False # 일반 자동 매칭과 동일하게
            logger.debug(f"  임시 설정: CONFIG['FIX_LABELS'] = False, CONFIG['MANUAL_SEARCH'] = False (FIXLABELS 자동)")

            total_selected_for_auto_fix = len(items_to_process_this_round_fix) # 자동 매칭 대상 총 아이템 수
            processed_count_in_auto_round_fix = 0 # 이번 자동 매칭 라운드에서 처리 시작된 아이템 수

            try:
                for item_idx_in_selection_auto_fix, item_data_auto_fix in enumerate(items_to_process_this_round_fix):
                    if SHUTDOWN_REQUESTED or user_has_quit_interaction_fix: break

                    # --match-limit 확인 (루프 시작 시)
                    if current_match_limit > 0 and processed_items_in_fix_mode >= current_match_limit:
                        logger.info(f"  --match-limit ({current_match_limit}개) 도달. 자동 매칭 중단 (fix-labels).")
                        break

                    processed_count_in_auto_round_fix += 1
                    percentage_auto_round_fix = (processed_count_in_auto_round_fix / total_selected_for_auto_fix) * 100 if total_selected_for_auto_fix > 0 else 0
                    logger.info(f"\033[36m[FIXLABELS-AUTO] 진행: {processed_count_in_auto_round_fix}/{total_selected_for_auto_fix} ({percentage_auto_round_fix:.1f}%) - ID {item_data_auto_fix['id']} 처리 시작...\033[0m")

                    item_id_auto_fix = item_data_auto_fix['id']
                    original_row_auto_fix = item_data_auto_fix['original_row']
                    original_file_pid_normalized_auto_fix = item_data_auto_fix.get('norm_file_pid')
                    original_file_pid_parts_for_extraction_auto_fix = None
                    if item_data_auto_fix.get('file_pid_raw'):
                        fn_parts_raw_auto_s_fix = item_data_auto_fix['file_pid_raw'].upper().split('-',1)
                        if len(fn_parts_raw_auto_s_fix) == 2 and fn_parts_raw_auto_s_fix[0] and fn_parts_raw_auto_s_fix[1]:
                            original_file_pid_parts_for_extraction_auto_fix = (fn_parts_raw_auto_s_fix[0], fn_parts_raw_auto_s_fix[1])

                    # logger.info(f">>> [FIXLABELS-AUTO] ID {item_id_auto_fix} ('{item_data_auto_fix['title']}') 처리 시작...")

                    unmatch_ok_auto_fix = await unmatch_plex_item(item_id_auto_fix)
                    if not unmatch_ok_auto_fix:
                        logger.error(f"  ID {item_id_auto_fix}: 자동 매칭 전 언매치 실패. 건너뜀.")
                        auto_match_results_info_fix.append({'id': item_id_auto_fix, 'success': False, 'status': 'FAILED_UNMATCH_FIXLABELS_AUTO', 'matched_guid': None, 'matched_title_pid_norm': None, 'original_file_pid_norm': original_file_pid_normalized_auto_fix})
                        continue
                    if SHUTDOWN_REQUESTED or user_has_quit_interaction_fix: break

                    try:
                        _ = await process_single_item_for_auto_rematch(original_row_auto_fix, f"FixLabels-AutoWorker-{item_id_auto_fix}")
                    except asyncio.CancelledError:
                        logger.warning(f"ID {item_id_auto_fix}: 자동 매칭 처리 중 명시적으로 취소되었습니다 (CancelledError 수신).")
                        if not CONFIG.get("DRY_RUN"): await await_sync(record_completed_task, item_id_auto_fix, status="CANCELLED_IN_FIXLABELS_AUTO")
                        pass # SHUTDOWN_REQUESTED는 내부에서 설정

                    if SHUTDOWN_REQUESTED or user_has_quit_interaction_fix: break

                    # --match-interval 적용 (다음 아이템 자동 처리 전)
                    is_last_in_auto_selection_fix = (item_idx_in_selection_auto_fix == len(items_to_process_this_round_fix) - 1)
                    if not SHUTDOWN_REQUESTED and not user_has_quit_interaction_fix and not is_last_in_auto_selection_fix:
                        if not (current_match_limit > 0 and processed_items_in_fix_mode >= current_match_limit): # 전체 match-limit도 확인
                            match_interval_s = CONFIG.get("MATCH_INTERVAL", 1)
                            if match_interval_s > 0:
                                logger.debug(f"  다음 항목 자동 처리 전 {match_interval_s}초 대기 (match-interval)...")
                                try:
                                    await asyncio.wait_for(asyncio.sleep(match_interval_s), timeout=match_interval_s + 0.5)
                                except asyncio.TimeoutError: pass
                                except asyncio.CancelledError: user_has_quit_interaction_fix = True; SHUTDOWN_REQUESTED = True; break

                    current_status_db_fix, matched_guid_db_fix, matched_title_db_fix = "UNKNOWN_AFTER_FIXAUTO", None, None
                    is_success_db_fix = False

                    completed_info_fix = await await_sync(get_completed_task_info, item_id_auto_fix)
                    if completed_info_fix:
                        current_status_db_fix = completed_info_fix['status']
                        matched_guid_db_fix = completed_info_fix['matched_guid']
                        if matched_guid_db_fix and ("COMPLETED" in current_status_db_fix or "DRYRUN" in current_status_db_fix):
                            is_success_db_fix = True
                            plex_db_row_auto_fix = await get_metadata_by_id(item_id_auto_fix)
                            if plex_db_row_auto_fix: matched_title_db_fix = plex_db_row_auto_fix['title']

                    matched_title_pid_norm_db_fix = None
                    if is_success_db_fix and matched_title_db_fix:
                        extracted_pid_fix = extract_product_id_from_title(matched_title_db_fix, original_file_pid_parts_for_extraction_auto_fix)
                        matched_title_pid_norm_db_fix = normalize_pid_for_comparison(extracted_pid_fix)

                    result_entry_fix = {
                        'id': item_id_auto_fix, 'success': is_success_db_fix, 'status': current_status_db_fix, 
                        'matched_guid': matched_guid_db_fix, 
                        'original_file_pid_norm': original_file_pid_normalized_auto_fix,
                        'matched_title_pid_norm': matched_title_pid_norm_db_fix,
                        'original_item_data': item_data_auto_fix 
                    }
                    auto_match_results_info_fix.append(result_entry_fix)

                    pid_mismatch_auto_fix = False
                    if is_success_db_fix:
                        if original_file_pid_normalized_auto_fix and matched_title_pid_norm_db_fix:
                            if original_file_pid_normalized_auto_fix != matched_title_pid_norm_db_fix: pid_mismatch_auto_fix = True
                        elif original_file_pid_normalized_auto_fix and not matched_title_pid_norm_db_fix:
                            pid_mismatch_auto_fix = True 

                    if not is_success_db_fix or pid_mismatch_auto_fix:
                        items_for_manual_fallback_data_fix.append(item_data_auto_fix)
                    elif is_success_db_fix and not pid_mismatch_auto_fix:
                        processed_items_in_fix_mode +=1

                if total_selected_for_auto_fix > 0 and not (SHUTDOWN_REQUESTED or user_has_quit_interaction_fix):
                    logger.info(f"\033[36m[FIXLABELS-AUTO] 진행: {processed_count_in_auto_round_fix}/{total_selected_for_auto_fix} (100.0%) 처리 완료.\033[0m")

                if SHUTDOWN_REQUESTED or user_has_quit_interaction_fix: # 루프 종료 후 확인
                    # pass
                    pass # finally에서 복원
            finally: # 자동매칭 임시 설정 복원
                CONFIG["FIX_LABELS"] = original_fix_labels_config
                CONFIG["MANUAL_SEARCH"] = original_manual_search_config
                logger.debug(f"  설정 복원: CONFIG['FIX_LABELS'] = {original_fix_labels_config}, CONFIG['MANUAL_SEARCH'] = {original_manual_search_config} (FIXLABELS 자동 후)")


            if SHUTDOWN_REQUESTED or user_has_quit_interaction_fix: break # 폴백 전 확인

            success_count_final_fix = sum(1 for r in auto_match_results_info_fix if r['success'] and not (r.get('original_file_pid_norm') and r.get('matched_title_pid_norm') and r['original_file_pid_norm'] != r['matched_title_pid_norm'] or (r.get('original_file_pid_norm') and not r.get('matched_title_pid_norm'))))
            fail_count_final_fix = sum(1 for r in auto_match_results_info_fix if not r['success'])
            pid_mismatch_count_final_fix = sum(1 for r in auto_match_results_info_fix if r['success'] and ((r.get('original_file_pid_norm') and r.get('matched_title_pid_norm') and r['original_file_pid_norm'] != r['matched_title_pid_norm']) or (r.get('original_file_pid_norm') and not r.get('matched_title_pid_norm'))))
            
            logger.info("--- [FIXLABELS] 자동 매칭 시도 결과 요약 ---")
            logger.info(f"  - 총 시도: {len(auto_match_results_info_fix)}건")
            logger.info(f"  - 성공 (품번 일치 또는 원본 품번 없음): {success_count_final_fix}건")
            logger.info(f"  - 성공 (품번 불일치 또는 매칭결과 품번추출실패): {pid_mismatch_count_final_fix}건 (수동 폴백 대상)")
            logger.info(f"  - 실패 (후보 없음 또는 오류): {fail_count_final_fix}건 (수동 폴백 대상)")

            seen_fallback_ids_fix = set()
            unique_items_for_manual_fallback_fix = []
            for item_fb_data_orig_fix in items_for_manual_fallback_data_fix:
                if item_fb_data_orig_fix['id'] not in seen_fallback_ids_fix:
                    unique_items_for_manual_fallback_fix.append(item_fb_data_orig_fix)
                    seen_fallback_ids_fix.add(item_fb_data_orig_fix['id'])

            if unique_items_for_manual_fallback_fix:
                logger.info(f"\n다음 {len(unique_items_for_manual_fallback_fix)}개 항목에 대해 수동 폴백 매칭을 시도할 수 있습니다:")
                for i_fb_fix, item_data_to_display_fb_fix in enumerate(unique_items_for_manual_fallback_fix):
                    reason_fb_str_fix = "자동매칭실패"
                    fb_item_result_detail_fix = next((r for r in auto_match_results_info_fix if r['id'] == item_data_to_display_fb_fix['id']), None)
                    if fb_item_result_detail_fix:
                        if fb_item_result_detail_fix['success']: 
                            reason_fb_str_fix = (f"품번불일치/추출실패 (파일:{fb_item_result_detail_fix.get('original_file_pid_norm','N/A')}, "
                                            f"매칭:{fb_item_result_detail_fix.get('matched_title_pid_norm','N/A')})")
                        else: 
                            reason_fb_str_fix = fb_item_result_detail_fix.get('status', '자동매칭실패')
                    logger.info(f"  {i_fb_fix+1}. ID: {item_data_to_display_fb_fix['id']}, Title: '{item_data_to_display_fb_fix['title'][:30]}...', 이유: {reason_fb_str_fix}")

                fallback_choice_input_fix = ''
                try:
                    fb_prompt_auto_fix = f"위 {len(unique_items_for_manual_fallback_fix)}개 항목에 대해 수동 매칭을 진행하시겠습니까? (Y/N): "
                    fallback_choice_input_fix = (await asyncio.get_running_loop().run_in_executor(None, input, fb_prompt_auto_fix)).lower().strip()
                except EOFError: user_has_quit_interaction_fix = True
                except Exception as e_fb_input_fix: logger.error(f"폴백 선택 입력 오류: {e_fb_input_fix}"); user_has_quit_interaction_fix = True

                if SHUTDOWN_REQUESTED or user_has_quit_interaction_fix: break

                if fallback_choice_input_fix == 'y':
                    logger.info(f"--- [FIXLABELS] 자동 매칭 후 수동 폴백 ({len(unique_items_for_manual_fallback_fix)}개 항목) ---")
                    CONFIG["MANUAL_SEARCH"] = True # 수동 폴백 시 manual=1 검색
                    CONFIG["FIX_LABELS"] = True    # get_plex_matches가 manual=1로 검색하도록
                    logger.debug(f"  임시 설정: CONFIG['MANUAL_SEARCH'] = True, CONFIG['FIX_LABELS'] = True (FIXLABELS 자동 후 폴백)")

                    total_selected_for_fallback_fix = len(unique_items_for_manual_fallback_fix)
                    processed_count_in_fallback_round_fix = 0

                    try:
                        for item_data_fb_manual_run_fix in unique_items_for_manual_fallback_fix:
                            if SHUTDOWN_REQUESTED or user_has_quit_interaction_fix: break

                            processed_count_in_fallback_round_fix += 1
                            percentage_fallback_round_fix = (processed_count_in_fallback_round_fix / total_selected_for_fallback_fix) * 100 if total_selected_for_fallback_fix > 0 else 0
                            logger.info(f"\033[36m[FIXLABELS-FALLBACK] 진행: {processed_count_in_fallback_round_fix}/{total_selected_for_fallback_fix} ({percentage_fallback_round_fix:.1f}%) - ID {item_data_fb_manual_run_fix['id']} 수동 폴백 시작...\033[0m")

                            unmatch_ok_fb_run_fix = await unmatch_plex_item(item_data_fb_manual_run_fix['id'])
                            if not unmatch_ok_fb_run_fix:
                                logger.error(f"  ID {item_data_fb_manual_run_fix['id']}: 수동 폴백 전 언매치 실패.")
                                if not CONFIG.get("DRY_RUN"): await await_sync(record_completed_task, item_data_fb_manual_run_fix['id'], status="FAILED_UNMATCH_FIXLABELS_AUTO_FB")
                                continue
                            if SHUTDOWN_REQUESTED or user_has_quit_interaction_fix: break

                            try:
                                rematch_success_fb_run_fix, status_msg_fb_fix = await run_manual_interactive_rematch_for_item(
                                    item_data_fb_manual_run_fix,
                                    calling_mode_tag="FIXLABELS_AUTO_FALLBACK"
                                )
                                if status_msg_fb_fix == "GLOBAL_QUIT_REQUESTED_BY_USER": user_has_quit_interaction_fix = True
                                if rematch_success_fb_run_fix: processed_items_in_fix_mode += 1

                            except asyncio.CancelledError:
                                logger.warning(f"ID {item_data_fb_manual_run_fix['id']}: 수동 폴백 처리 중 명시적으로 취소됨.")
                                if not CONFIG.get("DRY_RUN"): await await_sync(record_completed_task, item_data_fb_manual_run_fix['id'], status="CANCELLED_IN_FIXLABELS_FALLBACK")
                                pass
                            if SHUTDOWN_REQUESTED or user_has_quit_interaction_fix: break
                            await asyncio.sleep(0.1)
                    finally:
                        CONFIG["MANUAL_SEARCH"] = original_manual_search_config
                        CONFIG["FIX_LABELS"] = original_fix_labels_config
                        logger.debug(f"  설정 복원: CONFIG['MANUAL_SEARCH'] = {original_manual_search_config}, CONFIG['FIX_LABELS'] = {original_fix_labels_config} (FIXLABELS 폴백 후)")
                else:
                    logger.info("수동 폴백 매칭을 진행하지 않습니다.")
            else:
                logger.info("[FIXLABELS] 자동 매칭 후 추가 수동 폴백 대상이 없습니다.")

        if user_input_raw_str_fix == 'all' and (match_mode_choice_fix in ['a', 'm', 'd']):
            logger.info("'all' 옵션에 대한 처리가 완료되었습니다. --fix-labels 모드를 종료합니다.")
            user_has_quit_interaction_fix = True 
        else:
            # 'q'나 'c'가 아닌 유효한 작업이 수행되었을 경우
            if match_mode_choice_fix in ['a', 'm', 'd']:
                logger.info("선택한 항목에 대한 작업이 완료되었습니다. 목록을 새로고침하고 다음 작업을 준비합니다...")
                await asyncio.sleep(2)

        if user_has_quit_interaction_fix or SHUTDOWN_REQUESTED : break

    logger.info(f"--fix-labels 모드 완료 또는 중단됨. 총 {processed_items_in_fix_mode}개 아이템 재매칭 시도됨.")


async def run_no_poster_mode(args):
    global CONFIG, SHUTDOWN_REQUESTED

    # --- 1. 루프 외부에서 초기 설정 수행 ---
    lib_id_val = CONFIG.get("ID")
    if not lib_id_val:
        logger.error("--no-poster 모드에는 --section-id (라이브러리 ID)가 필수입니다.")
        return

    logger.info(f"--no-poster 모드 시작. 대상 라이브러리 ID: {lib_id_val}")
    
    query_no_poster_safe = """
    SELECT
        mi.id, mi.title, mi.guid, mi.library_section_id, mi.metadata_type,
        mi.year, mi.original_title, mi.title_sort, mi.user_thumb_url,
        MIN(mp.file) AS file_path
    FROM metadata_items mi
    LEFT JOIN media_items mpi ON mpi.metadata_item_id = mi.id
    LEFT JOIN media_parts mp ON mp.media_item_id = mpi.id
    WHERE
        mi.library_section_id = ? AND
        mi.metadata_type = 1 AND
        (
            mi.user_thumb_url IS NULL
            OR mi.user_thumb_url = ''
            OR mi.user_thumb_url NOT LIKE '%://%'
            OR mi.user_thumb_url LIKE 'media://%.bundle/Contents/Thumbnails/%'
        )
    GROUP BY mi.id
    ORDER BY mi.title_sort ASC
    """
    params_no_poster_safe = (str(lib_id_val),)
    
    processed_items_count = 0
    match_limit = CONFIG.get("MATCH_LIMIT", 0)
    original_manual_search_config = CONFIG.get("MANUAL_SEARCH", False)

    # --- 2. 메인 인터랙션 루프 시작 ---
    while not SHUTDOWN_REQUESTED:
        if match_limit > 0 and processed_items_count >= match_limit:
            logger.info(f"--match-limit ({match_limit}개) 도달. 모드를 종료합니다.")
            break

        logger.info("\n포스터 문제 아이템 목록을 새로고침합니다...")
        
        candidate_rows = await await_sync(fetch_all, query_no_poster_safe, params_no_poster_safe)

        if not candidate_rows:
            logger.info("포스터가 없거나 비정상적인 아이템이 더 이상 없습니다. 모드를 종료합니다.")
            break

        items_for_user_review = []
        for row in candidate_rows:
            reason = "알 수 없음"
            thumb_url = row.get('user_thumb_url', '')
            if not thumb_url: reason = "포스터 URL 없음"
            elif 'media://' in thumb_url: reason = "임시 썸네일 사용"
            elif '://' not in thumb_url: reason = "잘못된 URL 형식"

            file_path = row.get('file_path')
            file_pid_raw = extract_product_id_from_filename(os.path.basename(file_path)) if file_path else None

            items_for_user_review.append({
                'id': row['id'],
                'title': row.get('title', "제목 없음"),
                'guid': row.get('guid', "GUID 없음"),
                'reason': reason,
                'file_pid_raw': file_pid_raw,
                'norm_file_pid': normalize_pid_for_comparison(file_pid_raw),
                'original_row': row
            })

        print(f"\n--- 포스터 문제 아이템 목록 ({len(items_for_user_review)}개) ---")
        header_format = "{idx:<5s} | {id:<7s} | {reason:<18s} | {title:<45s} | {guid:<30s}"
        print(header_format.format(idx="No.", id="ID", reason="문제 사유", title="제목", guid="GUID"))
        print("-" * 130)

        for idx, item in enumerate(items_for_user_review):
            title_disp = item['title'][:48] + '..' if len(item['title']) > 50 else item['title']
            guid_disp = item['guid'][:40] + '..' if len(item['guid']) > 40 else item['guid']
            print(header_format.format(idx=str(idx + 1), id=str(item['id']), reason=item['reason'], title=title_disp, guid=guid_disp))

        print("-" * 130)

        # --- 3. 사용자 입력 받기 (아이템 선택) ---
        user_input = ""
        try:
            user_input = await asyncio.get_running_loop().run_in_executor(
                None, input, "\n처리할 항목의 번호(들) (쉼표/하이픈 범위), 'all'(전체), 또는 'q'(모드종료): ")
            user_input = user_input.lower().strip()
        except (EOFError, KeyboardInterrupt):
            logger.warning("\n입력 중단. 모드를 종료합니다.")
            break

        if user_input == 'q': break
        if not user_input: continue

        selected_indices = set()
        is_input_valid = True
        if user_input == 'all':
            selected_indices.update(range(len(items_for_user_review)))
        else:
            parts = user_input.split(',')
            for part in parts:
                part = part.strip()
                if '-' in part:
                    try:
                        start, end = map(int, part.split('-'))
                        if not (0 < start <= end <= len(items_for_user_review)): raise ValueError("범위 초과")
                        selected_indices.update(range(start - 1, end))
                    except ValueError:
                        logger.warning(f"잘못된 범위: '{part}'.")
                        is_input_valid = False; break
                elif part.isdigit():
                    try:
                        num = int(part)
                        if not (0 < num <= len(items_for_user_review)): raise ValueError("번호 초과")
                        selected_indices.add(num - 1)
                    except ValueError:
                        logger.warning(f"잘못된 번호: '{part}'.")
                        is_input_valid = False; break
                else:
                    logger.warning(f"잘못된 입력 형식: '{part}'.")
                    is_input_valid = False; break

        if not is_input_valid: continue

        items_to_process = [items_for_user_review[i] for i in sorted(list(selected_indices))]
        if not items_to_process: continue

        # --- 4. 처리 방식 선택 (A/M/D/C) ---
        match_mode_choice = ''
        user_has_quit_this_cycle = False
        while not match_mode_choice:
            if SHUTDOWN_REQUESTED: break
            try:
                prompt_msg = (
                    f"\n선택된 {len(items_to_process)}개 항목에 대해 어떤 방식으로 처리하시겠습니까?\n"
                    f"  (A) 자동 매칭 (언매치 후 재매칭)\n"
                    f"  (M) 수동 매칭 (언매치 후 재매칭)\n"
                    f"  (D) Plex Dance (라이브러리에서 완전히 제거 후 재추가)\n"
                    f"  (C) 취소 (항목 선택으로 돌아가기)\n"
                    f"입력 (A/M/D/C): "
                )
                mode_input = await asyncio.get_running_loop().run_in_executor(None, input, prompt_msg)
                match_mode_choice = mode_input.lower().strip()
                if match_mode_choice not in ['a', 'm', 'd', 'c']:
                    logger.warning("잘못된 입력입니다. A, M, D, C 중 하나를 입력하세요.")
                    match_mode_choice = ''
            except (EOFError, KeyboardInterrupt):
                user_has_quit_this_cycle = True
                break

        if SHUTDOWN_REQUESTED or user_has_quit_this_cycle: break
        if match_mode_choice == 'c': continue

        # --- 5. 선택된 항목 처리 ---
        logger.info(f"--- 선택된 {len(items_to_process)}개 항목에 대해 '{match_mode_choice.upper()}' 모드로 처리를 시작합니다 ---")

        for item_data in items_to_process:
            if SHUTDOWN_REQUESTED or user_has_quit_this_cycle: break
            if match_limit > 0 and processed_items_count >= match_limit: break

            item_id = item_data['id']
            item_title = item_data['title']
            logger.info(f"\n>>> ID {item_id} ('{item_title}') 처리 시작...")

            success = False
            if match_mode_choice == 'd':
                success = await run_plex_dance_for_item(item_id)
            elif match_mode_choice == 'm':
                unmatch_ok = await unmatch_plex_item(item_id)
                if unmatch_ok:
                    CONFIG["MANUAL_SEARCH"] = True
                    success, status_msg = await run_manual_interactive_rematch_for_item(item_data, "NO_POSTER_MANUAL")
                    CONFIG["MANUAL_SEARCH"] = original_manual_search_config
                    if status_msg == "GLOBAL_QUIT_REQUESTED_BY_USER": user_has_quit_this_cycle = True
                else:
                    logger.error(f"  ID {item_id}: 수동 매칭 전 언매치 실패.")
            elif match_mode_choice == 'a':
                unmatch_ok = await unmatch_plex_item(item_id)
                if unmatch_ok:
                    api_changed = await process_single_item_for_auto_rematch(item_data['original_row'], f"NoPoster-AutoWorker-{item_id}")
                    success = True 
                else:
                    logger.error(f"  ID {item_id}: 자동 매칭 전 언매치 실패.")

            if success:
                processed_items_count += 1

            if not SHUTDOWN_REQUESTED and not user_has_quit_this_cycle:
                await asyncio.sleep(CONFIG.get("MATCH_INTERVAL", 1))

        if user_input == 'all':
            logger.info("'all' 옵션 처리가 완료되었습니다. 모드를 종료합니다.")
            break
        else:
            logger.info("선택한 항목에 대한 작업이 완료되었습니다. 목록을 새로고침하고 다음 작업을 준비합니다...")
            try:
                await asyncio.sleep(3)
            except asyncio.CancelledError:
                break

    logger.info(f"--no-poster 모드 완료 또는 중단됨. 총 {processed_items_count}개 아이템 처리 시도됨.")


async def move_and_remove_item(item_id: int, target_dir: str) -> bool:
    """미디어 파일을 지정된 경로로 이동하고 Plex 라이브러리에서 아이템을 제거합니다."""
    global PLEX_SERVER_INSTANCE
    
    if not os.path.isdir(target_dir):
        logger.error(f"이동 대상 경로 '{target_dir}'가 존재하지 않거나 디렉터리가 아닙니다.")
        return False

    # 1. 이동할 파일 경로 가져오기
    source_path = await await_sync(get_media_file_path, item_id)
    if not source_path or not os.path.exists(source_path):
        logger.warning(f"ID {item_id}: 원본 미디어 파일을 찾을 수 없습니다. 경로: {source_path}. 이동 및 제거를 건너뜁니다.")
        # 파일이 없으면 라이브러리에서 아이템만 제거 시도
        try:
            if PLEX_SERVER_INSTANCE is None:
                PLEX_SERVER_INSTANCE = PlexServer(CONFIG["PLEX_URL"], CONFIG["PLEX_TOKEN"])
            item_to_delete = await await_sync(PLEX_SERVER_INSTANCE.fetchItem, item_id)
            await await_sync(item_to_delete.delete)
            logger.info(f"ID {item_id}: 파일은 없지만 라이브러리에서 아이템을 제거했습니다.")
        except Exception as e:
            logger.error(f"ID {item_id}: 파일 없는 아이템 제거 중 오류 발생: {e}")
        return True # 파일이 없는 것도 성공으로 간주

    filename = os.path.basename(source_path)
    target_path = os.path.join(target_dir, filename)

    # 2. 파일명 충돌 처리
    if os.path.exists(target_path):
        name, ext = os.path.splitext(filename)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        new_filename = f"{name}_{timestamp}{ext}"
        target_path = os.path.join(target_dir, new_filename)
        logger.warning(f"대상 경로에 파일이 이미 존재하여 새 이름으로 변경합니다: {new_filename}")

    # 3. 파일 이동
    try:
        logger.info(f"ID {item_id}: 파일 이동 중... \n  - 원본: {source_path}\n  - 대상: {target_path}")
        if not CONFIG.get("DRY_RUN"):
            await await_sync(shutil.move, source_path, target_path)
            logger.info(f"ID {item_id}: 파일 이동 성공.")
        else:
            logger.info(f"[DRY RUN] ID {item_id}: 파일 이동 시뮬레이션 완료.")
    except Exception as e:
        logger.error(f"ID {item_id}: 파일 이동 중 오류 발생: {e}", exc_info=True)
        return False

    # 4. Plex 라이브러리에서 아이템 제거
    try:
        logger.info(f"ID {item_id}: Plex 라이브러리에서 아이템 제거 중...")
        if not CONFIG.get("DRY_RUN"):
            if PLEX_SERVER_INSTANCE is None:
                PLEX_SERVER_INSTANCE = PlexServer(CONFIG["PLEX_URL"], CONFIG["PLEX_TOKEN"])

            item_to_delete = await await_sync(PLEX_SERVER_INSTANCE.fetchItem, item_id)
            await await_sync(item_to_delete.delete)
            logger.info(f"ID {item_id}: 라이브러리에서 아이템 제거 성공.")
        else:
            logger.info(f"[DRY RUN] ID {item_id}: 라이브러리 아이템 제거 시뮬레이션 완료.")
        return True
    except PlexApiNotFound:
        logger.warning(f"ID {item_id}: 라이브러리에서 아이템을 찾을 수 없습니다 (이미 제거됨).")
        return True
    except Exception as e:
        logger.error(f"ID {item_id}: 라이브러리에서 아이템 제거 중 오류 발생: {e}", exc_info=True)
        # 파일은 이미 이동되었을 수 있으므로 여기서 False를 반환할지 고민 필요
        # 일단 오류로 보고 False 반환
        return False


async def run_move_no_meta_mode(args):
    """메타데이터 없는 아이템을 찾아 이동/제거하는 인터랙티브 모드를 실행합니다."""
    global CONFIG, SHUTDOWN_REQUESTED

    lib_id = CONFIG.get("ID")
    move_path = CONFIG.get("MOVE_NO_META_PATH")

    if not lib_id:
        logger.error("--move-no-meta 모드에는 --section-id가 필수입니다.")
        return
    if not move_path:
        logger.error("--move-no-meta 모드를 사용하려면 설정 파일에 'MOVE_NO_META_PATH'를 지정해야 합니다.")
        return

    logger.info(f"--move-no-meta 모드 시작. 대상 라이브러리 ID: {lib_id}")
    logger.info(f"파일 이동 경로: {move_path}")
    logger.warning("이 모드는 식별된 파일들을 영구적으로 이동하고 라이브러리에서 제거합니다. 신중하게 진행하세요.")

    # 미매칭 아이템 식별 쿼리 (fix-labels와 유사)
    query = """
    SELECT mi.id, mi.title, mi.guid, MIN(mp.file) AS file_path
    FROM metadata_items mi
    LEFT JOIN media_items mpi ON mpi.metadata_item_id = mi.id
    LEFT JOIN media_parts mp ON mp.media_item_id = mpi.id
    WHERE mi.library_section_id = ? AND mi.metadata_type = 1
        AND (mi.guid IS NULL OR mi.guid = '' OR mi.guid LIKE 'local://%')
    GROUP BY mi.id
    ORDER BY mi.title_sort ASC
    """
    params = (str(lib_id),)

    processed_count = 0
    match_limit = CONFIG.get("MATCH_LIMIT", 0)

    while not SHUTDOWN_REQUESTED:
        if match_limit > 0 and processed_count >= match_limit:
            logger.info(f"--match-limit ({match_limit}개) 도달. 모드를 종료합니다.")
            break

        logger.info("\n이동 대상 아이템 목록을 새로고침합니다...")
        candidate_rows = await await_sync(fetch_all, query, params)

        if not candidate_rows:
            logger.info("이동할 대상 아이템이 더 이상 없습니다. 모드를 종료합니다.")
            break

        print(f"\n--- 미매칭 아이템 목록 ({len(candidate_rows)}개) ---")
        header = "{idx:<5s} | {id:<7s} | {title:<60s} | {guid}"
        print(header)
        print("-" * 120)
        for i, row in enumerate(candidate_rows):
            title_disp = row['title'][:58] + '..' if len(row['title']) > 60 else row['title']
            guid_disp = row.get('guid', 'GUID 없음')
            print(header.format(idx=str(i + 1), id=str(row['id']), title=title_disp, guid=guid_disp))
        print("-" * 120)

        # 사용자 입력 받기
        user_input = ""
        try:
            prompt = "\n이동할 항목의 번호(들) (쉼표/하이픈 범위), 'all'(전체), 또는 'q'(모드종료): "
            user_input = await asyncio.get_running_loop().run_in_executor(None, input, prompt)
            user_input = user_input.lower().strip()
        except (EOFError, KeyboardInterrupt):
            logger.warning("\n입력 중단. 모드를 종료합니다.")
            break

        if user_input == 'q': break
        if not user_input: continue

        selected_indices = set()

        is_input_valid = True
        if user_input == 'all':
            selected_indices.update(range(len(candidate_rows)))
        else:
            parts = user_input.split(',')
            for part in parts:
                part = part.strip()
                if '-' in part:
                    try:
                        start, end = map(int, part.split('-'))
                        if not (0 < start <= end <= len(candidate_rows)): raise ValueError("범위 초과")
                        selected_indices.update(range(start - 1, end))
                    except ValueError: is_input_valid = False; break
                elif part.isdigit():
                    try:
                        num = int(part)
                        if not (0 < num <= len(candidate_rows)): raise ValueError("번호 초과")
                        selected_indices.add(num - 1)
                    except ValueError: is_input_valid = False; break
                else: is_input_valid = False; break

        if not is_input_valid:
            logger.warning("잘못된 입력입니다. 다시 시도해주세요."); continue

        items_to_process = [candidate_rows[i] for i in sorted(list(selected_indices))]
        if not items_to_process: continue

        # 최종 확인
        final_confirm = ""
        try:
            final_confirm_prompt = f"\n경고: 선택된 {len(items_to_process)}개 아이템을 '{move_path}'로 이동하고 라이브러리에서 영구히 제거합니다. 계속하시겠습니까? (yes/no): "
            final_confirm = await asyncio.get_running_loop().run_in_executor(None, input, final_confirm_prompt)
            final_confirm = final_confirm.lower().strip()
        except (EOFError, KeyboardInterrupt): break

        if final_confirm != 'yes':
            logger.info("작업을 취소했습니다.")
            continue

        # 선택된 항목 처리
        logger.info(f"--- 선택된 {len(items_to_process)}개 항목에 대한 이동 및 제거 작업을 시작합니다 ---")
        for item_data in items_to_process:
            if SHUTDOWN_REQUESTED: break
            if match_limit > 0 and processed_count >= match_limit: break

            success = await move_and_remove_item(item_data['id'], move_path)
            if success:
                processed_count += 1

            if not SHUTDOWN_REQUESTED:
                await asyncio.sleep(0.5) # 각 작업 후 짧은 딜레이

        if user_input == 'all':
            logger.info("'all' 옵션 처리가 완료되었습니다. 모드를 종료합니다.")
            break

        if not SHUTDOWN_REQUESTED:
            logger.info("\n다음 선택을 위해 3초 후 목록을 새로고침합니다...")
            try: await asyncio.sleep(3)
            except asyncio.CancelledError: break

    logger.info(f"--move-no-meta 모드 완료 또는 중단됨. 총 {processed_count}개 아이템 처리 시도됨.")


async def run_force_complete_mode(args):
    """
    Plex 라이브러리에서 스크립트 DB에 '미완료' 상태인 아이템 목록을 보여주고,
    사용자 선택을 받아 '완료' 상태로 강제 처리합니다.
    """
    global CONFIG, SHUTDOWN_REQUESTED

    lib_id = CONFIG.get("ID")
    if not lib_id:
        logger.error("--force-complete 모드에는 --section-id가 필수입니다.")
        return

    logger.info(f"--force-complete 모드 시작. 대상 라이브러리 ID: {lib_id}")
    logger.info("Plex에 존재하지만 스크립트의 완료 DB에 기록되지 않은 항목을 찾습니다...")
    logger.info("이 기능은 사용자가 Plex UI에서 직접 수정한 항목을 스크립트가 더 이상 건드리지 않도록 할 때 유용합니다.")

    await await_sync(create_table_if_not_exists)

    processed_count = 0

    while not SHUTDOWN_REQUESTED:
        # 1. 대상 아이템 식별
        query = """
        SELECT id, title, guid, title_sort 
        FROM metadata_items 
        WHERE library_section_id = ? 
          AND (guid IS NULL OR guid NOT LIKE 'collection://%')
        """
        all_items_in_lib = await await_sync(fetch_all, query, (str(lib_id),))

        items_to_review = []
        for item in all_items_in_lib:
            if not await await_sync(is_task_completed, item['id']):
                items_to_review.append(item)

        if not items_to_review:
            logger.info("모든 항목이 이미 완료 처리되었거나, 라이브러리에 아이템이 없습니다. 모드를 종료합니다.")
            break

        try:
            items_to_review.sort(key=lambda x: natural_sort_key(x.get('title_sort') or x.get('title') or ''))
        except Exception as e_sort:
            logger.warning(f"목록 정렬 중 오류 발생: {e_sort}. 정렬 없이 진행합니다.")

        # 2. 인터랙티브 목록 제공
        print(f"\n--- 미완료 아이템 목록 ({len(items_to_review)}개) ---")
        header_format = "{idx:<5s} | {id:<7s} | {title:<50s} | {guid:<30s}"
        print(header_format.format(idx="No.", id="ID", title="Title", guid="GUID"))
        print("-" * 120)
        for i, item in enumerate(items_to_review):
            title_disp = (item.get('title') or '제목 없음')
            title_disp = title_disp[:58] + '..' if len(title_disp) > 60 else title_disp
            guid_disp = item.get('guid', 'GUID 없음')
            if '?' in guid_disp:
                guid_disp = guid_disp.split('?', 1)[0]
            guid_disp = guid_disp[:28] + '...' if len(guid_disp) > 30 else guid_disp
            print(header_format.format(idx=str(i + 1), id=str(item['id']), title=title_disp, guid=guid_disp))
        print("-" * 120)

        # 3. 사용자 입력 받기
        user_input = ""
        try:
            prompt = "\n완료 처리할 항목의 번호(들) (쉼표/하이픈, all, q): "
            user_input = await asyncio.get_running_loop().run_in_executor(None, input, prompt)
            user_input = user_input.lower().strip()
        except (EOFError, KeyboardInterrupt):
            logger.warning("\n입력 중단. 모드를 종료합니다."); break

        if user_input == 'q': break
        if not user_input: continue

        selected_indices = set()
        is_input_valid = True

        if user_input == 'all':
            selected_indices.update(range(len(items_to_review)))
        else:
            parts = user_input.split(',')
            for part in parts:
                part = part.strip()
                if '-' in part:
                    try:
                        start, end = map(int, part.split('-'))
                        if not (0 < start <= end <= len(items_to_review)): raise ValueError
                        selected_indices.update(range(start - 1, end))
                    except ValueError: is_input_valid = False; break
                elif part.isdigit():
                    try:
                        num = int(part)
                        if not (0 < num <= len(items_to_review)): raise ValueError
                        selected_indices.add(num - 1)
                    except ValueError: is_input_valid = False; break
                else: is_input_valid = False; break

        if not is_input_valid:
            logger.warning("잘못된 입력입니다. 다시 시도해주세요."); continue

        items_to_process = [items_to_review[i] for i in sorted(list(selected_indices))]
        if not items_to_process: continue

        # 4. 선택된 아이템 처리
        logger.info(f"--- 선택된 {len(items_to_process)}개 항목을 완료 처리합니다 ---")
        for item_data in items_to_process:
            if SHUTDOWN_REQUESTED: break
            item_id = item_data['id']
            current_guid = item_data.get('guid', 'N/A')
            status_message = "COMPLETED_MANUAL_OVERRIDE"

            if not CONFIG.get("DRY_RUN"):
                await await_sync(record_completed_task, item_id, status=status_message, matched_guid=current_guid)
                logger.info(f"ID {item_id}: 완료 DB에 '{status_message}'로 기록했습니다.")
            else:
                logger.info(f"[DRY RUN] ID {item_id}: 완료 DB에 기록 시뮬레이션.")
            processed_count += 1

        if user_input == 'all':
            logger.info("'all' 옵션 처리가 완료되었습니다. 모드를 종료합니다."); break

        logger.info("\n다음 선택을 위해 목록을 새로고침합니다...")
        await asyncio.sleep(1)

    logger.info(f"--force-complete 모드 완료. 총 {processed_count}개 아이템 처리됨.")


async def run_update_actors_mode(args):
    """
    Plex에 등록된 배우 이름이 외부 배우DB의 원어 이름과 일치하는 경우를 찾아
    한국어 이름으로 업데이트하도록 제안합니다.
    """
    global CONFIG, SHUTDOWN_REQUESTED, ACTORS_DB_CACHE

    lib_id = CONFIG.get("ID")
    if not lib_id:
        logger.error("--update-actors 모드에는 --section-id가 필수입니다.")
        return
    if not CONFIG.get("ACTORS_DB_PATH") or not os.path.exists(CONFIG.get("ACTORS_DB_PATH")):
        logger.error("ACTORS_DB_PATH가 설정되지 않았거나 파일이 존재하지 않습니다.")
        return

    # 모드 시작 시 캐시 초기화
    if not await await_sync(initialize_actors_cache):
        logger.error("배우 DB 캐시 초기화에 실패했습니다. --update-actors 모드를 종료합니다.")
        return

    logger.info(f"--update-actors 모드 시작. 대상 라이브러리 ID: {lib_id}")

    processed_count = 0
    match_limit = CONFIG.get("MATCH_LIMIT", 0)
    items_to_update = []

    while not SHUTDOWN_REQUESTED:
        if match_limit > 0 and processed_count >= match_limit:
            logger.info(f"--match-limit ({match_limit}개) 도달. 모드를 종료합니다.")
            break

        if not items_to_update:
            logger.info("\n업데이트가 필요한 아이템을 검색합니다... (라이브러리 크기에 따라 시간이 걸릴 수 있습니다)")
            query = "SELECT id, title, title_sort FROM metadata_items WHERE library_section_id = ? AND metadata_type = 1"
            all_items = await await_sync(fetch_all, query, (str(lib_id),))

            items_to_update_temp = []
            total_items = len(all_items)
            for i, item in enumerate(all_items):
                if SHUTDOWN_REQUESTED: break

                if (i > 0 and (i + 1) % 100 == 0) or i == total_items - 1:
                    logger.info(f"  ... {i+1}/{total_items}개 아이템 검사 완료")

                # plex_actors는 이제 ['배우1', '배우2'] 형태의 문자열 리스트
                plex_actors = await await_sync(get_actors_for_item_from_plex_db, item['id'])
                if not plex_actors: continue

                needs_update = False
                update_reasons = set()

                for plex_actor_name in plex_actors:
                    # Plex에 등록된 배우 이름(plex_actor_name)이 배우 DB의 "원어 이름" 중에 있는지 검색
                    actor_db_info = get_actor_info_from_jp_name(plex_actor_name)

                    # 찾았다면, 이 이름은 원어 이름이라는 뜻.
                    if actor_db_info:
                        db_kr_name = actor_db_info.get('inner_name_kr')

                        # DB에 등록된 한국어 이름이 있고, 그 이름이 현재 Plex에 등록된 원어 이름과 다르다면 업데이트 대상.
                        if db_kr_name and db_kr_name != plex_actor_name:
                            needs_update = True
                            update_reasons.add(f"'{plex_actor_name}' -> '{db_kr_name}'")

                if needs_update:
                    items_to_update_temp.append({
                        'id': item['id'],
                        'title': item['title'],
                        'reason': ", ".join(sorted(list(update_reasons)))
                    })

            items_to_update = items_to_update_temp

            if SHUTDOWN_REQUESTED: break
            if not items_to_update:
                logger.info("모든 아이템의 배우 정보가 최신입니다. 모드를 종료합니다.")
                break

            try:
                items_to_update.sort(key=lambda x: natural_sort_key(x.get('title') or ''))
            except Exception: pass

        # --- 인터랙티브 루프 ---
        while not SHUTDOWN_REQUESTED:
            print(f"\n--- 배우 정보 업데이트 대상 목록 ({len(items_to_update)}개) ---")
            header_format = "{idx:<5s} | {id:<7s} | {title:<60s} | {reason}"
            print(header_format.format(idx="No.", id="ID", title="Title", reason="Update Details"))
            print("-" * 130)
            for i, item in enumerate(items_to_update):
                title_disp = item['title'][:58] + '..' if len(item['title']) > 60 else item['title']
                reason_disp = item['reason'][:45] + '...' if len(item['reason']) > 45 else item['reason']
                print(header_format.format(idx=str(i + 1), id=str(item['id']), title=title_disp, reason=reason_disp))
            print("-" * 130)

            user_input = ""
            try:
                prompt = "\n새로고침할 항목 번호(들) (all, 목록 새로고침 R, 종료 Q): "
                user_input = (await asyncio.get_running_loop().run_in_executor(None, input, prompt)).lower().strip()
            except (EOFError, KeyboardInterrupt): user_input = 'q'

            if user_input == 'q':
                SHUTDOWN_REQUESTED = True; break
            if user_input == 'r':
                logger.info("목록을 새로고침합니다...")
                items_to_update = []
                break
            if not user_input: continue

            selected_indices = set()
            is_input_valid = True
            if user_input == 'all':
                selected_indices.update(range(len(items_to_update)))
            else:
                parts = user_input.split(',')
                for part in parts:
                    part = part.strip()
                    if '-' in part:
                        try:
                            start, end = map(int, part.split('-'))
                            if not (0 < start <= end <= len(items_to_update)): raise ValueError
                            selected_indices.update(range(start - 1, end))
                        except ValueError: is_input_valid = False; break
                    elif part.isdigit():
                        try:
                            num = int(part)
                            if not (0 < num <= len(items_to_update)): raise ValueError
                            selected_indices.add(num - 1)
                        except ValueError: is_input_valid = False; break
                    else: is_input_valid = False; break

            if not is_input_valid:
                logger.warning("잘못된 입력입니다. 다시 시도해주세요."); continue

            items_to_process = [items_to_update[i] for i in sorted(list(selected_indices))]
            if not items_to_process: continue

            logger.info(f"--- 선택된 {len(items_to_process)}개 항목에 대해 언매치를 통한 강제 메타데이터 갱신을 시작합니다 ---")

            items_processed_in_this_round_indices = []

            for item_data in items_to_process:
                if SHUTDOWN_REQUESTED: break
                if match_limit > 0 and processed_count >= match_limit: break

                success = await unmatch_and_verify_rematch(item_data['id'])

                if success:
                    processed_count += 1
                    # 성공한 항목의 원래 목록에서의 인덱스를 찾음
                    original_index = next((i for i, original_item in enumerate(items_to_update) if original_item['id'] == item_data['id']), None)
                    if original_index is not None:
                        items_processed_in_this_round_indices.append(original_index)

                if not SHUTDOWN_REQUESTED:
                    await asyncio.sleep(CONFIG.get("MATCH_INTERVAL", 1))

            # 처리된 항목들을 메인 목록에서 제거
            for index in sorted(items_processed_in_this_round_indices, reverse=True):
                del items_to_update[index]

            if user_input == 'all':
                logger.info("'all' 옵션 처리가 완료되었습니다. 목록을 새로고침합니다.")
                items_to_update = []
                break

            if not items_to_update:
                logger.info("목록의 모든 항목이 처리되었습니다. 새로고침합니다.")
                break

    logger.info(f"--update-actors 모드 완료. 총 {processed_count}개 아이템 새로고침 요청됨.")


async def unmatch_and_verify_rematch(item_id: int) -> bool:
    """
    아이템을 언매치하고, Plex가 자동으로 다시 매칭하여 메타데이터를
    완전히 업데이트할 때까지 기다리고 확인합니다.
    """
    logger.info(f"  ID {item_id}: 언매치를 통한 강제 메타데이터 갱신 시작...")
    
    # 1. 언매치 실행
    if not await unmatch_plex_item(item_id):
        logger.error(f"  ID {item_id}: 언매치 요청 실패. 갱신을 중단합니다.")
        return False

    if SHUTDOWN_REQUESTED: return False
    
    # 2. 자동 재매칭 후 업데이트 확인
    logger.info(f"  ID {item_id}: 언매치 완료. Plex의 자동 재매칭을 기다립니다...")
    start_timestamp = int(datetime.now().timestamp())
    
    rematch_confirmed = False
    for i in range(CONFIG.get("CHECK_COUNT", 10)):
        if SHUTDOWN_REQUESTED: break
        logger.debug(f"  ID {item_id}: 재매칭 확인 중... ({i+1}/{CONFIG.get('CHECK_COUNT', 10)})")
        if await check_plex_update(item_id, start_timestamp):
            rematch_confirmed = True
            break
        await asyncio.sleep(CONFIG.get("CHECK_INTERVAL", 4))

    if rematch_confirmed:
        logger.info(f"  ID {item_id}: 재매칭 및 메타데이터 업데이트를 성공적으로 확인했습니다.")
        return True

    if SHUTDOWN_REQUESTED: return False

    # 3. 자동 재매칭 미확인 시, 수동 새로고침 시도 (기존 rematch_plex_item 로직 재활용)
    logger.warning(f"  ID {item_id}: 자동 재매칭이 감지되지 않음. 강제 새로고침을 시도합니다...")
    if not await refresh_plex_item_metadata(item_id):
        logger.error(f"  ID {item_id}: 강제 새로고침 요청 실패.")
        return False

    logger.info(f"  ID {item_id}: 새로고침 요청 후 다시 업데이트를 확인합니다...")
    refresh_timestamp = int(datetime.now().timestamp())
    refresh_check_count = max(3, CONFIG.get("CHECK_COUNT", 10) // 2)

    for i in range(refresh_check_count):
        if SHUTDOWN_REQUESTED: break
        logger.debug(f"  ID {item_id}: 새로고침 후 확인 중... ({i+1}/{refresh_check_count})")
        if await check_plex_update(item_id, refresh_timestamp):
            logger.info(f"  ID {item_id}: 강제 새로고침 후 메타데이터 업데이트를 성공적으로 확인했습니다.")
            return True
        await asyncio.sleep(max(2, CONFIG.get("CHECK_INTERVAL", 4)))

    logger.error(f"  ID {item_id}: 모든 시도 후에도 메타데이터 업데이트를 최종 확인하지 못했습니다.")
    return False


async def run_find_dupes_mode(args):
    """라이브러리 내에서 중복된 품번을 가진 아이템을 찾아 사용자 조치를 받는 모드입니다."""
    global CONFIG, SHUTDOWN_REQUESTED, PLEX_SERVER_INSTANCE

    lib_id = CONFIG.get("ID")
    if not lib_id:
        logger.error("--find-dupes 모드에는 --section-id가 필수입니다.")
        return

    logger.info(f"--find-dupes 모드 시작. 대상 라이브러리 ID: {lib_id}")

    # 이 모드에서 처리된 아이템 수를 추적
    processed_items_count = 0
    match_limit = CONFIG.get("MATCH_LIMIT", 0)
    original_manual_search_config = CONFIG.get("MANUAL_SEARCH", False)

    # 루프를 추가하여 작업 후 목록을 새로고침하고 다시 선택할 수 있도록 함
    while not SHUTDOWN_REQUESTED:
        if match_limit > 0 and processed_items_count >= match_limit:
            logger.info(f"--match-limit ({match_limit}개) 도달. 모드를 종료합니다.")
            break

        logger.info("\n중복 아이템 목록을 새로고침합니다...")

        # 1. DB에서 모든 아이템 정보 가져오기
        query = """
        SELECT 
            mi.id, mi.title, mi.guid, mi.title_sort, mi.library_section_id, MIN(mp.file) AS file_path
        FROM metadata_items mi
        LEFT JOIN media_items mpi ON mpi.metadata_item_id = mi.id
        LEFT JOIN media_parts mp ON mp.media_item_id = mpi.id
        WHERE mi.library_section_id = ? AND mi.metadata_type = 1
        GROUP BY mi.id
        """
        all_items_in_lib = await await_sync(fetch_all, query, (str(lib_id),))

        if not all_items_in_lib:
            logger.info("라이브러리에 분석할 아이템이 없습니다."); break

        # 2. 품번 기준으로 아이템 그룹화
        items_by_pid = {}
        for item in all_items_in_lib:
            file_path = item.get('file_path')
            if not file_path: continue

            base_filename = os.path.basename(file_path)
            padded_pid = extract_product_id_from_filename(base_filename)
            normalized_pid = normalize_pid_for_comparison(padded_pid)

            if normalized_pid:
                if normalized_pid not in items_by_pid: items_by_pid[normalized_pid] = []
                items_by_pid[normalized_pid].append({
                    'id': item['id'],
                    'title': item['title'],
                    'guid': item['guid'],
                    'file_path': file_path,
                    'file_pid_raw': padded_pid,
                    'norm_file_pid': normalized_pid,
                    'original_row': item
                })

        # 3. 중복된 품번만 필터링 및 정렬
        duplicated_pids = {pid: items for pid, items in items_by_pid.items() if len(items) > 1}

        if not duplicated_pids:
            logger.info("분석 완료. 더 이상 중복된 품번이 없습니다. 모드를 종료합니다."); break

        sorted_dup_groups = sorted(duplicated_pids.values(), key=lambda items: natural_sort_key(items[0]['norm_file_pid']))

        # 4. 결과 출력
        print(f"\n--- 중복 품번 아이템 목록 ({len(sorted_dup_groups)} 그룹) ---")
        group_list_for_selection = []

        # 설정에서 SITE_CODE_MAP 가져오기
        site_code_map_from_config = CONFIG.get("SITE_CODE_MAP", {})
        base_sjva_agent_prefix = CONFIG.get("SJVA_AGENT_GUID_PREFIX")

        for i, group in enumerate(sorted_dup_groups):
            pid = group[0]['norm_file_pid']
            print(f"\n{i+1}. [품번: {pid}] - {len(group)}개 중복")
            group_list_for_selection.append(group)

            for item_info in group:
                item_guid = item_info.get('guid', '')
                display_guid = "No GUID"
                display_site = "N/A"

                if item_guid:
                    is_sjva_guid = False
                    site_code_map_from_config = CONFIG.get("SITE_CODE_MAP", {})
                    for site_key, full_prefixes_list in site_code_map_from_config.items():
                        if any(item_guid.startswith(p) for p in full_prefixes_list):
                            display_site = site_key.upper()

                            # 실제 일치한 접두사 찾기
                            matched_prefix = next(p for p in full_prefixes_list if item_guid.startswith(p))
                            display_guid = item_guid.replace(matched_prefix, "", 1)

                            is_sjva_guid = True
                            break # for-loop 탈출

                    if not is_sjva_guid and item_guid.startswith(base_sjva_agent_prefix):
                        display_site = "SJVA_ETC"
                        display_guid = item_guid.replace(base_sjva_agent_prefix, "", 1)
                    elif not is_sjva_guid:
                        if item_guid.startswith('local://'):
                            display_site = "LOCAL"
                            display_guid = item_guid
                        elif 'plex://' in item_guid:
                            display_site = "PLEX"
                            display_guid = item_guid.split('/')[-1]
                        else:
                            display_site = "OTHER"
                            display_guid = item_guid

                if '?' in display_guid:
                    display_guid = display_guid.split('?', 1)[0]

                # 보기 좋게 자르기
                guid_for_print = display_guid[:23] + '..' if len(display_guid) > 25 else display_guid
                title_disp = item_info['title'][:55] + '..' if len(item_info['title']) > 57 else item_info['title']
                print(f"   - ID: {item_info['id']:<7} | Site: {display_site:<7} | GUID: {guid_for_print:<25} | Title: {title_disp}")

        # 5. 사용자 입력 받기 (그룹 선택)
        user_input_str = ""
        try:
            prompt = "\n처리할 그룹의 번호(들) (쉼표/하이픈, all, q): "
            user_input_str = await asyncio.get_running_loop().run_in_executor(None, input, prompt)
            user_input_str = user_input_str.lower().strip()
        except (EOFError, KeyboardInterrupt):
            logger.warning("\n입력 중단. 모드를 종료합니다."); break

        if user_input_str == 'q': break
        if not user_input_str: continue

        selected_indices = set()
        is_input_valid = True
        if user_input_str == 'all':
            selected_indices.update(range(len(group_list_for_selection)))
        else:
            parts = user_input_str.split(',')
            for part in parts:
                part = part.strip()
                if '-' in part:
                    try:
                        start, end = map(int, part.split('-'))
                        if not (0 < start <= end <= len(group_list_for_selection)): raise ValueError("범위 초과")
                        selected_indices.update(range(start - 1, end))
                    except ValueError: is_input_valid = False; break
                elif part.isdigit():
                    try:
                        num = int(part)
                        if not (0 < num <= len(group_list_for_selection)): raise ValueError("번호 초과")
                        selected_indices.add(num - 1)
                    except ValueError: is_input_valid = False; break
                else: is_input_valid = False; break

        if not is_input_valid:
            logger.warning("잘못된 입력입니다. 다시 시도해주세요."); continue

        groups_to_process = [group_list_for_selection[i] for i in sorted(list(selected_indices))]
        if not groups_to_process: continue

        # 6. 사용자 입력 받기 (처리 방식 선택)
        action_choice = ''
        user_quit_action_select = False
        while not action_choice:
            if SHUTDOWN_REQUESTED: break
            try:
                prompt_msg = (
                    f"\n선택된 {len(groups_to_process)}개 그룹에 대해 어떤 작업을 하시겠습니까?\n"
                    f"  (M) 수동 병합/재매칭 (각 그룹을 하나씩 처리)\n"
                    f"  (D) Plex Dance (그룹 내 모든 아이템에 대해 실행)\n"
                    f"  (C) 취소 (그룹 선택으로 돌아가기)\n"
                    f"입력 (M/D/C): "
                )
                action_input = await asyncio.get_running_loop().run_in_executor(None, input, prompt_msg)
                action_choice = action_input.lower().strip()
                if action_choice not in ['m', 'd', 'c']:
                    logger.warning("잘못된 입력입니다."); action_choice = ''
            except (EOFError, KeyboardInterrupt):
                user_quit_action_select = True; break

        if SHUTDOWN_REQUESTED or user_quit_action_select: break
        if action_choice == 'c': continue

        # 7. 선택된 작업 수행
        for group in groups_to_process:
            if SHUTDOWN_REQUESTED or user_quit_action_select: break
            if match_limit > 0 and processed_items_count >= match_limit: break

            pid = group[0]['norm_file_pid']
            logger.info(f"\n>>> 그룹 '{pid}' 처리 시작... ({len(group)}개 아이템)")

            if action_choice == 'd':
                for item_data in group:
                    if SHUTDOWN_REQUESTED: break
                    success = await run_plex_dance_for_item(item_data['id'])
                    if success: processed_items_count += 1
                    await asyncio.sleep(CONFIG.get("MATCH_INTERVAL", 1))

            elif action_choice == 'm': # 수동 병합
                print(f"  그룹 '{pid}' 내 아이템들입니다. 하나를 기준으로 삼거나 모두 재매칭할 수 있습니다.")
                for i, item_data in enumerate(group):
                    item_guid_disp = item_data.get('guid', 'No GUID')
                    base_sjva_agent_prefix = CONFIG.get("SJVA_AGENT_GUID_PREFIX")

                    if base_sjva_agent_prefix and item_guid_disp.startswith(base_sjva_agent_prefix):
                        item_guid_disp = item_guid_disp.replace(base_sjva_agent_prefix, "", 1)

                    if '?' in item_guid_disp:
                        item_guid_disp = item_guid_disp.split('?', 1)[0]

                    item_guid_disp = item_guid_disp[:28] + '..' if len(item_guid_disp) > 30 else item_guid_disp
                    print(f"    {i+1}. ID: {item_data['id']:<7} | GUID: {item_guid_disp:<30} | File: {os.path.basename(item_data['file_path'])}")

                item_to_rematch = None
                while True:
                    if SHUTDOWN_REQUESTED: break
                    try:
                        choice_prompt = "  재매칭할 아이템 번호를 선택하세요 (모두 재매칭: all, 이 그룹 건너뛰기: s): "
                        item_choice_str = await asyncio.get_running_loop().run_in_executor(None, input, choice_prompt)
                        item_choice_str = item_choice_str.strip().lower()

                        if item_choice_str == 's':
                            item_to_rematch = None; break
                        elif item_choice_str == 'all':
                            item_to_rematch = group; break # 리스트 전체를 전달
                        elif item_choice_str.isdigit() and 0 < int(item_choice_str) <= len(group):
                            item_to_rematch = [group[int(item_choice_str) - 1]]; break # 리스트 형태로 전달
                        else:
                            logger.warning("  잘못된 입력입니다.")
                    except (EOFError, KeyboardInterrupt):
                        user_quit_action_select = True; break

                if SHUTDOWN_REQUESTED or user_quit_action_select: break
                if not item_to_rematch:
                    logger.info(f"  그룹 '{pid}'를 건너뜁니다."); continue

                # 선택된 아이템(들)에 대해 수동 재매칭 실행
                CONFIG["MANUAL_SEARCH"] = True
                try:
                    for item_data in item_to_rematch:
                        if SHUTDOWN_REQUESTED: break
                        unmatch_ok = await unmatch_plex_item(item_data['id'])
                        if unmatch_ok:
                            success, status_msg = await run_manual_interactive_rematch_for_item(item_data, "FIND_DUPES_MANUAL")
                            if status_msg == "GLOBAL_QUIT_REQUESTED_BY_USER": user_quit_action_select = True
                            if success: processed_items_count += 1
                        else:
                            logger.error(f"  ID {item_data['id']}: 수동 매칭 전 언매치 실패.")
                        await asyncio.sleep(CONFIG.get("MATCH_INTERVAL", 1))
                finally:
                    CONFIG["MANUAL_SEARCH"] = original_manual_search_config

        if user_input_str == 'all':
            logger.info("'all' 옵션에 대한 처리가 완료되었습니다. --find-dupes 모드를 종료합니다."); break
        else:
            logger.info("선택한 그룹에 대한 작업이 완료되었습니다. 목록을 새로고침하고 다음 작업을 준비합니다...")

    logger.info(f"--find-dupes 모드 완료. 총 {processed_items_count}개 아이템 처리 시도됨.")


async def main():
    global CONFIG, SHUTDOWN_REQUESTED
    parser = argparse.ArgumentParser(description="Plex 미디어 아이템 재매칭", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    info_group = parser.add_argument_group('정보 조회 옵션')
    info_group.add_argument("--check-id", "--list-sections", action="store_true", help="Plex DB의 모든 라이브러리 섹션 정보를 출력하고 종료합니다.")
    info_group.add_argument("--find-dupes", action="store_true", help="지정된 섹션 내에서 중복된 품번을 가진 아이템들을 찾아 목록으로 보여줍니다.")
    info_group.add_argument("-v", "--verbose", action="count", default=0, help="상세 로깅 수준을 높입니다 (-v: INFO, -vv: DEBUG).")

    rematch_group = parser.add_argument_group('재매칭 및 필터링 옵션')
    rematch_group.add_argument("--section-id", type=int, dest="id", help="[필수] 작업 대상 라이브러리 섹션 ID")
    rematch_group.add_argument("--config", type=str, default=DEFAULT_YAML_CONFIG_PATH, 
                               help=f"사용자 설정 YAML 파일 경로 (기본값: 스크립트 디렉터리 내 {os.path.basename(DEFAULT_YAML_CONFIG_PATH)})")
    # 나머지 rematch_group 인자들은 default를 FALLBACK_DEFAULT_CONFIG에서 가져오지 않고, 나중에 병합 시 처리
    rematch_group.add_argument("--include", action="append", default=[], metavar="KEYWORD", help="제목/정렬제목에 포함할 키워드 (여러 번 사용 시 OR 조건).")
    rematch_group.add_argument("--exclude", action="append", default=[], metavar="KEYWORD", help="제목/정렬제목에서 제외할 키워드 (여러 번 사용 시 AND 조건).")
    rematch_group.add_argument("--dry-run", action="store_true", help="실제 변경 없이 작업 내용만 출력")
    rematch_group.add_argument("--force-rematch", action="store_true", help="매칭 전 로컬 JSON 삭제 및 아이템 언매치 후 강제 재매칭 시도.")
    rematch_group.add_argument("--include-completed", action="store_true", help="이미 완료 DB에 기록된 아이템도 작업 대상에 포함합니다.")
    rematch_group.add_argument("--fix-labels", action="store_true", help="인터랙티브 품번 수정 모드.")
    rematch_group.add_argument("--no-poster", action="store_true", help="포스터가 없는 아이템을 찾아 인터랙티브 재매칭을 시작합니다.")
    rematch_group.add_argument("--manual-search", action="store_true", help="Plex '일치항목 검색' 시 수동 모드(manual=1)로 검색.")
    rematch_group.add_argument("--workers", type=int, help=f"동시에 실행할 작업자 수 (기본값: YAML 또는 {FALLBACK_DEFAULT_CONFIG['WORKERS']})")
    rematch_group.add_argument("--score-min", type=int, help=f"Plex API 매칭 후보의 최소 점수 (0-100, 기본값: YAML 또는 {FALLBACK_DEFAULT_CONFIG['SCORE_MIN']})")
    rematch_group.add_argument("--plex-db", type=str, help="Plex DB 파일 경로 (YAML에서 로드 가능)")
    rematch_group.add_argument("--plex-url", type=str, help="Plex 서버 URL (YAML에서 로드 가능)")
    rematch_group.add_argument("--plex-token", type=str, help="Plex API 토큰 (YAML에서 로드 가능)")
    rematch_group.add_argument("--completion-db", type=str, help=f"완료 작업 기록 DB 파일 경로 (기본값: YAML 또는 {FALLBACK_DEFAULT_CONFIG['COMPLETION_DB']})")
    rematch_group.add_argument("--numeric-padding-length", type=int, help=f"품번 숫자 부분 0-패딩 목표 길이 (기본값: YAML 또는 {FALLBACK_DEFAULT_CONFIG['NUMERIC_PADDING_LENGTH']})")
    rematch_group.add_argument("--check-count", type=int, help=f"메타 업데이트 후 반영 확인 시도 횟수 (기본값: YAML 또는 {FALLBACK_DEFAULT_CONFIG['CHECK_COUNT']})")
    rematch_group.add_argument("--check-interval", type=int, help=f"메타 업데이트 후 반영 확인 간격 (초) (기본값: YAML 또는 {FALLBACK_DEFAULT_CONFIG['CHECK_INTERVAL']})")
    rematch_group.add_argument("--timeout-get", type=int, dest='requests_timeout_get', help=f"GET 요청 타임아웃 (초) (기본값: YAML 또는 {FALLBACK_DEFAULT_CONFIG['REQUESTS_TIMEOUT_GET']})")
    rematch_group.add_argument("--timeout-put", type=int, dest='requests_timeout_put', help=f"PUT 요청 타임아웃 (초) (기본값: YAML 또는 {FALLBACK_DEFAULT_CONFIG['REQUESTS_TIMEOUT_PUT']})")
    rematch_group.add_argument("--match-limit", type=int, help=f"처리할 최대 아이템 개수 (0이면 무제한, 기본값: YAML 또는 {FALLBACK_DEFAULT_CONFIG['MATCH_LIMIT']})")
    rematch_group.add_argument("--match-interval", type=int, help=f"각 아이템 처리 후 대기 시간 (초) (기본값: YAML 또는 {FALLBACK_DEFAULT_CONFIG['MATCH_INTERVAL']})")
    rematch_group.add_argument("--move-no-meta", action="store_true", help="메타데이터가 없는 아이템을 지정된 경로로 이동하고 라이브러리에서 제거합니다.")
    rematch_group.add_argument("--force-complete", action="store_true", help="미완료 리스트에서 선택 항목을 '완료' 상태로 강제 처리합니다.")
    rematch_group.add_argument("--update-actors", action="store_true", help="외부 배우 DB와 비교하여 업데이트 된 배우정보로 새로고침합니다.")

    scan_group = parser.add_argument_group('라이브러리 스캔 옵션')
    scan_group.add_argument("--scan-full", action="store_true", help="섹션 경로를 정해진 depth로 분할하여 순차적으로 스캔합니다.")
    scan_group.add_argument("--scan-path", type=str, metavar="PATH", help="지정된 특정 경로만 스캔합니다.")
    scan_group.add_argument("--scan-no-wait", action="store_true", help="--scan-path와 함께 사용. Plex의 현재 작업 상태를 확인하지 않고 즉시 스캔을 요청하고 종료합니다.")
    scan_group.add_argument("--scan-depth", type=int, help=f"분할 스캔 시 탐색할 하위 디렉터리 깊이 (기본값: YAML 또는 {FALLBACK_DEFAULT_CONFIG['SCAN_DEPTH']})")

    search_exclusive_group = parser.add_argument_group('검색 전용 옵션 (정보 조회 후 종료)')
    search_exclusive_group.add_argument("--search", type=str, metavar="KEYWORD", help="검색할 키워드.")
    search_exclusive_group.add_argument("--search-field", type=str, choices=['title', 'label', 'site'], default='title', help="--search 옵션 사용 시 검색 대상 필드.")

    args = parser.parse_args()

    # asyncio 이벤트 루프 설정 (signal 핸들러 등록을 위해 먼저 필요)
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    # 종료 신호(Ctrl+C 등) 핸들러 등록
    for sig_name_enum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig_name_enum, functools.partial(handle_signal, sig_name_enum, None))
        except (NotImplementedError, AttributeError): # Windows 등 일부 환경에서는 미지원
            signal.signal(sig_name_enum, functools.partial(handle_signal, sig_name_enum, None))

    # 로깅 설정 (우선 실행)
    setup_logging(args.verbose)

    # YAML 설정 파일 로드
    yaml_config_data = load_yaml_config(args.config)

    # YAML 파일의 절대 경로 (args.config는 사용자가 제공한 경로일 수 있음)
    yaml_file_abs_path = os.path.abspath(args.config)
    yaml_file_dir = os.path.dirname(yaml_file_abs_path)

    # 경로 관련 설정 키 목록 (예시)
    path_keys_in_config = ["COMPLETION_DB", "PLEX_DB"]

    for key in path_keys_in_config:
        if key in yaml_config_data and yaml_config_data[key] is not None:
            original_path = yaml_config_data[key]
            if not os.path.isabs(original_path):
                resolved_path = os.path.abspath(os.path.join(yaml_file_dir, original_path))
                logger.debug(f"YAML 내 상대 경로 '{original_path}' (키: {key})를 '{resolved_path}' (절대 경로)로 변환합니다.")
                yaml_config_data[key] = resolved_path

    # 설정 병합: 1. FALLBACK -> 2. YAML -> 3. CLI
    merged_config = FALLBACK_DEFAULT_CONFIG.copy()

    if yaml_config_data:
        for key, value in yaml_config_data.items():
            if value is not None:
                merged_config[key.upper()] = value

    for arg_key_from_parser, arg_value_from_parser in vars(args).items():
        config_key_target = arg_key_from_parser.upper()

        is_cli_option_provided = False
        if hasattr(parser, 'get_default'):
            if arg_value_from_parser != parser.get_default(arg_key_from_parser):
                is_cli_option_provided = True
        elif arg_value_from_parser is not None:
            if isinstance(getattr(parser._actions_map.get(f'--{arg_key_from_parser.replace("_", "-")}', None), 'const', None), bool):
                if arg_value_from_parser:
                    is_cli_option_provided = True
            elif arg_value_from_parser is not None:
                is_cli_option_provided = True

        if is_cli_option_provided:
            if arg_key_from_parser not in ["verbose", "config", "check_id"]:
                merged_config[config_key_target] = arg_value_from_parser
        elif config_key_target not in merged_config:
            pass

    CONFIG.update(merged_config)

    try:
        guid_prefix = CONFIG.get("SJVA_AGENT_GUID_PREFIX", "com.plexapp.agents.sjva_agent://")
        simple_site_map = CONFIG.get("SITE_CODE_MAP", {})

        full_site_code_map = {
            key: [guid_prefix + code for code in codes]
            for key, codes in simple_site_map.items()
        }

        CONFIG["SITE_CODE_MAP"] = full_site_code_map
        logger.debug("SITE_CODE_MAP이 GUID 접두사와 조합되어 재구성되었습니다.")

    except Exception as e:
        logger.error(f"SITE_CODE_MAP 재구성 중 오류 발생: {e}", exc_info=True)
        sys.exit(1)

    compile_parsing_rules()

    # --check-id 옵션 처리 (설정 로드 후)
    if args.check_id:
        logger.info("Plex 라이브러리 섹션 정보 조회 중...")
        try:
            db_path_for_listing = CONFIG.get("PLEX_DB")
            if not db_path_for_listing:
                logger.critical("오류: Plex DB 경로가 설정되지 않았습니다. YAML 설정 파일이나 --plex-db 옵션으로 경로를 지정해주세요.")
                sys.exit(1)

            # DB 경로를 사용하도록 네임스페이스 전달
            temp_args_for_db_path = argparse.Namespace(plex_db=db_path_for_listing)
            sections_list = list_library_sections_from_db(args_namespace=temp_args_for_db_path)

            if not sections_list:
                logger.info("라이브러리 섹션 정보를 찾을 수 없습니다.")
                sys.exit(0)

            print("\n--- Plex 라이브러리 섹션 목록 ---")
            header_str = f"{'ID':<5} | {'이름':<30} | {'타입':<15} | {'에이전트':<50} | {'언어':<5}"
            print(header_str); print("-" * (len(header_str) + 5))

            for sec_dict_row in sections_list:
                s_type_code = sec_dict_row['section_type'] if 'section_type' in sec_dict_row.keys() else None
                s_type_str = PLEX_SECTION_TYPE_VALUES.get(s_type_code, str(s_type_code))
                s_name_val = sec_dict_row['name'] if 'name' in sec_dict_row.keys() and sec_dict_row['name'] else ''
                s_name_str = (s_name_val[:27]+'...') if len(s_name_val) > 30 else s_name_val
                section_agent_val = sec_dict_row['agent'] if 'agent' in sec_dict_row.keys() and sec_dict_row['agent'] is not None else ''
                section_lang_val = sec_dict_row['language'] if 'language' in sec_dict_row.keys() and sec_dict_row['language'] is not None else 'N/A'
                s_agent_str = (section_agent_val[:47]+'...') if len(section_agent_val) > 50 else section_agent_val
                s_lang_str = section_lang_val
                print(f"{sec_dict_row['id']:<5} | {s_name_str:<30} | {s_type_str:<15} | {s_agent_str:<50} | {s_lang_str:<5}")
            print("-" * (len(header_str) + 5)); print("\n")
        except Exception as e_list_sections:
            logger.error(f"섹션 정보 조회 오류: {e_list_sections}", exc_info=True)
        sys.exit(0)

    # 필수 설정값 (PLEX_DB, PLEX_TOKEN) 확인
    if not CONFIG.get("PLEX_DB"):
        logger.critical("오류: PLEX_DB 경로가 설정되지 않았습니다. YAML 파일 또는 --plex-db 옵션으로 지정해주세요.")
        sys.exit(1)
    if not CONFIG.get("PLEX_TOKEN") or "_YOUR_PLEX_TOKEN_" in CONFIG.get("PLEX_TOKEN",""):
        logger.critical("오류: PLEX_TOKEN이 설정되지 않았거나 기본값입니다. YAML 파일 또는 --plex-token 옵션으로 설정해주세요.")
        sys.exit(1)

    # --id (라이브러리 ID)는 여전히 필수 (단, --check-id 제외)
    if not args.check_id and CONFIG.get("ID") is None:
        logger.critical("--section-id 옵션은 라이브러리 재매칭 작업에 필수입니다.")
        sys.exit("--section-id 옵션이 필요합니다. (라이브러리 ID)")

    # 작업 대상 라이브러리 정보 출력
    target_lib_id_val = CONFIG.get("ID")
    if target_lib_id_val:
        logger.info(f"\n--- 작업 대상 라이브러리 섹션 정보 (ID: {target_lib_id_val}) ---")
        try:
            section_data_obj = await get_section_by_id(target_lib_id_val)
            if section_data_obj:
                s_name_disp = section_data_obj['name'] if 'name' in section_data_obj.keys() else "N/A"
                s_type_code_val = section_data_obj['section_type'] if 'section_type' in section_data_obj.keys() else None
                s_type_name_disp = PLEX_SECTION_TYPE_VALUES.get(s_type_code_val, f"Unknown ({s_type_code_val})")
                s_agent_name_disp = section_data_obj['agent'] if 'agent' in section_data_obj.keys() and section_data_obj['agent'] is not None else "N/A"

                logger.info(f"  이름  : {s_name_disp}")
                logger.info(f"  타입  : {s_type_name_disp}")
                logger.info(f"  에이전트: {s_agent_name_disp}")
            else: 
                logger.warning(f"  ID {target_lib_id_val}에 해당하는 라이브러리 섹션 정보를 찾을 수 없습니다.")
        except Exception as e_get_section: 
            logger.error(f"  라이브러리 섹션 정보 조회 중 오류 발생: {e_get_section}", exc_info=True)
        logger.info("--------------------------------------------------")

    try:
        if CONFIG.get("FORCE_COMPLETE"):
            await run_force_complete_mode(args)
            return

        if CONFIG.get("FIND_DUPES"):
            await run_find_dupes_mode(args)
            return

        if CONFIG.get("UPDATE_ACTORS"):
            await run_update_actors_mode(args)
            return

        if CONFIG.get("SCAN_FULL") or CONFIG.get("SCAN_PATH"):
            if not PLEXAPI_AVAILABLE:
                logger.critical("plexapi 라이브러리가 없어 스캔 기능을 실행할 수 없습니다. 'pip install plexapi'로 설치해주세요.")
                return
            await run_scan_mode(args)
            return

        if CONFIG.get("MOVE_NO_META"):
            await run_move_no_meta_mode(args)

        if CONFIG.get("FIX_LABELS"):
            await run_fix_labels_mode(args)

        elif CONFIG.get("NO_POSTER"):
            await run_no_poster_mode(args)

        elif CONFIG.get("SEARCH"):
            if not CONFIG.get("ID"):
                logger.critical("--search 옵션 사용 시 --id (라이브러리 ID)도 함께 지정해야 합니다.")
                sys.exit("--id 옵션 필요 (검색 대상 라이브러리 ID)")
            await run_interactive_search_and_rematch_mode(args)
            sys.exit(0)

        else: # 자동 재매칭 모드 (기본)
            await run_auto_rematch_mode(args)
    except asyncio.CancelledError:
        logger.warning("메인 작업 루프가 외부 신호 또는 내부 로직에 의해 명시적으로 취소되었습니다.")
    finally:
        if SHUTDOWN_REQUESTED:
            logger.info("애플리케이션 종료 절차가 진행되었으며, 현재 모든 작업을 중단/완료했습니다.")
        logger.info("스크립트 실행을 최종적으로 종료합니다.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n프로그램이 사용자에 의해 강제 종료되었습니다 (KeyboardInterrupt - 외부).")
    except SystemExit:
        pass # argparse의 error() 호출 시 또는 명시적 sys.exit() 시 발생
    except asyncio.CancelledError:
        print(f"\n프로그램이 비동기 작업 취소로 인해 종료되었습니다 (최상위).")
    except Exception as e_top_level:
        print(f"스크립트 최상위 레벨에서 처리되지 않은 심각한 오류 발생: {e_top_level}\n{traceback.format_exc()}")
