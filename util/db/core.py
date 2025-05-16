import json
from typing import Dict, List, Any, Optional, Union

from .validators import is_safe_query
from .mysql_handler import handle_mysql_query
from .postgresql_handler import handle_postgresql_query
from .oracle_handler import handle_oracle_query
from .mongodb_handler import handle_mongodb_query
from .redis_handler import handle_redis_query

def execute_database_query(
    db_type: str,
    connection_params: Dict[str, Any],
    query: str,
    params: Optional[Union[List, Dict]] = None,
    options: Optional[Dict[str, Any]] = None
) -> str:
    """
    여러 데이터베이스 시스템에서 쿼리를 실행하고 결과를 반환합니다.
    
    Args:
        db_type: 데이터베이스 유형 ('mysql', 'postgresql', 'oracle', 'mongodb', 'redis')
        connection_params: 데이터베이스 연결 정보
        query: 실행할 쿼리 또는 명령어
        params: 쿼리 파라미터 (선택 사항)
        options: 추가 옵션 (선택 사항)
        
    Returns:
        쿼리 실행 결과 (JSON 문자열)
    """
    # 기본 옵션 설정
    default_options = {
        "max_rows": 1000,  # 최대 반환 행 수
        "timeout": 30,     # 쿼리 타임아웃(초)
        "safe_mode": True  # 안전 모드 (위험한 쿼리 방지)
    }
    
    if options is None:
        options = {}
        
    # 기본 옵션과 사용자 옵션 병합
    for key, value in default_options.items():
        if key not in options:
            options[key] = value
    
    # 안전 모드가 활성화되어 있으면 쿼리 검증
    if options["safe_mode"] and db_type not in ["mongodb", "redis"]:
        if not is_safe_query(query):
            return json.dumps({
                "success": False, 
                "error": "Potentially unsafe query detected. Disable safe_mode if you want to run this query."
            })
            
    try:
        # 데이터베이스 유형에 따라 적절한 핸들러 호출
        if db_type.lower() == "mysql":
            return handle_mysql_query(connection_params, query, params, options)
        elif db_type.lower() == "postgresql":
            return handle_postgresql_query(connection_params, query, params, options)
        elif db_type.lower() == "oracle":
            return handle_oracle_query(connection_params, query, params, options)
        elif db_type.lower() == "mongodb":
            return handle_mongodb_query(connection_params, query, params, options)
        elif db_type.lower() == "redis":
            return handle_redis_query(connection_params, query, params, options)
        else:
            return json.dumps({
                "success": False,
                "error": f"Unsupported database type: {db_type}"
            })
            
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        })
