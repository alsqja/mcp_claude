import json
from typing import Dict, List, Any, Optional, Union

def handle_postgresql_query(connection_params: Dict[str, Any], query: str, params: Optional[Union[List, Dict]], options: Dict[str, Any]) -> str:
    """PostgreSQL 쿼리 실행 및 결과 반환"""
    
    import psycopg2
    from psycopg2 import Error
    from psycopg2.extras import RealDictCursor
    
    conn = None
    cursor = None
    
    try:
        # 연결 파라미터 구성
        connect_params = {
            "host": connection_params.get("host", "localhost"),
            "user": connection_params.get("user", "postgres"),
            "password": connection_params.get("password", ""),
            "dbname": connection_params.get("database", "")
        }
        
        # 포트가 명시되었으면 추가
        if "port" in connection_params:
            connect_params["port"] = connection_params["port"]
            
        # 연결 타임아웃 설정
        connect_params["connect_timeout"] = options.get("timeout", 30)
        
        # 데이터베이스 연결
        conn = psycopg2.connect(**connect_params)
        cursor = conn.cursor(cursor_factory=RealDictCursor)  # 결과를 딕셔너리로 반환
        
        # 쿼리 실행
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
            
        # SELECT 쿼리인 경우 결과 반환
        if query.strip().upper().startswith("SELECT"):
            results = cursor.fetchmany(options["max_rows"])
            
            # RealDictRow 객체를 일반 딕셔너리로 변환
            dict_results = [dict(row) for row in results]
            
            return json.dumps({
                "success": True, 
                "count": len(dict_results),
                "max_rows_reached": len(dict_results) >= options["max_rows"],
                "results": dict_results
            })
        else:
            # 데이터 변경 쿼리인 경우 커밋 및 영향 받은 행 수 반환
            conn.commit()
            
            return json.dumps({
                "success": True,
                "affected_rows": cursor.rowcount
            })
            
    except Error as e:
        if conn:
            conn.rollback()  # 오류 발생 시 롤백
            
        return json.dumps({
            "success": False,
            "error": str(e)
        })
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
