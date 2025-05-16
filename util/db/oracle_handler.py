import json
from typing import Dict, List, Any, Optional, Union

def handle_oracle_query(connection_params: Dict[str, Any], query: str, params: Optional[Union[List, Dict]], options: Dict[str, Any]) -> str:
    """Oracle 쿼리 실행 및 결과 반환"""
    
    import cx_Oracle
    
    conn = None
    cursor = None
    
    try:
        # 연결 파라미터 구성
        connect_string = "{}/{}@{}:{}/{}".format(
            connection_params.get("user", "system"),
            connection_params.get("password", ""),
            connection_params.get("host", "localhost"),
            connection_params.get("port", 1521),
            connection_params.get("service_name", "XE")
        )
        
        # 데이터베이스 연결
        conn = cx_Oracle.connect(connect_string)
        cursor = conn.cursor()
        
        # 쿼리 실행
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
            
        # SELECT 쿼리인 경우 결과 반환
        if query.strip().upper().startswith("SELECT"):
            # 컬럼 이름 가져오기
            columns = [col[0].lower() for col in cursor.description]
            
            # 결과를 딕셔너리 리스트로 변환
            results = []
            for row in cursor.fetchmany(options["max_rows"]):
                results.append(dict(zip(columns, row)))
            
            return json.dumps({
                "success": True, 
                "count": len(results),
                "max_rows_reached": len(results) >= options["max_rows"],
                "results": results
            })
        else:
            # 데이터 변경 쿼리인 경우 커밋 및 영향 받은 행 수 반환
            conn.commit()
            
            return json.dumps({
                "success": True,
                "affected_rows": cursor.rowcount
            })
            
    except cx_Oracle.Error as e:
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
