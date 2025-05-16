import sys
import traceback
import json

from mcp.server.fastmcp import FastMCP
from util.db.core import execute_database_query
import requests

# MCP 서버 생성
mcp = FastMCP("mcp_project")

@mcp.tool()
def test_server(method: str, url: str, body, access_token: str) -> str:
    """test server"""
    headers = {"Content-Type": "application/json"}

    if access_token:
        headers["Authorization"] = f"Bearer {access_token}"

    try:
        # body가 이미 딕셔너리인 경우 그대로 사용하고, 문자열인 경우 파싱
        if isinstance(body, str):
            json_body = json.loads(body)
        else:
            json_body = body

        response = requests.request(method, url, headers=headers, json=json_body)
        response.raise_for_status()

        try:
            return json.dumps(response.json())
        except:
            return json.dumps({"response": response.text})
    except requests.RequestException as e:
        error_response = {"error": str(e)}
        return json.dumps(error_response)

# 통합 데이터베이스 쿼리 도구
@mcp.tool()
def db_query(db_type: str, connection_params: dict, query: str, params=None, options=None) -> str:
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
    try:
        return execute_database_query(db_type, connection_params, query, params, options)
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__
        })

if __name__ == "__main__":
    try:
        mcp.run()
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
