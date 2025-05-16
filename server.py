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
    """
    HTTP 요청을 보내 API 서버를 테스트하는 도구입니다.

   Args:
       method: HTTP 메서드 (GET, POST, PUT, DELETE 등)
       url: 요청을 보낼 대상 URL
       body: 요청 본문 (JSON 데이터)
           - 데이터가 필요없는 GET 요청이라도 항상 빈 객체 "{}" 전달 필요
           - 문자열로 전달할 경우 유효한 JSON 형식이어야 함
           - 딕셔너리 객체로 직접 전달 가능
       access_token: Bearer 인증에 사용할 토큰 (필요 없을 경우 빈 문자열 "")

   Returns:
       서버 응답을 JSON 문자열로 반환합니다.
       성공적인 응답은 서버가 반환한 JSON 데이터를 그대로 반환합니다.
       오류 응답은 {"error": "오류 메시지"} 형식으로 반환됩니다.

   사용 예시:
   1. GET 요청 (body 필수):
      test_server(method="GET", url="http://localhost:8080/api/users", body="{}", access_token="")

   2. POST 요청 (데이터 포함):
      test_server(method="POST", url="http://localhost:8080/api/users",
                  body='{"name":"John","email":"john@example.com"}', access_token="")

   3. 인증이 필요한 요청:
      test_server(method="GET", url="http://localhost:8080/api/protected",
                  body="{}", access_token="eyJhbGciOiJIUzI1...")

   주의사항:
   - 모든 요청에서 body 파라미터는 필수이며, 데이터가 없는 GET 요청에도 빈 객체 "{}"를 전달해야 합니다.
   - JSON 응답이 아닌 경우 {"response": "텍스트 응답"} 형식으로 반환됩니다.
   - HTTP 상태 코드가 4xx 또는 5xx인 경우 예외가 발생하여 오류 메시지가 반환됩니다.
    :param method:
    :param url:
    :param body:
    :param access_token:
    :return:
    """
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
