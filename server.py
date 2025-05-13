import sys
import traceback

from mcp.server.fastmcp import FastMCP
import requests
import json

# Create an MCP server
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

if __name__ == "__main__":
    try:
        # print("About to run MCP server...", file=sys.stderr)
        mcp.run()
    except Exception as e:
        # print(f"Server error: {e}", file=sys.stderr)
        # print("Full traceback:", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)