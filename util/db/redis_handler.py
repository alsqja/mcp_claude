import json
from typing import Dict, List, Any, Optional, Union

def handle_redis_query(connection_params: Dict[str, Any], query: str, params: Optional[Dict], options: Dict[str, Any]) -> str:
    """Redis 명령 실행 및 결과 반환"""
    
    import redis
    
    client = None
    
    try:
        # 연결 파라미터 구성
        host = connection_params.get("host", "localhost")
        port = connection_params.get("port", 6379)
        db = connection_params.get("database", 0)
        password = connection_params.get("password", None)
        
        # Redis 연결
        client = redis.Redis(
            host=host,
            port=port,
            db=db,
            password=password,
            socket_timeout=options["timeout"]
        )
        
        # 명령어 파싱
        try:
            command_parts = query.strip().split()
            command = command_parts[0].upper()
            args = command_parts[1:]
        except Exception:
            return json.dumps({
                "success": False,
                "error": "Invalid Redis command format"
            })
            
        # 명령 실행
        if command == "GET":
            if len(args) != 1:
                return json.dumps({"success": False, "error": "GET command requires exactly one key"})
                
            value = client.get(args[0])
            
            # 바이너리 데이터를 문자열로 변환
            if value is not None and isinstance(value, bytes):
                try:
                    value = value.decode('utf-8')
                except UnicodeDecodeError:
                    value = str(value)
                    
            return json.dumps({
                "success": True,
                "result": value
            })
            
        elif command == "SET":
            if len(args) < 2:
                return json.dumps({"success": False, "error": "SET command requires at least key and value"})
                
            key = args[0]
            value = args[1]
            
            # 추가 옵션이 있는 경우 처리
            remaining_args = args[2:] if len(args) > 2 else []
            
            # EX/PX 옵션 처리
            ex = None
            px = None
            nx = False
            xx = False
            
            i = 0
            while i < len(remaining_args):
                if remaining_args[i].upper() == "EX" and i + 1 < len(remaining_args):
                    ex = int(remaining_args[i + 1])
                    i += 2
                elif remaining_args[i].upper() == "PX" and i + 1 < len(remaining_args):
                    px = int(remaining_args[i + 1])
                    i += 2
                elif remaining_args[i].upper() == "NX":
                    nx = True
                    i += 1
                elif remaining_args[i].upper() == "XX":
                    xx = True
                    i += 1
                else:
                    i += 1
                    
            result = client.set(key, value, ex=ex, px=px, nx=nx, xx=xx)
            
            return json.dumps({
                "success": True,
                "result": result
            })
            
        elif command == "DEL":
            if not args:
                return json.dumps({"success": False, "error": "DEL command requires at least one key"})
                
            result = client.delete(*args)
            
            return json.dumps({
                "success": True,
                "deleted_count": result
            })
            
        elif command == "EXISTS":
            if not args:
                return json.dumps({"success": False, "error": "EXISTS command requires at least one key"})
                
            result = client.exists(*args)
            
            return json.dumps({
                "success": True,
                "exists_count": result
            })
            
        elif command == "KEYS":
            if len(args) != 1:
                return json.dumps({"success": False, "error": "KEYS command requires exactly one pattern"})
                
            pattern = args[0]
            keys = client.keys(pattern)
            
            # 바이너리 데이터를 문자열로 변환
            str_keys = []
            for key in keys:
                if isinstance(key, bytes):
                    try:
                        str_keys.append(key.decode('utf-8'))
                    except UnicodeDecodeError:
                        str_keys.append(str(key))
                else:
                    str_keys.append(str(key))
                    
            return json.dumps({
                "success": True,
                "keys": str_keys
            })
            
        elif command == "HGETALL":
            if len(args) != 1:
                return json.dumps({"success": False, "error": "HGETALL command requires exactly one key"})
                
            result = client.hgetall(args[0])
            
            # 바이너리 데이터를 문자열로 변환
            str_result = {}
            for k, v in result.items():
                key = k.decode('utf-8') if isinstance(k, bytes) else str(k)
                
                if isinstance(v, bytes):
                    try:
                        value = v.decode('utf-8')
                    except UnicodeDecodeError:
                        value = str(v)
                else:
                    value = str(v)
                    
                str_result[key] = value
                
            return json.dumps({
                "success": True,
                "result": str_result
            })
            
        else:
            # 기타 명령은 redis-py의 execute_command를 사용하여 실행
            result = client.execute_command(command, *args)
            
            # 결과 타입에 따른 변환
            if isinstance(result, bytes):
                try:
                    result = result.decode('utf-8')
                except UnicodeDecodeError:
                    result = str(result)
            elif isinstance(result, list):
                decoded_result = []
                for item in result:
                    if isinstance(item, bytes):
                        try:
                            decoded_result.append(item.decode('utf-8'))
                        except UnicodeDecodeError:
                            decoded_result.append(str(item))
                    else:
                        decoded_result.append(item)
                result = decoded_result
                
            return json.dumps({
                "success": True,
                "result": result
            })
            
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": str(e)
        })
        
    finally:
        if client:
            client.close()
