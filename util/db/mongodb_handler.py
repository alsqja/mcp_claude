import json
from typing import Dict, List, Any, Optional, Union

def handle_mongodb_query(connection_params: Dict[str, Any], query: str, params: Optional[Dict], options: Dict[str, Any]) -> str:
    """MongoDB 쿼리 실행 및 결과 반환"""
    
    from pymongo import MongoClient
    import bson.json_util
    
    client = None
    
    try:
        # 연결 문자열 생성
        if "connection_string" in connection_params:
            connection_string = connection_params["connection_string"]
        else:
            # 기본 연결 정보로 문자열 구성
            user = connection_params.get("user", "")
            password = connection_params.get("password", "")
            host = connection_params.get("host", "localhost")
            port = connection_params.get("port", 27017)
            
            auth_part = ""
            if user and password:
                auth_part = f"{user}:{password}@"
                
            connection_string = f"mongodb://{auth_part}{host}:{port}/"
            
        # 데이터베이스 연결
        client = MongoClient(connection_string, serverSelectionTimeoutMS=options["timeout"] * 1000)
        db_name = connection_params.get("database", "admin")
        db = client[db_name]
        
        # 명령어 처리
        try:
            command = json.loads(query)
        except json.JSONDecodeError:
            return json.dumps({
                "success": False,
                "error": "Invalid MongoDB command. Must be valid JSON."
            })
            
        # 컬렉션 실행
        collection_name = params.get("collection", "") if params else ""
        
        if collection_name:
            collection = db[collection_name]
            
            # 명령에 따라 적절한 메서드 호출
            if "find" in command:
                # 검색 쿼리
                filter_query = command["find"]
                projection = command.get("projection", None)
                sort = command.get("sort", None)
                limit = min(command.get("limit", options["max_rows"]), options["max_rows"])
                
                cursor = collection.find(filter_query, projection)
                
                if sort:
                    cursor = cursor.sort(list(sort.items()))
                    
                cursor = cursor.limit(limit)
                
                results = list(cursor)
                
                # BSON 객체를 JSON으로 변환
                return json.dumps({
                    "success": True,
                    "count": len(results),
                    "max_rows_reached": len(results) >= options["max_rows"],
                    "results": json.loads(bson.json_util.dumps(results))
                })
                
            elif "insert" in command:
                # 삽입 쿼리
                documents = command["insert"]
                if not isinstance(documents, list):
                    documents = [documents]
                    
                result = collection.insert_many(documents)
                
                return json.dumps({
                    "success": True,
                    "inserted_count": len(result.inserted_ids),
                    "inserted_ids": json.loads(bson.json_util.dumps(result.inserted_ids))
                })
                
            elif "update" in command:
                # 업데이트 쿼리
                filter_query = command.get("filter", {})
                update_query = command["update"]
                upsert = command.get("upsert", False)
                
                if command.get("many", False):
                    result = collection.update_many(filter_query, update_query, upsert=upsert)
                else:
                    result = collection.update_one(filter_query, update_query, upsert=upsert)
                    
                return json.dumps({
                    "success": True,
                    "matched_count": result.matched_count,
                    "modified_count": result.modified_count,
                    "upserted_id": json.loads(bson.json_util.dumps(result.upserted_id)) if result.upserted_id else None
                })
                
            elif "delete" in command:
                # 삭제 쿼리
                filter_query = command["delete"]
                
                if command.get("many", False):
                    result = collection.delete_many(filter_query)
                else:
                    result = collection.delete_one(filter_query)
                    
                return json.dumps({
                    "success": True,
                    "deleted_count": result.deleted_count
                })
                
            else:
                return json.dumps({
                    "success": False,
                    "error": "Unsupported MongoDB command"
                })
        else:
            # 데이터베이스 직접 명령 실행
            result = db.command(command)
            
            return json.dumps({
                "success": True,
                "result": json.loads(bson.json_util.dumps(result))
            })
            
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": str(e)
        })
        
    finally:
        if client:
            client.close()
