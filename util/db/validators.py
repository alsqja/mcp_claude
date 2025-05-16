def is_safe_query(query: str) -> bool:
    """
    위험한 SQL 명령어가 포함되어 있는지 검사합니다.
    
    Args:
        query: 검사할 SQL 쿼리
        
    Returns:
        안전한 쿼리인 경우 True, 그렇지 않으면 False
    """
    # 위험할 수 있는 키워드 목록
    dangerous_keywords = [
        "DROP TABLE", "DROP DATABASE", "TRUNCATE TABLE",
        "ALTER TABLE", "DELETE FROM", "UPDATE", "INSERT INTO"
    ]
    
    query_upper = query.upper()
    
    # 관리자 명령이나 시스템 테이블 접근 확인
    if any(keyword in query_upper for keyword in dangerous_keywords):
        return False
        
    return True
