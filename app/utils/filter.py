import logging
from typing import List, Optional
from app.db import get_connection

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  

def get_filtered_data(
    price_each_range: Optional[List[float]] = None,
    quantity_ordered: Optional[int] = None,
    products: Optional[List[str]] = None,
    categories: Optional[List[str]] = None
) -> List[dict]:

    try:
        conn = get_connection()
        cursor = conn.cursor()

        conditions = []
        params = []

       
        filters = [
              ("Price Each", price_each_range),
              ("Quantity Ordered", quantity_ordered),
              ("Product", products),
              ("categorie", categories)
              ]

        for field, value in filters:
              if value:
                if field == "Price Each" and len(value) == 2:
                       conditions.append(f"CAST([{field}] AS FLOAT) BETWEEN ? AND ?")
                       params.extend(value)
                
                elif isinstance(value, list):
                      placeholders = ",".join("?" * len(value))
                      conditions.append(f"[{field}] IN ({placeholders})")
                      params.extend(value)
                
                else:
                      conditions.append(f"CAST([{field}] AS INT) = ?")
                      params.append(value)

                
        where_clause = " AND ".join(conditions) 
        query = f"SELECT * FROM [FinalCSV] WHERE {where_clause}"

        logger.info(f"Executing query: {query} with params: {params}")
        cursor.execute(query, params)

        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        result = [dict(zip(columns, row)) for row in rows]

        cursor.close()
        conn.close()
        return result
    
    except Exception as e:
        logger.error("Error fetching filtered data")
        return [{"error": str(e)}]

