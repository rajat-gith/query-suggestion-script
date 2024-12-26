from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, List, Optional
import uvicorn
import os

app = FastAPI(title="SQL Query Generator API")

class TableStructure(BaseModel):
    columns: Dict[str, str]

class QueryRequest(BaseModel):
    natural_query: str
    table_name: str

class TableRequest(BaseModel):
    natural_query:str
    table_name: str
    columns: Dict[str, str]

class SQLQueryGenerator:
    def __init__(self):
        self.tables = {}
        
    def add_table(self, table_name: str, columns: Dict[str, str]):
        """Add table structure to the generator"""
        self.tables[table_name] = columns
        
    def parse_query(self, natural_query: str, table_name: str) -> str:
        """Convert natural language query to SQL"""
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' not found")
            
        query = natural_query.lower()
        
        if any(word in query for word in ['show', 'select', 'get', 'find', 'display']):
            return self._generate_select(query, table_name)
        elif any(word in query for word in ['add', 'insert', 'create']):
            return self._generate_insert(query, table_name)
        elif any(word in query for word in ['update', 'modify', 'change']):
            return self._generate_update(query, table_name)
        elif any(word in query for word in ['delete', 'remove']):
            return self._generate_delete(query, table_name)
        else:
            raise ValueError("Could not determine query type")
    
    def _generate_select(self, query: str, table_name: str) -> str:
        """Generate SELECT query"""
        columns = []
        conditions = []
        table_columns = self.tables[table_name]
        
        for col in table_columns.keys():
            if col.lower() in query:
                columns.append(col)
                
        if not columns:
            columns = ['*']
            
        if 'where' in query:
            condition_part = query.split('where')[1].strip()
            conditions.append(condition_part)
                
        sql = f"SELECT {', '.join(columns)} FROM {table_name}"
        if conditions:
            sql += f" WHERE {' AND '.join(conditions)}"
        
        return sql + ";"
        
    def _generate_insert(self, query: str, table_name: str) -> str:
        """Generate INSERT query"""
        columns = list(self.tables[table_name].keys())
        placeholders = ['%s'] * len(columns)
        
        return f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)});"
        
    def _generate_update(self, query: str, table_name: str) -> str:
        """Generate UPDATE query"""
        columns = list(self.tables[table_name].keys())
        set_clause = ', '.join([f"{col} = %s" for col in columns])
        
        return f"UPDATE {table_name} SET {set_clause} WHERE condition = %s;"
        
    def _generate_delete(self, query: str, table_name: str) -> str:
        """Generate DELETE query"""
        return f"DELETE FROM {table_name} WHERE condition = %s;"

query_generator = SQLQueryGenerator()

@app.post("/process-query")
async def process_query(request:TableRequest):
    """Add table structure and generate query in one step."""
    try:
        print(request)
        query_generator.add_table(request.table_name, request.columns)
        
        sql_query = query_generator.parse_query(request.natural_query, request.table_name)
        
        return {
            "message": f"Table '{request.table_name}' added and query generated successfully",
            "sql_query": sql_query
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)

