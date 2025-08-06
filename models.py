from fastapi import Query
from pydantic import BaseModel



class FolderPathRequest(BaseModel):
    folder_path: str


    