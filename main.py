from typing import List, Optional
from fastapi import FastAPI, Query
from app.utils.filter import get_filtered_data
from models import FolderPathRequest
from app.utils.file_loader import save_csv
from fastapi import Response
import pandas as pd
from io import BytesIO



app = FastAPI()


@app.post("/upload-folder")
async def upload_folder(request: FolderPathRequest):
    message = await save_csv(request.folder_path)
    return {"message": message}


@app.get("/filter")
async def filter_orders(
    price_each_range: Optional[List[float]] = Query(None, min_items=2, max_items=2),
    quantity_ordered: Optional[int] = Query(None),
    products: Optional[List[str]] = Query(None),
    categories: Optional[List[str]] = Query(None)
):
    result = get_filtered_data(price_each_range, quantity_ordered, products, categories)
    return {"Data": result}


@app.get("/filter/export", response_description="Download the filtered data as Excel")
async def export_filtered_data(
    price_each_range: Optional[List[float]] = Query(None, min_items=2, max_items=2),
    quantity_ordered: Optional[int] = Query(None),
    products: Optional[List[str]] = Query(None),
    categories: Optional[List[str]] = Query(None)
):
    result = get_filtered_data(price_each_range, quantity_ordered, products, categories)
    
    df = pd.DataFrame(result)
    output = BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)

    headers = {
        "Content-Disposition": "attachment; filename=filtered_file.xlsx"
    }

    return Response(content=output.read(), media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers=headers)