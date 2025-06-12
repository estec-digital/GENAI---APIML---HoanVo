from fastapi import FastAPI
from src.plot_dashboard import *
from dotenv import load_dotenv
import os
import boto3
import pandas as pd
from pydantic import BaseModel
from typing import Optional, List

load_dotenv()
app = FastAPI()

# Config
DYNAMODB_TABLE = os.getenv('DYNAMODB_TABLE')
S3_BUCKET = os.getenv('S3_BUCKET')
REGION_DYNAMODB_TABLE = os.getenv('REGION_DYNAMODB_TABLE')
REGION_S3_BUCKET = os.getenv('REGION_S3_BUCKET')

dynamodb = boto3.resource('dynamodb', region_name=REGION_DYNAMODB_TABLE)
s3 = boto3.client('s3', region_name=REGION_S3_BUCKET)
prefix_s3_plot = 'plots'

@app.get("/")
def read_root():
    return {"message": "Hello from GENAI Machine on EC2!"}

class StatusRequest(BaseModel):
    sensor_ids: List[str]
    start_time: str
    end_time: str
    aggregation: Optional[str] = 'raw'
    factory_id: Optional[str] = 'F_xGc676J6PH'

@app.post("/plotdashboard")
def api_plotdashboard(input_data: StatusRequest):
    input_data = input_data.dict()
    if not input_data:
        return {"error": "No input data provided."}
    output = plotdashboard(input_data, dynamodb, s3, prefix_s3_plot, DYNAMODB_TABLE, S3_BUCKET, REGION_S3_BUCKET)
    return output
