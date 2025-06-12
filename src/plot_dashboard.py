import json
import boto3
import io
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime
from urllib.parse import quote
import os
from boto3.dynamodb.conditions import Key

def plotdashboard(input_data, dynamodb, s3, prefix_s3, DYNAMODB_TABLE, S3_BUCKET, REGION_S3_BUCKET):
    try:
        body = input_data
        sensor_ids = body['sensor_ids']
        print("sensor_ids:", sensor_ids)
        start_time = body['start_time']
        start_time = datetime.fromisoformat(start_time)
        end_time = body['end_time']
        end_time = datetime.fromisoformat(end_time)
        if start_time > end_time:
            start_time, end_time = end_time, start_time
        start_date = start_time.date()
        end_date = end_time.date()
        start_time = start_time.time()
        end_time = end_time.time()
        aggregation = body.get('aggregation', 'raw')
        factory_id = body.get('factory_id', 'F_xGc676J6PH')

        results = {}
        table = dynamodb.Table(DYNAMODB_TABLE)

        columns = []
        columns.append('Date')
        columns.append('Time')

        expression_attribute_names = {}
        projection_expression_parts = []
        for sensor_id in sensor_ids:
            columns.append(f"{sensor_id}")
        for i, col in enumerate(columns):
            alias = f"#col{i}"
            expression_attribute_names[alias] = col
            projection_expression_parts.append(alias)
        projection_expression = ", ".join(projection_expression_parts)
        print("projection_expression:", projection_expression)
        print("expression_attribute_names:", expression_attribute_names)
        print(start_date, end_date)
        print(start_time, end_time)
        # Query DynamoDB
        query_data = []
        date_list = pd.date_range(start=start_date, end=end_date).date
        print("date_list:", date_list)
        for date in date_list:
            # factory_id_date_prefix = f"{factory_id}::{date.isoformat()}"
            factory_id_date_prefix = f"{factory_id}:{date.isoformat()}"
            print("factory_id_date_prefix:", factory_id_date_prefix)
            response = table.query(
                KeyConditionExpression=Key('FactoryId_Date').eq(factory_id_date_prefix) & 
                                    Key('Time').between(start_time.isoformat(), end_time.isoformat()),
                ProjectionExpression=projection_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ScanIndexForward=False
            )
            items = response['Items']
            print("items:", items)
        
        for item in items:
            item['Date'] = date.isoformat()
            item['Time'] = item['Time']
            query_data.append(item)

        df = pd.DataFrame(query_data)
        print("df:", df)
        df['timestamp'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])
        df.drop(columns=['Date', 'Time'], inplace=True)

        valid_sensor_ids = []
        for sensor_id in sensor_ids:
            if sensor_id in df.columns:
                df[sensor_id] = pd.to_numeric(df[sensor_id], errors='coerce')
                valid_sensor_ids.append(sensor_id)
            
        df.set_index('timestamp', inplace=True)
        df = df.sort_index(ascending=True)
        print("df:", df)
        try:
            df = df.fillna(method='ffill')
        except:
            df = df.fillna(method='bfill')
        
        # Aggregation
        freq_map = {
            'raw': None,
            'hourly': 'H',
            'daily': 'D',
            'monthly': 'M',
            'yearly': 'Y'
        }
        freq = freq_map.get(aggregation)
        if freq:
            df = df.resample(freq).mean()

        # Plotting
        plt.figure(figsize=(14, 6))
        
        for column in df.columns:
            plt.plot(df.index, df[column], label=column)
        plt.title(f"Biểu đồ cảm biến từ {start_date} đến {end_date}")
        plt.xlabel(f"Thời gian từ {start_date} đến {end_date}")
        plt.ylabel("Giá trị cảm biến")
        plt.legend()
        plt.grid(True)
        plt.xticks(rotation=45)
        plt.tight_layout()

        # Save to buffer
        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)
        file_name = f"{prefix_s3}/dashboard_{aggregation}_{datetime.now().strftime('%Y%m%d%H%M%S')}.png"
        s3.put_object(Bucket=S3_BUCKET, Key=file_name, Body=buf, ContentType='image/png')
        plot_url = f"https://{S3_BUCKET}.s3.{REGION_S3_BUCKET}.amazonaws.com/{quote(file_name)}"
        plt.close()

        # Trích xuất chuỗi truy vấn data tóm tắt (để agent xử lý)
        data_summary = df.describe().to_dict()

        results = {
            "plot_url": plot_url,
            "data_query": json.dumps(data_summary),
            "dataframe": df.to_dict(orient='records'),
            "action": "plotdashboard"
        }

        return {
            "statusCode": 200,
            "body": json.dumps(results),
            "headers": {
                "Content-Type": "application/json"
            }
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}),
            "headers": {
                "Content-Type": "application/json"
            }
        }
