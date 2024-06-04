import pandas as pd
import functions_framework
from google.cloud import storage
from flask import jsonify
import json
import requests
from google.cloud import bigquery
import pandas_gbq as gbq
from prophet import Prophet

@functions_framework.http

def extract_data():
    try:
        url = "https://air-quality.p.rapidapi.com/history/airquality"
        lat, lon = 18.50956983949341, 73.85882813593227
        querystring = {"lon":lon,"lat":lat}

        headers = {
            "x-rapidapi-key": "572e8dc2ffmshe32f9c0ffbf7f45p1e5487jsn997315c6051b",
             "x-rapidapi-host": "air-quality.p.rapidapi.com"
        }

        response = requests.get(url, headers=headers, params=querystring)
        data = response.json()
        df = pd.json_normalize(data)
        df = df.explode('data', ignore_index=True)
        df = df.join(pd.json_normalize(df['data'])).drop('data', axis=1)
        return df
    except Exception as e:
        print(e)
        pass

def transform_data():
    try:
        df=extract_data()
        df=df.fillna('')
        df=df.drop_duplicates()
        return df
    except Exception as e:
        print(e)
        pass

def load_data():
    try:
        df=transform_data()
        insert_data=df.astype(str)
        gbq.to_gbq(insert_data, 'testing.AirQulity', 'fenixwork-projects', if_exists='append')
    except Exception as e:
        print(e)
        pass
    

def get_data():
    try:
        # data=load_data()
        client = bigquery.Client()
        project = "fenixwork-projects"
        dataset_id = "testing"

        dataset_ref = bigquery.DatasetReference(project, dataset_id)
        table_ref = dataset_ref.table("AirQulity")
        table = client.get_table(table_ref)

        df = client.list_rows(table).to_dataframe()
        df=df.drop_duplicates(subset=['timestamp_local'],keep='last')
        return df
    except Exception as e:
        print(e)
        pass

def prediction_data():
    try:
        df=get_data()
        df['ds']=df['timestamp_local']
        df['y']=df['aqi']
        df1=df[['ds','y']]
        m = Prophet()
        m.fit(df1)
        future = m.make_future_dataframe(periods=365,freq='H')
        forecast = m.predict(future)
        return forecast.to_dict('list')
    except Exception as e:
        print(e)
        dfx=[]
        return dfx
     
def get_full_data(query):
    try:
        query_lst=[]
        query_lst.append(query.lower())
        df=get_data()
        df=df.fillna('')
        df['city_name']=df["city_name"].map(lambda x: x.lower())
        df=df.query('city_name in @query_lst')
        return df.to_dict('list')
    except Exception as e:
        print(e)
        dfx=[]
        return dfx

def air_quality(request):
    final_result=[]
    final_dict=dict()
    try:
        query=request.json['query']
        data=get_full_data(query)
        final_dict['full_dataset']=data
        # prediction=prediction_data()
        # final_dict['prediction']=prediction
        final_result.append(final_dict)
    except Exception as e:
        print(e)
        final_result=[{'Error':'something wrong'}]
        
    resp = jsonify(final_result)
    resp.headers.add('Access-Control-Allow-Origin', '*')
    return resp


########################
def extract_data_traffic():
    try:
        url = "https://waze.p.rapidapi.com/alerts-and-jams"
        bottom_left = '18.405358059520015, 73.73454530741854'
        top_right = '18.63714281319877, 73.99409729919435'
        querystring = {"bottom_left":bottom_left,"top_right":top_right,"max_alerts":"100000","max_jams":"100000"}

        headers = {
            "x-rapidapi-key": "572e8dc2ffmshe32f9c0ffbf7f45p1e5487jsn997315c6051b",
            "x-rapidapi-host": "waze.p.rapidapi.com"
        }

        response = requests.get(url, headers=headers, params=querystring)

        data = response.json()

        df = pd.json_normalize(data)
        df = df.explode('data.jams', ignore_index=True)
        df = pd.json_normalize(df['data.jams'])
        df = df.explode('line_coordinates', ignore_index=True)
        dfx = pd.json_normalize(df['line_coordinates'])
        df = df.join(dfx).drop('line_coordinates', axis=1)
        return df
    except Exception as e:
        print(e)
        pass

def transform_data_traffic():
    try:
        df=extract_data_traffic()
        df=df.fillna('')
        df=df.drop_duplicates()
        return df
    except Exception as e:
        print(e)
        pass

def load_data_traffic():
    try:
        df=transform_data_traffic()
        insert_data=df.astype(str)
        gbq.to_gbq(insert_data, 'testing.Traffic', 'fenixwork-projects', if_exists='append')
    except Exception as e:
        print(e)
        pass
    

def get_data_traffic():
    try:
        # data=load_data_traffic()
        client = bigquery.Client()
        project = "fenixwork-projects"
        dataset_id = "testing"

        dataset_ref = bigquery.DatasetReference(project, dataset_id)
        table_ref = dataset_ref.table("Traffic")
        table = client.get_table(table_ref)

        df = client.list_rows(table).to_dataframe()
        df=df.drop_duplicates(subset=['jam_id','publish_datetime_utc','update_datetime_utc'],keep='last')
        return df
    except Exception as e:
        print(e)
        pass
    
def get_full_data_traffic(query):
    try:
        query_lst=[]
        query_lst.append(query.lower())
        df=get_data_traffic()
        df=df.fillna('')
        print(df.columns)
        df['city']=df["city"].map(lambda x: x.lower())
        df=df.query('city in @query_lst')
        return df.to_dict('list')
    except Exception as e:
        print(e)
        dfx=[]
        return dfx


def traffic(request):
    final_result=[]
    final_dict=dict()
    try:
        query=request.json['query']
        data=get_full_data_traffic(query)
        final_dict['full_dataset']=data
        # prediction=prediction_data()
        # final_dict['prediction']=prediction
        final_result.append(final_dict)
    except Exception as e:
        print(e)
        final_result=[{'Error':'something wrong'}]
        
    resp = jsonify(final_result)
    resp.headers.add('Access-Control-Allow-Origin', '*')
    return resp



def main(request):
    if request.method == 'OPTIONS':
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }

        return ('', 204, headers)

    path = (request.path)

    if (path == "/air_quality"):
        return air_quality(request)
    elif (path == "/traffic"):
        return traffic(request)