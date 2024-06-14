import pandas as pd
import functions_framework
from google.cloud import storage
from flask import jsonify
import json
import requests
from google.cloud import bigquery
import pandas_gbq as gbq
from prophet import Prophet


def get_air_data():
    try:
        # data=load_data()
        client = bigquery.Client()
        project = "fenixwork-projects"
        dataset_id = "testing"

        dataset_ref = bigquery.DatasetReference(project, dataset_id)
        table_ref = dataset_ref.table("Air_Qulity")
        table = client.get_table(table_ref)

        df = client.list_rows(table).to_dataframe()
        return df
    except Exception as e:
        print(e)
        pass

def air_qulity_per_month():
    try:
        df=get_air_data()
        df=df.fillna('')
        df['Date']=pd.to_datetime(df['Date'])
        df['AQI']=df['AQI'].astype(float)
        d={}
        df=df[df.AQI!='']
        for key,grp in df.groupby('Location'):
            a=grp.groupby(grp['Date'].dt.month)['AQI'].mean()
            d[key]=a
        monthly_avg=pd.DataFrame(d)
        monthly_avg.index.name='Months'
        months={1:'Jan',2:'Feb',3:'Mar',4:'Apr',5:'May',6:'June',7:'July',8:'Aug',9:'Sep',10:'Oct',11:'Nov',12:'Dec'}
        monthly_avg.reset_index(inplace=True)
        monthly_avg['Months'].replace(months,inplace=True)
        monthly_avg.set_index('Months',inplace=True)
        # monthly_avg['Month']=''
        # month_dict={0:'Jan',1:'Feb',2:'Mar',3:'Apr',4:'May',5:'June',6:'July',7:'Aug',8:'Sep',9:'Oct',10:'Nov',11:'Dec'}
        # for k,v in month_dict.items():
        #     monthly_avg['Month'].iloc[k]=v
        return monthly_avg.to_dict('list')
    except Exception as e:
        print("Error in air_qulity_per_month",e) 
        return []

def take_first_date(x):
    if str(x).endswith('01'):
        return x
    else:
        return ''

def future_air_Predict():
    try:
        dfy=get_air_data()
        dfy=dfy[dfy.AQI!='']
        dfy = dfy[['Date', 'AQI']]
        predict_ip_dfx = dfy.rename(columns={'Date':'ds', 'AQI':'y'})
        model = Prophet()
        model.fit(predict_ip_dfx)
        future = model.make_future_dataframe(periods=730)
        forecast = model.predict(future)
        forecast['date'] = forecast['ds'].dt.date
        forecast['date']=forecast['date'].apply(take_first_date)
        forecast=forecast[forecast.date!='']
        forecast['date']=forecast['date'].astype('datetime64[ns]')
        forecast['year'] = forecast['date'].dt.year
        yearly_trend = forecast.groupby('year')['trend'].sum().reset_index()
        yearly_trend['trend']=yearly_trend['trend']/12
        return yearly_trend.to_dict('list')
    except Exception as e:
        print("Error in future_air_Predict",e) 
        return []


def air_quality(request):
    final_result=[]
    final_dict=dict()
    try:
        data=air_qulity_per_month()
        final_dict['full_dataset']=data
        prediction=future_air_Predict()
        final_dict['prediction']=prediction
        final_result.append(final_dict)
    except Exception as e:
        print(e)
        final_result=[{'Error':'something wrong'}]
        
    resp = jsonify(final_result)
    resp.headers.add('Access-Control-Allow-Origin', '*')
    return resp


######################## electricity #####################

def get_data_electricity():
    try:
        client = bigquery.Client()
        project = "fenixwork-projects"
        dataset_id = "testing"

        dataset_ref = bigquery.DatasetReference(project, dataset_id)
        table_ref = dataset_ref.table("Electricity")
        table = client.get_table(table_ref)

        df = client.list_rows(table).to_dataframe()
        # df=df.drop_duplicates(subset=['Year'],keep='last')
        return df
    except Exception as e:
        print(e)
        pass
    
def get_full_data_electricity():
    try:
        df=get_data_electricity()
        df=df.fillna('')
        return df.to_dict('list')
    except Exception as e:
        print(e)
        dfx=[]
        return dfx
    
def monthly_data():
    try:
        client = bigquery.Client()
        project = "fenixwork-projects"
        dataset_id = "testing"
        dataset_ref = bigquery.DatasetReference(project, dataset_id)
        table_ref = dataset_ref.table("Monthly_Electricity")
        table = client.get_table(table_ref)
        df = client.list_rows(table).to_dataframe()
        return df
    except Exception as e:
        print(e)
        pass

def future_Predict(dfy):
    try:
        dfy = dfy[['Date', 'Total_Consumption']]
        predict_ip_dfx = dfy.rename(columns={'Date':'ds', 'Total_Consumption':'y'})
        model = Prophet()
        model.fit(predict_ip_dfx)
        future = model.make_future_dataframe(periods=730)
        forecast = model.predict(future)
        forecast['date'] = forecast['ds'].dt.date
        forecast['date']=forecast['date'].apply(take_first_date)
        forecast=forecast[forecast.date!='']
        forecast['date']=forecast['date'].astype('datetime64[ns]')
        forecast['year'] = forecast['date'].dt.year
        yearly_trend = forecast.groupby('year')['trend'].sum().reset_index()
        return yearly_trend.to_dict('list')
    except Exception as e:
        print("Error in future_predict",e)
        pass

def electricity(request):
    final_result=[]
    final_dict=dict()
    try:
        data=get_full_data_electricity()
        final_dict['full_dataset']=data
        df=get_data_electricity()
        df=df.fillna('')
        monthly_df=monthly_data()
        prediction=future_Predict(monthly_df)
        final_dict['prediction']=prediction
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
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }

        return ('', 204, headers)

    path = (request.path)

    if (path == "/air_quality"):
        return air_quality(request)
    elif (path == "/electricity"):
        return electricity(request)