import os
import joblib
import xgboost as xgb
import numpy as np
import pandas as pd
import json

from fastapi import FastAPI
from fastapi.responses import RedirectResponse

app = FastAPI()


@app.get("/")
async def docs_redirect():
    return RedirectResponse(url="/docs")


@app.get("/stocks_predict")
async def predict_volumme(vol_moving_avg: float, adj_close_rolling_med: float):
    if not isinstance(vol_moving_avg, (float, int)) or not isinstance(adj_close_rolling_med, (float, int)):
        return {"error": "Invalid input. Please enter numeric values for vol_moving_avg and adj_close_rolling_med."}
    __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
    model_path = os.path.join(__location__, 'model', 'saved_model', 'xgb_model_stocks.joblib')
    # Load the trained XGBoost model
    xgb_model = joblib.load(model_path)
    vol_moving_avg = float(vol_moving_avg)
    adj_close_rolling_med = float(adj_close_rolling_med)
    new_data = {'vol_moving_avg': [vol_moving_avg], 'Adj_close_rolling_med': [adj_close_rolling_med]}
    dmatrix = xgb.DMatrix(pd.DataFrame(new_data))
    predicted_volume = xgb_model.predict(dmatrix)
    arr_lst = predicted_volume.tolist()
    json_str = json.dumps(arr_lst)
    py_obj = json.loads(json_str)
    my_int = int(py_obj[0])
    if my_int < 0:
        return {"Volume cannot be negative": "Invalid Output. Please enter valid values for vol_moving_avg and "
                                             "adj_close_rolling_med."}
    msg = {"Prediction": f"The predicted stock volume given {vol_moving_avg, adj_close_rolling_med} is {my_int}."}

    return msg


@app.get("/etfs_predict")
async def predict_volumme(vol_moving_avg: float, adj_close_rolling_med: float):
    if not isinstance(vol_moving_avg, (float, int)) or not isinstance(adj_close_rolling_med, (float, int)):
        return {"error": "Invalid input. Please enter numeric values for vol_moving_avg and adj_close_rolling_med."}
    __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
    model_path = os.path.join(__location__, 'model', 'saved_model', 'xgb_model_etfs.joblib')
    # Load the trained XGBoost model
    xgb_model = joblib.load(model_path)
    vol_moving_avg = float(vol_moving_avg)
    adj_close_rolling_med = float(adj_close_rolling_med)
    new_data = {'vol_moving_avg': [vol_moving_avg], 'Adj_close_rolling_med': [adj_close_rolling_med]}
    dmatrix = xgb.DMatrix(pd.DataFrame(new_data))
    predicted_volume = xgb_model.predict(dmatrix)
    arr_lst = predicted_volume.tolist()
    json_str = json.dumps(arr_lst)
    py_obj = json.loads(json_str)
    my_int = int(py_obj[0])
    if my_int < 0:
        return {"Volume cannot be negative": "Invalid Output. Please enter valid values for vol_moving_avg and "
                                             "adj_close_rolling_med."}
    msg = {"Prediction": f"The predicted etfs volume given {vol_moving_avg, adj_close_rolling_med} is {my_int}."}
    return msg
