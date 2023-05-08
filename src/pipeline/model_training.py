
import pandas as pd
import numpy as np
import xgboost as xgb
import joblib
from sklearn.metrics import mean_absolute_error, mean_squared_error
from datetime import datetime
import os

from sklearn.ensemble import GradientBoostingRegressor
from sklearn.feature_extraction import FeatureHasher
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import GridSearchCV
from pathlib import Path


def showdata():
    # parquet_file_name = "../data/processed_data/test_stocks/stocks.parquet"
    # parquet_file_name = f"../data/processed_data/stocks/stocks.parquet"
    parquet_file_name = "../data/processed_data/Task2/stocks_MA_RM/calculated.parquet"
    df_one = pd.read_parquet(parquet_file_name)
    print("total form parquet", df_one.shape[0])
    print(df_one.count())
    print(df_one.head(10))
    print(df_one.tail(20))


def train_stocks_model():
    __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
    # Define the data and model paths
    data_dir = os.path.join(__location__, 'model')
    ml_dir = os.path.join(data_dir, 'saved_model')
    model_path = os.path.join(ml_dir, 'xgb_model_stocks.joblib')

    # Create the necessary directories if they don't exist
    if not os.path.exists(ml_dir):
        os.makedirs(ml_dir)

    # Get the absolute path of the data directory using __location__
    __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
    data_dir = os.path.abspath(os.path.join(__location__, 'data'))
    # Construct the file path to calculated.parquet
    file_path = os.path.join(data_dir, 'processed_data', 'Task2', 'stocks_MA_RM', 'calculated.parquet')
    df = pd.read_parquet(file_path)
    df['Date'] = pd.to_datetime(df['Date'])
    df.set_index('Date', inplace=True)
    df.dropna(inplace=True)
    df = df.drop_duplicates(subset=['vol_moving_avg', 'Adj_close_rolling_med', 'Volume'], keep='first')
    # Calculate the number of rows for 50% sample
    n = int(len(df) * 0.5)
    df = df.sample(n=n, random_state=42)

    features = ['vol_moving_avg', 'Adj_close_rolling_med']
    target = 'Volume'

    # Split the data into train and test sets
    train_size = int(0.8 * len(df))
    train_df = df[:train_size]
    test_df = df[train_size:]

    # Create DMatrix objects for train and test data
    dtrain = xgb.DMatrix(train_df[features], label=train_df[target])
    dtest = xgb.DMatrix(test_df[features], label=test_df[target])

    # define parameter grid with different num_round values
    param_grid = {'n_estimators': [50, 100, 150, 200]}
    xgb_model = xgb.XGBRegressor(learning_rate=0.1, max_depth=3, subsample=0.8)
    grid_search = GridSearchCV(estimator=xgb_model, param_grid=param_grid, scoring='neg_mean_squared_error', cv=5)
    grid_search.fit(dtrain.get_data(), dtrain.get_label())
    print("Best parameters: ", grid_search.best_params_)
    print("Best score: ", grid_search.best_score_)
    best_n_estimators = grid_search.best_params_['n_estimators']
    # Define the XGBoost hyperparameters
    params = {
        'objective': 'reg:squarederror',
        'eval_metric': 'mae',
        'max_depth': 6,
        'learning_rate': 0.1,
        'subsample': 0.8,
        'colsample_bytree': 0.8,
        'seed': 42
    }

    # Train the XGBoost model
    eval_list = [(dtest, 'eval'), (dtrain, 'train')]
    bst = xgb.train(params, dtrain, num_boost_round = best_n_estimators, evals = eval_list, early_stopping_rounds= 500, verbose_eval= 50)

    # Save the model to disk
    joblib.dump(bst, model_path)

    # Use the trained model to make predictions on the test data
    y_true = test_df[target].values
    y_pred = bst.predict(dtest)

    # Calculate the evaluation metrics
    mae = mean_absolute_error(y_true, y_pred)
    mse = mean_squared_error(y_true, y_pred)
    r2 = r2_score(y_true, y_pred)
    rmse = mean_squared_error(y_true, y_pred, squared=False)

    __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))

    # Create the data/ML directory if it doesn't exist
    if not os.path.exists(os.path.join(__location__, 'model/logs')):
        os.makedirs(os.path.join(__location__, 'model/logs'))

    # Create the training_logs.txt file inside the data/ML directory and write to it
    with open(os.path.join(__location__, 'model/logs/training_logs.txt'), 'a') as f:
        f.write(f'Training stats of stocks data\n')
        f.write(f'Training started at: {datetime.now()}\n')
        f.write(f'Training set size: {len(train_df)}\n')
        f.write(f'Test set size: {len(test_df)}\n')
        f.write(f'Number of rounds: {bst.best_iteration}\n')
        f.write(f'Minimum MAE: {bst.best_score}\n')
        f.write(f'Final MAE: {mae}\n')
        f.write(f'Final MSE: {mse}\n')
        f.write(f'Final R2: {r2}\n')
        f.write(f'Final RMSE: {rmse}\n')
        f.write('\n')


def train_etfs_model():
    __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
    # Define the data and model paths
    data_dir = os.path.join(__location__, 'model')
    ml_dir = os.path.join(data_dir, 'saved_model')
    model_path = os.path.join(ml_dir, 'xgb_model_etfs.joblib')

    # Create the necessary directories if they don't exist
    if not os.path.exists(ml_dir):
        os.makedirs(ml_dir)

    # Get the absolute path of the data directory using __location__
    __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
    data_dir = os.path.abspath(os.path.join(__location__, 'data'))
    # Construct the file path to calculated.parquet
    file_path = os.path.join(data_dir, 'processed_data', 'Task2', 'etfs_MA_RM', 'calculated.parquet')
    df = pd.read_parquet(file_path)
    df['Date'] = pd.to_datetime(df['Date'])
    df.set_index('Date', inplace=True)
    df.dropna(inplace=True)
    df = df.drop_duplicates(subset=['vol_moving_avg', 'Adj_close_rolling_med', 'Volume'], keep='first')
    # Calculate the number of rows for 50% sample
    n = int(len(df) * 0.5)
    df = df.sample(n=n, random_state=42)

    features = ['vol_moving_avg', 'Adj_close_rolling_med']
    target = 'Volume'

    # Split the data into train and test sets
    train_size = int(0.8 * len(df))
    train_df = df[:train_size]
    test_df = df[train_size:]

    # Create DMatrix objects for train and test data
    dtrain = xgb.DMatrix(train_df[features], label=train_df[target])
    dtest = xgb.DMatrix(test_df[features], label=test_df[target])

    # define parameter grid with different num_round values
    param_grid = {'n_estimators': [50, 100, 150, 200]}
    xgb_model = xgb.XGBRegressor(learning_rate=0.1, max_depth=3, subsample=0.8)
    grid_search = GridSearchCV(estimator=xgb_model, param_grid=param_grid, scoring='neg_mean_squared_error', cv=5)
    grid_search.fit(dtrain.get_data(), dtrain.get_label())
    print("Best parameters: ", grid_search.best_params_)
    print("Best score: ", grid_search.best_score_)
    best_n_estimators = grid_search.best_params_['n_estimators']
    # Define the XGBoost hyperparameters
    params = {
        'objective': 'reg:squarederror',
        'eval_metric': 'mae',
        'max_depth': 6,
        'learning_rate': 0.1,
        'subsample': 0.8,
        'colsample_bytree': 0.8,
        'seed': 42
    }

    # Train the XGBoost model
    eval_list = [(dtest, 'eval'), (dtrain, 'train')]
    bst = xgb.train(params, dtrain, num_boost_round=best_n_estimators, evals=eval_list, early_stopping_rounds=500,
                    verbose_eval=50)

    # Save the model to disk
    joblib.dump(bst, model_path)

    # Use the trained model to make predictions on the test data
    y_true = test_df[target].values
    y_pred = bst.predict(dtest)

    # Calculate the evaluation metrics
    mae = mean_absolute_error(y_true, y_pred)
    mse = mean_squared_error(y_true, y_pred)
    r2 = r2_score(y_true, y_pred)
    rmse = mean_squared_error(y_true, y_pred, squared=False)

    __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))

    # Create the data/ML directory if it doesn't exist
    if not os.path.exists(os.path.join(__location__, 'model/logs')):
        os.makedirs(os.path.join(__location__, 'model/logs'))

    # Create the training_logs.txt file inside the data/ML directory and write to it
    with open(os.path.join(__location__, 'model/logs/training_logs.txt'), 'a') as f:
        f.write(f'Training stats of etfs data\n')
        f.write(f'Training started at: {datetime.now()}\n')
        f.write(f'Training set size: {len(train_df)}\n')
        f.write(f'Test set size: {len(test_df)}\n')
        f.write(f'Number of rounds: {bst.best_iteration}\n')
        f.write(f'Minimum MAE: {bst.best_score}\n')
        f.write(f'Final MAE: {mae}\n')
        f.write(f'Final MSE: {mse}\n')
        f.write(f'Final R2: {r2}\n')
        f.write(f'Final RMSE: {rmse}\n')
        f.write('\n')

if __name__ == "__main__":
    # train_stocks_model()
    train_etfs_model()
    # test()
    # train_model_pyspark()

    # showdata()
