import streamlit as st
import pandas as pd
import pyodbc
from sqlalchemy import create_engine
from databricks import sql
from design_flow import TestDesign
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from dictionaries import CHANNELS_DICT, COUNTRIES_DICT, SOURCES_DICT, TARGETS_DICT, CATEGORIES_DICT


target_metric = "Выручка"
channels=['Доставка', 'Ресторан', 'Самовывоз']
sources=['Все']
if sources == ['Все']:
    sources = list(SOURCES_DICT.keys())
categories=['комбо', 'пицца', 'напитки', 'закуски', 'соусы', 'десерты', 'кусочки']

#Тут на вход получаем параметры, которы получаем от пользователя в Streamlit
test_params = {
    "target_metric": TARGETS_DICT[target_metric],
    "alpha": 0.05,
    "beta": 0.2,
    "start_period": str(datetime.now().date() - timedelta(days=60)),
    "end_period": str(datetime.now().date() - timedelta(days=1)),
}

query_params = {
    'channel': TestDesign.mapping_values(channels, CHANNELS_DICT),
    'source': TestDesign.mapping_values(sources, SOURCES_DICT),
    'category': TestDesign.mapping_values(categories, CATEGORIES_DICT)
}

# Создаем экземпляр класс
design = TestDesign(params_dict=test_params)

orders_table = "delta.`abfss://deltalake@p0dbsbb0sa0dbrks.dfs.core.windows.net/data/gold/OrderCompositionExtended`"
deps_table = "delta.`abfss://deltalake@p0dbsbb0sa0dbrks.dfs.core.windows.net/data/gold/DepartmentUnitsInfo`"

query = f"""
        select ord.UnitUUId,  
            dep.Name,
          ord.SaleDate,
          sum(ord.ProductTotalPrice) as revenue,
          count(distinct ord.OrderUUId) as cnt_orders
    from {orders_table} ord
    inner join {deps_table} dep
          on ord.UnitUUId=dep.UUId
    where 1=1
    and ord.BusinessId='DodoPizza'
    and ord.CountryId_int=643
    and ord.SaleDate>='{design.start_cuped_period}'
    and ord.SaleDate<'{design.end_period}'
    and ord.OrderType in ({query_params['channel']})
    and ord.OrderSource in ({query_params['source']})
    and ord.ComboProductCategoryId in ({query_params['category']})
    group by ord.UnitUUId,  
            dep.name,
          ord.SaleDate
    """

df_metrics_by_unit = TestDesign.read_sql(query=query)

# Осталяем только данные по юнитам без пропусков
df_metrics_by_unit['cnt_unique_dates'] = (df_metrics_by_unit
                                          .groupby(['UnitUUId'])['SaleDate']
                                          .transform('nunique')
                                          )
df_metrics_by_unit = (df_metrics_by_unit
                      [df_metrics_by_unit['cnt_unique_dates']
                       == df_metrics_by_unit['cnt_unique_dates'].max()]
                      .drop(columns=['cnt_unique_dates'])
                      )

df_metrics_by_unit['SaleDate'] = pd.to_datetime(df_metrics_by_unit['SaleDate'])
df_metrics_by_unit[design.target_metric] = df_metrics_by_unit[design.target_metric].astype(float)
df = df_metrics_by_unit[df_metrics_by_unit['SaleDate'] >= design.start_period].reset_index(drop=True)
# Исторические данные для CUPED
df_history = df_metrics_by_unit[df_metrics_by_unit['SaleDate'] < design.start_period].reset_index(drop=True)
# Считаем CUPED метрику
df_cuped = design.calculate_cuped_metric(df, df_history)

#Считаем матрицу эффект для CUPED метрики
df_sample_size = design.get_sample_size_matrix(design.get_sample_size_standart, df_cuped, is_cuped=True)
df_matrix, hm = design.get_day_matrix(df_sample_size)

print(df_matrix.head(10))
