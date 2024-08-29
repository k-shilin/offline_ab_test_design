import streamlit as st

import pandas as pd
import pyodbc
from sqlalchemy import create_engine

from databricks import sql


st.write("привет")

databricks_host = "adb-5812797581086196.16.azuredatabricks.net"
http_path = "/sql/1.0/warehouses/cbe60f5e62a3c9cb"
access_token = "dapi844cdc97068e58f28fe254d3e8c308bb-2"



connection = sql.connect(
    server_hostname="adb-5812797581086196.16.azuredatabricks.net",
    http_path="/sql/1.0/warehouses/cbe60f5e62a3c9cb",
    access_token="dapi844cdc97068e58f28fe254d3e8c308bb-2",
)

cursor = connection.cursor()

cursor.execute("SELECT * FROM delta.`abfss://deltalake@p0dbsbb0sa0dbrks.dfs.core.windows.net/data/gold/OrderCompositionExtended` limit 10")

# print(cursor.fetchall())

# cursor.close()
# connection.close()


# Получение всех строк результата
rows = cursor.fetchall()

# Получение имен столбцов
columns = [desc[0] for desc in cursor.description]

# Преобразование результата в DataFrame
df = pd.DataFrame.from_records(rows, columns=columns)

# Закрытие соединения
cursor.close()
connection.close()

# Вывод DataFrame
print(df)




# UI Streamlit
st.title("Streamlit приложение для чтения данных из Databricks")

query = st.text_area("Введите SQL запрос", "SELECT * FROM delta.`abfss://deltalake@p0dbsbb0sa0dbrks.dfs.core.windows.net/data/gold/OrderCompositionExtended` limit 10")

if st.button("Загрузить данные"):
    data = df
#     data = load_data(query)
    st.write("Загруженные данные:")
    st.dataframe(data)



st.write("Hello World")