import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
import matplotlib.pyplot as plt
import seaborn as sns
import scipy.stats as stats
from typing import Callable, Optional, List, Dict
from databricks import sql
from sqlalchemy import create_engine
import pyodbc


class TestDesign:
    def __init__(self, *, params_dict: dict):
        """
        Инициализация базовых атрибутов класса
        """
        self.target_metric = params_dict["target_metric"]
        self.alpha = params_dict["alpha"]
        self.beta = params_dict["beta"]
        self.start_period = params_dict["start_period"]
        self.end_period = params_dict["end_period"]
        self.start_cuped_period = str(
            (
                    pd.to_datetime(params_dict["start_period"])
                    - timedelta(
                days=(
                        pd.to_datetime(params_dict["end_period"])
                        - pd.to_datetime(params_dict["start_period"])
                ).days
            )
            ).date()
        )

    @staticmethod
    def mapping_values(lst:List, dct:Dict)->str:
        lst = list(map(lambda value: dct[value], lst))
        lst = ", ".join([f"'{value}'" for value in lst])
        return lst
    @staticmethod
    def read_sql(query: str,
                 server_hostname,
                 http_path,
                 access_token) -> pd.DataFrame:

        # Создание подключения
        connection = sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token
        )

        try:
            # Выполнение запроса
            cursor = connection.cursor()
            cursor.execute(query)

            # Получение данных и имен столбцов
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            # Преобразование результата в DataFrame
            df = pd.DataFrame.from_records(rows, columns=columns)

        finally:
            # Закрытие курсора и соединения
            cursor.close()
            connection.close()

        return df


    def get_sample_size_standart(self, df: pd.DataFrame, mde: float, is_cuped=False) -> int:
        """
        Классическая формула расчета размера выборок для группы.

        args:
            df - датафрейм со среднедневной метрикой,
            mde - мин. детектируемый эффект
        return:
            sample_size - размер выборки для теста,
            'standart' - тип метода
        """
        if is_cuped:
            mean = np.mean(df[self.target_metric + '_cuped'])
            std = np.std(df[self.target_metric + '_cuped'])
        else:
            mean = np.mean(df[self.target_metric])
            std = np.std(df[self.target_metric])

        # расчет delta
        delta = mde * mean

        # расчет z-значений
        z_alpha_2 = stats.norm.ppf(1 - self.alpha / 2)
        z_beta = stats.norm.ppf(1 - self.beta)

        # Размер выборки
        n = ((z_alpha_2 + z_beta) * std / delta) ** 2
        # n = 2*((z_alpha_2 + z_beta)**2)*(std**2/delta**2) #если для обоих групп
        return np.ceil(n)

    def get_sample_size_matrix(
            self, sample_size_func: Callable,
            df: pd.DataFrame,
            effect_bounds=np.linspace(0.01, 0.10, 19),
            is_cuped=False
    ) -> pd.DataFrame:
        """
        Строит матрицу значений размера выборки в зависимости от mde.

        args:
            sample_size_func - функция расчета размера выборки,
            mu - среднее выборки на исторических данных,
            std - стан. отклонение выборки на исторических данных,
        return:
            df_res - датафрейм
        """
        res = []
        for eff in effect_bounds:
            sample_size = sample_size_func(df, eff, is_cuped)
            res.append((eff, int(sample_size)))
        return pd.DataFrame(res, columns=["effects", "sample_size"])

    def get_day_matrix(
            self, df: pd.DataFrame,
            days_list= [7, 14, 21, 28, 35, 42, 49, 56]
            #plot=False
    ):
        """
        Строит итоговую матрицу для определения кол-ва юнитов/дней для теста.

        args:
            df - датафрейм размер выборки-mde,
            days_list - список дней, по которым итерируемся,
        return:
            df_ - датафрейм с итоговой матрицей
        """
        df_ = df.copy()
        for i in days_list:
            df_[f"units_cnt_{i}_days"] = round(df_["sample_size"] / i, 0).astype("int")

        #if plot:
        for_pic = df_.copy()
        for_pic["effects"] = round(for_pic["effects"] * 100, 1).astype("str")
        for_pic = for_pic.drop(columns=["sample_size"]).set_index(["effects"])

        fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(20, 8))
        sns.heatmap(
            for_pic,
            annot=True,
            fmt="d",
            cmap="Blues",
            linewidths=2,
            center=np.percentile(for_pic[for_pic.columns[0]].values, 50),
            vmax=np.percentile(for_pic[for_pic.columns[0]].values, 90),
            vmin=np.percentile(for_pic[for_pic.columns[0]].values, 10),
            cbar=False,
            ax=axes,
        )
        axes.set_yticklabels(axes.get_yticklabels(), rotation=0)
        axes.set_ylabel("Эффект в %", fontsize=14)
        axes.set_xlabel("Кол-во дней теста", fontsize=14)
        axes.set_title("Матрица кол-ва юнитов на тест", fontsize=18)
            #plt.show()

        return df_, fig

    def _calculate_theta(self, *, y_history: np.array, y: np.array) -> float:
        """
        Вычисляем Theta

        Args:
            y_prepilot (np.array): значения метрики во время пилота
            y_pilot (np.array): значения ковариант (той же самой метрики) на препилоте

        Returns:
            float: значение коэффициента тета
        """
        covariance = np.cov(y_history.astype(float), y.astype(float))[0, 1]
        variance = np.var(y_history)
        theta = covariance / variance
        return theta

    def _sort_merge_for_cuped(self, df: pd.DataFrame, df_history: pd.DataFrame):
        """
        Формирование и сортировка датафреймов для cuped'a

        Args:
            df: датафрейм со значением метрики
            df_history: датафрейм со значение метрики на периоде до

        Returns:
            df, df_history: отсортированные для расчета CUPED датафреймы
        """

        df['weekday'] = df['SaleDate'].apply(lambda x: x.weekday() + 1)
        df_history['weekday'] = df_history['SaleDate'].apply(lambda x: x.weekday() + 1)

        df["row_number"] = [i for i in range(0, len(df))]
        df_history["row_number"] = [i for i in range(0, len(df_history))]
        df = df.sort_values(by=['Name', 'weekday', 'row_number']).reset_index(drop=True)
        df_history = df_history.sort_values(by=['Name', 'weekday', 'row_number']).reset_index(drop=True)
        return df, df_history

    def calculate_cuped_metric(self, df: pd.DataFrame, df_history: pd.DataFrame):
        df, df_history = self._sort_merge_for_cuped(df, df_history)

        df_cuped = pd.concat([df, df_history[[self.target_metric]].rename(
            columns={self.target_metric: f'{self.target_metric}_history'})], axis=1, ignore_index=False)

        theta = self._calculate_theta(y_history=np.array(df_cuped[self.target_metric + '_history']),
                                      y=np.array(df_cuped[self.target_metric]))

        df_cuped[f'{self.target_metric}_cuped'] = (
                df_cuped[self.target_metric] - theta * (
                    df_cuped[self.target_metric + '_history'] - np.mean(df_cuped[self.target_metric + '_history']))
        )
        return df_cuped