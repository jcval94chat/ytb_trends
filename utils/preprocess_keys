import numpy as np
import pandas as pd

def calculate_daily_stats(df):
  # Convertir la columna `date` a nivel día
  df['date'] = pd.to_datetime(df['date'])
  df['day'] = df['date'].dt.date

  # Convert 'interest' to numeric, handling errors
  df['interest'] = pd.to_numeric(df['interest'], errors='coerce')

  # Calcular métricas para cada día
  aggregations = {
      'interest': ['max', 'min', 'mean', 'median', 'std']
  }

  daily_stats = df.groupby(['day', 'keyword', 'country']).agg(aggregations).reset_index()

  # Aplanar los nombres de columnas
  daily_stats.columns = ['day', 'keyword', 'country',
                         'max_interest', 'min_interest',
                         "mean_interest", 'median_interest', 'std_interest']

  return daily_stats

def calculate_cumulative_interest(df, cum_inter = 'cumulative_max_interest', ascending=True):
  """Calculates the cumulative sum of max_interest for each keyword-country series over time."""

  # Ensure the 'day' column is datetime objects for proper sorting
  df['day'] = pd.to_datetime(df['day'])

  # Sort the DataFrame by date
  df_sorted = df.sort_values(by=['keyword', 'country', 'day'], ascending=[True, True, ascending])

  # Calculate the cumulative sum of 'max_interest'
  df_sorted[cum_inter] = df_sorted.groupby(['keyword', 'country'])['max_interest'].cumsum()

  return df_sorted



def filter_recent_high_median_interest(dataframe,
                                       days_threshold=30,
                                       percentile_threshold=25,
                                       date_column='day',
                                       median_column='median_interest',
                                       group_by_columns=['keyword', 'country']):
    """
    Filters rows of a DataFrame to retain only categories (e.g., keyword, country)
    that in the last specified days have median interest above a given percentile threshold.

    Parameters:
    - dataframe (pd.DataFrame): Input DataFrame with interest statistics.
    - days_threshold (int): Number of days to consider as the recent period.
    - percentile_threshold (float): Percentile threshold for filtering (0-100).
    - date_column (str): Column representing the date.
    - median_column (str): Column representing the median interest.
    - group_by_columns (list): List of columns to group by for percentile calculation.

    Returns:
    - pd.DataFrame: Filtered DataFrame with only the rows above the specified percentile within recent days.
    """
    # Convert date column to datetime if not already
    dataframe[date_column] = pd.to_datetime(dataframe[date_column])

    # Get the max date and filter rows within the last `days_threshold` days
    max_date = dataframe[date_column].max()
    recent_start_date = max_date - timedelta(days=days_threshold)
    recent_df = dataframe[dataframe[date_column] >= recent_start_date]

    # Calculate the threshold for the specified percentile within recent data
    thresholds = recent_df.groupby(group_by_columns)[median_column].quantile(percentile_threshold / 100).to_dict()
    # print(thresholds)

    # Filter the original DataFrame to retain rows above the calculated thresholds
    def is_above_threshold(row):
        group_key = tuple(row[group_by_columns])
        return row[median_column] > thresholds.get(group_key, float('-inf'))

    filtered_df = dataframe[dataframe.apply(is_above_threshold, axis=1)]

    return filtered_df


def rank_categories(dataframe, metric_column='median_interest', group_by_columns=['keyword', 'country']):
    """
    Ranks categories (e.g., keyword, country) based on the specified metric.

    Parameters:
    - dataframe (pd.DataFrame): Input DataFrame with interest statistics.
    - metric_column (str): Column name to rank the categories.
    - group_by_columns (list): List of columns to group by for ranking.

    Returns:
    - pd.DataFrame: DataFrame with categories ranked from best to worst.
    """
    # Calculate the mean of the metric for each group
    rankings = dataframe.groupby(group_by_columns)[metric_column].mean().reset_index()
    rankings = rankings.sort_values(by=metric_column, ascending=False).reset_index(drop=True)
    rankings['rank'] = rankings.index + 1
    return rankings

def plot_rankings(rankings, metric_column='median_interest', group_by_columns=['keyword', 'country'], top_n=10):
    """
    Plots the rankings of categories based on the specified metric.

    Parameters:
    - rankings (pd.DataFrame): DataFrame with ranked categories.
    - metric_column (str): Column name representing the ranking metric.
    - group_by_columns (list): List of columns that define categories.
    - top_n (int): Number of top categories to display in the plot.
    """
    # Combine group_by_columns into a single column for display
    rankings['category'] = rankings[group_by_columns[0]] + " - " + rankings[group_by_columns[1]]

    # Get the top N rankings
    top_rankings = rankings.head(top_n)

    # Plot
    plt.figure(figsize=(10, 6))
    plt.barh(top_rankings['category'], top_rankings[metric_column], color='skyblue')
    plt.xlabel(metric_column.replace('_', ' ').capitalize())
    plt.ylabel('Category')
    plt.title(f'Top {top_n} Categories by {metric_column.replace("_", " ").capitalize()}')
    plt.gca().invert_yaxis()  # Invert y-axis for ranking display
    plt.tight_layout()
    plt.show()


def filtrar_mejores(df_resultado):
    # prompt: de df_resultado obtén el último día de la columna derivada_daily para cada trend y después su derivada_daily usando merge

    # Agrupa por 'trend' y obtiene el último día
    last_days = df_resultado.groupby('trend')['day'].max().reset_index()

    # Renombra la columna 'day' a 'last_day' para evitar conflictos en el merge
    last_days.rename(columns={'day': 'last_day'}, inplace=True)

    # Une los DataFrames usando 'trend' como clave
    df_merged = pd.merge(df_resultado, last_days, on='trend', how='left')

    # Filtra las filas que corresponden al último día para cada tendencia
    df_last_day_data = df_merged[df_merged['day'] == df_merged['last_day']]

    # Ahora df_last_day_data contiene la información del último día de cada tendencia
    # incluyendo la columna 'derivada_daily'
    df_last_day_data = df_last_day_data[((df_last_day_data['derivada_daily']>-50)\
                                         |(df_last_day_data['interest_daily']>200))\
                                        &(df_last_day_data['interest_daily']>0)]['trend']

    return df_last_day_data.to_list()

def obtener_top_por_modo(
    df_in,
    top_n=10,
    w_daily=1.0,
    w_weekly=1.0,
    w_monthly=1.0,
    decay_base=0.75,
    type_metric='max'
):
    """
    Retorna un DataFrame con las tendencias (keywords) top por país,
    considerando scores diarios, semanales y mensuales basados en mean_interest.
    
    Parámetros
    ----------
    df_in : pd.DataFrame
        Debe contener columnas: 
        ['day', 'keyword', 'country', 'max_interest', 'min_interest', 
         'mean_interest', 'median_interest', 'std_interest']
    top_n : int
        Número de tendencias top a retornar por país.
    w_daily : float
        Peso para el score diario.
    w_weekly : float
        Peso para el score semanal.
    w_monthly : float
        Peso para el score mensual.
    decay_base : float
        Base del factor de decaimiento exponencial (por defecto 0.5).
    
    Retorna
    -------
    df_top : pd.DataFrame
        DataFrame con las top_n tendencias por country, ordenadas por su score_total.
        Incluye las columnas ['country', 'keyword', 'score_daily', 'score_weekly', 'score_monthly', 'score_total'].
    """
    # =======================
    # 1) Preparación de datos
    # =======================
    df = df_in.copy()
    
    # Aseguramos que 'day' sea de tipo fecha
    df['day'] = pd.to_datetime(df['day'], errors='coerce')
    df.dropna(subset=['day'], inplace=True)  # Eliminamos filas con 'day' NaN
    
    # Aseguramos que mean_interest sea numérico
    df['mean_interest'] = pd.to_numeric(df[type_metric+'_interest'], errors='coerce').fillna(0)
    
    # =======================
    # 2) Identificar el último día por (country, keyword)
    # =======================
    df_max_day = (
        df.groupby(['country', 'keyword'], as_index=False)['day']
          .max()
          .rename(columns={'day': 'max_day'})
    )
    
    # Merge para tener 'max_day' en cada fila
    df = df.merge(df_max_day, on=['country', 'keyword'], how='left')
    
    # =======================
    # 3) Calcular la diferencia en días con respecto a 'max_day'
    # =======================
    df['days_diff'] = (df['max_day'] - df['day']).dt.days
    
    # =======================
    # 4) Definir factor de decaimiento = decay_base^(days_diff)
    # =======================
    df['decay_factor'] = decay_base ** df['days_diff']
    
    # =======================
    # 5) Calcular score diario
    # =======================
    # Score diario = w_daily * log1p(mean_interest) * decay_factor
    # df['score_daily'] = (
    #     w_daily * np.log1p(df['mean_interest'].clip(lower=0)) * df['decay_factor']
    # )
    df['score_daily'] = (
        w_daily * np.log1p((df['mean_interest']+1).clip(lower=0)) * df['decay_factor']
    )
    
    # Agrupar por (country, keyword) para sumar los scores diarios
    df_daily = (
        df.groupby(['country', 'keyword'], as_index=False)['score_daily']
          .sum()
    )
    
    # =======================
    # 6) Calcular score semanal basado en estacionalidad
    # =======================
    # Definir ventana de la última semana y semanas anteriores
    # Para cada (country, keyword), definir el rango de la última semana
    # y calcular la diferencia de medias
    
    # Definir función para calcular score semanal
    def calcular_score_semanal(group):
        max_day = group['max_day'].max()
        last_week_start = max_day - pd.Timedelta(days=6)  # Últimos 7 días
        
        # Datos de la última semana
        last_week_data = group[group['day'] >= last_week_start]
        previous_weeks_data = group[group['day'] < last_week_start]
        
        # Calcular medias
        last_week_mean = last_week_data['mean_interest'].mean() if not last_week_data.empty else 0
        previous_weeks_mean = previous_weeks_data['mean_interest'].mean() if not previous_weeks_data.empty else 0
        
        # Diferencia de medias
        diff_week = last_week_mean - previous_weeks_mean
        
        # Opcional: aplicar factor de decaimiento si se desea
        # Aquí no aplicamos decaimiento adicional
        
        # Calcular score semanal
        score_weekly = w_weekly * diff_week
        
        return pd.Series({'score_weekly': score_weekly})
    
    # Aplicar la función a cada grupo (country, keyword)
    df_weekly = (
        df.groupby(['country', 'keyword'])
          .apply(calcular_score_semanal)
          .reset_index()
    )
    
    # =======================
    # 7) Calcular score mensual basado en estacionalidad
    # =======================
    # Definir ventana del último mes y meses anteriores
    # Para cada (country, keyword), definir el rango del último mes
    # y calcular la diferencia de medias
    
    # Definir función para calcular score mensual
    def calcular_score_mensual(group):
        max_day = group['max_day'].max()
        last_month_start = max_day - pd.Timedelta(days=29)  # Últimos 30 días
        
        # Datos del último mes
        last_month_data = group[group['day'] >= last_month_start]
        previous_months_data = group[group['day'] < last_month_start]
        
        # Calcular medias
        last_month_mean = last_month_data['mean_interest'].mean() if not last_month_data.empty else 0
        previous_months_mean = previous_months_data['mean_interest'].mean() if not previous_months_data.empty else 0
        
        # Diferencia de medias
        diff_month = last_month_mean - previous_months_mean
        
        # Opcional: aplicar factor de decaimiento si se desea
        # Aquí no aplicamos decaimiento adicional
        
        # Calcular score mensual
        score_monthly = w_monthly * diff_month
        
        return pd.Series({'score_monthly': score_monthly})
    
    # Aplicar la función a cada grupo (country, keyword)
    df_monthly = (
        df.groupby(['country', 'keyword'])
          .apply(calcular_score_mensual)
          .reset_index()
    )
    
    # =======================
    # 8) Combinar los scores diarios, semanales y mensuales
    # =======================
    # Merge de los DataFrames de scores
    df_combined = df_daily.merge(df_weekly, on=['country', 'keyword'], how='left')
    df_combined = df_combined.merge(df_monthly, on=['country', 'keyword'], how='left')
    
    # Rellenar NaN con 0 en los scores semanales y mensuales (en caso de faltar datos)
    df_combined[['score_weekly', 'score_monthly']] = df_combined[['score_weekly', 'score_monthly']].fillna(0)
    
    # Calcular score_total
    df_combined['score_total'] = df_combined['score_daily'] + df_combined['score_weekly'] + df_combined['score_monthly']
    
    # =======================
    # 9) Seleccionar top_n por country basado en score_total
    # =======================
    def get_top_n_por_country(g):
        return g.nlargest(top_n, 'score_total')
    
    df_top = (
        df_combined.groupby('country', group_keys=False)
                    .apply(get_top_n_por_country)
    )
    
    # Ordenar el resultado final
    df_top = df_top.sort_values(by=['country', 'score_total'], ascending=[True, False]).reset_index(drop=True)
    
    # Seleccionar y ordenar columnas relevantes
    df_top = df_top[['country', 'keyword', 'score_daily', 'score_weekly', 'score_monthly', 'score_total']]
    
    return df_top


def get_best_vids_metric(df_daily_filtrado):
    # Calculate the first derivative of the histogram
    def inflection_point(hist_data):
        """
        Finds the inflection point of a decreasing curve represented by histogram data.
        """
        gradient = np.gradient(hist_data)
        second_derivative = np.gradient(gradient)
        
        # Find the index where the second derivative changes sign from positive to negative.
        # This indicates a change from concave up to concave down, which corresponds to an inflection point.
        inflection_indices = np.where(np.diff(np.sign(second_derivative)))[0]
        
        if len(inflection_indices) > 0:  # Check if there are inflection points
            return inflection_indices[0] # return the first inflection point
        else:
            return None  # No clear inflection point found

    # Create the histogram and get the data
    hist_data, bin_edges = np.histogram(df_daily_filtrado['mean_interest'], bins=35)  # Adjust bins if necessary

    #Find the inflection point
    inflection_index = inflection_point(hist_data)

    percentil_70 = np.percentile(df_daily_filtrado['mean_interest'], 70)

    if inflection_index is not None:
      inflection_value = bin_edges[inflection_index]
      if inflection_value>7:
        return inflection_value*.85
      else:
        return percentil_70
      
    else:
      return percentil_70


def obtener_top_por_metricas(
    df_in,
    metrics=['mean_interest', 'min_interest', 'max_interest'],
    top_n=10,
    w_daily=1.0,
    w_weekly=1.0,
    w_monthly=1.0,
    decay_base=0.5
):
    """
    Retorna un diccionario con las top tendencias (keywords) por país para cada métrica especificada,
    considerando scores diarios, semanales y mensuales basados en la métrica correspondiente.
    
    Parámetros
    ----------
    df_in : pd.DataFrame
        Debe contener columnas: 
        ['day', 'keyword', 'country', 'max_interest', 'min_interest', 
         'mean_interest', 'median_interest', 'std_interest']
    metrics : list of str
        Lista de nombres de métricas a procesar, por ejemplo: ['mean_interest', 'min_interest', 'max_interest'].
    top_n : int
        Número de tendencias top a retornar por país para cada métrica.
    w_daily : float
        Peso para el score diario.
    w_weekly : float
        Peso para el score semanal.
    w_monthly : float
        Peso para el score mensual.
    decay_base : float
        Base del factor de decaimiento exponencial (por defecto 0.5).
    
    Retorna
    -------
    dict_of_top : dict
        Diccionario donde las claves son los nombres de las métricas y los valores son DataFrames con las top_n tendencias por país.
        Cada DataFrame contiene las columnas:
        ['country', 'keyword', 'score_daily', 'score_weekly', 'score_monthly', 'score_total'].
    """
    
    # Inicializar el diccionario de resultados
    dict_of_top = {}
    
    # Validar que las métricas existan en el DataFrame
    for metric in metrics:
        if metric not in df_in.columns:
            raise ValueError(f"La métrica '{metric}' no existe en el DataFrame de entrada.")
    
    # Procesar cada métrica de forma independiente
    for metric in metrics:
        # =======================
        # 1) Preparación de datos
        # =======================
        df = df_in.copy()
        
        # Aseguramos que 'day' sea de tipo fecha
        df['day'] = pd.to_datetime(df['day'], errors='coerce')
        df.dropna(subset=['day'], inplace=True)  # Eliminamos filas con 'day' NaN
        
        # Aseguramos que la métrica actual sea numérica
        df[metric] = pd.to_numeric(df[metric], errors='coerce').fillna(0)
        
        # =======================
        # 2) Identificar el último día por (country, keyword)
        # =======================
        df_max_day = (
            df.groupby(['country', 'keyword'], as_index=False)['day']
              .max()
              .rename(columns={'day': 'max_day'})
        )
        
        # Merge para tener 'max_day' en cada fila
        df = df.merge(df_max_day, on=['country', 'keyword'], how='left')
        
        # =======================
        # 3) Calcular la diferencia en días con respecto a 'max_day'
        # =======================
        df['days_diff'] = (df['max_day'] - df['day']).dt.days
        
        # =======================
        # 4) Definir factor de decaimiento = decay_base^(days_diff)
        # =======================
        df['decay_factor'] = decay_base ** df['days_diff']
        
        # =======================
        # 5) Calcular score diario
        # =======================
        # Score diario = w_daily * log1p(metric).clip(lower=0) * decay_factor
        # Para métricas que pueden tener valores negativos, se pueden ajustar según sea necesario.
        # Aquí, se usa log1p para comprimir la escala y manejar valores cero.
        # Si la métrica puede ser negativa y deseas manejarlo diferente, ajusta esta parte.
        df['score_daily'] = (
            w_daily * np.log1p(df[metric].clip(lower=0)) * df['decay_factor']
        )
        
        # Agrupar por (country, keyword) para sumar los scores diarios
        df_daily = (
            df.groupby(['country', 'keyword'], as_index=False)['score_daily']
              .sum()
        )
        
        # =======================
        # 6) Calcular score semanal basado en estacionalidad
        # =======================
        # Definir función para calcular score semanal
        def calcular_score_semanal(group):
            max_day = group['max_day'].max()
            last_week_start = max_day - pd.Timedelta(days=6)  # Últimos 7 días
            
            # Datos de la última semana
            last_week_data = group[group['day'] >= last_week_start]
            previous_weeks_data = group[group['day'] < last_week_start]
            
            # Calcular medias
            last_week_mean = last_week_data[metric].mean() if not last_week_data.empty else 0
            previous_weeks_mean = previous_weeks_data[metric].mean() if not previous_weeks_data.empty else 0
            
            # Diferencia de medias
            diff_week = last_week_mean - previous_weeks_mean
            
            # Calcular score semanal
            score_weekly = w_weekly * diff_week
            
            return pd.Series({'score_weekly': score_weekly})
        
        # Aplicar la función a cada grupo (country, keyword)
        df_weekly = (
            df.groupby(['country', 'keyword'])
              .apply(calcular_score_semanal)
              .reset_index()
        )
        
        # =======================
        # 7) Calcular score mensual basado en estacionalidad
        # =======================
        # Definir función para calcular score mensual
        def calcular_score_mensual(group):
            max_day = group['max_day'].max()
            last_month_start = max_day - pd.Timedelta(days=29)  # Últimos 30 días
            
            # Datos del último mes
            last_month_data = group[group['day'] >= last_month_start]
            previous_months_data = group[group['day'] < last_month_start]
            
            # Calcular medias
            last_month_mean = last_month_data[metric].mean() if not last_month_data.empty else 0
            previous_months_mean = previous_months_data[metric].mean() if not previous_months_data.empty else 0
            
            # Diferencia de medias
            diff_month = last_month_mean - previous_months_mean
            
            # Calcular score mensual
            score_monthly = w_monthly * diff_month
            
            return pd.Series({'score_monthly': score_monthly})
        
        # Aplicar la función a cada grupo (country, keyword)
        df_monthly = (
            df.groupby(['country', 'keyword'])
              .apply(calcular_score_mensual)
              .reset_index()
        )
        
        # =======================
        # 8) Combinar los scores diarios, semanales y mensuales
        # =======================
        # Merge de los DataFrames de scores
        df_combined = df_daily.merge(df_weekly, on=['country', 'keyword'], how='left')
        df_combined = df_combined.merge(df_monthly, on=['country', 'keyword'], how='left')
        
        # Rellenar NaN con 0 en los scores semanales y mensuales (en caso de faltar datos)
        df_combined[['score_weekly', 'score_monthly']] = df_combined[['score_weekly', 'score_monthly']].fillna(0)
        
        # Calcular score_total
        df_combined['score_total'] = df_combined['score_daily'] + df_combined['score_weekly'] + df_combined['score_monthly']
        
        # =======================
        # 9) Seleccionar top_n por country basado en score_total
        # =======================
        def get_top_n_por_country(g):
            return g.nlargest(top_n, 'score_total')
        
        df_top = (
            df_combined.groupby('country', group_keys=False)
                        .apply(get_top_n_por_country)
        )
        
        # Ordenar el resultado final
        df_top = df_top.sort_values(by=['country', 'score_total'], ascending=[True, False]).reset_index(drop=True)
        
        # Seleccionar y ordenar columnas relevantes
        df_top = df_top[['country', 'keyword', 'score_daily', 'score_weekly', 'score_monthly', 'score_total']]
        
        # Añadir al diccionario de resultados
        dict_of_top[metric] = df_top
    
    return dict_of_top


def preprocesar_keys(combined_df_keys):
    # prompt: para cada serie compuesta de keyword, country, obtén la suma acumulada de max_interest en el tiempo
    df_daily = calculate_daily_stats(combined_df_keys)
    df_daily = calculate_cumulative_interest(df_daily,'cum_int_T',1)
    df_daily = calculate_cumulative_interest(df_daily,'cum_int_F',0)
    df_daily = df_daily[df_daily['cum_int_T']*df_daily['cum_int_F']>0]
    df_daily.drop(columns=['cum_int_T','cum_int_F'], inplace=True)
    # Encuentra la fecha máxima en el DataFrame
    fecha_maxima = df_daily['day'].max()
    fecha_limite = fecha_maxima - timedelta(days=60)
    df_daily_filtrado = df_daily[df_daily['day'] >= fecha_limite]

    punto_de_corte = get_best_vids_metric(df_daily_filtrado)
    punto_de_corte *=.8
    # print(punto_de_corte)
    best_50 = pd.pivot_table(df_daily_filtrado,index =['keyword','country'],
                            values=['mean_interest']).sort_values('mean_interest',ascending=False).head(75)
                            
    best_50_index = best_50[best_50['mean_interest']>punto_de_corte].index

    worst_40 = pd.pivot_table(df_daily_filtrado,index =['keyword','country'],
                            values=['mean_interest']).sort_values('mean_interest',ascending=False).tail(40)

    worst_40_index = worst_40[worst_40['mean_interest']<=(punto_de_corte/2)].index

    df_daily_filtrado_BS = df_daily_filtrado[df_daily_filtrado.apply(lambda row: (row['keyword'], row['country']) in best_50_index, axis=1)]
    df_daily_filtrado_WS = df_daily_filtrado[df_daily_filtrado.apply(lambda row: (row['keyword'], row['country']) in worst_40_index, axis=1)]

    inc_trends_max = obtener_top_por_metricas(df_daily_filtrado_BS, ['mean_interest', 
                                                                    'min_interest', 
                                                                    'max_interest'],30)

    all_dfs = []
    for key, df in inc_trends_max.items():
        df['metric'] = key 
        all_dfs.append(df)

    concatenated_df = pd.concat(all_dfs, ignore_index=True)

    return concatenated_df, df_daily_filtrado_BS, df_daily_filtrado_WS
