import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import time
from pytrends.request import TrendReq
import logging
import gspread
import os
import json
from google.oauth2.service_account import Credentials
import base64
import traceback

# Configuración de logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Crear formato de logging
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Crear manejador para archivo
file_handler = logging.FileHandler('google_trends_data.log', mode='a')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Crear manejador para consola
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

def split_list(lst, n):
    """Divide una lista en bloques de tamaño n."""
    return [lst[i:i + n] for i in range(0, len(lst), n)]

def get_tendencias(pytrends, countries, football_keywords, timeframes=['now 7-d', 'today 1-m'], plot=False):
    """
    Obtiene tendencias generales para los países y periodos especificados.
    Retorna un diccionario de DataFrames con columnas consistentes.
    """
    trends_list = []  # Lista para almacenar los datos de tendencias

    for country_name, codes in countries.items():
        country_code_geo = codes['geo']
        country_code_pn = codes['pn']
        for timeframe in timeframes:
            try:
                logger.info(f"Obteniendo tendencias para {country_name} en el periodo {timeframe}")
                # Obtener tendencias diarias para el país
                daily_trends = pytrends.trending_searches(pn=country_code_pn)
                daily_trends.columns = ['trend']  # Renombrar la columna
                daily_trends['country'] = country_name

                # Filtrar las tendencias para eliminar temas relacionados con fútbol
                filtered_trends = [trend for trend in daily_trends['trend'] if not any(keyword.lower() in trend.lower() for keyword in football_keywords)]
                filtered_trends = list(set(filtered_trends))  # Eliminar duplicados

                if not filtered_trends:
                    logger.info(f"No hay tendencias filtradas para {country_name} en el periodo {timeframe}")
                    continue

                # Dividir las tendencias en grupos para evitar límites de la API
                trends_chunks = split_list(filtered_trends, 5)

                for chunk in trends_chunks:
                    logger.info(f"Construyendo payload para {chunk} en {country_name}, periodo {timeframe}")
                    pytrends.build_payload(chunk, timeframe=timeframe, geo=country_code_geo)
                    trends_data = pytrends.interest_over_time()

                    if trends_data.empty:
                        logger.info(f"No hay datos de interés para {chunk} en {country_name}, periodo {timeframe}")
                        continue

                    # Eliminar la columna "isPartial" si está presente
                    if 'isPartial' in trends_data.columns:
                        trends_data = trends_data.drop(columns=['isPartial'])

                    # Reestructurar el DataFrame para tener columnas consistentes
                    trends_data = trends_data.reset_index().melt(id_vars=['date'], var_name='trend', value_name='interest')

                    # Añadir información de país y periodo
                    trends_data['country'] = country_name
                    trends_data['timeframe'] = timeframe

                    # Añadir a la lista de tendencias
                    trends_list.append(trends_data)

                    if plot:
                        for trend in chunk:
                            data_to_plot = trends_data[trends_data['trend'] == trend]
                            plt.plot(data_to_plot['date'], data_to_plot['interest'], label=trend)
                        plt.title(f"Tendencias en {country_name} para {timeframe}")
                        plt.xlabel('Fecha')
                        plt.ylabel('Interés')
                        plt.legend()
                        plt.show()
                        time.sleep(5)
            except Exception as e:
                logger.error(f"Error al obtener tendencias para {country_name} en el periodo {timeframe}: {str(e)}")
                logger.error(traceback.format_exc())
                continue

    # Concatenar todos los DataFrames en uno solo
    if trends_list:
        trends_df = pd.concat(trends_list, ignore_index=True)
        logger.info(f"DataFrame de tendencias creado con {len(trends_df)} registros.")
    else:
        trends_df = pd.DataFrame(columns=['date', 'trend', 'interest', 'country', 'timeframe'])
        logger.warning("No se obtuvieron datos de tendencias.")

    # Retornar el DataFrame final en un diccionario para mantener consistencia con el formato original
    trends_dict = {'trends_data': trends_df}

    return trends_dict

def print_trends(pytrends, keywords, countries, timeframes=['now 7-d', 'today 1-m'], plot=False):
    """
    Obtiene el interés a lo largo del tiempo para palabras clave específicas.
    Retorna un diccionario de DataFrames con columnas consistentes.
    """
    trends_list = []  # Lista para almacenar los datos de interés por palabra clave

    keywords_chunks = split_list(keywords, 5)

    for country_name, codes in countries.items():
        country_code_geo = codes['geo']
        for timeframe in timeframes:
            for chunk in keywords_chunks:
                try:
                    logger.info(f"Construyendo payload para {chunk} en {country_name}, periodo {timeframe}")
                    pytrends.build_payload(chunk, timeframe=timeframe, geo=country_code_geo)
                    interest_over_time = pytrends.interest_over_time()

                    if interest_over_time.empty:
                        logger.info(f"No hay datos de interés para {chunk} en {country_name}, periodo {timeframe}")
                        continue

                    # Eliminar la columna "isPartial" si está presente
                    if 'isPartial' in interest_over_time.columns:
                        interest_over_time = interest_over_time.drop(columns=['isPartial'])

                    # Reestructurar el DataFrame para tener columnas consistentes
                    interest_over_time = interest_over_time.reset_index().melt(id_vars=['date'], var_name='keyword', value_name='interest')

                    # Añadir información de país y periodo
                    interest_over_time['country'] = country_name
                    interest_over_time['timeframe'] = timeframe

                    # Añadir a la lista de tendencias
                    trends_list.append(interest_over_time)

                    if plot:
                        for keyword in chunk:
                            data_to_plot = interest_over_time[interest_over_time['keyword'] == keyword]
                            plt.plot(data_to_plot['date'], data_to_plot['interest'], label=keyword)
                        plt.title(f"Interés en {country_name} para {timeframe}")
                        plt.xlabel('Fecha')
                        plt.ylabel('Interés')
                        plt.legend()
                        plt.show()
                        time.sleep(5)
                except Exception as e:
                    logger.error(f"Error al obtener interés para {chunk} en {country_name}, periodo {timeframe}: {str(e)}")
                    logger.error(traceback.format_exc())
                    continue

    # Concatenar todos los DataFrames en uno solo
    if trends_list:
        interest_df = pd.concat(trends_list, ignore_index=True)
        logger.info(f"DataFrame de interés por palabras clave creado con {len(interest_df)} registros.")
    else:
        interest_df = pd.DataFrame(columns=['date', 'keyword', 'interest', 'country', 'timeframe'])
        logger.warning("No se obtuvieron datos de interés por palabras clave.")

    # Retornar el DataFrame final en un diccionario para mantener consistencia con el formato original
    trends_dict = {'keywords_interest': interest_df}

    return trends_dict

def save_dataframe_to_gsheet(dataframe, spreadsheet_id):
    try:
        # Convertir todas las columnas datetime a strings
        datetime_columns = dataframe.select_dtypes(include=['datetime64[ns]', 'datetime64[ns, UTC]']).columns
        for col in datetime_columns:
            dataframe[col] = dataframe[col].dt.strftime('%Y-%m-%d %H:%M:%S')
            logger.info(f"Columna '{col}' convertida a string.")

        # Abrir la hoja de cálculo
        sheet = gc.open_by_key(spreadsheet_id)

        # Usar la primera hoja del documento
        worksheet = sheet.sheet1
        worksheet.clear()

        # Convertir el DataFrame a una lista de listas
        data = [dataframe.columns.values.tolist()] + dataframe.values.tolist()

        # Actualizar la hoja con los datos
        worksheet.update(data)
        logger.info(f"Datos actualizados en la hoja de cálculo con ID '{spreadsheet_id}'.")
    except Exception as e:
        logger.error(f"Error al actualizar la hoja de cálculo con ID '{spreadsheet_id}': {str(e)}")
        logger.error(traceback.format_exc())

# Ejemplo de uso
if __name__ == "__main__":
    # Inicializar pytrends
    pytrends = TrendReq(hl='es-MX', tz=360)

    # Definir países con sus códigos 'geo' y 'pn'
    countries = {
        'Mexico': {'geo': 'MX', 'pn': 'mexico'},
        'United States': {'geo': 'US', 'pn': 'united_states'}
    }

    # Lista de palabras clave de fútbol para filtrar
    football_keywords = ['fútbol', 'football', 'soccer', 'gol', 'liga', 'champions', 'mundial']

    # Obtener tendencias
    tendencias = get_tendencias(pytrends, countries, football_keywords, plot=False)

    # Lista de palabras clave
    keywords = ['python', 'data science', 'machine learning']

    # Obtener interés por tiempo
    interes = print_trends(pytrends, keywords, countries, plot=False)

    # Cargar las credenciales de Google Sheets desde la variable de entorno
    google_creds_json = os.environ.get('GOOGLE_SHEETS_CREDS_BASE64')
    if not google_creds_json:
        logger.error("Las credenciales de Google Sheets no están configuradas en 'GOOGLE_SHEETS_CREDS_BASE64'")
        exit(1)

    # Decodificar las credenciales de base64
    try:
        decoded_creds = base64.b64decode(google_creds_json)
        creds_dict = json.loads(decoded_creds)
        credentials = Credentials.from_service_account_info(
            creds_dict,
            scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        )
        gc = gspread.authorize(credentials)
    except Exception as e:
        logger.error(f"Error al cargar las credenciales de Google Sheets: {str(e)}")
        logger.error(traceback.format_exc())
        exit(1)

    # IDs de las hojas de cálculo de Google Sheets
    spreadsheet_id_trends = os.environ.get('SPREADSHEET_ID_TRENDS')
    if not spreadsheet_id_trends:
        logger.error("El ID de la hoja de cálculo para tendencias no está configurado en 'SPREADSHEET_ID_TRENDS'")
        exit(1)

    spreadsheet_id_keywords = os.environ.get('SPREADSHEET_ID_KEYWORDS')
    if not spreadsheet_id_keywords:
        logger.error("El ID de la hoja de cálculo para palabras clave no está configurado en 'SPREADSHEET_ID_KEYWORDS'")
        exit(1)

    # Guardar los DataFrames en diferentes documentos
    try:
        # Guardar las tendencias generales
        trends_df = tendencias['trends_data']
        save_dataframe_to_gsheet(trends_df, spreadsheet_id_trends)

        # Guardar el interés por palabras clave
        interest_df = interes['keywords_interest']
        save_dataframe_to_gsheet(interest_df, spreadsheet_id_keywords)

        logger.info("Datos guardados exitosamente en documentos de Google Sheets separados.")
    except Exception as e:
        logger.error(f"Error al guardar los datos en Google Sheets: {str(e)}")
        logger.error(traceback.format_exc())
        exit(1)
