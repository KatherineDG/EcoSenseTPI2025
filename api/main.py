from fastapi import FastAPI, Query
from spark import spark_connection
from utils import aggregate_trend

app = FastAPI(title="API Metrics of Sensors")

# Conexión de Spark
spark = spark_connection.create_spark_session()


@app.get("/")
def root():
    return {"message": "API de Métricas de Sensores con Spark"}


@app.get("/trend")
def get_sensor_data(
        sensor: str = Query(...),
        sede: str = Query(...),
        start_date: str = Query(None),
        end_date: str = Query(None),
        year: int = Query(None),
        start_year: int = Query(None),
        end_year: int = Query(None),
        trend_by: str = Query("day", enum=["hour","day","month","year"])):

    """
    Trae datos de un sensor usando Spark, filtrando por fecha de inicio y fin.
    También puede devolver tendencia histórica si trend=True (order).
    """
    try:
        # Leer datos desde Mongo
        df = spark_connection.read_mongo(spark, sensor)
        df_trend = aggregate_trend(df, trend_by, start_date, end_date, year, start_year, end_year)
        # Convertir a Pandas devolviendo en JSON
        return df_trend.toPandas().to_dict(orient="records")

    except Exception as e:
        return {"error": str(e)}





