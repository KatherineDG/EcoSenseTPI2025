from pyspark.sql.functions import col, to_timestamp, to_date, date_format, avg, min, max, lit, expr, explode, sequence, coalesce

def aggregate_trend(df, trend_by: str = "day", start_date: str = None, end_date: str = None, year: int = None, start_year: int = None, end_year: int = None):
    """
    Devuelve tendencia agregada según la frecuencia:
    'hour', 'day', 'month', 'year'
    """
    # Convertir timestamp
    df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
    
    # Filtrar por rango de fechas si se pasan
    # if start_date:
    #     df = df.filter(col("timestamp") >= to_timestamp(lit(start_date), "yyyy-MM-dd"))
    # if end_date:
    #     df = df.filter(col("timestamp") <= to_timestamp(lit(end_date), "yyyy-MM-dd"))
    
    # Crear columna de agrupación según trend_by
    if trend_by == "hour":
        if not start_date:
            raise ValueError("Para 'hour', start_date debe estar definido")
        
        df = df.filter(
            (col("timestamp") >= to_timestamp(lit(start_date), "yyyy-MM-dd")) &
            (col("timestamp") < to_timestamp(lit(start_date), "yyyy-MM-dd") + expr("INTERVAL 1 DAY"))
        )
        df = df.withColumn("period", date_format(col("timestamp"), "yyyy-MM-dd HH:00:00"))

    elif trend_by == "day":
        if start_date:
            df = df.filter(col("timestamp") >= to_timestamp(lit(start_date), "yyyy-MM-dd"))
        if end_date:
            df = df.filter(col("timestamp") <= to_timestamp(lit(end_date), "yyyy-MM-dd"))
        df = df.withColumn("period", to_date(col("timestamp")))

    elif trend_by == "month":
        if not year:
            raise ValueError("Para 'month', year debe estar definido")
        df = df.filter(date_format(col("timestamp"), "yyyy") == year)
        df.show(5)
        df = df.withColumn("period", date_format(col("timestamp"), "yyyy-MM"))
    elif trend_by == "year":
        df = df.filter(
                (date_format(col("timestamp"), "yyyy") >= start_year) &
                (date_format(col("timestamp"), "yyyy") <= end_year)
            )

        df = df.withColumn("period", date_format(col("timestamp"), "yyyy"))
    
    # Agregación
    df_agg = df.groupBy("period").agg(
        avg("value").alias("avg_value"),
        min("value").alias("min_value"),
        max("value").alias("max_value")
    ).orderBy("period")
    
    return df_agg