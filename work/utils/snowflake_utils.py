import os
from pyspark.sql import SparkSession


def get_snowflake_options(schema: str = None):
    """
    Opciones necesarias para conectar con Snowflake via Spark Connector.
    """
    db = os.environ.get("SNOWFLAKE_DATABASE")
    schema_default = os.environ.get("SNOWFLAKE_SCHEMA_RAW", "RAW")
    opts = {
        "sfURL": f"{os.environ['SNOWFLAKE_ACCOUNT']}.snowflakecomputing.com",
        "sfUser": os.environ["SNOWFLAKE_USER"],
        "sfPassword": os.environ["SNOWFLAKE_PASSWORD"],
        "sfRole": os.environ.get("SNOWFLAKE_ROLE"),
        "sfWarehouse": os.environ.get("SNOWFLAKE_WAREHOUSE"),
        "sfDatabase": db,
        "sfSchema": schema or schema_default,
    }
    # sanity check
    if not db or not opts["sfSchema"]:
        raise ValueError("Faltan variables SNOWFLAKE_DATABASE o SNOWFLAKE_SCHEMA_RAW en .env")
    return opts


def get_spark_session(app_name="spark-snowflake-app"):
    """
    Crea o retorna una SparkSession configurada con el conector Snowflake.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        # Ajustes de tiempo
        .config("spark.sql.timestampType", "TIMESTAMP_LTZ")
        .config("spark.sql.session.timeZone", "UTC")
        # Paquetes del conector (usa la versión compatible con tu Spark)
        .config(
            "spark.jars.packages",
            "net.snowflake:snowflake-jdbc:3.13.33,"
            "net.snowflake:spark-snowflake_2.12:2.9.3-spark_3.1"
        )
        .getOrCreate()
    )

    # === Configurar automáticamente contexto Snowflake actual ===
    try:
        from pyspark import SparkFiles

        # Leer valores del entorno
        sf_opts = get_snowflake_options()
        jvm = spark._jvm
        jmap = jvm.java.util.HashMap()
        for k, v in sf_opts.items():
            if v:
                jmap.put(k, v)

        # Ejecutar USE DATABASE / SCHEMA / ROLE / WAREHOUSE
        db = sf_opts.get("sfDatabase")
        sc = sf_opts.get("sfSchema")
        wh = sf_opts.get("sfWarehouse")
        role = sf_opts.get("sfRole")

        if role:
            jvm.net.snowflake.spark.snowflake.Utils.runQuery(jmap, f"USE ROLE {role}")
        if wh:
            jvm.net.snowflake.spark.snowflake.Utils.runQuery(jmap, f"USE WAREHOUSE {wh}")
        if db:
            jvm.net.snowflake.spark.snowflake.Utils.runQuery(jmap, f"USE DATABASE {db}")
        if sc:
            jvm.net.snowflake.spark.snowflake.Utils.runQuery(jmap, f"USE SCHEMA {sc}")

        print(f"✓ Snowflake context activo: DB={db}, SCHEMA={sc}, WH={wh}, ROLE={role}")
    except Exception as e:
        print("[WARN] No se pudo establecer contexto Snowflake automático:", e)

    return spark
