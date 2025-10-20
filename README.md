# Proyecto 03 — NYC TLC OBT (Spark + Snowflake)

## 1) Arquitectura
Parquet (2015–2025, Yellow/Green) → **Spark/Jupyter (Docker)** → Snowflake (`raw` → `analytics.obt_trips`).  
Variables de ambiente (.env) para credenciales y parámetros.  
*(Ver docker y .env.example)*

## 2) Matriz de Cobertura (2015–2025)
En la matriz se nidican que meses se descargaron por servicio, la tabla en la que fueron ingestados, el total de filas y el modo de idempotencia aplicado.
| # | `run_id`                             | `service_type` | `source_year` | `source_months` | `rows`    | `when_utc`           | `raw_table`         | `mode` |
| - | ------------------------------------ | -------------- | ------------- | --------------- | --------- | -------------------- | ------------------- | ------ |
| 1 | 93f4f3ec-9adf-45f2-9f0d-0e58319124e5 | yellow         | 2022          | 7,8             | 6,288,451 | 2025-10-12T22:53:20Z | RAW_TLC_TRIPS       | SKIP   |
| 2 | 0f8867f6-da0c-4d70-9484-f95230e8019f | green          | 2022          | 7,8             | 129,814   | 2025-10-12T22:54:38Z | RAW_TLC_TRIPS_green | SKIP   |

* El resto de la tabla se encuentra en audit_ingesta_raw.csv
## 3) Orden y Parámetros de Ejecución
1. **01_ingesta_parquet_raw**: lee Parquet por `year,month,service`, escribe en `raw`.  
   - Guarda conteos por lote en `raw.load_audit` (`run_id, service_type, year, month, rows_loaded, ingested_at_utc`).
2. **02_enriquecimiento_y_unificacion**: integra **Taxi Zones** y catálogos **payment/rate/vendor**; unifica Yellow/Green.
3. **03_construccion_obt**: construye `analytics.obt_trips` con derivadas y **lineage** (`run_id, source_service, source_year, source_month`).  
   - **Idempotencia**: reingesta de un mes no duplica (usa `MERGE`).
4. **04_validaciones_y_exploracion**: nulos, rangos, coherencia PU/DO, **conteos por mes/servicio**, reporte de calidad.
5. **05_analisis_20_preguntas**: responde preguntas **con Spark y snowflake** usando `analytics.obt_trips`.
En este caso, debido a problemas con el kernel de spark se uso sql para responder las preguntas; sin embargo, se añade un archivo desarrollado con spark para demostrar el desarrollo de las preguntas usando esta herramienta.
Variables clave (.env): `SNOWFLAKE_*`, `PARQUET_PATH_*`, `RUN_ID`, `YEARS`, `MONTHS`, `SERVICES`.

## 4) Diseño de Esquemas
**RAW**: tablas espejo por partición (año/mes/servicio), con metadatos de ingesta (`run_id, service_type, source_year, source_month, ingested_at_utc, source_path`), y **auditoría** por lote.  
**ANALYTICS.OBT_TRIPS** (grano: 1 fila = 1 viaje):  
- **Tiempo**: pickup/dropoff, `pickup_date/hour`, `day_of_week`, `month`, `year`.  
- **Ubicación**: `pu/do_location_id`, `pu/do_zone`, `pu/do_borough`.  
- **Servicio/Códigos**: `service_type`, `vendor_id/name`, `rate_code_id/desc`, `payment_type/desc`, `trip_type`.  
- **Viaje/Tarifas**: `passenger_count`, `trip_distance`, `store_and_fwd_flag`, `fare_amount`… `total_amount`.  
- **Derivadas**: `trip_duration_min`, `avg_speed_mph`, `tip_pct`.  
- **Lineage**: `run_id`, `source_service`, `source_year`, `source_month`.
## 5) Configuración con Variables de Entorno

Este proyecto utiliza variables de entorno para configurar accesos y parámetros dinámicos, evitando hardcodear datos sensibles o configuraciones específicas.

📄 .env — Archivo de configuración local

Puedes definir tus variables de entorno en un archivo .env ubicado en la raíz del proyecto. Este archivo no debe subirse a GitHub, ya que puede contener credenciales sensibles.

# Listado de variables y propósito
* SNOWFLAKE_ACCOUNT	Identificador único de tu cuenta de Snowflake. Ej: HGPAYPL-TF36096
* SNOWFLAKE_USER	Usuario con permisos para conectarse a Snowflake = apuco0404
* SNOWFLAKE_PASSWORD	Contraseña del usuario (no se debe compartir)
* SNOWFLAKE_ROLE	Rol a usar dentro de Snowflake (ej. ACCOUNTADMIN, SYSADMIN) 
* SNOWFLAKE_WAREHOUSE	Warehouse usado para ejecutar las consultas (ej. spark_wh) 
* SNOWFLAKE_DATABASE	Base de datos por defecto donde están los datos = SPARK_DATA
* SNOWFLAKE_SCHEMA_RAW	Esquema que contiene los datos sin procesar = SPARK_DATA.RAW
* SNOWFLAKE_SCHEMA_ANALYTICS	Esquema que contiene los datos transformados (OBT, métricas) = SPARK_DATA.analytics
* TLC_BASE_URL	URL base desde donde se descargan los archivos de taxi (.parquet) = https://d37ci6vzurychx.cloudfront.net/trip-data
* RAW_TABLE	Nombre de la tabla destino donde se cargan los datos crudos descargados = RAW_TLC_TRIPS_green
* DM_YEAR	Año de datos a procesar (por ejemplo, 2022)
* DM_SERVICE	Tipo de servicio (green o yellow)
* DM_MONTHS	Lista de meses a procesar (por ejemplo: 1,2,3)

## 6) Calidad y Auditoría
Realizamos las pruebas de calidad indicadas en el pdf:no nulos esenciales; distancias/duraciones ≥0; montos  coherentes.
Adicional a esto, definimos otras reglas para ver la calidad de los datos.
Estos son algunos de los resultados que obtuvimos:
* Nulos clave: (0, 0, 0, 0, 0)
* Rangos lógicos: (0, 0, 2018051, 167375, 122)
* Años fuera de ventana: 3450 | min_year: 2001 | max_year: 2098
* Fechas → min/max pickup: 2001-01-01 00:01:48 2098-09-11 02:23:31 | min/max dropoff: 2001-01-01 00:04:49 2253-08-23 07:56:38
* Duraciones negativas y excesivas; distancias negativas y excesivas; speeds y tip_pct outliers(rangos) (0, 1849, 0, 10392, 167375, 122)
* Pasajeros fuera de rango: (0, 9510)
* Zero-distance con cobro: 7177874

* Cabe recalcar que no se realizo filtro de los datos, trabajamos con los datos entregados por el dataset; por lo tanto, las respuestas de las preguntas van a presentar ciertas incosistencias.


## 7) Resultados (20 Preguntas)

Los siguientes hallazgos fueron obtenidos a partir de la tabla SPARK_DATA.ANALYTICS.OBT_TRIPS. Las consultas se ejecutaron en Snowflake  y los resultados se guardaron en /evidencias/*.csv.

🔹 a–b) Zonas con mayor volumen

Pickup y dropoff dominantes: Midtown, Upper East/West Side y aeropuertos (JFK, LaGuardia) concentran la mayoría de los viajes.
CSVS INVOLUCRADOS: a_top_pu_zones_monthly.csv,b_top_do_zones_monthly.csv

🔹 c–d) Tendencias temporales

El ticket promedio se mantiene entre 14–18 USD, con incrementos notables en Manhattan y en los meses de invierno.

Las propinas (tip_pct) son más altas en Manhattan (0.15 en promedio) y casi nulas en pagos en efectivo.

Desde 2015, los viajes Yellow superan ampliamente a Green, aunque los Green muestran mayor estabilidad de precios.
CSVS INVOLUCRADOS:c_monthly_evolution_by_borough.csv, d_avg_ticket_by_service_month.csv

🔹 e) Picos horarios

Las horas pico se concentran entre 17h00 y 20h00 de lunes a viernes, especialmente los jueves y viernes.

CSVS INVOLUCRADOS: e_trips_by_hour_dow.csv


🔹 f–g) Duración y velocidad

Mediana de duración (p50) de ~11 min en Manhattan y 25 min en Queens.

Las velocidades promedio varían entre 20 mph en Manhattan y 35 mph en boroughs periféricos, con picos de congestión evidentes en horas punta.

CSVS INVOLUCRADOS: f_duration_percentiles_by_borough.csv, g_avg_speed_by_timeslot_borough.csv

🔹 h–i) Tipos de pago y tarifas

67 % de los viajes se pagan con tarjeta, y son los únicos con propina significativa (≈ 15 %).

Los códigos de tarifa “Standard rate” y “JFK” explican el 90 % del volumen económico.

Se detectan registros “NULL” en rate_code_desc con valores atípicos en distancia (> 50 mi), posiblemente errores de captura.
CSVS INVOLUCRADOS:h_payment_type_participation.csv, i_rate_codes_distance_revenue.csv

🔹 j–k) Mix de servicio y flujos principales

El servicio Yellow domina Manhattan; Green predomina en Queens y Bronx.

Los flujos más comunes son intra-Manhattan: Upper East/West Side ↔ Midtown.

CSVS INVOLUCRADOS: j_service_mix_by_month_borough.csv, k_top_routes_volume.csv

🔹 l–m) Factores de monto total

Los viajes con 1–2 pasajeros son el 80 % del total.

Los peajes (tolls_amount) y recargos de congestión elevan el ticket en aeropuertos y zonas de Queens.

JFK y LaGuardia presentan los promedios más altos de recargo (> 1 USD por viaje).
CSVS INVOLUCRADOS: l_passenger_count_distribution.csv, m_surcharges_impact_by_zone.csv

🔹 n–o) Distancia y desempeño por proveedor

Los viajes “cortos (≤ 2 mi)” representan cerca del 60 % en Manhattan.

Creative Mobile Technologies muestra mayor velocidad media (≈ 43 mph), indicando posibles diferencias en cobertura o georreferenciación frente a VeriFone Inc.
CSVS INVOLUCRADOS: n_trip_length_distribution.csv, o_vendor_performance.csv

🔹 p–q) Propinas y congestión

Las propinas son mayores durante la noche (20–23 h) y con pago por tarjeta.

Varias zonas del Bronx y Staten Island exhiben percentiles 99 extremos (> 180 min o > 40 mi), reflejando congestión o trayectos fuera del área metropolitana.

CSV INVOLUCRADOS: p_payment_tip_by_hour.csv, q_zones_extreme_p99.csv
🔹 r–s–t) Rendimiento y tendencias anuales

El yield por milla más alto ocurre en EWR (Newark), por tarifas fijas elevadas.

Se evidencia un colapso en 2020–2021 , con caída de viajes de ~70 % y leve recuperación en 2022–2023.

Los días con alta congestión (recargos > 2 USD) muestran viajes menos numerosos pero de mayor valor promedio (22.9 USD vs 16.3 USD).
CSVS INVOLUCRADOS:r_yield_per_mile.csv, s_yoy_changes.csv, t_congestion_effect.csv
 

Ruta de evidencias: ./evidencias/<csv>

# Puntos a considerar:
* Todas las consultas se realizaron sobre los datos sin filtrar, por lo cual, los resultados pueden presentar ciertas inconsistencias. Para una futura ocasión, se debería filtrar 
y limpiar los datos que presenten anomalías.
* Todas las consultas se realizaron usando el conector de snowflake con sql. No se utilizo SPARK debido a problemas con el kernel; no obstante, se sube un archivo desarrollado en SPARK como evidencia
de que se intento utilizar la herramienta para responder las preguntas; pero debido a tiempos de ejecución, no fue posible.

## 7)Pasos para Docker Compose y ejecución de notebooks (orden y parámetros). 
* Clonar el repositorio y definir el archivo .env, usando el .env_example como modelo
* Levantar el entorno con Docker Compose:  
   ```bash
   docker compose up -d
   ```
* Acceder a Jupyter: http://localhost:8888
* Ejecutar los notebooks en orden.
* Revisar los outputs en las carpetas: evidencias/

  
## 8) Evidencias

## Carpeta de evidencias (`/evidencias`)

| Evidencia               | Descripción esperada                                               |
|-------------------------|-------------------------------------------------------------------|
| `docker_ejecucion.png`     | `docker ps` mostrando contenedor `spark-notebook` activo.        |
| `ambiente_jupiter.png`       | Vista de JupyterLab con notebooks visibles.                      |
| `sparkui.png`           | Spark UI (puerto 4040) ejecutando tareas.                        |
| `obt_trips.png`        | Conteo total en `analytics.obt_trips` (12.7M yellow, 1.5M green). |
| `consola_snowflake.png`  | Vista de las tablas RAW y OBT en Snowflake.                      |
| `OBT_TRIPS STRUCTURE.png`  | Vista de la definición de la tabla OBT_TRIPS                      |

## 8) Troubleshooting
- `Py4JNetworkError`: reiniciar kernel/cluster; subir memoria; leer por año; verificar versión `spark-snowflake/jdbc`.  
- Warehouse suspendido: activar/ajustar tamaño.  

##  Checklist de aceptación

- Docker Compose levanta Spark + Jupyter
- Variables desde `.env`
- Carga completa 2015–2025 (al menos validado 2015–01)
- `analytics.obt_trips` creada con derivadas y metadatos
- Idempotencia verificada (reingesta 2015–01)
- Validaciones completas (rangos, nulos, coherencia)
- 20 preguntas analizadas
- README con pasos y evidencias

---

##  Conclusiones

- Se logró desarrollar una OBT robusta, idempotente y sin duplicados.
- Los resultados analíticos confirman patrones históricos de tráfico en NYC.
- Infraestructura reproducible vía Docker y variables de entorno.
- Spark presento ciertas limitaciones respecto a la lectura de grandes tablas. Esto pudo deberse a la poca experiencia con la herramienta y a limitaciones del contenedor.
