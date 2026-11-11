# ADR-004 - Capa Raw en el Data Lake y Checkpoints en Spark Streaming

- **Estado:** Aprobado  
- **Fecha:** 10 de noviembre de 2026  
- **Equipo 5 (Variante 2: Seguridad del Repartidor)**

## Contexto / problema
Antes, el pipeline procesaba y filtraba los datos directo a la capa final, pero nos faltaba guardar los eventos originales tal cual venían de Kafka en una capa *Raw*. Además, si el consumidor de Spark se reiniciaba, corríamos el riesgo de perder el rastro de los mensajes (*offsets*) o tropezarnos con errores de particionamiento antes de llegar a las capas *Processed* y *Curated*.

## Opciones consideradas
- **A. Guardar solo los datos transformados:** Descartar los eventos originales. Lo descartamos porque perdemos la trazabilidad y la opción de auditar los datos crudos de Kafka.
- **B. Dejar los checkpoints en la memoria volátil del contenedor:** Los metadatos de Spark se guardaban por dentro y se borraban cada vez que reiniciábamos los servicios con Docker.
- **C. Crear una arquitectura de almacenamiento en capas (Raw y Processed) con particionamiento temporal y checkpoints persistidos en un volumen local del Data Lake.**

## Decisión tomada
Elegimos la **Opción C**:
- Armamos un flujo exclusivo (`raw_q`) para respaldar los eventos de telemetría de manera cruda, directo desde Kafka y en formato Parquet, dentro de `datalake/raw/security_telemetry`.
- Pasamos la creación de la columna de particionamiento `event_date` al DataFrame principal (`parsed_df`) justo antes de escribir, evitando el error de `AnalysisException`.
- Configuramos el parámetro `.option("checkpointLocation", "/opt/datalake/checkpoint/...")` conectado directo a la carpeta compartida en nuestra PC (`./datalake/checkpoint`), asegurando que Spark conserve el estado y los offsets si se apaga el contenedor.
- **Integración por Pull Request a `main`:** Se unificaron todos los cambios mediante un PR hacia la rama principal para consolidar la arquitectura de la versión `v1.0.0`. Esto permitió además habilitar de forma oficial el disparador manual de GitHub Actions (`workflow_dispatch`), ya que la interfaz de GitHub requiere que el archivo de workflow se encuentre en `main` para mostrar el botón de ejecución interactiva.

## Evidencia utilizada
- El contenedor `rg-spark-consumer` corre de forma estable y procesa los eventos en tiempo real emitidos por el producer.
- La estructuración y el particionamiento por fecha (`event_date=2026-...`) se ejecutan de forma automatizada mediante la escritura particionada de Spark, organizando tanto la zona *Raw* como la zona *Processed* (previa a la capa *Curated*).
- La consola valida la correcta persistencia de metadatos sin interrupciones por incompatibilidad de checkpoints.
- Todo esto ya quedó verificado y funcionando en el cierre del tag `v1.0.0`.

## Consecuencias / trade-offs
- **A favor:** Se asegura la trazabilidad y persistencia de los eventos desde el origen en Kafka hasta su transformación, garantizando un pipeline completo, funcional y tolerante a fallos gracias al manejo de checkpoints.
- **En contra:** Al mantener el almacenamiento dual de los registros (en formato crudo en *Raw* y estructurado en *Processed/Curated*), se incrementa el consumo de almacenamiento local, el cual se gestiona correctamente excluyendo las rutas de checkpoints en el `.gitignore`.