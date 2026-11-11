# ADR-003 - Orquestación y recuperación ante fallos en Airflow

- **Estado:** Aprobado ·
- **Fecha:** 5 de noviembre de 2026 
- **Equipo 5 (Variante 2: Seguridad del Repartidor)**

## Contexto / problema
Spark escribe Parquet de forma continua, pero nada comprobaba que el flujo siguiera vivo antes de calcular los KPIs. La primera versión del DAG (PR #35) usaba KPIs fijos y un fallo simulado por variable de entorno, lo que no demostraba resiliencia real.

## Opciones consideradas
- **A. Fallo simulado con bandera de entorno:** simple, pero no lee datos reales.
- **B. Mismos reintentos para todas las tareas:** reintentar un esquema roto o un dato inválido no lo arregla.
- **C. Clasificar fallos (transitorios con reintento, lógicos sin reintento), con chequeo de frescura y registro de incidentes.**

## Decisión tomada
Opción **C**:
- `verificar_frescura` lanza `AirflowException` si el último Parquet supera 3 min y se reintenta 3 veces cada 2 min.
- `validar_esquema` y `verificar_calidad_datos` lanzan `AirflowFailException` y no se reintentan.
- `verificar_frescura` devuelve `corte_mtime` por XCom para que los 3 KPIs lean los mismos datos.
- Los callbacks de fallo y reintento escriben en `incidentes_<fecha>.jsonl`.
- El CI importa el DAG con `DagBag` y usa Python 3.11 (Airflow 2.8.1 no es compatible con 3.13).

## Evidencia utilizada
- Log: *"Ultimo Parquet escrito hace 23.1 min (maximo 3.0 min)"* y `ERROR - Task failed with exception`.
- 7 incidentes en `incidentes_2026-11-05.jsonl`, con antigüedades entre 6.0 y 23.1 min.
- Tras reiniciar `spark-consumer`, el intento 5 marcó *"hace 0.2 min"* y terminó en SUCCESS.
- Cambios en el [PR #36](https://github.com/NicolasfGonzalez/RapidGo-Seguridad/pull/36). Detalle en el [checkpoint de la Semana 3](../checkpoints/Semana3.md).


## Consecuencias / trade-offs
- **A favor:** el DAG detecta un pipeline detenido con un mensaje accionable, los KPIs usan datos consistentes y no se reintenta lo que no se arregla solo.
- **En contra:** los reintentos cubren solo ~6 min y la caída duró ~23 min, por lo que se necesitó *Clear task* manual.
- **En contra:** 3 min de umbral puede dar falsos positivos si el producer se detiene a propósito (`FRESHNESS_MAX_MIN` es configurable).
- **En contra:** `datalake/` está en `.gitignore`, así que la evidencia de los incidentes depende de capturas y logs.
