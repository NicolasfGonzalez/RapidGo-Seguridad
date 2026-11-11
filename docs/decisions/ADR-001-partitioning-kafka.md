# ADR-001: Decisión sobre Key/Particiones en Kafka y Arquitectura del Pipeline (Variante 2 - Seguridad del Repartidor)

- **Estado:** Aprobado
- **Fecha:** 21 de octubre de 2026
- **Autores:** Equipo 5 (Vivian Karith, Nicolas Gonzalez, Laura Ortiz)
- **Componente:** Arquitectura de Datos / Event Streaming y Procesamiento



## 1. Decisión Arquitectónica Tomada
Se define el uso del campo **`driver_id` como clave de particionamiento (*Partition Key*) obligatoria**, y se aprovisiona el topic `driver_security_events` con una configuración de **3 particiones** como núcleo de la capa de *Event Streaming*.


## 2. Justificación Técnica en la Arquitectura del Pipeline

1. **Integración con la Capa de Procesamiento (Preservación del Orden):**
   Apache Kafka garantiza el orden cronológico de los mensajes únicamente a nivel de partición. Al utilizar `driver_id` como *Key*, el algoritmo de hashing redirige de forma determinista el 100% de la telemetría de un conductor a la misma partición. Esto entrega un flujo de datos perfectamente ordenado a Spark.

2. **Eficiencia en el Motor de Streaming (Eliminación de *Shuffling*):**
   Dentro del pipeline, la fase de *Real-Time Processing* aplica agrupaciones por ventana deslizante de 5 minutos sobre el `driver_id`. Dado que los datos ya vienen particionados por esta clave desde Kafka, Spark ejecuta los cálculos (velocidad promedio, desviación de ruta, inactividad) directamente en la memoria local de cada partición, eliminando la necesidad de intercambiar datos por red entre los nodos (*shuffling*).

3. **Evaluación Determinista de Reglas hacia el Data Lake:**
   Al garantizar el orden lógico de los eventos, el motor de reglas de Spark puede evaluar de manera determinista las siguientes alertas, derivando los registros hacia la capa *CURATED* del Data Lake (Alertas e historial de violaciones):
   * **Exceso de Velocidad:** `speed_kmh > 45`.
   * **Batería Crítica / Pérdida de Señal:** `battery_percent < 15%` **O** `gps_signal_active == false`.
   * **Repartidor Desviado:** `route_deviation_m > 500`. Este umbral permite un rango de flexibilidad operativa para que el repartidor tome atajos viales sin detonar falsos positivos.

4. **Balanceo de Carga y Paralelismo (3 Particiones):**
   La elección de 3 particiones asegura un paralelismo óptimo entre la capa de ingestión y el consumidor (Spark), distribuyendo la carga de los diferentes `driver_id` equitativamente sin saturar la memoria RAM en la infraestructura local orquestada por Docker Compose.
