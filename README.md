# Demostración de Observabilidad con OpenTelemetry, Kafka y Jaeger

Este proyecto es una demostración práctica de trazabilidad distribuida (observabilidad) en una arquitectura basada en eventos utilizando Python, Apache Kafka y Jaeger como colector y visualizador de trazas.

## Descripción del Proyecto

El sistema simula un flujo de agendamiento de citas médicas y envío de notificaciones compuesto por los siguientes módulos:

1. **Productor (app/producer.py)**: Simula el proceso de creación de una cita médica (validación de disponibilidad y almacenamiento). Posteriormente, publica un evento en el tópico de Kafka llamado `cita-confirmada`.
2. **Consumidor (app/consumer.py)**: Escucha el tópico `cita-confirmada` de Kafka. Al recibir un evento, inicia el procesamiento de la notificación simulando la preparación y el envío de un correo electrónico (incluyendo la simulación de errores de red en el servidor SMTP) y registra el resultado.
3. **Infraestructura (app/docker-compose.yml)**: Provee los servicios necesarios para el entorno:
   - **Apache Kafka** (en modo KRaft, sin ZooKeeper) como intermediario de mensajería.
   - **Jaeger** como sistema de backend para recopilar, almacenar y visualizar las trazas distribuidas generadas por OpenTelemetry.

Tanto el productor como el consumidor utilizan la auto-instrumentación y la API de OpenTelemetry para generar tramos (spans) que permiten seguir el ciclo de vida completo de una solicitud a través de la red y los procesos.

## Requisitos Previos

- Python 3.8 o superior instalado en el sistema.
- Docker y Docker Compose instalados y en ejecución.

## Instalación y Configuración del Entorno de Python

1. Posiciónese en la raíz del proyecto.
2. Cree un entorno virtual de Python:
   ```bash
   python3 -m venv .venv
   ```
3. Active el entorno virtual:
   - En macOS / Linux:
     ```bash
     source .venv/bin/activate
     ```
   - En Windows (PowerShell):
     ```bash
     .venv\Scripts\Activate.ps1
     ```
4. Instale las dependencias especificadas en el archivo `requirements.txt`:
   ```bash
   pip install --upgrade pip
   pip install -r app/requirements.txt
   ```

## Ejecución de la Infraestructura

Para levantar los servicios de Kafka y Jaeger, ejecute el siguiente comando desde el directorio `app`:

```bash
cd app
docker compose up -d
```

Puede verificar que los contenedores están activos ejecutando:

```bash
docker compose ps
```

## Ejecución de la Aplicación

Para que las trazas sean capturadas y exportadas de forma automática a Jaeger, ambos scripts de Python deben ejecutarse utilizando la herramienta de auto-instrumentación de OpenTelemetry.

Asegúrese de tener el entorno virtual activo antes de ejecutar los comandos.

### 1. Iniciar el Consumidor de Notificaciones

Ejecute el consumidor para que quede en espera de nuevos eventos enviados al tópico `cita-confirmada`:

```bash
export OTEL_SERVICE_NAME="notificaciones"
export OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:4318"
opentelemetry-instrument python app/consumer.py
```

*(Nota para Windows: Utilice `set OTEL_SERVICE_NAME="notificaciones"` y `set OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:4318"` antes de ejecutar `opentelemetry-instrument python app/consumer.py`)*

### 2. Iniciar el Productor de Agendamientos

Ejecute el productor para generar una nueva cita y enviar el mensaje correspondiente a Kafka:

```bash
export OTEL_SERVICE_NAME="agendamiento"
export OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:4318"
opentelemetry-instrument python app/producer.py
```

*(Nota para Windows: Utilice `set OTEL_SERVICE_NAME="agendamiento"` y `set OTEL_EXPORTER_OTLP_ENDPOINT="http://localhost:4318"` antes de ejecutar `opentelemetry-instrument python app/producer.py`)*

Cada ejecución del productor generará un identificador de cita único (`cita_id`) y enviará la traza asociada.

## Visualización de Trazas en Jaeger

Una vez ejecutados el productor y el consumidor:

1. Abra su navegador web e ingrese a la interfaz de usuario de Jaeger: http://localhost:16686
2. En el menú lateral izquierdo, seleccione el servicio deseado (por ejemplo, `agendamiento` o `notificaciones`) dentro del panel de **Service**.
3. Haga clic en **Find Traces** para visualizar los ciclos de vida de las operaciones y analizar la jerarquía de los spans, los tiempos de respuesta y los errores registrados.
