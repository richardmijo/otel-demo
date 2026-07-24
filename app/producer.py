import json
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from opentelemetry import trace


KAFKA_SERVER = "localhost:29092"
TOPIC = "cita-confirmada"

tracer = trace.get_tracer("agendamiento")


def entrega_confirmada(error, mensaje):
    """Callback ejecutado cuando Kafka confirma o rechaza el mensaje."""

    if error is not None:
        print(f"Error al entregar el mensaje: {error}")
        return

    print(
        "Mensaje entregado correctamente: "
        f"tópico={mensaje.topic()}, "
        f"partición={mensaje.partition()}, "
        f"offset={mensaje.offset()}"
    )


def main():
    producer = Producer(
        {
            "bootstrap.servers": KAFKA_SERVER,
            "client.id": "productor-agendamiento",
        }
    )

    cita_id = str(uuid.uuid4())

    evento = {
        "evento": "CITA_CONFIRMADA",
        "cita_id": cita_id,
        "paciente_id": "PAC-001",
        "hora": "10:00",
        "fecha_generacion": datetime.now(timezone.utc).isoformat(),
    }

    with tracer.start_as_current_span("procesar-cita") as span_principal:
        span_principal.set_attribute("cita.id", cita_id)
        span_principal.set_attribute("paciente.id", "PAC-001")
        span_principal.set_attribute("modulo", "agendamiento")

        with tracer.start_as_current_span("validar-disponibilidad"):
            print("Validando disponibilidad...")
            time.sleep(0.08)

        with tracer.start_as_current_span("guardar-cita-en-bd"):
            print("Guardando cita...")
            time.sleep(0.12)

        with tracer.start_as_current_span("publicar-evento-kafka") as span_kafka:
            span_kafka.set_attribute("messaging.destination.name", TOPIC)

            producer.produce(
                topic=TOPIC,
                key="PAC-001",
                value=json.dumps(evento).encode("utf-8"),
                callback=entrega_confirmada,
            )

            producer.flush(10)

    print(f"Evento publicado. cita_id={cita_id}")


if __name__ == "__main__":
    main()