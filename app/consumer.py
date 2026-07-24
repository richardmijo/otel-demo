import json
import signal
import time

from confluent_kafka import Consumer, KafkaError
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

KAFKA_SERVER = "localhost:29092"
TOPIC = "cita-confirmada"

tracer = trace.get_tracer("notificaciones")

ejecutando = True


def detener_consumidor(signal_number, frame):
    global ejecutando
    print("\nSe recibió una señal de cierre.")
    ejecutando = False


def main():
    signal.signal(signal.SIGINT, detener_consumidor)
    signal.signal(signal.SIGTERM, detener_consumidor)

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_SERVER,
            "group.id": "notificaciones-grupo",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )

    consumer.subscribe([TOPIC])

    print(f"Esperando eventos en el tópico '{TOPIC}'...")

    try:
        while ejecutando:
            mensaje = consumer.poll(1.0)

            if mensaje is None:
                continue

            if mensaje.error():
                if mensaje.error().code() == KafkaError._PARTITION_EOF:
                    continue

                print(f"Error de Kafka: {mensaje.error()}")
                continue

            try:
                evento = json.loads(mensaje.value().decode("utf-8"))

                with tracer.start_as_current_span(
                    "procesar-notificacion"
                ) as span_principal:
                    span_principal.set_attribute(
                        "cita.id",
                        evento.get("cita_id", "desconocido"),
                    )
                    span_principal.set_attribute(
                        "messaging.destination.name",
                        mensaje.topic(),
                    )
                    span_principal.set_attribute(
                        "messaging.kafka.partition",
                        mensaje.partition(),
                    )
                    span_principal.set_attribute(
                        "messaging.kafka.message.offset",
                        mensaje.offset(),
                    )

                    #with tracer.start_as_current_span("preparar-email"):
                    #    print("Preparando correo...")
                    #    time.sleep(0.10)

                    with tracer.start_as_current_span("enviar-email"):
                        print("Enviando correo...")
                        time.sleep(2)
                    with tracer.start_as_current_span("enviar-email") as span_email:
                        try:
                            time.sleep(0.20)
                            raise RuntimeError("Servidor SMTP no disponible")

                        except Exception as error:
                            span_email.record_exception(error)
                            span_email.set_status(
                                Status(
                                    StatusCode.ERROR,
                                    str(error),
                                )
                            )

                            print(f"Error enviando correo: {error}")

                    with tracer.start_as_current_span("registrar-notificacion"):
                        print("Registrando notificación...")
                        time.sleep(0.06)

                print(
                    "Evento procesado: "
                    f"cita_id={evento.get('cita_id')}, "
                    f"paciente={evento.get('paciente_id')}"
                )

            except json.JSONDecodeError:
                print("El mensaje recibido no contiene un JSON válido.")

            except Exception as error:
                print(f"Error procesando el mensaje: {error}")

    finally:
        consumer.close()
        print("Consumidor cerrado correctamente.")


if __name__ == "__main__":
    main()