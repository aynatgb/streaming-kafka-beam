import json
import logging
from confluent_kafka import Consumer, Producer, KafkaError
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows


class KafkaReadDoFn(beam.DoFn):
    """Lector continuo de eventos desde Kafka."""
    def __init__(self, bootstrap_servers, topic, group_id):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.consumer = None

    def setup(self):
        conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest'
        }
        self.consumer = Consumer(conf)
        self.consumer.subscribe([self.topic])
        logging.info("Conectado a Kafka correctamente.")

    def process(self, element):
        while True:
            msg = self.consumer.poll(timeout=0.5)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logging.error(f"Error en Kafka: {msg.error()}")
                    break
            
            try:
                data = json.loads(msg.value().decode('utf-8'))
                event_id = data.get("event_id", "sin_id")
                yield (event_id, data)
            except Exception as e:
                logging.error(f"Error parseando JSON: {e}")

    def teardown(self):
        if self.consumer:
            self.consumer.close()


class DeduplicateDoFn(beam.DoFn):
    """Filtra eventos duplicados dentro de la misma ventana temporal."""
    def process(self, element):
        event_id, events = element
        first_event = list(events)[0]
        yield first_event


class KafkaWriteDoFn(beam.DoFn):
    """Escribe eventos procesados de forma limpia hacia el tópico de salida."""
    def __init__(self, bootstrap_servers, topic):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None

    def setup(self):
        self.producer = Producer({'bootstrap.servers': self.bootstrap_servers})

    def process(self, element):
        try:
            payload = json.dumps(element).encode('utf-8')
            # Garantiza idempotencia usando event_id como clave de Kafka
            key = element.get('event_id', '').encode('utf-8')
            self.producer.produce(self.topic, key=key, value=payload)
            self.producer.flush()
            logging.info(f"📤 Evento enviado a {self.topic}: {element}")
        except Exception as e:
            logging.error(f"Error escribiendo en Kafka: {e}")


def run():
    options = PipelineOptions(["--runner=DirectRunner"])

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Impulse" >> beam.Create([None])
            | "ReadFromKafka" >> beam.ParDo(KafkaReadDoFn(
                bootstrap_servers='localhost:9092',
                topic='eventos-entrada',
                group_id='beam-pipeline-v5'
            ))
            | "Windowing" >> beam.WindowInto(FixedWindows(10))
            | "GroupByID" >> beam.GroupByKey()
            | "Deduplicate" >> beam.ParDo(DeduplicateDoFn())
            | "LogEvents" >> beam.Map(lambda event: print(f"✨ Evento Procesado: {event}"))
            | "WriteToKafka" >> beam.ParDo(KafkaWriteDoFn(
                bootstrap_servers='localhost:9092',
                topic='eventos-salida'
            ))
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()