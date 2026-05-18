import json

from kafka import KafkaProducer

from domain.entities.traffic_incident import TrafficIncident


class KafkaIncidentProducer:
    def __init__(self, broker: str, topic: str) -> None:
        self.topic = topic
        self._producer = KafkaProducer(
            bootstrap_servers=[broker],
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            key_serializer=lambda key: key.encode("utf-8"),
        )

    def send(self, incident: TrafficIncident) -> None:
        self._producer.send(
            self.topic,
            key=str(incident.incident_id),
            value=incident.to_message(),
        )

    def flush(self) -> None:
        self._producer.flush()
