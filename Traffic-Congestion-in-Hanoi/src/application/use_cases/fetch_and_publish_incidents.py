import logging
import time
from collections.abc import Callable

from domain.services.traffic_rules import is_reportable_congestion, summarize_types

LOGGER = logging.getLogger(__name__)


class FetchAndPublishIncidents:
    def __init__(
        self,
        incident_client,
        event_producer,
        data_lake_writer,
        poll_seconds: int,
        clock: Callable[[], str],
    ) -> None:
        self.incident_client = incident_client
        self.event_producer = event_producer
        self.data_lake_writer = data_lake_writer
        self.poll_seconds = poll_seconds
        self.clock = clock

    def run_forever(self) -> None:
        LOGGER.info("Starting Hanoi traffic scan loop")
        while True:
            LOGGER.info("Scanning Hanoi area at %s", self.clock())
            incidents = self.incident_client.fetch_incidents()
            try:
                self.data_lake_writer.write_incident_batch(incidents)
            except Exception as exc:
                LOGGER.warning("Skipping MinIO write for this batch: %s", exc)

            LOGGER.info("Fetched %s incidents from TomTom", len(incidents))
            LOGGER.info("Incident type breakdown: %s", summarize_types(incidents))

            jam_count = 0
            for incident in incidents:
                self.event_producer.send(incident)
                if is_reportable_congestion(incident):
                    jam_count += 1
                    LOGGER.info(
                        "Jam detected | id=%s | type=%s | magnitude=%s | from=%s | to=%s | reason=%s | length_km=%.2f | delay_seconds=%s",
                        incident.incident_id,
                        incident.incident_type,
                        incident.magnitude,
                        incident.road_from,
                        incident.road_to,
                        incident.description,
                        incident.length_meters / 1000.0,
                        incident.delay_seconds,
                    )

            self.event_producer.flush()
            LOGGER.info("Scan completed with %s reportable congestion events", jam_count)
            LOGGER.info("Sleeping %s seconds before next scan", self.poll_seconds)
            time.sleep(self.poll_seconds)
