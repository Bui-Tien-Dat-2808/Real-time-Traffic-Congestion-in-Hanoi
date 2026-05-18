from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class TrafficIncident:
    incident_id: str
    incident_type: int
    magnitude: int
    delay_seconds: int
    length_meters: float
    description: str
    road_from: str
    road_to: str
    coordinates: list[float]
    detected_at: datetime

    def to_message(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["id"] = payload.pop("incident_id")
        payload["type"] = payload.pop("incident_type")
        payload["timestamp"] = payload.pop("detected_at").astimezone(timezone.utc).isoformat()
        return payload
