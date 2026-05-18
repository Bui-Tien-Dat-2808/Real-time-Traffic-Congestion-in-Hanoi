from datetime import datetime, timezone

import requests

from domain.entities.traffic_incident import TrafficIncident


class TomTomIncidentClient:
    def __init__(
        self,
        api_key: str,
        bbox: str,
        language: str = "en-GB",
        timeout_seconds: int = 30,
    ) -> None:
        if not api_key:
            raise ValueError("TOMTOM_API_KEY is required")
        self.api_key = api_key
        self.bbox = bbox
        self.language = language
        self.timeout_seconds = timeout_seconds

    def fetch_incidents(self) -> list[TrafficIncident]:
        fields = (
            "{incidents{properties{id,iconCategory,magnitudeOfDelay,events{description},"
            "from,to,length,delay},geometry{type,coordinates}}}"
        )
        url = (
            "https://api.tomtom.com/traffic/services/5/incidentDetails"
            f"?key={self.api_key}&bbox={self.bbox}&fields={fields}&language={self.language}"
        )
        response = requests.get(url, timeout=self.timeout_seconds)
        if response.status_code >= 400:
            raise requests.HTTPError(
                f"TomTom API request failed with status {response.status_code}: {response.text}",
                response=response,
            )
        data = response.json()
        return [self._map_incident(item) for item in data.get("incidents", [])]

    def _map_incident(self, item: dict) -> TrafficIncident:
        props = item.get("properties", {})
        geom = item.get("geometry", {})
        coords = geom.get("coordinates", [])
        first_coord = [0.0, 0.0]

        if geom.get("type") == "LineString" and coords:
            first_coord = coords[0]
        elif geom.get("type") == "Point" and len(coords) == 2:
            first_coord = coords

        events = props.get("events", [])
        description = events[0].get("description", "Unknown") if events else "Unknown"

        return TrafficIncident(
            incident_id=props.get("id", datetime.now(timezone.utc).isoformat()),
            incident_type=int(props.get("iconCategory", 0) or 0),
            magnitude=int(props.get("magnitudeOfDelay", 0) or 0),
            delay_seconds=int(props.get("delay", 0) or 0),
            length_meters=float(props.get("length", 0.0) or 0.0),
            description=description,
            road_from=props.get("from", "Unknown point"),
            road_to=props.get("to", "Unknown point"),
            coordinates=[float(value) for value in first_coord[:2]],
            detected_at=datetime.now(timezone.utc),
        )
