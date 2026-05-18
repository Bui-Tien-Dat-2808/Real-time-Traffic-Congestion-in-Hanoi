from domain.entities.traffic_incident import TrafficIncident


def is_reportable_congestion(incident: TrafficIncident) -> bool:
    return incident.incident_type in {1, 6} and (
        incident.delay_seconds > 15 or incident.magnitude >= 2
    )


def summarize_types(incidents: list[TrafficIncident]) -> dict[int, int]:
    summary: dict[int, int] = {}
    for incident in incidents:
        summary[incident.incident_type] = summary.get(incident.incident_type, 0) + 1
    return summary
