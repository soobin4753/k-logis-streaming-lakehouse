import random
import uuid
from datetime import datetime, timedelta

EVENT_SEQUENCE = ["CREATED", "ASSIGNED", "PICKUP", "IN_TRANSIT", "ARRIVED_HUB", "OUT_FOR_DELIVERY", "DELIVERED"]

CENTRAL_HUBS = {
    "REG_01": "HUB_001",
    "REG_02": "HUB_003",
    "REG_03": "HUB_005",
    "REG_04": "HUB_006",
    "REG_05": "HUB_007",
    "REG_06": "HUB_008",
    "REG_07": "HUB_009",
    "REG_08": "HUB_010",
    "REG_09": "HUB_011",
    "REG_10": "HUB_012",
}


def build_route(shipment_row):
    route = [shipment_row["origin_hub_id"]]

    if shipment_row["origin_region_id"] != shipment_row["destination_region_id"]:
        origin_center = CENTRAL_HUBS.get(shipment_row["origin_region_id"])
        destination_center = CENTRAL_HUBS.get(shipment_row["destination_region_id"])

        if origin_center and origin_center not in route:
            route.append(origin_center)

        if destination_center and destination_center not in route:
            route.append(destination_center)

    if shipment_row["destination_hub_id"] not in route:
        route.append(shipment_row["destination_hub_id"])

    return route


def risk_from_delay(delay_probability):
    if delay_probability >= 0.7:
        return "HIGH"
    if delay_probability >= 0.4:
        return "MEDIUM"
    return "LOW"


def clamp(value, min_value, max_value):
    return max(min_value, min(value, max_value))


def interpolate_position(start_lat, start_lon, end_lat, end_lon, progress):
    lat = start_lat + (end_lat - start_lat) * progress
    lon = start_lon + (end_lon - start_lon) * progress

    lat += random.uniform(-0.004, 0.004)
    lon += random.uniform(-0.004, 0.004)

    return round(lat, 6), round(lon, 6)


def get_exception(status, base_delay_probability):
    if status in ["CREATED", "ASSIGNED"]:
        return None

    if status == "DELIVERED":
        return random.choices(
            [None, "CUSTOMER_ABSENT", "ADDRESS_ISSUE"],
            weights=[0.95, 0.03, 0.02],
            k=1,
        )[0]

    base_weights = [0.86, 0.04, 0.04, 0.04, 0.02]

    if base_delay_probability >= 0.7:
        base_weights = [0.72, 0.08, 0.08, 0.08, 0.04]
    elif base_delay_probability >= 0.4:
        base_weights = [0.80, 0.06, 0.06, 0.06, 0.02]

    return random.choices(
        [None, "TRAFFIC_ACCIDENT", "BAD_WEATHER", "HUB_CONGESTION", "VEHICLE_ISSUE"],
        weights=base_weights,
        k=1,
    )[0]


def build_event_schedule(created_at, lead_time_days, delay_probability):
    planned_total_minutes = max(180, int(lead_time_days * 24 * 60))

    delay_minutes = 0
    if delay_probability >= 0.7:
        delay_minutes = random.randint(120, 480)
    elif delay_probability >= 0.4:
        delay_minutes = random.randint(30, 180)
    else:
        delay_minutes = random.randint(-60, 60)

    total_minutes = max(180, planned_total_minutes + delay_minutes)

    return {
        "CREATED": created_at,
        "ASSIGNED": created_at + timedelta(minutes=random.randint(5, 30)),
        "PICKUP": created_at + timedelta(minutes=random.randint(40, 180)),
        "IN_TRANSIT": created_at + timedelta(minutes=int(total_minutes * random.uniform(0.25, 0.55))),
        "ARRIVED_HUB": created_at + timedelta(minutes=int(total_minutes * random.uniform(0.55, 0.75))),
        "OUT_FOR_DELIVERY": created_at + timedelta(minutes=int(total_minutes * random.uniform(0.75, 0.90))),
        "DELIVERED": created_at + timedelta(minutes=total_minutes),
    }


def status_progress(status):
    return {
        "CREATED": 0.0,
        "ASSIGNED": 0.03,
        "PICKUP": 0.10,
        "IN_TRANSIT": random.uniform(0.30, 0.60),
        "ARRIVED_HUB": random.uniform(0.65, 0.80),
        "OUT_FOR_DELIVERY": random.uniform(0.80, 0.95),
        "DELIVERED": 1.0,
    }[status]


def weighted_signal(dataset_row, status):
    traffic = float(dataset_row["traffic_congestion_level"])
    weather = float(dataset_row["weather_severity"])
    hub_congestion = float(dataset_row["hub_congestion_level"])
    eta_variation = float(dataset_row["eta_variation_minutes"])
    delay_probability = float(dataset_row["delay_probability"])

    if status == "CREATED":
        traffic *= 0.2
        weather *= 0.3
        hub_congestion *= 0.5
        eta_variation *= 0.1
        delay_probability *= 0.3

    elif status == "ASSIGNED":
        traffic *= 0.4
        weather *= 0.4
        hub_congestion *= 0.6
        eta_variation *= 0.2
        delay_probability *= 0.4

    elif status == "PICKUP":
        traffic *= 0.7
        weather *= 0.6
        hub_congestion *= 0.7
        eta_variation *= 0.5
        delay_probability *= 0.6

    elif status == "IN_TRANSIT":
        traffic *= 1.2
        weather *= 1.0
        hub_congestion *= 0.8
        eta_variation *= 1.0
        delay_probability *= 1.0

    elif status == "ARRIVED_HUB":
        traffic *= 0.8
        weather *= 0.7
        hub_congestion *= 1.3
        eta_variation *= 0.8
        delay_probability *= 0.9

    elif status == "OUT_FOR_DELIVERY":
        traffic *= 1.1
        weather *= 0.9
        hub_congestion *= 0.7
        eta_variation *= 0.7
        delay_probability *= 0.8

    elif status == "DELIVERED":
        traffic *= 0.6
        weather *= 0.5
        hub_congestion *= 0.5
        eta_variation *= 0.4
        delay_probability *= 0.6

    exception_type = get_exception(status, delay_probability)

    if exception_type == "TRAFFIC_ACCIDENT":
        traffic += random.uniform(2.0, 3.5)
        eta_variation += random.randint(40, 120)
        delay_probability += 0.20

    elif exception_type == "BAD_WEATHER":
        weather += random.uniform(2.0, 4.0)
        eta_variation += random.randint(30, 100)
        delay_probability += 0.15

    elif exception_type == "HUB_CONGESTION":
        hub_congestion += random.uniform(2.0, 4.0)
        eta_variation += random.randint(30, 90)
        delay_probability += 0.15

    elif exception_type == "VEHICLE_ISSUE":
        eta_variation += random.randint(60, 180)
        delay_probability += 0.25

    elif exception_type in ["CUSTOMER_ABSENT", "ADDRESS_ISSUE"]:
        eta_variation += random.randint(20, 80)
        delay_probability += 0.10

    traffic = round(clamp(traffic, 0, 10), 2)
    weather = round(clamp(weather, 0, 10), 2)
    hub_congestion = round(clamp(hub_congestion, 0, 10), 2)
    eta_variation = round(max(0, eta_variation), 2)
    delay_probability = round(clamp(delay_probability, 0, 1), 3)

    return {
        "traffic_congestion_level": traffic,
        "weather_severity": weather,
        "hub_congestion_level": hub_congestion,
        "eta_variation_minutes": eta_variation,
        "delay_probability": delay_probability,
        "risk_classification": risk_from_delay(delay_probability),
        "exception_type": exception_type,
    }


class ShipmentEventStream:
    def __init__(self, shipment_row, dataset_row):
        self.shipment_row = shipment_row
        self.dataset_row = dataset_row
        self.route = build_route(shipment_row)
        self.index = 0
        self.schedule = build_event_schedule(
            created_at=shipment_row["created_at"],
            lead_time_days=shipment_row["lead_time_days"],
            delay_probability=float(dataset_row["delay_probability"]),
        )

    def is_done(self):
        return self.index >= len(EVENT_SEQUENCE)

    def next_event(self):
        if self.is_done():
            return None

        status = EVENT_SEQUENCE[self.index]
        route_total_steps = max(len(self.route) - 1, 1)
        progress = status_progress(status)

        if status in ["CREATED", "ASSIGNED", "PICKUP"]:
            route_step = 0
            current_hub_id = self.route[0]
            next_hub_id = self.route[1] if len(self.route) > 1 else self.route[0]

        elif status in ["IN_TRANSIT", "ARRIVED_HUB", "OUT_FOR_DELIVERY"]:
            route_step = min(int(progress * route_total_steps), route_total_steps - 1)
            current_hub_id = self.route[route_step]
            next_hub_id = self.route[route_step + 1]

        else:
            route_step = route_total_steps
            current_hub_id = self.route[-1]
            next_hub_id = self.route[-1]

        current_coord = self.shipment_row["hub_coordinates"][current_hub_id]
        next_coord = self.shipment_row["hub_coordinates"][next_hub_id]

        latitude, longitude = interpolate_position(
            current_coord["latitude"],
            current_coord["longitude"],
            next_coord["latitude"],
            next_coord["longitude"],
            progress,
        )

        signal = weighted_signal(self.dataset_row, status)
        event_timestamp = self.schedule[status]

        event = {
            "event_id": f"EVT_{uuid.uuid4().hex[:12]}",
            "shipment_id": self.shipment_row["shipment_id"],
            "dispatch_id": self.shipment_row["dispatch_id"],
            "driver_id": self.shipment_row["driver_id"],
            "vehicle_id": self.shipment_row["vehicle_id"],

            "origin_region_id": self.shipment_row["origin_region_id"],
            "destination_region_id": self.shipment_row["destination_region_id"],
            "origin_hub_id": self.shipment_row["origin_hub_id"],
            "destination_hub_id": self.shipment_row["destination_hub_id"],
            "current_hub_id": current_hub_id,
            "next_hub_id": next_hub_id,

            "cargo_type": self.shipment_row["cargo_type"],
            "cargo_weight_kg": float(self.shipment_row["cargo_weight_kg"]),
            "shipping_costs": float(self.shipment_row["shipping_costs"]),
            "lead_time_days": float(self.shipment_row["lead_time_days"]),
            "promised_delivery_at": self.shipment_row["promised_delivery_at"].isoformat(),

            "event_status": status,
            "event_sequence": self.index + 1,
            "event_timestamp": event_timestamp.isoformat(),
            "processing_timestamp": datetime.now().isoformat(),

            "latitude": latitude,
            "longitude": longitude,
            "route_step": route_step,
            "route_total_steps": route_total_steps,

            "traffic_congestion_level": signal["traffic_congestion_level"],
            "weather_severity": signal["weather_severity"],
            "hub_congestion_level": signal["hub_congestion_level"],
            "eta_variation_minutes": signal["eta_variation_minutes"],
            "delay_probability": signal["delay_probability"],
            "risk_classification": signal["risk_classification"],
            "exception_type": signal["exception_type"],
        }

        self.index += 1
        return event