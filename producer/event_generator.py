import random
import uuid
from datetime import datetime, timedelta

EVENT_FLOW = ["CREATED", "ASSIGNED", "PICKUP", "IN_TRANSIT", "DELIVERED"]

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


def build_route(row):
    route = [row["origin_hub_id"]]

    if row["origin_region_id"] != row["destination_region_id"]:
        origin_center = CENTRAL_HUBS.get(row["origin_region_id"])
        destination_center = CENTRAL_HUBS.get(row["destination_region_id"])

        if origin_center and origin_center not in route:
            route.append(origin_center)

        if destination_center and destination_center not in route:
            route.append(destination_center)

    if row["destination_hub_id"] not in route:
        route.append(row["destination_hub_id"])

    return route


def interpolate_position(start_lat, start_lon, end_lat, end_lon, progress):
    lat = start_lat + (end_lat - start_lat) * progress
    lon = start_lon + (end_lon - start_lon) * progress

    lat += random.uniform(-0.005, 0.005)
    lon += random.uniform(-0.005, 0.005)

    return round(lat, 6), round(lon, 6)


def get_status_event_time(created_at, status_index):
    offsets = {
        0: 0,
        1: random.randint(10, 40),
        2: random.randint(40, 120),
        3: random.randint(120, 360),
        4: random.randint(360, 900),
    }

    return created_at + timedelta(minutes=offsets[status_index])


def get_exception():
    return random.choices(
        [None, "TRAFFIC_ACCIDENT", "BAD_WEATHER", "HUB_CONGESTION", "VEHICLE_ISSUE"],
        weights=[0.88, 0.03, 0.04, 0.04, 0.01],
        k=1,
    )[0]


def normalize_risk(delay_probability):
    if delay_probability >= 0.7:
        return "HIGH"
    if delay_probability >= 0.4:
        return "MEDIUM"
    return "LOW"


def apply_status_weight(signal, status):
    signal = signal.copy()

    if status in ["CREATED", "ASSIGNED"]:
        signal["traffic_congestion_level"] *= 0.6
        signal["weather_severity"] *= 0.7
        signal["hub_congestion_level"] *= 0.8
        signal["eta_variation_minutes"] *= 0.3
        signal["delay_probability"] *= 0.5

    elif status == "PICKUP":
        signal["eta_variation_minutes"] *= 0.6
        signal["delay_probability"] *= 0.7

    elif status == "DELIVERED":
        signal["eta_variation_minutes"] *= 0.5

    signal["traffic_congestion_level"] = round(min(max(signal["traffic_congestion_level"], 0), 10), 2)
    signal["weather_severity"] = round(min(max(signal["weather_severity"], 0), 10), 2)
    signal["hub_congestion_level"] = round(min(max(signal["hub_congestion_level"], 0), 10), 2)
    signal["eta_variation_minutes"] = round(signal["eta_variation_minutes"], 2)
    signal["delay_probability"] = round(min(max(signal["delay_probability"], 0), 1), 3)

    return signal


class ShipmentEventState:
    def __init__(self, shipment_row, dataset_row):
        self.row = shipment_row
        self.dataset_row = dataset_row
        self.status_index = 0
        self.route = build_route(shipment_row)
        self.created_at = shipment_row["created_at"]

    def is_done(self):
        return self.status_index >= len(EVENT_FLOW)

    def build_signal(self, status):
        signal = {
            "traffic_congestion_level": float(self.dataset_row["traffic_congestion_level"]),
            "weather_severity": float(self.dataset_row["weather_severity"]),
            "hub_congestion_level": float(self.dataset_row["hub_congestion_level"]),
            "eta_variation_minutes": float(self.dataset_row["eta_variation_minutes"]),
            "delay_probability": float(self.dataset_row["delay_probability"]),
        }

        signal = apply_status_weight(signal, status)
        exception_type = get_exception()

        if exception_type == "TRAFFIC_ACCIDENT":
            signal["traffic_congestion_level"] = min(10, signal["traffic_congestion_level"] + 2.5)
            signal["delay_probability"] = min(1, signal["delay_probability"] + 0.2)

        elif exception_type == "BAD_WEATHER":
            signal["weather_severity"] = min(10, signal["weather_severity"] + 3.0)
            signal["delay_probability"] = min(1, signal["delay_probability"] + 0.15)

        elif exception_type == "HUB_CONGESTION":
            signal["hub_congestion_level"] = min(10, signal["hub_congestion_level"] + 3.0)
            signal["delay_probability"] = min(1, signal["delay_probability"] + 0.15)

        elif exception_type == "VEHICLE_ISSUE":
            signal["eta_variation_minutes"] += random.randint(40, 120)
            signal["delay_probability"] = min(1, signal["delay_probability"] + 0.25)

        signal["risk_classification"] = normalize_risk(signal["delay_probability"])
        signal["exception_type"] = exception_type

        return signal

    def next_event(self):
        if self.is_done():
            return None

        status = EVENT_FLOW[self.status_index]
        route_total_steps = max(len(self.route) - 1, 1)

        if status in ["CREATED", "ASSIGNED"]:
            route_step = 0
            current_hub_id = self.route[0]
            next_hub_id = self.route[1] if len(self.route) > 1 else self.route[0]
            progress = 0.0
        elif status == "PICKUP":
            route_step = 0
            current_hub_id = self.route[0]
            next_hub_id = self.route[1] if len(self.route) > 1 else self.route[-1]
            progress = 0.1
        elif status == "IN_TRANSIT":
            route_step = min(random.randint(0, route_total_steps - 1), route_total_steps - 1)
            current_hub_id = self.route[route_step]
            next_hub_id = self.route[route_step + 1]
            progress = random.uniform(0.25, 0.85)
        else:
            route_step = route_total_steps
            current_hub_id = self.route[-1]
            next_hub_id = self.route[-1]
            progress = 1.0

        current_coord = self.row["hub_coordinates"][current_hub_id]
        next_coord = self.row["hub_coordinates"][next_hub_id]

        latitude, longitude = interpolate_position(
            current_coord["latitude"],
            current_coord["longitude"],
            next_coord["latitude"],
            next_coord["longitude"],
            progress,
        )

        event_timestamp = get_status_event_time(self.created_at, self.status_index)
        signal = self.build_signal(status)

        event = {
            "event_id": f"EVT_{uuid.uuid4().hex[:12]}",
            "shipment_id": self.row["shipment_id"],
            "dispatch_id": self.row["dispatch_id"],
            "driver_id": self.row["driver_id"],
            "vehicle_id": self.row["vehicle_id"],

            "origin_region_id": self.row["origin_region_id"],
            "destination_region_id": self.row["destination_region_id"],
            "origin_hub_id": self.row["origin_hub_id"],
            "destination_hub_id": self.row["destination_hub_id"],
            "current_hub_id": current_hub_id,
            "next_hub_id": next_hub_id,

            "cargo_type": self.row["cargo_type"],
            "cargo_weight_kg": float(self.row["cargo_weight_kg"]),
            "shipping_costs": float(self.row["shipping_costs"]),
            "lead_time_days": float(self.row["lead_time_days"]),
            "promised_delivery_at": self.row["promised_delivery_at"].isoformat(),

            "event_status": status,
            "event_timestamp": event_timestamp.isoformat(),
            "processing_timestamp": datetime.now().isoformat(),

            "latitude": latitude,
            "longitude": longitude,
            "route_step": route_step,
            "route_total_steps": route_total_steps,

            "traffic_congestion_level": round(signal["traffic_congestion_level"], 2),
            "weather_severity": round(signal["weather_severity"], 2),
            "hub_congestion_level": round(signal["hub_congestion_level"], 2),
            "eta_variation_minutes": round(signal["eta_variation_minutes"], 2),
            "delay_probability": round(signal["delay_probability"], 3),
            "risk_classification": signal["risk_classification"],
            "exception_type": signal["exception_type"],
        }

        self.status_index += 1
        return event