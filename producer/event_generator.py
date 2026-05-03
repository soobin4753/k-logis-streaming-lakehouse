import random
import uuid
from datetime import datetime, timedelta

EVENT_SEQUENCE = [
    "CREATED",
    "ASSIGNED",
    "PICKUP",
    "IN_TRANSIT",
    "ARRIVED_HUB",
    "OUT_FOR_DELIVERY",
    "DELIVERED",
]

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

REGION_DELAY_WEIGHT = {
    "REG_01": 0.78,  # 서울
    "REG_02": 0.88,  # 경기
    "REG_03": 0.95,  # 인천
    "REG_04": 1.05,  # 부산
    "REG_05": 1.00,  # 대구
    "REG_06": 0.96,  # 대전
    "REG_07": 1.08,  # 광주
    "REG_08": 1.18,  # 울산
    "REG_09": 1.35,  # 강원
    "REG_10": 1.55,  # 제주
}

HUB_DELAY_WEIGHT = {
    "HUB_001": 0.75,  # 서울 동부 MAIN
    "HUB_002": 0.82,  # 서울 서부 SUB
    "HUB_003": 0.88,  # 경기 남부 MAIN
    "HUB_004": 0.95,  # 경기 북부 SUB
    "HUB_005": 1.05,  # 인천 PORT
    "HUB_006": 1.12,  # 부산 PORT
    "HUB_007": 1.00,  # 대구 MAIN
    "HUB_008": 0.95,  # 대전 MAIN
    "HUB_009": 1.08,  # 광주 MAIN
    "HUB_010": 1.28,  # 울산 INDUSTRIAL
    "HUB_011": 1.42,  # 강원 SUB
    "HUB_012": 1.65,  # 제주 ISLAND
}

HUB_TYPE_WEIGHT = {
    "MAIN": 0.95,
    "SUB": 1.08,
    "PORT": 1.15,
    "INDUSTRIAL": 1.25,
    "ISLAND": 1.55,
}


def clamp(value, min_value, max_value):
    return max(min_value, min(value, max_value))


def safe_float(value, default_value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default_value


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
    if delay_probability >= 0.70:
        return "HIGH"
    if delay_probability >= 0.38:
        return "MEDIUM"
    return "LOW"


def get_region_weight(shipment_row):
    return REGION_DELAY_WEIGHT.get(shipment_row["destination_region_id"], 1.0)


def get_hub_weight(shipment_row):
    return HUB_DELAY_WEIGHT.get(shipment_row["destination_hub_id"], 1.0)


def get_route_weight(shipment_row):
    if shipment_row["origin_region_id"] == shipment_row["destination_region_id"]:
        return 0.88

    if shipment_row["destination_region_id"] == "REG_10":
        return 1.25

    if shipment_row["destination_region_id"] == "REG_09":
        return 1.15

    return 1.08


def realistic_delay_probability(dataset_row, shipment_row):
    base = random.betavariate(2, 7)

    csv_val = safe_float(dataset_row.get("delay_probability"), 0.3)
    base = (base * 0.75) + (csv_val * 0.25)

    base *= get_region_weight(shipment_row)
    base *= get_hub_weight(shipment_row)
    base *= get_route_weight(shipment_row)

    base += random.uniform(-0.04, 0.07)

    return round(clamp(base, 0.03, 0.88), 3)


def interpolate_position(start_lat, start_lon, end_lat, end_lon, progress):
    lat = start_lat + (end_lat - start_lat) * progress
    lon = start_lon + (end_lon - start_lon) * progress

    lat += random.uniform(-0.003, 0.003)
    lon += random.uniform(-0.003, 0.003)

    return round(lat, 6), round(lon, 6)


def get_exception(status, delay_probability):
    if status in ["CREATED", "ASSIGNED"]:
        return None

    if status == "DELIVERED":
        return random.choices(
            [None, "CUSTOMER_ABSENT", "ADDRESS_ISSUE"],
            weights=[0.97, 0.02, 0.01],
            k=1,
        )[0]

    if delay_probability >= 0.70:
        weights = [0.72, 0.08, 0.07, 0.09, 0.04]
    elif delay_probability >= 0.38:
        weights = [0.86, 0.04, 0.03, 0.05, 0.02]
    else:
        weights = [0.95, 0.015, 0.015, 0.015, 0.005]

    return random.choices(
        [None, "TRAFFIC_ACCIDENT", "BAD_WEATHER", "HUB_CONGESTION", "VEHICLE_ISSUE"],
        weights=weights,
        k=1,
    )[0]


def build_event_schedule(created_at, lead_time_days, delay_probability):
    planned_total_minutes = max(180, int(lead_time_days * 24 * 60))

    is_delayed_case = random.random() < delay_probability

    if is_delayed_case:
        if delay_probability >= 0.70:
            delay_minutes = random.randint(120, 420)
        elif delay_probability >= 0.38:
            delay_minutes = random.randint(45, 210)
        else:
            delay_minutes = random.randint(15, 90)
    else:
        delay_minutes = random.randint(-180, 20)

    natural_variation = random.uniform(-0.08, 0.10)
    total_minutes = int(planned_total_minutes * (1 + natural_variation)) + delay_minutes
    total_minutes = max(180, total_minutes)

    return {
        "CREATED": created_at,
        "ASSIGNED": created_at + timedelta(minutes=random.randint(5, 30)),
        "PICKUP": created_at + timedelta(minutes=random.randint(40, 180)),
        "IN_TRANSIT": created_at + timedelta(minutes=int(total_minutes * random.uniform(0.25, 0.50))),
        "ARRIVED_HUB": created_at + timedelta(minutes=int(total_minutes * random.uniform(0.55, 0.75))),
        "OUT_FOR_DELIVERY": created_at + timedelta(minutes=int(total_minutes * random.uniform(0.78, 0.92))),
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


def weighted_signal(dataset_row, shipment_row, status):
    region_weight = get_region_weight(shipment_row)
    hub_weight = get_hub_weight(shipment_row)
    route_weight = get_route_weight(shipment_row)

    traffic = safe_float(dataset_row.get("traffic_congestion_level"), random.uniform(2, 6))
    weather = safe_float(dataset_row.get("weather_severity"), random.uniform(1, 5))
    hub_congestion = safe_float(dataset_row.get("hub_congestion_level"), random.uniform(2, 6))
    eta_variation = safe_float(dataset_row.get("eta_variation_minutes"), random.uniform(10, 80))

    delay_probability = realistic_delay_probability(dataset_row, shipment_row)

    traffic = traffic * 0.70 * region_weight * route_weight + random.uniform(-0.6, 1.0)
    weather = weather * 0.65 * region_weight + random.uniform(-0.5, 0.9)
    hub_congestion = hub_congestion * 0.75 * hub_weight + random.uniform(-0.6, 1.1)
    eta_variation = eta_variation * 0.60 * region_weight * hub_weight + random.uniform(-10, 30)

    if status == "CREATED":
        traffic *= 0.25
        weather *= 0.30
        hub_congestion *= 0.45
        eta_variation *= 0.15
        delay_probability *= 0.35

    elif status == "ASSIGNED":
        traffic *= 0.40
        weather *= 0.40
        hub_congestion *= 0.55
        eta_variation *= 0.25
        delay_probability *= 0.45

    elif status == "PICKUP":
        traffic *= 0.70
        weather *= 0.60
        hub_congestion *= 0.70
        eta_variation *= 0.50
        delay_probability *= 0.65

    elif status == "IN_TRANSIT":
        traffic *= 1.15
        weather *= 1.00
        hub_congestion *= 0.85
        eta_variation *= 1.00
        delay_probability *= 1.00

    elif status == "ARRIVED_HUB":
        traffic *= 0.80
        weather *= 0.75
        hub_congestion *= 1.25
        eta_variation *= 0.90
        delay_probability *= 1.00

    elif status == "OUT_FOR_DELIVERY":
        traffic *= 1.05
        weather *= 0.90
        hub_congestion *= 0.75
        eta_variation *= 0.80
        delay_probability *= 0.90

    elif status == "DELIVERED":
        traffic *= 0.55
        weather *= 0.50
        hub_congestion *= 0.50
        eta_variation *= 0.45
        delay_probability *= 0.70

    exception_type = get_exception(status, delay_probability)

    if exception_type == "TRAFFIC_ACCIDENT":
        traffic += random.uniform(1.5, 3.0)
        eta_variation += random.randint(30, 90)
        delay_probability += random.uniform(0.10, 0.20)

    elif exception_type == "BAD_WEATHER":
        weather += random.uniform(1.5, 3.0)
        eta_variation += random.randint(20, 80)
        delay_probability += random.uniform(0.08, 0.16)

    elif exception_type == "HUB_CONGESTION":
        hub_congestion += random.uniform(1.5, 3.0)
        eta_variation += random.randint(20, 70)
        delay_probability += random.uniform(0.08, 0.16)

    elif exception_type == "VEHICLE_ISSUE":
        eta_variation += random.randint(45, 120)
        delay_probability += random.uniform(0.12, 0.22)

    elif exception_type in ["CUSTOMER_ABSENT", "ADDRESS_ISSUE"]:
        eta_variation += random.randint(20, 60)
        delay_probability += random.uniform(0.05, 0.12)

    traffic = round(clamp(traffic, 0, 10), 2)
    weather = round(clamp(weather, 0, 10), 2)
    hub_congestion = round(clamp(hub_congestion, 0, 10), 2)
    eta_variation = round(clamp(eta_variation, 0, 240), 2)
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

        base_delay_probability = realistic_delay_probability(dataset_row, shipment_row)

        self.schedule = build_event_schedule(
            created_at=shipment_row["created_at"],
            lead_time_days=shipment_row["lead_time_days"],
            delay_probability=base_delay_probability,
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

        signal = weighted_signal(self.dataset_row, self.shipment_row, status)
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