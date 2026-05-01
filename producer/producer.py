import json
import random
import time
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
from kafka import KafkaProducer

from producer.config import (
    CLEAN_DATASET_PATH,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)
from producer.event_generator import ShipmentEventStream

CARGO_PROFILES = [
    {"cargo_type": "일반화물", "min_weight": 5, "max_weight": 2500},
    {"cargo_type": "신선식품", "min_weight": 5, "max_weight": 1200},
    {"cargo_type": "냉장식품", "min_weight": 5, "max_weight": 1500},
    {"cargo_type": "전자제품", "min_weight": 1, "max_weight": 800},
    {"cargo_type": "의류", "min_weight": 1, "max_weight": 600},
    {"cargo_type": "생활용품", "min_weight": 2, "max_weight": 1000},
    {"cargo_type": "산업자재", "min_weight": 200, "max_weight": 8000},
]


def get_conn():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        port=POSTGRES_PORT,
    )


def load_dataset_rows():
    df = pd.read_csv(CLEAN_DATASET_PATH)
    df = df.where(pd.notnull(df), None)

    if df.empty:
        raise Exception("clean_logistics_dataset.csv 데이터가 비어 있습니다.")

    return df.to_dict(orient="records")


def load_master_data():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT hub_id, region_id, latitude, longitude FROM hub")
    hubs = [
        {
            "hub_id": row[0],
            "region_id": row[1],
            "latitude": float(row[2]),
            "longitude": float(row[3]),
        }
        for row in cur.fetchall()
    ]

    cur.execute("SELECT driver_id FROM driver WHERE driver_status = 'ACTIVE'")
    drivers = [row[0] for row in cur.fetchall()]

    cur.execute("SELECT vehicle_id FROM vehicle WHERE vehicle_status = 'AVAILABLE'")
    vehicles = [row[0] for row in cur.fetchall()]

    cur.close()
    conn.close()

    if not hubs or not drivers or not vehicles:
        raise Exception("hub / active driver / available vehicle 데이터가 부족합니다.")

    hub_coordinates = {
        hub["hub_id"]: {
            "latitude": hub["latitude"],
            "longitude": hub["longitude"],
        }
        for hub in hubs
    }

    return hubs, drivers, vehicles, hub_coordinates


def calculate_shipping_cost(cargo_type, cargo_weight_kg, distance_factor, base_dataset_cost):
    base_cost = float(base_dataset_cost or 15000)

    cargo_multiplier = {
        "일반화물": 1.0,
        "신선식품": 1.25,
        "냉장식품": 1.35,
        "전자제품": 1.30,
        "의류": 0.90,
        "생활용품": 0.95,
        "산업자재": 1.50,
    }.get(cargo_type, 1.0)

    weight_cost = cargo_weight_kg * random.uniform(8, 20)
    distance_cost = distance_factor * random.uniform(3000, 8000)

    return round((base_cost + weight_cost + distance_cost) * cargo_multiplier, 2)


def estimate_distance_factor(origin_hub, destination_hub):
    lat_gap = abs(origin_hub["latitude"] - destination_hub["latitude"])
    lon_gap = abs(origin_hub["longitude"] - destination_hub["longitude"])
    return max(1.0, (lat_gap + lon_gap) * 10)


def create_shipment_and_dispatch(cur, hubs, drivers, vehicles, hub_coordinates, dataset_row):
    origin_hub, destination_hub = random.sample(hubs, 2)

    now = datetime.now()
    unique_suffix = f"{now.strftime('%Y%m%d%H%M%S%f')}_{random.randint(1000, 9999)}"

    shipment_id = f"SHP_{unique_suffix}"
    dispatch_id = f"DSP_{unique_suffix}"

    created_at = now

    raw_lead_time = float(dataset_row.get("lead_time_days") or random.uniform(1, 3))
    lead_time_days = round(max(0.3, min(raw_lead_time, 10)), 2)
    promised_delivery_at = created_at + timedelta(days=lead_time_days)

    driver_id = random.choice(drivers)
    vehicle_id = random.choice(vehicles)

    cargo_profile = random.choice(CARGO_PROFILES)
    cargo_type = cargo_profile["cargo_type"]
    cargo_weight_kg = round(
        random.uniform(cargo_profile["min_weight"], cargo_profile["max_weight"]),
        2,
    )

    distance_factor = estimate_distance_factor(origin_hub, destination_hub)

    shipping_costs = calculate_shipping_cost(
        cargo_type=cargo_type,
        cargo_weight_kg=cargo_weight_kg,
        distance_factor=distance_factor,
        base_dataset_cost=dataset_row.get("shipping_costs"),
    )

    cur.execute(
        """
        INSERT INTO shipment(
            shipment_id,
            origin_region_id,
            destination_region_id,
            origin_hub_id,
            destination_hub_id,
            origin_latitude,
            origin_longitude,
            destination_latitude,
            destination_longitude,
            cargo_type,
            cargo_weight_kg,
            shipping_costs,
            lead_time_days,
            created_at,
            promised_delivery_at,
            shipment_status
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            shipment_id,
            origin_hub["region_id"],
            destination_hub["region_id"],
            origin_hub["hub_id"],
            destination_hub["hub_id"],
            origin_hub["latitude"],
            origin_hub["longitude"],
            destination_hub["latitude"],
            destination_hub["longitude"],
            cargo_type,
            cargo_weight_kg,
            shipping_costs,
            lead_time_days,
            created_at,
            promised_delivery_at,
            "CREATED",
        ),
    )

    assigned_at = created_at + timedelta(minutes=random.randint(5, 30))

    cur.execute(
        """
        INSERT INTO dispatch(
            dispatch_id,
            shipment_id,
            driver_id,
            vehicle_id,
            assigned_at,
            dispatch_status
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            dispatch_id,
            shipment_id,
            driver_id,
            vehicle_id,
            assigned_at,
            "ASSIGNED",
        ),
    )

    return {
        "shipment_id": shipment_id,
        "dispatch_id": dispatch_id,
        "driver_id": driver_id,
        "vehicle_id": vehicle_id,
        "origin_region_id": origin_hub["region_id"],
        "destination_region_id": destination_hub["region_id"],
        "origin_hub_id": origin_hub["hub_id"],
        "destination_hub_id": destination_hub["hub_id"],
        "cargo_type": cargo_type,
        "cargo_weight_kg": cargo_weight_kg,
        "shipping_costs": shipping_costs,
        "lead_time_days": lead_time_days,
        "created_at": created_at,
        "promised_delivery_at": promised_delivery_at,
        "hub_coordinates": hub_coordinates,
    }


def update_current_status(cur, shipment_id, dispatch_id, event_status):
    cur.execute(
        """
        UPDATE shipment
        SET shipment_status = %s
        WHERE shipment_id = %s
        """,
        (event_status, shipment_id),
    )

    cur.execute(
        """
        UPDATE dispatch
        SET dispatch_status = %s
        WHERE dispatch_id = %s
        """,
        (event_status, dispatch_id),
    )


def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8"),
        retries=3,
    )


def main():
    dataset_rows = load_dataset_rows()
    hubs, drivers, vehicles, hub_coordinates = load_master_data()
    producer = create_kafka_producer()

    active_streams = []
    dataset_idx = 0

    print("Kafka Producer 시작")
    print("현실 재현 데이터 생성 방식:")
    print("CSV 1줄 → shipment 1건 생성 → 배송 상태 이벤트 7단계 생성")
    print("CREATED → ASSIGNED → PICKUP → IN_TRANSIT → ARRIVED_HUB → OUT_FOR_DELIVERY → DELIVERED")
    print(f"topic: {KAFKA_TOPIC}")

    while True:
        try:
            if len(active_streams) < 25:
                conn = get_conn()
                cur = conn.cursor()

                for _ in range(random.randint(1, 4)):
                    dataset_row = dataset_rows[dataset_idx % len(dataset_rows)]
                    dataset_idx += 1

                    shipment_row = create_shipment_and_dispatch(
                        cur,
                        hubs,
                        drivers,
                        vehicles,
                        hub_coordinates,
                        dataset_row,
                    )

                    active_streams.append(
                        ShipmentEventStream(
                            shipment_row=shipment_row,
                            dataset_row=dataset_row,
                        )
                    )

                conn.commit()
                cur.close()
                conn.close()

            stream = random.choice(active_streams)
            event = stream.next_event()

            if event is None:
                active_streams.remove(stream)
                continue

            conn = get_conn()
            cur = conn.cursor()
            update_current_status(
                cur,
                event["shipment_id"],
                event["dispatch_id"],
                event["event_status"],
            )
            conn.commit()
            cur.close()
            conn.close()

            producer.send(KAFKA_TOPIC, key=event["shipment_id"], value=event)
            producer.flush()

            print(
                f"[EVENT] {event['shipment_id']} | "
                f"{event['event_sequence']} | "
                f"{event['event_status']} | "
                f"{event['current_hub_id']}->{event['next_hub_id']} | "
                f"delay={event['delay_probability']} | "
                f"risk={event['risk_classification']} | "
                f"exception={event['exception_type']}"
            )

            if stream.is_done():
                active_streams.remove(stream)

            time.sleep(random.uniform(0.2, 0.7))

        except KeyboardInterrupt:
            print("Producer 종료")
            break

        except Exception as e:
            print(f"producer error: {e}")
            time.sleep(1)

    producer.close()


if __name__ == "__main__":
    main()