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
    MAX_SHIPMENTS,
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

    rows = df.to_dict(orient="records")
    random.shuffle(rows)
    return rows


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

    random.shuffle(hubs)
    random.shuffle(drivers)
    random.shuffle(vehicles)

    hub_coordinates = {
        hub["hub_id"]: {
            "latitude": hub["latitude"],
            "longitude": hub["longitude"],
        }
        for hub in hubs
    }

    return hubs, drivers, vehicles, hub_coordinates


def get_float(value, default_value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default_value


def estimate_distance_factor(origin_hub, destination_hub):
    lat_gap = abs(origin_hub["latitude"] - destination_hub["latitude"])
    lon_gap = abs(origin_hub["longitude"] - destination_hub["longitude"])

    base_distance = max(1.0, (lat_gap + lon_gap) * 10)
    noise = random.uniform(0.90, 1.15)

    return round(base_distance * noise, 2)


def calculate_shipping_cost(cargo_type, cargo_weight_kg, distance_factor, base_dataset_cost):
    base_cost = get_float(base_dataset_cost, random.uniform(10000, 25000))

    cargo_multiplier = {
        "일반화물": random.uniform(0.95, 1.05),
        "신선식품": random.uniform(1.10, 1.30),
        "냉장식품": random.uniform(1.15, 1.35),
        "전자제품": random.uniform(1.10, 1.30),
        "의류": random.uniform(0.85, 1.00),
        "생활용품": random.uniform(0.90, 1.05),
        "산업자재": random.uniform(1.25, 1.55),
    }.get(cargo_type, random.uniform(0.9, 1.15))

    weight_cost = cargo_weight_kg * random.uniform(6, 18)
    distance_cost = distance_factor * random.uniform(2500, 7000)
    surcharge = random.uniform(0, 8000)

    return round((base_cost + weight_cost + distance_cost + surcharge) * cargo_multiplier, 2)


def create_shipment_and_dispatch(cur, hubs, drivers, vehicles, hub_coordinates, dataset_row):
    origin_hub, destination_hub = random.sample(hubs, 2)

    now = datetime.now()
    unique_suffix = f"{now.strftime('%Y%m%d%H%M%S%f')}_{random.randint(100000, 999999)}"

    shipment_id = f"SHP_{unique_suffix}"
    dispatch_id = f"DSP_{unique_suffix}"

    raw_lead_time = get_float(dataset_row.get("lead_time_days"), random.uniform(1.0, 3.5))

    # 현실적인 배송 리드타임: 기존 CSV 기반 + 약간의 변동
    lead_time_noise = random.uniform(-0.4, 0.8)
    lead_time_days = round(max(0.5, min(raw_lead_time + lead_time_noise, 7.0)), 2)

    # 시간별 트렌드가 보이도록 최근 3일 안에서 생성
    created_at = now - timedelta(
        hours=random.randint(0, 72),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )

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

    assigned_at = created_at + timedelta(minutes=random.randint(5, 35))

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
        linger_ms=5,
        batch_size=32768,
        retries=3,
    )


def main():
    dataset_rows = load_dataset_rows()
    hubs, drivers, vehicles, hub_coordinates = load_master_data()
    producer = create_kafka_producer()

    active_streams = []
    dataset_idx = random.randint(0, len(dataset_rows) - 1)
    created_shipments = 0
    sent_events = 0

    print("Kafka Producer 시작")
    print(f"목표 shipment 수: {MAX_SHIPMENTS}")
    print("현실적인 지연율 생성 모드: 예상 지연율 약 25~45%")
    print("CSV 1줄 → shipment 1건 → 배송 상태 이벤트 7단계 생성")

    try:
        while True:
            if created_shipments < MAX_SHIPMENTS and len(active_streams) < 100:
                conn = get_conn()
                cur = conn.cursor()

                batch_size = min(
                    random.randint(5, 10),
                    MAX_SHIPMENTS - created_shipments,
                )

                for _ in range(batch_size):
                    dataset_row = dataset_rows[dataset_idx % len(dataset_rows)]
                    dataset_idx += random.randint(1, 5)

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

                    created_shipments += 1

                conn.commit()
                cur.close()
                conn.close()

            if active_streams:
                conn = get_conn()
                cur = conn.cursor()

                event_batch_size = min(random.randint(20, 50), len(active_streams))

                for stream in random.sample(active_streams, event_batch_size):
                    event = stream.next_event()

                    if event is None:
                        continue

                    update_current_status(
                        cur,
                        event["shipment_id"],
                        event["dispatch_id"],
                        event["event_status"],
                    )

                    producer.send(
                        KAFKA_TOPIC,
                        key=event["shipment_id"],
                        value=event,
                    )

                    sent_events += 1

                    if stream.is_done():
                        active_streams.remove(stream)

                conn.commit()
                cur.close()
                conn.close()

            if created_shipments >= MAX_SHIPMENTS and len(active_streams) == 0:
                print(
                    f"Producer 종료: shipment {MAX_SHIPMENTS}건 생성 완료, "
                    f"event {sent_events}건 전송 완료"
                )
                break

            time.sleep(random.uniform(0.005, 0.03))

    except KeyboardInterrupt:
        print("Producer 수동 종료")

    except Exception as e:
        print(f"producer error: {e}")
        raise

    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()