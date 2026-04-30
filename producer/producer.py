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
from producer.event_generator import ShipmentEventState

CARGO_TYPES = [
    "일반화물",
    "신선식품",
    "냉장식품",
    "전자제품",
    "의류",
    "생활용품",
    "산업자재",
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


def create_shipment_and_dispatch(cur, hubs, drivers, vehicles, hub_coordinates, dataset_row):
    origin_hub, destination_hub = random.sample(hubs, 2)

    now = datetime.now()
    unique_suffix = f"{now.strftime('%Y%m%d%H%M%S%f')}_{random.randint(1000, 9999)}"

    shipment_id = f"SHP_{unique_suffix}"
    dispatch_id = f"DSP_{unique_suffix}"

    created_at = now
    lead_time_days = float(dataset_row.get("lead_time_days") or random.uniform(1, 3))
    promised_delivery_at = created_at + timedelta(days=max(0.3, lead_time_days))

    driver_id = random.choice(drivers)
    vehicle_id = random.choice(vehicles)

    cargo_type = random.choice(CARGO_TYPES)
    cargo_weight_kg = round(random.uniform(5, 5000), 2)
    shipping_costs = float(dataset_row.get("shipping_costs") or random.uniform(5000, 300000))

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

    assigned_at = created_at + timedelta(minutes=random.randint(5, 40))

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


def update_status(cur, shipment_id, dispatch_id, event_status):
    cur.execute(
        """
        UPDATE shipment
        SET shipment_status = %s
        WHERE shipment_id = %s
        """,
        (event_status, shipment_id),
    )

    if event_status in ["ASSIGNED", "PICKUP", "IN_TRANSIT", "DELIVERED"]:
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

    active_shipments = []
    dataset_idx = 0

    print("Kafka Producer 시작")
    print(f"topic: {KAFKA_TOPIC}")
    print(f"clean dataset rows: {len(dataset_rows)}")
    print("shipment / dispatch는 Producer 실행 중 생성됩니다.")

    while True:
        try:
            if len(active_shipments) < 30:
                conn = get_conn()
                cur = conn.cursor()

                for _ in range(random.randint(1, 5)):
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

                    active_shipments.append(
                        ShipmentEventState(
                            shipment_row=shipment_row,
                            dataset_row=dataset_row,
                        )
                    )

                conn.commit()
                cur.close()
                conn.close()

            shipment_state = random.choice(active_shipments)
            event = shipment_state.next_event()

            if event is None:
                active_shipments.remove(shipment_state)
                continue

            conn = get_conn()
            cur = conn.cursor()
            update_status(cur, event["shipment_id"], event["dispatch_id"], event["event_status"])
            conn.commit()
            cur.close()
            conn.close()

            producer.send(KAFKA_TOPIC, key=event["shipment_id"], value=event)
            producer.flush()

            print(
                f"[SEND] {event['shipment_id']} | "
                f"{event['event_status']} | "
                f"{event['current_hub_id']}->{event['next_hub_id']} | "
                f"traffic={event['traffic_congestion_level']} | "
                f"weather={event['weather_severity']} | "
                f"delay={event['delay_probability']} | "
                f"risk={event['risk_classification']}"
            )

            if shipment_state.is_done():
                active_shipments.remove(shipment_state)

            time.sleep(0.3)

        except KeyboardInterrupt:
            print("Producer 종료")
            break

        except Exception as e:
            print(f"producer error: {e}")
            time.sleep(1)

    producer.close()


if __name__ == "__main__":
    main()