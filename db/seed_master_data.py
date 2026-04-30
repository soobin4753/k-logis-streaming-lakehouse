import random
from faker import Faker
from psycopg2.extras import execute_values

from db.db_config import get_postgres_conn

fake = Faker("ko_KR")
random.seed(42)

REGIONS = [
    ("REG_01", "서울", "수도권"),
    ("REG_02", "경기", "수도권"),
    ("REG_03", "인천", "수도권"),
    ("REG_04", "부산", "영남권"),
    ("REG_05", "대구", "영남권"),
    ("REG_06", "대전", "충청권"),
    ("REG_07", "광주", "호남권"),
    ("REG_08", "울산", "영남권"),
    ("REG_09", "강원", "강원권"),
    ("REG_10", "제주", "제주권"),
]

HUBS = [
    ("HUB_001", "서울 동부 물류센터", "REG_01", 37.5665, 127.0500, "MAIN"),
    ("HUB_002", "서울 서부 물류센터", "REG_01", 37.5560, 126.9100, "SUB"),
    ("HUB_003", "경기 남부 물류허브", "REG_02", 37.2636, 127.0286, "MAIN"),
    ("HUB_004", "경기 북부 물류허브", "REG_02", 37.7380, 127.0450, "SUB"),
    ("HUB_005", "인천 항만 물류센터", "REG_03", 37.4563, 126.7052, "PORT"),
    ("HUB_006", "부산 항만 물류센터", "REG_04", 35.1796, 129.0756, "PORT"),
    ("HUB_007", "대구 내륙 물류센터", "REG_05", 35.8714, 128.6014, "MAIN"),
    ("HUB_008", "대전 중앙 물류센터", "REG_06", 36.3504, 127.3845, "MAIN"),
    ("HUB_009", "광주 호남 물류센터", "REG_07", 35.1595, 126.8526, "MAIN"),
    ("HUB_010", "울산 산업 물류센터", "REG_08", 35.5384, 129.3114, "INDUSTRIAL"),
    ("HUB_011", "강원 권역 물류센터", "REG_09", 37.8854, 127.7298, "SUB"),
    ("HUB_012", "제주 물류센터", "REG_10", 33.4996, 126.5312, "ISLAND"),
]


def seed_regions(cur):
    execute_values(
        cur,
        """
        INSERT INTO region(region_id, region_name, region_group)
        VALUES %s
        ON CONFLICT (region_id) DO NOTHING
        """,
        REGIONS,
    )


def seed_hubs(cur):
    execute_values(
        cur,
        """
        INSERT INTO hub(hub_id, hub_name, region_id, latitude, longitude, hub_type)
        VALUES %s
        ON CONFLICT (hub_id) DO NOTHING
        """,
        HUBS,
    )


def seed_drivers(cur, count=300):
    rows = []
    for i in range(1, count + 1):
        rows.append(
            (
                f"DRV_{i:04d}",
                fake.name(),
                random.choice(REGIONS)[0],
                random.choice(["1종 대형", "1종 보통", "화물운송자격"]),
                random.choice(["ACTIVE", "ACTIVE", "ACTIVE", "REST"]),
            )
        )

    execute_values(
        cur,
        """
        INSERT INTO driver(driver_id, driver_name, region_id, license_type, driver_status)
        VALUES %s
        ON CONFLICT (driver_id) DO NOTHING
        """,
        rows,
    )


def seed_vehicles(cur, count=500):
    vehicle_types = [
        ("1톤 탑차", 1000),
        ("2.5톤 탑차", 2500),
        ("5톤 윙바디", 5000),
        ("11톤 카고", 11000),
        ("냉장 탑차", 2000),
        ("전기 배송차", 800),
    ]

    rows = []
    for i in range(1, count + 1):
        vehicle_type, capacity = random.choice(vehicle_types)
        rows.append(
            (
                f"VEH_{i:04d}",
                vehicle_type,
                capacity,
                random.choice(REGIONS)[0],
                random.choice(["AVAILABLE", "AVAILABLE", "AVAILABLE", "MAINTENANCE"]),
            )
        )

    execute_values(
        cur,
        """
        INSERT INTO vehicle(vehicle_id, vehicle_type, capacity_kg, region_id, vehicle_status)
        VALUES %s
        ON CONFLICT (vehicle_id) DO NOTHING
        """,
        rows,
    )


def main():
    conn = get_postgres_conn()
    cur = conn.cursor()

    seed_regions(cur)
    seed_hubs(cur)
    seed_drivers(cur)
    seed_vehicles(cur)

    conn.commit()
    cur.close()
    conn.close()

    print("region / hub / driver 300 / vehicle 500 seed 완료")
    print("shipment / dispatch는 Producer 실행 중 생성됩니다.")


if __name__ == "__main__":
    main()