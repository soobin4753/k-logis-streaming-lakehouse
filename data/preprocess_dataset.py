import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

RAW_DATASET_PATH = os.getenv("RAW_DATASET_PATH")
CLEAN_DATASET_PATH = os.getenv("CLEAN_DATASET_PATH")

if not RAW_DATASET_PATH or not CLEAN_DATASET_PATH:
    raise ValueError("RAW_DATASET_PATH 또는 CLEAN_DATASET_PATH 환경변수가 없습니다.")

REQUIRED_COLUMNS = [
    "timestamp",
    "vehicle_gps_latitude",
    "vehicle_gps_longitude",
    "eta_variation_hours",
    "traffic_congestion_level",
    "weather_condition_severity",
    "port_congestion_level",
    "delay_probability",
    "risk_classification",
    "shipping_costs",
    "lead_time_days",
    "driver_behavior_score",
    "fatigue_monitoring_score",
    "disruption_likelihood_score",
    "delivery_time_deviation",
]

RENAME_COLUMNS = {
    "timestamp": "dataset_timestamp",
    "vehicle_gps_latitude": "source_latitude",
    "vehicle_gps_longitude": "source_longitude",
    "eta_variation_hours": "eta_variation_hours",
    "traffic_congestion_level": "traffic_congestion_level",
    "weather_condition_severity": "weather_severity",
    "port_congestion_level": "hub_congestion_level",
    "delay_probability": "delay_probability",
    "risk_classification": "risk_classification",
    "shipping_costs": "shipping_costs",
    "lead_time_days": "lead_time_days",
    "driver_behavior_score": "driver_behavior_score",
    "fatigue_monitoring_score": "fatigue_monitoring_score",
    "disruption_likelihood_score": "disruption_likelihood_score",
    "delivery_time_deviation": "delivery_time_deviation",
}


def normalize_risk(value, delay_probability):
    if isinstance(value, str):
        v = value.strip().upper()
        if v in ["LOW", "MEDIUM", "HIGH"]:
            return v
        if "HIGH" in v:
            return "HIGH"
        if "MED" in v:
            return "MEDIUM"
        if "LOW" in v:
            return "LOW"

    if delay_probability >= 0.7:
        return "HIGH"
    if delay_probability >= 0.4:
        return "MEDIUM"
    return "LOW"


def main():
    df = pd.read_csv(RAW_DATASET_PATH)

    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"원본 데이터셋에 필요한 컬럼이 없습니다: {missing}")

    df = df[REQUIRED_COLUMNS].rename(columns=RENAME_COLUMNS)

    numeric_columns = [
        "source_latitude",
        "source_longitude",
        "eta_variation_hours",
        "traffic_congestion_level",
        "weather_severity",
        "hub_congestion_level",
        "delay_probability",
        "shipping_costs",
        "lead_time_days",
        "driver_behavior_score",
        "fatigue_monitoring_score",
        "disruption_likelihood_score",
        "delivery_time_deviation",
    ]

    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["dataset_timestamp"] = pd.to_datetime(df["dataset_timestamp"], errors="coerce")

    df["traffic_congestion_level"] = df["traffic_congestion_level"].clip(0, 10).fillna(0)
    df["weather_severity"] = df["weather_severity"].clip(0, 10).fillna(0)
    df["hub_congestion_level"] = df["hub_congestion_level"].clip(0, 10).fillna(0)

    df["eta_variation_minutes"] = (df["eta_variation_hours"].fillna(0) * 60).round(2)

    df["delay_probability"] = df["delay_probability"].fillna(0)
    df.loc[df["delay_probability"] > 1, "delay_probability"] = (
        df.loc[df["delay_probability"] > 1, "delay_probability"] / 100
    )
    df["delay_probability"] = df["delay_probability"].clip(0, 1)

    df["risk_classification"] = df.apply(
        lambda row: normalize_risk(row["risk_classification"], row["delay_probability"]),
        axis=1,
    )

    df["shipping_costs"] = df["shipping_costs"].fillna(df["shipping_costs"].median())
    df["lead_time_days"] = df["lead_time_days"].fillna(df["lead_time_days"].median())

    output_columns = [
        "dataset_timestamp",
        "source_latitude",
        "source_longitude",
        "traffic_congestion_level",
        "weather_severity",
        "hub_congestion_level",
        "eta_variation_minutes",
        "delay_probability",
        "risk_classification",
        "shipping_costs",
        "lead_time_days",
        "driver_behavior_score",
        "fatigue_monitoring_score",
        "disruption_likelihood_score",
        "delivery_time_deviation",
    ]

    df = df[output_columns]
    df.to_csv(CLEAN_DATASET_PATH, index=False, encoding="utf-8")

    print("데이터셋 전처리 완료")
    print(f"input : {RAW_DATASET_PATH}")
    print(f"output: {CLEAN_DATASET_PATH}")
    print(f"rows  : {len(df)}")


if __name__ == "__main__":
    main()