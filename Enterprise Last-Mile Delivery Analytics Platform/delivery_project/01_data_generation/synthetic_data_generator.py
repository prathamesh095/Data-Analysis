# Databricks notebook source
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import uuid
import random
import os
import shutil

# ---------------- CONFIG ----------------
NUM_ORDERS = 20000
NUM_RIDERS = 500
START_DATE = datetime(2026, 1, 1)
END_DATE = datetime(2026, 2, 1)

BASE_PATH = "/Volumes/delivery_analytics/bronze/raw_landing/raw_source/"

random.seed(42)
np.random.seed(42)

# Clean previous run (idempotent)
if os.path.exists(BASE_PATH):
    shutil.rmtree(BASE_PATH)

def rand_ts():
    return START_DATE + timedelta(
        seconds=random.randint(0, int((END_DATE - START_DATE).total_seconds()))
    )

def uid(n=8):
    return str(uuid.uuid4()).replace("-", "")[:n]

print("🚀 Creating directory structure...")

entities = [
    "orders", "deliveries", "riders", "shifts",
    "cancellations", "pings", "pricing", "support"
]

for e in entities:
    os.makedirs(BASE_PATH + e, exist_ok=True)

# ---------------- RIDERS ----------------
riders = []
for i in range(NUM_RIDERS):
    riders.append({
        "rider_id": uid(),
        "rider_name": f"Rider_{i}",
        "city": random.choice(["London", "NYC", "Mumbai"]),
        "home_zone": random.choice(["North", "South", "Central"]),
        "join_date": START_DATE - timedelta(days=random.randint(30, 365)),
        "vehicle_type": random.choice(["Scooter", "Bike", "Car"]),
        "license_verified_flag": random.choice([True, False]),
        "background_check_flag": random.choice([True, False]),
        "employment_type": random.choice(["Full-time", "Gig"]),
        "status": random.choice(["Active", "Inactive"]),
        "rating_avg": round(random.uniform(3.0, 5.0), 2),
        "total_deliveries_lifetime": random.randint(100, 5000),
        "last_active_ts": rand_ts(),
        "device_os": random.choice(["iOS", "Android"]),
        "device_model": random.choice(["Samsung", "iPhone", "Xiaomi"]),
        "bank_account_verified": random.choice([True, False]),
        "incentive_tier": random.choice(["A", "B", "C"]),
        "ingestion_ts": datetime.utcnow(),
        "source_file": "hr_export.csv"
    })

pd.DataFrame(riders).to_csv(BASE_PATH+"riders/riders_raw.csv", index=False)

# ---------------- SHIFTS ----------------
shifts = []
for _ in range(NUM_RIDERS * 10):
    start = rand_ts()
    end = start + timedelta(hours=random.randint(4, 10))
    shifts.append({
        "shift_id": uid(),
        "rider_id": random.choice(riders)["rider_id"],
        "city": random.choice(["London", "NYC", "Mumbai"]),
        "shift_start_ts": start,
        "shift_end_ts": end,
        "scheduled_hours": round((end-start).seconds/3600, 2),
        "actual_login_ts": start + timedelta(minutes=random.randint(-10,10)),
        "actual_logout_ts": end + timedelta(minutes=random.randint(-10,10)),
        "shift_status": random.choice(["Completed","Cancelled"]),
        "late_start_flag": random.choice([True, False]),
        "early_logout_flag": random.choice([True, False]),
        "overtime_flag": random.choice([True, False]),
        "shift_type": random.choice(["Regular","Peak"]),
        "manager_id": uid(),
        "ingestion_ts": datetime.utcnow(),
        "source_file": "shift_system.csv"
    })

pd.DataFrame(shifts).to_csv(BASE_PATH+"shifts/shifts_raw.csv", index=False)

# ---------------- MAIN EVENT STREAM ----------------
orders, deliveries, pricing, cancels, pings, support = [], [], [], [], [], []

for _ in range(NUM_ORDERS):
    oid = uid(12)
    ts = rand_ts()
    rider = random.choice(riders)["rider_id"]
    outcome = random.choices(["success","cancel","fail"], weights=[0.75,0.15,0.10])[0]

    # ORDERS
    orders.append({
        "order_id": oid,
        "customer_id": uid(10),
        "city": random.choice(["London","NYC","Mumbai"]),
        "restaurant_id": uid(),
        "delivery_zone": random.choice(["Z1","Z2","Z3"]),
        "created_ts": ts,
        "accepted_ts": ts + timedelta(minutes=2),
        "promised_ts": ts + timedelta(minutes=45),
        "estimated_pickup_ts": ts + timedelta(minutes=10),
        "estimated_drop_ts": ts + timedelta(minutes=40),
        "estimated_distance_km": round(random.uniform(2,15),2),
        "estimated_duration_min": random.randint(20,60),
        "order_value": round(random.uniform(20,120),2),
        "tax_amount": round(random.uniform(1,10),2),
        "tip_amount": round(random.uniform(0,10),2),
        "payment_method": random.choice(["Card","Cash","Wallet"]),
        "payment_status": "Paid",
        "order_status": outcome,
        "priority_flag": random.choice([True, False]),
        "device_type": random.choice(["Android","iOS"]),
        "app_version": "5."+str(random.randint(1,9)),
        "customer_rating_expected": random.randint(3,5),
        "ingestion_ts": datetime.utcnow(),
        "source_file": "orders_export.csv"
    })

    # PRICING
    pricing.append({
        "pricing_event_id": uid(),
        "order_id": oid,
        "pricing_ts": ts,
        "base_fare": 5,
        "distance_fare": round(random.uniform(3,10),2),
        "time_fare": round(random.uniform(1,5),2),
        "surge_multiplier": random.choice([1.0,1.5,2.0]),
        "service_fee": 2,
        "platform_fee": 1,
        "discount_amount": random.choice([0,5,10]),
        "promo_code_id": random.choice(["SAVE10","NEWUSER",None]),
        "promo_type": "Percent",
        "company_subsidy": random.choice([0,3]),
        "rider_bonus": random.choice([0,2]),
        "final_customer_price": round(random.uniform(15,150),2),
        "pricing_version": "v2",
        "ingestion_ts": datetime.utcnow(),
        "source_file": "pricing_engine.csv"
    })

    if outcome != "fail":
        did = uid()
        pickup = ts + timedelta(minutes=random.randint(5,15))
        drop = pickup + timedelta(minutes=random.randint(10,30)) if outcome=="success" else None

        deliveries.append({
            "delivery_id": did,
            "order_id": oid,
            "rider_id": rider,
            "dispatch_ts": ts + timedelta(minutes=2),
            "arrival_at_pickup_ts": pickup - timedelta(minutes=2),
            "pickup_ts": pickup,
            "departure_from_pickup_ts": pickup + timedelta(minutes=1),
            "arrival_at_drop_ts": drop - timedelta(minutes=2) if drop else None,
            "drop_ts": drop,
            "delivery_status": outcome,
            "failure_reason": None if outcome=="success" else "Customer cancel",
            "distance_km_actual": round(random.uniform(2,20),2),
            "duration_min_actual": random.randint(15,60),
            "traffic_delay_min": random.randint(0,10),
            "weather_flag": random.choice([True, False]),
            "proof_of_delivery_flag": random.choice([True, False]),
            "customer_rating": random.randint(1,5),
            "rider_rating": random.randint(1,5),
            "ingestion_ts": datetime.utcnow(),
            "source_file": "fleet_logs.csv"
        })

        if outcome == "success":
            for _ in range(3):
                pings.append({
                    "ping_id": uid(),
                    "rider_id": rider,
                    "delivery_id": did,
                    "lat": random.uniform(40,41),
                    "lon": random.uniform(-74,-73),
                    "speed_kmh": random.randint(0,60),
                    "heading": random.randint(0,360),
                    "accuracy_meters": random.randint(5,30),
                    "battery_level": random.randint(10,100),
                    "network_type": random.choice(["4G","5G","WiFi"]),
                    "ping_ts": pickup + timedelta(minutes=random.randint(1,5)),
                    "gps_provider": "device",
                    "is_mock_location_flag": False,
                    "ingestion_ts": datetime.utcnow(),
                    "source_file": "gps_stream.csv"
                })

    if outcome == "cancel":
        cancels.append({
            "cancel_id": uid(),
            "order_id": oid,
            "cancel_ts": ts + timedelta(minutes=10),
            "cancel_stage": "pre_drop",
            "cancel_actor": random.choice(["Customer","Rider","System"]),
            "cancel_reason_code": "SLOW",
            "cancel_reason_text": "Too slow",
            "refund_amount": 5,
            "compensation_paid": 2,
            "auto_cancel_flag": False,
            "linked_support_ticket": uid(),
            "ingestion_ts": datetime.utcnow(),
            "source_file": "cancel_service.csv"
        })

        support.append({
            "ticket_id": uid(),
            "order_id": oid,
            "customer_id": uid(),
            "created_ts": ts + timedelta(minutes=12),
            "issue_category": "Late",
            "issue_subcategory": "Driver delay",
            "priority_level": "Medium",
            "ticket_channel": "App",
            "agent_id": uid(),
            "resolution_status": "Refunded",
            "resolution_ts": ts + timedelta(minutes=20),
            "compensation_amount": 5,
            "csat_score": random.randint(1,5),
            "reopened_flag": False,
            "notes_text": "Customer unhappy",
            "ingestion_ts": datetime.utcnow(),
            "source_file": "support_system.csv"
        })

# ---------------- SAVE (safe empty handling) ----------------
pd.DataFrame(orders).to_csv(BASE_PATH+"orders/orders_raw.csv", index=False)
pd.DataFrame(deliveries).to_csv(BASE_PATH+"deliveries/deliveries_raw.csv", index=False)
pd.DataFrame(pricing).to_csv(BASE_PATH+"pricing/pricing_raw.csv", index=False)
pd.DataFrame(cancels if cancels else [{}]).to_csv(BASE_PATH+"cancellations/cancellations_raw.csv", index=False)
pd.DataFrame(pings if pings else [{}]).to_csv(BASE_PATH+"pings/pings_raw.csv", index=False)
pd.DataFrame(support if support else [{}]).to_csv(BASE_PATH+"support/support_raw.csv", index=False)

print("✅ Synthetic Bronze data generated successfully")
