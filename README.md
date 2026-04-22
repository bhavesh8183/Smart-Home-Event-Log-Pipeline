import pandas as pd
import sqlite3
import logging
import os
from datetime import timedelta

logging.basicConfig(
    filename="pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

DB_NAME = "smart_home.db"


# ---------- DATA INGESTION ----------
def ingest_data():
    file_path = os.path.join(os.path.dirname(__file__), "sensor_data.csv")
    print("Reading:", file_path)
    df = pd.read_csv(file_path)
    return df


# ---------- VALIDATION ----------
def validate_data(df):
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
    df = df.dropna(subset=["Timestamp"])
    df = df.drop_duplicates()
    return df


# ---------- NORMALIZATION ----------
def normalize_events(df):
    df["Device"] = df["Device"].str.lower()
    df["Event"] = df["Event"].astype(str).str.upper()
    df = df.sort_values("Timestamp")
    return df


# ---------- RULE BASED ANOMALY ----------
def detect_anomalies(df):
    anomalies = []

    for _, row in df.iterrows():
        ts = row["Timestamp"].strftime("%Y-%m-%d %H:%M:%S")   # FIX HERE
        device = row["Device"]
        event = row["Event"]
        device_id = row["Device_ID"]
        hour = row["Timestamp"].hour

        if device == "light" and event == "ON" and hour <= 5:
            anomalies.append([
                ts, device_id, device, event,
                "Light ON at night", "medium",
                "Light turned ON during night"
            ])

        if device == "door" and event == "OPEN" and hour <= 5:
            anomalies.append([
                ts, device_id, device, event,
                "Door opened at night", "high",
                "Door opened during night hours"
            ])

    return anomalies


# ---------- TIME WINDOW ----------
def detect_combination(df):
    anomalies = []
    window = timedelta(minutes=5)

    df = df.sort_values("Timestamp").reset_index(drop=True)

    for i in range(len(df)):
        for j in range(i + 1, len(df)):

            if df.iloc[j]["Timestamp"] - df.iloc[i]["Timestamp"] > window:
                break

            if (
                df.iloc[i]["Device"] == "door"
                and df.iloc[i]["Event"] == "OPEN"
                and df.iloc[j]["Device"] == "light"
                and df.iloc[j]["Event"] == "ON"
            ):
                ts = df.iloc[j]["Timestamp"].strftime("%Y-%m-%d %H:%M:%S")

                anomalies.append([
                    ts,
                    df.iloc[j]["Device_ID"],
                    df.iloc[j]["Device"],
                    df.iloc[j]["Event"],
                    "Door + Light",
                    "critical",
                    "Door open and light ON within 5 minutes"
                ])

    return anomalies


# ---------- STORE ----------
def store(anomalies):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS Home_Audit(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        device_id TEXT,
        device_type TEXT,
        event TEXT,
        rule TEXT,
        severity TEXT,
        description TEXT
    )
    """)

    cur.executemany("""
    INSERT INTO Home_Audit(
        timestamp,device_id,device_type,event,rule,severity,description
    ) VALUES (?,?,?,?,?,?,?)
    """, anomalies)

    conn.commit()
    conn.close()


# ---------- REPORT ----------
def report():
    conn = sqlite3.connect(DB_NAME)

    total = pd.read_sql("SELECT COUNT(*) as total FROM Home_Audit", conn)
    severity = pd.read_sql("""
        SELECT severity, COUNT(*) as count
        FROM Home_Audit
        GROUP BY severity
    """, conn)

    device = pd.read_sql("""
        SELECT device_id, COUNT(*) as count
        FROM Home_Audit
        GROUP BY device_id
    """, conn)

    conn.close()

    print("\n===== REPORT =====")
    print("\nTotal anomalies:")
    print(total)

    print("\nSeverity:")
    print(severity)

    print("\nDevice wise:")
    print(device)


# ---------- MAIN ----------
def main():
    df = ingest_data()
    df = validate_data(df)
    df = normalize_events(df)

    a1 = detect_anomalies(df)
    a2 = detect_combination(df)

    all_anomalies = a1 + a2

    print("Anomalies found:", len(all_anomalies))

    store(all_anomalies)
    report()


if __name__ == "__main__":
    main()
