import pandas as pd
import sqlite3
from datetime import timedelta
import os

# read csv file
def read_data():
    path = os.path.join(os.path.dirname(__file__), "sensor_data.csv")
    df = pd.read_csv(path)
    return df


# clean data
def clean_data(df):
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
    df = df.dropna()
    df = df.drop_duplicates()
    df = df.sort_values("Timestamp")
    return df


# detect simple anomalies
def find_anomalies(df):
    result = []

    for i in range(len(df)):
        row = df.iloc[i]

        time = row["Timestamp"]
        device = row["Device"]
        event = str(row["Event"])
        device_id = row["Device_ID"]

        hour = time.hour

        # light at night
        if device.lower() == "light" and event.upper() == "ON" and hour <= 5:
            result.append([
                str(time),
                device_id,
                device,
                event,
                "Light ON at night",
                "medium",
                "light turned on late night"
            ])

        # door at night
        if device.lower() == "door" and event.upper() == "OPEN" and hour <= 5:
            result.append([
                str(time),
                device_id,
                device,
                event,
                "Door open at night",
                "high",
                "door opened late night"
            ])

    return result


# check combination events
def check_combination(df):
    res = []
    window = timedelta(minutes=5)

    for i in range(len(df)):
        for j in range(i + 1, len(df)):

            t1 = df.iloc[i]["Timestamp"]
            t2 = df.iloc[j]["Timestamp"]

            if t2 - t1 > window:
                break

            d1 = df.iloc[i]["Device"]
            d2 = df.iloc[j]["Device"]

            e1 = str(df.iloc[i]["Event"])
            e2 = str(df.iloc[j]["Event"])

            if d1.lower() == "door" and e1.upper() == "OPEN":
                if d2.lower() == "light" and e2.upper() == "ON":
                    res.append([
                        str(t2),
                        df.iloc[j]["Device_ID"],
                        d2,
                        e2,
                        "Door + Light",
                        "critical",
                        "door open and light on together"
                    ])

    return res


# save in sqlite
def save_db(data):
    conn = sqlite3.connect("smart_home.db")
    cur = conn.cursor()

    cur.execute("""
    create table if not exists Home_Audit(
        id integer primary key autoincrement,
        timestamp text,
        device_id text,
        device_type text,
        event text,
        rule text,
        severity text,
        description text
    )
    """)

    for row in data:
        cur.execute("""
        insert into Home_Audit
        (timestamp,device_id,device_type,event,rule,severity,description)
        values (?,?,?,?,?,?,?)
        """, row)

    conn.commit()
    conn.close()


# print report
def show_report():
    conn = sqlite3.connect("smart_home.db")

    print("\nTotal anomalies")
    print(pd.read_sql("select count(*) from Home_Audit", conn))

    print("\nSeverity")
    print(pd.read_sql("select severity,count(*) from Home_Audit group by severity", conn))

    print("\nDevice wise")
    print(pd.read_sql("select device_id,count(*) from Home_Audit group by device_id", conn))

    conn.close()


# main
def main():
    df = read_data()
    df = clean_data(df)

    a1 = find_anomalies(df)
    a2 = check_combination(df)

    final = a1 + a2

    print("Total found:", len(final))

    save_db(final)
    show_report()


main()
