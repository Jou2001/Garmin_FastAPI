from fastapi import FastAPI, Body
from datetime import datetime
import pytz
import psycopg2
import json
from decouple import config


app = FastAPI()

# Set DB connection parameters
db_connect_params = {
    "host": config("POSTGRESQL_HOST"),
    "port": config("POSTGRESQL_PORT"),
    "database": config("POSTGRESQL_DB"),
    "user": config("POSTGRESQL_USER"),
    "password": config("POSTGRESQL_PWD"),
}

# Specify the timezone (Asia/Taipei for Taiwan)
TW_TIMEZONE = pytz.timezone("Asia/Taipei")

# * Function for extracting data. Return complete user's data and Garmin User ID.
def extract_data(raw, KEYWORD):
    print(f"raw: {raw}")
    details = raw[KEYWORD][0]
    print(f"details: {details}")
    print(f"type: {type(details)}")
    garmin_user_id = details.get("userId")

    return details, garmin_user_id

@app.post("/Activities")
async def add_activities(activities: dict = Body(...)):
    # Set parameters
    KEYWORD = "activities"

    # Connect to Render DB
    con = psycopg2.connect(**db_connect_params)
    cur = con.cursor()
    print("Connect successfully!")

    # Extract the data
    details, garmin_user_id = extract_data(activities, KEYWORD)

    # Get datetime
    now = datetime.now(TW_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S.%f")

    # Merge the data
    data = {**details, "get_data_datetime": now}

    # Convert to json format
    json_data = json.dumps(data)

    # Insert to DB
    cur.execute(
        """
        INSERT INTO cg_activities (details, garmin_user_id) VALUES (%s, %s)
    """,
        (json_data, garmin_user_id),
    )

    # Commit the changes
    con.commit()
    print("Create successfully!")

    # Close the connection
    cur.close()
    con.close()
    print("Connection closed!")

    return {"result": activities}


@app.post("/Dailies")
async def add_dailies(dailies: dict = Body(...)):
    # Set parameters
    KEYWORD = "dailies"

    # Connect to Render DB
    con = psycopg2.connect(**db_connect_params)
    cur = con.cursor()
    print("Connect successfully!")

    # Extract the data
    details, garmin_user_id = extract_data(dailies, KEYWORD)

    # Get today's datetime
    now = datetime.now(TW_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S.%f")

    # Merge the data
    data = {**details, "get_data_datetime": now}

    # Convert to json format
    json_data = json.dumps(data)

    # Insert to DB
    cur.execute(
        """
        INSERT INTO cg_dailies (details, garmin_user_id) VALUES (%s, %s)
    """,
        (json_data, garmin_user_id),
    )

    # Commit the changes
    con.commit()
    print("Create successfully!")

    # Close the connection
    cur.close()
    con.close()
    print("Connection closed!")

    return {"result": dailies}


@app.post("/PulseOx")
async def add_pulseox(pulseox: dict = Body(...)):
    print("PulseOx data received:", pulseox)
    # Set parameters
    KEYWORD = "pulseox"
    
    # Connect to Render DB
    con = psycopg2.connect(**db_connect_params)
    cur = con.cursor()
    print("Connect successfully!")

    # Extract the data
    details, garmin_user_id = extract_data(pulseox, KEYWORD)

    # Get today's datetime
    now = datetime.now(TW_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S.%f")

    # Merge the data
    data = {**details, "get_data_datetime": now}

    # Convert to json format
    json_data = json.dumps(data)

    # Insert to DB
    cur.execute(
        """
        INSERT INTO cg_pulseox (details, garmin_user_id) VALUES (%s, %s)
    """,
        (json_data, garmin_user_id),
    )

    # Commit the changes
    con.commit()
    print("Create successfully!")

    # Close the connection
    cur.close()
    con.close()
    print("Connection closed!")

    return {"result": pulseox}

@app.post("/Stress")
async def add_stress(stress: dict = Body(...)):
    print("Stress data received:", stress)
    # Set parameters
    KEYWORD = "stress"
    
    # Connect to Render DB
    con = psycopg2.connect(**db_connect_params)
    cur = con.cursor()
    print("Connect successfully!")

    # Extract the data
    details, garmin_user_id = extract_data(stress, KEYWORD)

    # Get today's datetime
    now = datetime.now(TW_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S.%f")

    # Merge the data
    data = {**details, "get_data_datetime": now}

    # Convert to json format
    json_data = json.dumps(data)

    # Insert to DB
    cur.execute(
        """
        INSERT INTO stress (details, garmin_user_id) VALUES (%s, %s)
    """,
        (json_data, garmin_user_id),
    )

    # Commit the changes
    con.commit()
    print("Create successfully!")

    # Close the connection
    cur.close()
    con.close()
    print("Connection closed!")

    return {"result": stress}
