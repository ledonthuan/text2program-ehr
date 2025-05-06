import sqlite3
import random

# Connect to the original database (mimic.db)
conn = sqlite3.connect('mimic_db/mimic.db')
cursor = conn.cursor()

# Create the new sample database
sample_conn = sqlite3.connect('mimic_db/sample_mimic.db')
sample_cursor = sample_conn.cursor()

# Create the tables in the new database based on the schemas from mimic.db
sample_cursor.execute('''
    CREATE TABLE DEMOGRAPHIC (
        SUBJECT_ID TEXT,
        HADM_ID TEXT,
        NAME TEXT,
        MARITAL_STATUS TEXT,
        AGE INTEGER,
        DOB TEXT,
        GENDER TEXT,
        LANGUAGE TEXT,
        RELIGION TEXT,
        ADMISSION_TYPE TEXT,
        DAYS_STAY INTEGER,
        INSURANCE TEXT,
        ETHNICITY TEXT,
        EXPIRE_FLAG INTEGER,
        ADMISSION_LOCATION TEXT,
        DISCHARGE_LOCATION TEXT,
        DIAGNOSIS TEXT,
        DOD TEXT,
        DOB_YEAR INTEGER,
        DOD_YEAR REAL,
        ADMITTIME TEXT,
        DISCHTIME TEXT,
        ADMITYEAR INTEGER
    );
''')

sample_cursor.execute('''
    CREATE TABLE DIAGNOSES (
        SUBJECT_ID TEXT,
        HADM_ID TEXT,
        ICD9_CODE TEXT,
        SHORT_TITLE TEXT,
        LONG_TITLE TEXT
    );
''')

sample_cursor.execute('''
    CREATE TABLE LAB (
        SUBJECT_ID TEXT,
        HADM_ID TEXT,
        ITEMID TEXT,
        CHARTTIME TEXT,
        FLAG TEXT,
        VALUE_UNIT TEXT,
        LABEL TEXT,
        FLUID TEXT,
        CATEGORY TEXT
    );
''')

sample_cursor.execute('''
    CREATE TABLE PRESCRIPTIONS (
        SUBJECT_ID TEXT,
        HADM_ID TEXT,
        ICUSTAY_ID TEXT,
        DRUG_TYPE TEXT,
        DRUG TEXT,
        FORMULARY_DRUG_CD TEXT,
        ROUTE TEXT,
        DRUG_DOSE TEXT
    );
''')

sample_cursor.execute('''
    CREATE TABLE PROCEDURES (
        SUBJECT_ID TEXT,
        HADM_ID TEXT,
        ICD9_CODE TEXT,
        SHORT_TITLE TEXT,
        LONG_TITLE TEXT
    );
''')

# Get a random sample of SUBJECT_IDs (e.g., 100 people)
cursor.execute("SELECT DISTINCT SUBJECT_ID FROM DEMOGRAPHIC")
subject_ids = [row[0] for row in cursor.fetchall()]
sample_subject_ids = random.sample(subject_ids, 100)

# Insert data into the new database for each table based on the sample of SUBJECT_IDs
for subject_id in sample_subject_ids:
    # Get a random HADM_ID for each selected SUBJECT_ID
    cursor.execute(f"SELECT DISTINCT HADM_ID FROM DEMOGRAPHIC WHERE SUBJECT_ID = ?", (subject_id,))
    hadm_ids = [row[0] for row in cursor.fetchall()]
    sample_hadm_id = random.choice(hadm_ids)  # Randomly select one HADM_ID per SUBJECT_ID

    # Sample data from the DEMOGRAPHIC table
    cursor.execute(f"SELECT * FROM DEMOGRAPHIC WHERE SUBJECT_ID = ? AND HADM_ID = ?", (subject_id, sample_hadm_id))
    demographic_data = cursor.fetchone()
    sample_cursor.execute('''
        INSERT INTO DEMOGRAPHIC (SUBJECT_ID, HADM_ID, NAME, MARITAL_STATUS, AGE, DOB, GENDER, LANGUAGE, RELIGION,
                                 ADMISSION_TYPE, DAYS_STAY, INSURANCE, ETHNICITY, EXPIRE_FLAG, ADMISSION_LOCATION,
                                 DISCHARGE_LOCATION, DIAGNOSIS, DOD, DOB_YEAR, DOD_YEAR, ADMITTIME, DISCHTIME, ADMITYEAR)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    ''', demographic_data)

    # Sample data from the DIAGNOSES table
    cursor.execute(f"SELECT * FROM DIAGNOSES WHERE SUBJECT_ID = ? AND HADM_ID = ?", (subject_id, sample_hadm_id))
    diagnoses_data = cursor.fetchall()
    for row in diagnoses_data:
        sample_cursor.execute('''
            INSERT INTO DIAGNOSES (SUBJECT_ID, HADM_ID, ICD9_CODE, SHORT_TITLE, LONG_TITLE)
            VALUES (?, ?, ?, ?, ?);
        ''', row)

    # Sample data from the LAB table
    cursor.execute(f"SELECT * FROM LAB WHERE SUBJECT_ID = ? AND HADM_ID = ?", (subject_id, sample_hadm_id))
    lab_data = cursor.fetchall()
    for row in lab_data:
        sample_cursor.execute('''
            INSERT INTO LAB (SUBJECT_ID, HADM_ID, ITEMID, CHARTTIME, FLAG, VALUE_UNIT, LABEL, FLUID, CATEGORY)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
        ''', row)

    # Sample data from the PRESCRIPTIONS table
    cursor.execute(f"SELECT * FROM PRESCRIPTIONS WHERE SUBJECT_ID = ? AND HADM_ID = ?", (subject_id, sample_hadm_id))
    prescriptions_data = cursor.fetchall()
    for row in prescriptions_data:
        sample_cursor.execute('''
            INSERT INTO PRESCRIPTIONS (SUBJECT_ID, HADM_ID, ICUSTAY_ID, DRUG_TYPE, DRUG, FORMULARY_DRUG_CD, ROUTE, DRUG_DOSE)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        ''', row)

    # Sample data from the PROCEDURES table
    cursor.execute(f"SELECT * FROM PROCEDURES WHERE SUBJECT_ID = ? AND HADM_ID = ?", (subject_id, sample_hadm_id))
    procedures_data = cursor.fetchall()
    for row in procedures_data:
        sample_cursor.execute('''
            INSERT INTO PROCEDURES (SUBJECT_ID, HADM_ID, ICD9_CODE, SHORT_TITLE, LONG_TITLE)
            VALUES (?, ?, ?, ?, ?);
        ''', row)

# Commit the changes and close the connection
sample_conn.commit()
sample_conn.close()
conn.close()

print("Sample database created successfully.")
