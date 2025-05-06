import os
import sqlite3

# Fix the import path based on your structure
from mimic_sparql.mimicsql.evaluation.process_mimic_db.process_tables import (
    build_demographic_table,
    build_diagnoses_table,
    build_procedures_table,
    build_prescriptions_table,
    build_lab_table
)

# === Configuration ===
data_dir = 'mimic-iii-clinical-database-1.4'
out_dir = 'mimic_db'
db_path = os.path.join(out_dir, 'mimic.db')

# === Ensure output directory exists ===
os.makedirs(out_dir, exist_ok=True)

# === Create SQLite database connection ===
conn = sqlite3.connect(db_path)

# === Build all tables ===
build_demographic_table(data_dir, out_dir, conn)
build_diagnoses_table(data_dir, out_dir, conn)
build_procedures_table(data_dir, out_dir, conn)
build_prescriptions_table(data_dir, out_dir, conn)
build_lab_table(data_dir, out_dir, conn)

# === Close connection ===
conn.close()

print(f"\n✅ Done! SQLite DB generated at: {db_path}")
