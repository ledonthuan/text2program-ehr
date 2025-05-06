import os
import csv
import shutil
import pandas
import numpy as np
from datetime import datetime
from pathlib import Path
from .utils import *
from tqdm import tqdm  # Import tqdm for progress bars

def build_demographic_table(data_dir, out_dir, conn):
    # Set up the patient name lookup
    csv_path = Path(__file__).parent
    pat_id2name = get_patient_name(csv_path)
    pat_info = read_table(data_dir, 'PATIENTS.csv.gz')
    adm_info = read_table(data_dir, 'ADMISSIONS.csv.gz')

    # Process PATIENTS
    print('Processing PATIENTS...')
    for cnt, itm in enumerate(tqdm(pat_info, desc="Processing PATIENTS", unit="rows")):
        itm['NAME'] = pat_id2name[itm['SUBJECT_ID']]
        dob = datetime.strptime(itm['DOB'], '%Y-%m-%d %H:%M:%S')
        itm['DOB_YEAR'] = str(dob.year)
        if len(itm['DOD']) > 0:
            dod = datetime.strptime(itm['DOD'], '%Y-%m-%d %H:%M:%S')
            itm['DOD_YEAR'] = str(dod.year)
        else:
            itm['DOD_YEAR'] = ''

    pat_dic = {ky['SUBJECT_ID']: ky for ky in pat_info}

    # Process ADMISSIONS
    print('Processing ADMISSIONS...')
    for cnt, itm in enumerate(tqdm(adm_info, desc="Processing ADMISSIONS", unit="rows")):
        # Patients
        for ss in pat_dic[itm['SUBJECT_ID']]:
            if ss == 'ROW_ID' or ss == 'SUBJECT_ID':
                continue
            itm[ss] = pat_dic[itm['SUBJECT_ID']][ss]
        # Admissions
        admtime = datetime.strptime(itm['ADMITTIME'], '%Y-%m-%d %H:%M:%S')
        itm['ADMITYEAR'] = str(admtime.year)
        dctime = datetime.strptime(itm['DISCHTIME'], '%Y-%m-%d %H:%M:%S')
        itm['DAYS_STAY'] = str((dctime - admtime).days)
        itm['AGE'] = str(int(itm['ADMITYEAR']) - int(itm['DOB_YEAR']))
        if int(itm['AGE']) > 89:
            itm['AGE'] = str(89 + int(itm['AGE']) - 300)

    # Write the table
    header = [
        'SUBJECT_ID', 'HADM_ID', 'NAME', 'MARITAL_STATUS', 'AGE', 'DOB', 'GENDER', 'LANGUAGE', 'RELIGION',
        'ADMISSION_TYPE', 'DAYS_STAY', 'INSURANCE', 'ETHNICITY', 'EXPIRE_FLAG', 'ADMISSION_LOCATION', 
        'DISCHARGE_LOCATION', 'DIAGNOSIS', 'DOD', 'DOB_YEAR', 'DOD_YEAR', 'ADMITTIME', 'DISCHTIME', 'ADMITYEAR'
    ]
    with open(os.path.join(out_dir, 'DEMOGRAPHIC.csv'), 'w') as fout:
        fout.write('\"' + '\",\"'.join(header) + "\"\n")
        for itm in tqdm(adm_info, desc="Writing DEMOGRAPHIC table", unit="rows"):
            arr = [itm[wd] for wd in header]
            fout.write('\"' + '\",\"'.join(arr) + "\"\n")
    
    # Write SQL
    print('Writing SQL...')
    data = pandas.read_csv(
        os.path.join(out_dir, 'DEMOGRAPHIC.csv'),
        dtype={'HADM_ID': str, "DOD_YEAR": float, "SUBJECT_ID": str}
    )
    data.to_sql('DEMOGRAPHIC', conn, if_exists='replace', index=False)

def build_diagnoses_table(data_dir, out_dir, conn):
    print('Processing DIAGNOSES table...')
    left = pandas.read_csv(os.path.join(data_dir, 'DIAGNOSES_ICD.csv.gz'), dtype=str)
    right = pandas.read_csv(os.path.join(data_dir, 'D_ICD_DIAGNOSES.csv.gz'), dtype=str)
    left = left.drop(columns=['ROW_ID', 'SEQ_NUM'])
    right = right.drop(columns=['ROW_ID'])
    
    out = pandas.merge(left, right, on='ICD9_CODE')
    out = out.sort_values(by='HADM_ID')

    # Write the table
    print('Writing DIAGNOSES table...')
    out.to_csv(os.path.join(out_dir, 'DIAGNOSES.csv'), sep=',', index=False)
    
    # Write SQL
    print('Writing SQL...')
    out.to_sql('DIAGNOSES', conn, if_exists='replace', index=False)

def build_procedures_table(data_dir, out_dir, conn):
    print('Processing PROCEDURES table...')
    left = pandas.read_csv(os.path.join(data_dir, 'PROCEDURES_ICD.csv.gz'), dtype=str)
    right = pandas.read_csv(os.path.join(data_dir, 'D_ICD_PROCEDURES.csv.gz'), dtype=str)
    left = left.drop(columns=['ROW_ID', 'SEQ_NUM'])
    right = right.drop(columns=['ROW_ID'])
    
    out = pandas.merge(left, right, on='ICD9_CODE')
    out = out.sort_values(by='HADM_ID')

    # Write the table
    print('Writing PROCEDURES table...')
    out.to_csv(os.path.join(out_dir, 'PROCEDURES.csv'), sep=',', index=False)
    
    # Write SQL
    print('Writing SQL...')
    out.to_sql('PROCEDURES', conn, if_exists='replace', index=False)

def build_prescriptions_table(data_dir, out_dir, conn):
    print('Processing PRESCRIPTIONS table...')
    data = pandas.read_csv(os.path.join(data_dir, 'PRESCRIPTIONS.csv.gz'), dtype=str)
    data = data.drop(columns=['ROW_ID', 'GSN', 'DRUG_NAME_POE', 'DRUG_NAME_GENERIC', 'NDC', 'PROD_STRENGTH',
                              'FORM_VAL_DISP', 'FORM_UNIT_DISP', 'STARTDATE', 'ENDDATE'])
    data = data.dropna(subset=['DOSE_VAL_RX', 'DOSE_UNIT_RX'])
    data['DRUG_DOSE'] = data[['DOSE_VAL_RX', 'DOSE_UNIT_RX']].apply(lambda x: ''.join(x), axis=1)
    data = data.drop(columns=['DOSE_VAL_RX', 'DOSE_UNIT_RX'])

    # Write the table
    print('Writing PRESCRIPTIONS table...')
    data.to_csv(os.path.join(out_dir, 'PRESCRIPTIONS.csv'), sep=',', index=False)
    
    # Write SQL
    print('Writing SQL...')
    data.to_sql('PRESCRIPTIONS', conn, if_exists='replace', index=False)

def build_lab_table(data_dir, out_dir, conn):
    print('Processing LAB table...')
    cnt = 0
    left = pandas.read_csv(os.path.join(data_dir, 'LABEVENTS.csv.gz'), dtype=str)
    cnt += 1
    right = pandas.read_csv(os.path.join(data_dir, 'D_LABITEMS.csv.gz'), dtype=str)
    cnt += 1
    left = left.dropna(subset=['HADM_ID', 'VALUE', 'VALUEUOM'])
    left = left.drop(columns=['ROW_ID', 'VALUENUM'])
    left['VALUE_UNIT'] = left[['VALUE', 'VALUEUOM']].apply(lambda x: ''.join(x), axis=1)
    left = left.drop(columns=['VALUE', 'VALUEUOM'])
    right = right.drop(columns=['ROW_ID', 'LOINC_CODE'])
    out = pandas.merge(left, right, on='ITEMID')

    # Write the table
    print('Writing LAB table...')
    out.to_csv(os.path.join(out_dir, 'LAB.csv'), sep=',', index=False)
    
    # Write SQL
    print('Writing SQL...')
    out.to_sql('LAB', conn, if_exists='replace', index=False)
