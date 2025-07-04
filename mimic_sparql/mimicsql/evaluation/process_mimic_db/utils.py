import os
import csv
import sys
import gzip

def get_patient_name(data_dir):
    pat_id2name = {}
    file_ = os.path.join(data_dir, 'id2name.csv')
    fp = open(file_, 'r')
    for line in csv.reader(fp, delimiter=','):
        pat_id2name[line[0]] = line[1]
    fp.close()
    return pat_id2name

def read_table(data_dir, data_file):
    out_info = []
    
    file_ = os.path.join(data_dir, data_file)
    
    # Check if file is gzipped
    if file_.endswith('.gz'):
        with gzip.open(file_, 'rt') as fp:  # 'rt' is for text mode
            reader = csv.reader(fp, delimiter=',')
            header = next(reader)
            for line in reader:
                arr = {}
                for k in range(len(header)):
                    arr[header[k]] = line[k]
                out_info.append(arr)
    else:
        with open(file_, 'r') as fp:
            reader = csv.reader(fp, delimiter=',')
            header = next(reader)
            for line in reader:
                arr = {}
                for k in range(len(header)):
                    arr[header[k]] = line[k]
                out_info.append(arr)
    
    return out_info

def show_progress(a, b):
    cc = int(round(100.0*float(a)/float(b)))
    dstr = '[' + '>'*cc + ' '*(100-cc) + ']'
    sys.stdout.write(dstr + str(cc) + '%' +'\r')
    sys.stdout.flush()
    
    