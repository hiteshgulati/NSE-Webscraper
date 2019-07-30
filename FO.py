__author__ = "Hitesh Gulati"

import requests
import zipfile
import os
import shutil
from datetime import datetime, timedelta, date
import time
import pandas as pd
from hiteshutils import basicutils
from hiteshutils.driveutils import drivedb
import urllib


def run():
    print("Start")
    t_begin = time.time()
    excel_directory = os.path.join(os.getcwd(), "fofiles")
    fetch_from_exchange(excel_directory)
    print("Fetch Complete")
    # filterout_bad_rows(excel_directory)
    # print("Filter Complete")
    # merge_files(excel_directory)
    # print("Merge Complete")
    # filterout_bad_files(excel_directory)
    # print("filter complete")
    # playground()
    sample_directory = os.path.join(os.getcwd(), "samplefiles")
    t_end = time.time()
    print(t_end-t_begin)


def playground():
    idx_directory = os.path.join(os.getcwd(), "idxfiles")
    file_name = os.path.join(idx_directory,'01_01_2013.csv')
    file = pd.read_csv(file_name)
    print(file.columns.values)
    not_found_string = '<HEAD><META HTTP-EQUIV="Content-Type" CONTENT="text/html;charset=ISO-8859-1"><TITLE>Not Found</TITLE></HEAD>'
    if not_found_string in file.columns.values:
        print("Yes")
    else:
        print("No")
    pass


def fetch_from_exchange(excel_directory):
    zip_directory = os.path.join(os.getcwd(), "zipfiles")
    # excel_directory = os.path.join(os.getcwd(), "excelfiles")
    if not os.path.exists(zip_directory):
        os.makedirs(zip_directory)
    start_date = date(2012,1,1)
    end_date = date(2012,1,5)
    for fetch_date in basicutils.daterange(start_date, end_date, inclusive=True):
        print(fetch_date.strftime('%d_%m_%Y'))
        file_path = os.path.join(zip_directory, fetch_date.strftime('%d_%m_%Y') + ".zip")
        url = generate_url(fetch_date)
        r = requests.get(url, stream=True)
        with open(file_path, 'wb') as fd:
            for chunk in r.iter_content(chunk_size=256):
                fd.write(chunk)
        try:
            zip_file = zipfile.ZipFile(file_path,'r')
            zip_file.extractall(excel_directory)
            zip_file.close()
        except zipfile.BadZipFile:
            pass
    # shutil.rmtree(zip_directory)


def fetch_from_exchange_idx(file_directory, header_to_be_fetched, start_date = date(2011,1,1), end_date  = date(2017,9,12)):
    for fetch_date in basicutils.daterange(start_date, end_date, inclusive=True):
        print(fetch_date.strftime('%d_%m_%Y'))
        file_path = os.path.join(file_directory, fetch_date.strftime('%d_%m_%Y') + ".csv")
        url = generate_url_idx(fetch_date)
        r = requests.get(url, stream=True)
        with open(file_path, 'wb') as fd:
            for chunk in r.iter_content(chunk_size=256):
                fd.write(chunk)


def generate_url_idx(url_date):
    url_pre = "https://www.nseindia.com/content/indices/ind_close_all_"
    url_post = ".csv"
    url = url_pre + url_date.strftime('%d%m%Y') + url_post
    return url


def generate_url(url_date):
    url_pre = "https://www.nseindia.com/content/historical/DERIVATIVES"
    url_post = "bhav.csv.zip"
    month_lookup = {1: 'JAN', 2: 'FEB', 3: 'MAR', 4: 'APR', 5: 'MAY', 6: 'JUN',
                    7: 'JUL', 8: 'AUG', 9: 'SEP', 10: 'OCT', 11: 'NOV', 12:'DEC'}
    url_year = str(url_date.year)
    url_month = month_lookup[url_date.month]
    url_day = 0
    if url_date.day <= 9:
        day_lookup = {1: '01', 2: '02', 3:'03', 4:'04', 5:'05', 6: '06', 7:'07',
                      8: '08', 9:'09'}
        url_day = day_lookup[url_date.day]
    else:
        url_day = str(url_date.day)
    url = url_pre + "/" + url_year + "/" + url_month + "/" + "fo" + url_day + url_month + \
          url_year + url_post
    return url


def filterout_bad_rows(directory_path):
    list_of_files = os.listdir(directory_path)
    for i in range(len(list_of_files)):
        list_of_files[i] = os.path.join(directory_path,list_of_files[i])
    for file_name in list_of_files:
        print(file_name)
        if file_name[-3:]=="csv":
            file = pd.read_csv(file_name)
            high_file = file[file['CONTRACTS']>0]
            relevent_file = high_file[high_file['INSTRUMENT'].isin (['FUTIDX', 'OPTIDX'])]
            # relevent_file = relevent_file.drop('Unnamed: 15', axis=1)
            relevent_file.to_csv(file_name,index=False)
        else:
            os.remove(file_name)
            print("removed file: ", file_name)
    print(list_of_files)


def filterout_bad_files(directory_path):
    list_of_files = os.listdir(directory_path)
    not_found_string = '<HEAD><META HTTP-EQUIV="Content-Type" CONTENT="text/html;charset=ISO-8859-1"><TITLE>Not Found</TITLE></HEAD>'
    for i in range(len(list_of_files)):
        list_of_files[i] = os.path.join(directory_path,list_of_files[i])
    for file_name in list_of_files:
        print(file_name)
        if file_name[-3:]=="csv":
            file = pd.read_csv(file_name)
            if not_found_string in file.columns.values:
                os.remove(file_name)
                print("removed bad file: ", file_name)
        else:
            os.remove(file_name)
            print("removed file: ", file_name)
    print(list_of_files)


def merge_files(directory_path, base_file_name="Default", delete_original=False):
    if base_file_name == "Default":
        base_file_name = directory_path[directory_path.rfind('/')+1:] + ".csv"
    list_of_files = os.listdir(directory_path)
    for i in range(len(list_of_files)):
        list_of_files[i] = os.path.join(directory_path,list_of_files[i])
        print(list_of_files[i])
    print(list_of_files)
    merged_file = pd.concat([pd.read_csv(f) for f in list_of_files])
    print("Concat finished")
    print(list(merged_file))
    merged_file.to_csv(base_file_name, index=False)
    print("made csv file")
    # merged_file.to_excel("excelfiles.xlsx", index=False)

    # first file
    # print(list_of_files[0])
    # for line in open(os.path.join(directory_path,list_of_files[0])):
    #     merged_file.write(line)
    # # now the rest files
    # for file_name in list_of_files[1:5]:
    #     print(file_name)
    #     file = open(os.path.join(directory_path,file_name))
    #     file.next()
    #     for line in file:
    #         merged_file.write(line)
    # merged_file.close()
    # if delete_original == True:
    #     shutil.rmtree(directory_path)








if __name__ == '__main__':
    run()