__author__ = "Hitesh Gulati"

import requests
import zipfile
import os
import shutil
from datetime import datetime, timedelta, date
import time
import pandas as pd
from hiteshutils import basicutils
from hiteshutils.fileutils import converters
from hiteshutils.finutils.option import Option


def run():
    print("Start")
    t_begin = time.time()
    heads_to_be_fetched = {'Equity Derivatives': ['Market Activity Report', 'Daily Volatility files',
                                                  'Daily Settlement Price files','NSE Open Intrest'],
                           'Currency Derivatives': ['Daily Settlement Prices', 'Daily Volatility',
                                                    'Daily Bhavcopy'],
                           'IR Futures': ['Daily Settlement Prices', 'Daily Volatility', 'Daily Bhavcopy',
                                          'Exchange Level Overall Position Limit']}
    # heads_to_be_fetched = {'Currency Derivatives': ['Daily Bhavcopy'],
    #                        'IR Futures': ['Daily Bhavcopy','Exchange Level Overall Position Limit']}
    file_directory = os.path.join(os.getcwd(), "NSE_JAN17", "Equity Derivatives")
    # file_directory = os.path.join("/Users/hiteshg/code", "NSE")
    # fetch_from_exchange(file_directory, heads_to_be_fetched, start_date=date(2017,1,1), end_date=date(2017,1,15))
    # print("Fetch Complete")
    # clean_bad_files(file_directory, runs=2)
    # print("Clean Complete")
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
    url = generate_url(url_date=date(2014,9,9), head= 'IR Futures', report= 'Daily Bhavcopy')
    print(url)
    pass


def make_options_file(base_address, start_date=date.today(), end_date=date.today()):
    options_headers_key = {'Date': 'Date of the record',
                           'Symbol_Underlying': 'NSE Symbol of the Underlying',
                           'Expiry': 'Expiry date of the option',
                           'Strike': 'Strike price of the option',
                           'Option_Type': 'Call or Put. CE: Call European, PE: Put European ',
                           'Open': 'Open price of the option',
                           'High': 'High price of the option',
                           'Low': 'Low price of the option',
                           'Close': 'Close  price of the option',
                           'Open_Interest': 'Open Interest in the option',
                           'Contracts_Traded': 'No of contracts traded for the day',
                           'Lot_Size': 'Lot size of the option',
                           'Spot': 'Spot close price of the underlying',
                           'Futures_Near': 'Close price of nearest expiring futures',
                           'Futures_Expiry': 'Close price of futures of same expiry date',
                           'Volatility_Underlying': 'Annual Volatility of the underlying',
                           'Volatility_Futures': 'Annual volatility of near futures',
                           'Volatility_Applicable': 'Annual volatility max of underlying or near futures',
                           'Moneyness': 'Call: Stop - Strike, Put: Strike - Spot',
                           'Option_Premium': 'Price - Moneyness. Premium paid for option.',
                           'Days_To_Expiry': 'Number of days to expiry. Excluding today.',
                           'Delta': 'Greeks Delta',
                           'Gamma': 'Greeks Gamma',
                           'Vega': 'Greeks Vega',
                           'Theta': 'Greeks Theta',
                           'Rho': 'Greeks Rho',
                           'Implied_Volatility': 'Implied Volatility by Black Scholes i=10%'}
    options_file = pd.DataFrame(columns=options_headers_key.keys())
    for working_date in basicutils.daterange(start_date=start_date,end_date=end_date,inclusive=True):
        print(working_date)
        try:
            base_file = pd.read_csv(
                get_file_address(base_address=base_address, file_type="base_file",
                                 file_date=working_date))
            base_file = base_file.rename(columns={x: str.strip(x) for x in list(base_file)})
            volatility_file = pd.read_csv(
                get_file_address(base_address=base_address, file_type="volatility_file",
                                 file_date=working_date))
            volatility_file = volatility_file.rename(columns={x: str.strip(x)
                                                              for x in list(volatility_file)})
            settle_file = pd.read_csv(
                get_file_address(base_address=base_address, file_type="settle_file",
                                 file_date=working_date))
            settle_file = settle_file.rename(columns={x: str.strip(x) for x in list(settle_file)})
            for (index, record) in base_file.iterrows():
                entry = dict.fromkeys(options_headers_key.keys())
                entry['Date'] = working_date
                entry['Symbol_Underlying'] = record['SYMBOL']
                entry['Expiry'] = record['EXP_DATE']
                entry['Strike'] = record['STR_PRICE']
                entry['Option_Type'] = record['OPT_TYPE']
                option_entry = Option(strike=entry['Strike'], expiry_date=entry['Expiry'],
                                      type=entry['Option_Type'],
                                      underlying=entry['Symbol_Underlying'])
                entry['Open'] = record['OPEN_PRICE']
                entry['High'] = record['HI_PRICE']
                entry['Low'] = record['LO_PRICE']
                entry['Close'] = record['CLOSE_PRICE']
                entry['Open_Interest'] = record['OPEN_INT*']
                entry['Contracts_Traded'] = record['NO_OF_CONT']
                entry['Lot_Size'] = record['TRD_QTY'] / record['NO_OF_CONT']
                entry['Spot'] = volatility_file['Underlying Close Price (A)'][
                    volatility_file['Symbol']==entry['Symbol_Underlying']].iloc[0]
                entry['Futures_Near'] = volatility_file['Futures Close Price (G)'][
                    volatility_file['Symbol'] == entry['Symbol_Underlying']]
                entry['Futures_Expiry'] = settle_file['MTM SETTLEMENT'][
                    (settle_file['UNDERLYING']==entry['Symbol_Underlying']) &
                    (settle_file['EXPIRY DATE'] == entry['Expiry'].strftime('%d-%b-%Y').upper())]\
                    .iloc[0]
                entry['Volatility_Underlying'] = \
                    volatility_file['Underlying Annualised Volatility (F) = E*sqrt(365)'][
                    volatility_file['Symbol']==entry['Symbol_Underlying']].iloc[0]
                entry['Volatility_Futures'] = \
                    volatility_file['Futures Annualised Volatility (L) = K*sqrt(365)'][
                    volatility_file['Symbol'] == entry['Symbol_Underlying']].iloc[0]
                entry['Volatility_Applicable'] = \
                    volatility_file['Applicable Annualised Volatility (N) = Max (F or L)'][
                        volatility_file['Symbol'] == entry['Symbol_Underlying']].iloc[0]
                entry['Moneyness'] = option_entry.moneyness(spot=entry['Spot'])
                entry['Option_Premium'] = option_entry.premium(spot=entry['Spot'],
                                                               price=entry['Close'])
                entry['Days_To_Expiry'] = option_entry.days_to_expiry(from_date=entry['Date'])
                entry['Delta'] = option_entry.greeks(greek='delta', spot=entry['Spot'],
                                                     actual_price=entry['Close'],
                                                     on_date=entry['Date'])
                entry['Gamma'] = option_entry.greeks(greek='gamma', spot=entry['Spot'],
                                                     actual_price=entry['Close'],
                                                     on_date=entry['Date'])
                entry['Vega'] = option_entry.greeks(greek='vega', spot=entry['Spot'],
                                                     actual_price=entry['Close'],
                                                     on_date=entry['Date'])
                entry['Theta'] = option_entry.greeks(greek='theta', spot=entry['Spot'],
                                                     actual_price=entry['Close'],
                                                     on_date=entry['Date'])
                entry['Rho'] = option_entry.greeks(greek='Rho', spot=entry['Spot'],
                                                     actual_price=entry['Close'],
                                                     on_date=entry['Date'])
                entry['Implied_Volatility'] = option_entry.implied_volatility(
                    spot=entry['Spot'], actual_price=entry['Close'], on_date=entry['Date'])





        except FileNotFoundError:
            print("No record for: ", working_date)
        pass
    pass


def get_file_address(base_address, category="options", file_type="base_file", file_date=date.today()):
    file_address = base_address
    if category == "options":
        if file_type == "base_file":
            file_address = os.path.join(base_address, 'Market Activity Report',
                                        ("op" + file_date.strftime('%d%m%Y') + ".csv"))
        elif file_type == "volatility_file":
            file_address = os.path.join(base_address, 'Daily Volatility files',
                                        (file_date.strftime('%Y_%m_%d') + ".csv"))
        elif file_type == "settle_file":
            file_address = os.path.join(base_address, 'Daily Settlement Price files',
                                        (file_date.strftime('%Y_%m_%d') + ".csv"))
    return file_address


def clean_bad_files(folder_path, runs=1):
    for item in os.listdir(folder_path):
        file_path = os.path.join(folder_path, item)
        print(file_path[31:])
        if item[-4:] == ".csv":
            print("CSV file")
            csv_file = pd.read_csv(file_path)
            bad_file_string = \
                '<HEAD><META HTTP-EQUIV="Content-Type" CONTENT="text/html;charset=ISO-8859-1"><TITLE>Not Found</TITLE></HEAD>'
            if bad_file_string in csv_file.columns.values:
                print("Bad CSV file.")
                os.remove(file_path)
            del csv_file
        elif item[-4:] == ".zip":
            print("ZIP file")
            try:
                zip_file = zipfile.ZipFile(file_path,'r')
                zip_file.extractall(folder_path)
                zip_file.close()
            except zipfile.BadZipFile:
                print("Bad ZIP file")
                pass
            os.remove(file_path)
        elif item[-4:] == ".dbf":
            converters.dbf2csv(file_path)
            os.remove(file_path)
        elif item[-4:-3] == ".":
            print(item[-3:].upper(), " file.")
            os.remove(file_path)
        elif (item[:1]).isalpha():
            print("Folder")
            clean_bad_files(file_path)
        else:
            print("Bad File.")
            os.remove(file_path)
    runs -= 1
    if runs > 0:
        clean_bad_files(folder_path, runs)


def fetch_from_exchange(file_directory, heads_to_be_fetched, start_date = date.today(), end_date  = date.today()):
    for head in heads_to_be_fetched.keys():
        print("Starting: ", head)
        for report in heads_to_be_fetched[head]:
            print("Starting: ", report)
            save_in_directory = os.path.join(file_directory, head, report)
            for fetch_date in basicutils.daterange(start_date, end_date, inclusive=True):
                print(head,"_",report,"_",fetch_date.strftime('%d_%m_%Y'))
                file_path = os.path.join(save_in_directory, fetch_date.strftime('%Y_%m_%d') +
                                         get_report_extension(head,report,fetch_date))
                url = generate_url(fetch_date, head, report)
                basicutils.file_download(file_path=file_path, file_url=url)


def generate_url(url_date, head, report):
    pre = "https://www.nseindia.com/archives/"
    if head == "Equity Derivatives":
        if report == "Market Activity Report":
            report_fix = "fo/mkt/fo"
            datevar = url_date.strftime('%d%m%Y')
        elif report == "Daily Volatility files":
            report_fix = "nsccl/volt/FOVOLT_"
            datevar = url_date.strftime('%d%m%Y')
        elif report == "Daily Settlement Price files":
            report_fix = "nsccl/sett/FOSett_prce_"
            datevar = url_date.strftime('%d%m%Y')
        elif report == "NSE Open Intrest":
            report_fix = "nsccl/mwpl/nseoi_"
            datevar = url_date.strftime('%d%m%Y')
        else:
            print ("Error with: ", head, "_", report)
    elif head == "Currency Derivatives":
        if report == "Daily Settlement Prices":
            report_fix = "cd/sett/CDSett_prce_"
            datevar = url_date.strftime('%d%m%Y')
        elif report == "Daily Volatility":
            report_fix = "cd/volt/X_VOLT_"
            datevar = url_date.strftime('%d%m%Y')
        elif report == "Daily Bhavcopy":
            report_fix = "cd/bhav/CD_Bhavcopy"
            datevar = url_date.strftime('%d%m%y')
        else:
            print("problem with: ",head,"_",report)
    elif head == "IR Futures":
        if report == "Daily Settlement Prices":
            report_fix = "ird/sett/CDSett_prce_IRF_"
            datevar = url_date.strftime('%d%m%Y')
        elif report == "Daily Volatility":
            report_fix = "ird/volt/I_VOLT_"
            datevar = url_date.strftime('%d%m%Y')
        elif report == "Daily Bhavcopy":
            report_fix = "ird/bhav/IRF_NSE"
            datevar = url_date.strftime('%d%m%y')
        elif report == "Exchange Level Overall Position Limit":
            report_fix = "ird/ewpl/EWPL_"
            datevar = url_date.strftime('%d%m%Y')
        else:
            print("Error with: ",head,"_",report)
    else:
        print("Error with HEAD: ",head)

    url = pre + report_fix + datevar + get_report_extension(head,report,url_date)
    return url


def get_report_extension(head,report, report_date = date.today()):
    zip = ".zip"
    csv = ".csv"
    if head == "Equity Derivatives":
        if report == "Market Activity Report":
            extension = zip
        elif report == "Daily Volatility files":
            extension = csv
        elif report == "Daily Settlement Price files":
            extension = csv
        elif report == "NSE Open Intrest":
            extension = zip
        else:
            print ("Error in extension with: ", head, "_", report)
    elif head == "Currency Derivatives":
        if report == "Daily Settlement Prices":
            extension = csv
        elif report == "Daily Volatility":
            extension = csv
        elif report == "Daily Bhavcopy":
            extension = zip
        else:
            print("Error in extension with: ",head,"_",report)
    elif head == "IR Futures":
        if report == "Daily Settlement Prices":
            extension = csv
        elif report == "Daily Volatility":
            extension = csv
        elif report == "Daily Bhavcopy":
            if report_date <= date(2016,2,15):
                extension = ".dbf.zip"
            else:
                extension = csv
        elif report == "Exchange Level Overall Position Limit":
            extension = csv
        else:
            print("Error in extension with: ",head,"_",report)
    else:
        print("Error in extension with HEAD: ",head)
    return extension


def fetch_from_exchange_zip(excel_directory):
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


def generate_url_zip(url_date):
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