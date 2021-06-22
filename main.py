import os
import pymongo
from pprint import pprint
from numpy import mean
# from playsound import playsound
from datetime import datetime, timedelta
from dotenv import load_dotenv


load_dotenv()
USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')


client = pymongo.MongoClient(
    'mongodb+srv://{}:{}@cluster0.0e5pv.mongodb.net/air?retryWrites=true&w=majority'.format(USERNAME, PASSWORD))
change_stream = client.air.air.watch()  # real-time data changes on collection

labels = ['PM25', 'PM10']


def min2sec(m):
    return m * 60


def check_wrapper(dict_data, checking_checkertion):
    print('checking', checking_checkertion.__name__[6:])
    reached = []
    for label in labels:
        if checking_checkertion([d[label] for d in dict_data]):
            reached.append(label)
    return reached


def check_sudden_rise(data):
    avg_1min = mean(data[-min2sec(1):])
    for i in range(1, 11):  # 10 seconds
        if data[-i] < avg_1min * 1.3 and data[-i] > 3:  # increased 30%
            return False
    # print('sud {:.2f} < {}'.format(avg_1min, data[-11:-1]))
    return True


def check_continuous_rise(data):
    avg1 = mean(data[-10:])
    avg2 = mean(data[-20:-10])
    avg3 = mean(data[-30:-20])
    if avg3 * 1.15 < avg2 and avg2 * 1.15 < avg1:  # continuously increased 15% in 30 seconds
        # print('avg {:.2f} < {:.2f} < {:.2f}'.format(avg3, avg2, avg1))
        return True
    return False


def activate_warning(msg):
    print('WARNING!!', msg.upper())
    # playsound('./warning.wav', block=False)
    # you can send message here


def construct_name(warning_name, label):
    return f'{warning_name}[{label}]'


def check_timing(warnings, warning):
    if warning in warnings:
        diff = datetime.now() - warnings[warning]
        if diff < timedelta(minutes=1):
            print('filter {} {}s'.format(warning, diff.seconds))
            return False
    warnings[warning] = datetime.now()
    return True


warnings = {}
data = []
checkers = [check_sudden_rise, check_continuous_rise]

for change in change_stream:
    d = change['fullDocument']
    # pprint(d)
    data.append(d)
    # print(len(data))

    # start checking after 1 minutes so we have enough data
    if len(data) < min2sec(1):
        print('waiting for data...', len(data))
        continue
    # after 10 minutes, start popping data to avoid data size too large
    if len(data) > min2sec(10):
        data.pop(0)

    for checker in checkers:
        reached_lables = check_wrapper(data, checker)
        if not reached_lables:
            continue
        checker_name = checker.__name__[6:]
        for lable in reached_lables:
            warning = construct_name(checker_name, lable)
            if check_timing(warnings, warning):
                activate_warning(warning)
