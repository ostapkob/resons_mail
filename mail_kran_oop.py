#!/usr/bin/env python
import os
from sqlalchemy import create_engine
# from middleware.list_mechanisms import kran as list_kran  # TODO request to db
from rich import print
from datetime import datetime, timedelta, date
import sys
# from dataclasses import dataclass
from typing import List, Dict, Literal, NamedTuple
# from enum import Enum
# from collections import defaultdict
sys.path.insert(0, '/home/admin/nmtport')
mail_pass = os.environ['YANDEX_MAIL']

krans_UT = [45, 34, 53, 69, 21, 37, 4, 41, 5, 36, 40, 32,
            25, 11, 33, 20, 8, 22, 12, 13, 6, 26, 47, 54, 14, 16, 82]
krans_GUT = [28, 18, 1, 35, 31, 17, 58, 60, 49, 38, 39, 23, 48, 72, 65, 10]
HOURS = 10
ServerName = "192.168.99.106"
Database = "nmtport"
UserPwd = "ubuntu:Port2020"
Driver = "driver=ODBC Driver 17 for SQL Server"


# @dataclass(frozen=True)
# class diapozonesType(Enum):
#     START = "START"
#     WORK_1 = "WORK_1"
#     LANCH_START = "LANCH_START"
#     LANCH_FINISH = "LANCH_FINISH"
#     WORK_2 = "WORK_2"
#     TEA_START = "TEA_START"
#     TEA_FINISH = "TEA_FINISH"
#     WORK_3 = "WORK_3"
#     FINISH = "FINISH"


# @dataclass(frozen=True)
# class itemKran:
#     time_item: datetime
#     value_item: int


class Period(NamedTuple):
    begin: datetime
    stop: datetime


class PostKran(NamedTuple):
    timestamp: datetime
    value: int


diapozonesType = Literal["START",
                         "WORK_1",
                         "START",
                         "LANCH_START",
                         "LANCH_FINISH",
                         "WORK_2",
                         "TEA",
                         "TEA_START",
                         "TEA_FINISH",
                         "WORK_3",
                         "FINISH"
                         ]


class Mechanism:
    cursor: Dict[datetime, List[float]] = {}
    diapozones: Dict[diapozonesType, Period] = {}

    def __init__(self, mech_id: int, date_shift: date, shift: int):
        assert date_shift <= datetime.now().date()
        self.mech_id = mech_id
        self.date_shift = date_shift
        self.shift = shift
        self.cursor = self._get_cursor()
        self.diapozones = self._get_yellow_diapozones()
        # print(self.diapozones['START'].begin, self.diapozones['WORK_1'].stop)
        self.work_periods = [
            self.diapozones['WORK_1'],
            self.diapozones['WORK_2'],
            self.diapozones['WORK_3'],
        ]

    def _get_cursor(self):
        "in kran need only value in USM need value is lever, value3 is roll"
        engine = create_engine('mssql+pyodbc://' + UserPwd + '@' +
                               ServerName + '/' + Database + "?" + Driver)
        sql = """
        SELECT TOP (1000)
             dateadd(hour, """ + str(HOURS) + """, [timestamp]) as time
            ,[value]
            ,[value3]
        FROM [nmtport].[dbo].[post]
        where
        mechanism_id=""" + str(self.mech_id) + """ and
        date_shift='""" + str(self.date_shift) + """' and
        shift=""" + str(self.shift) + """
        order by timestamp  """

        tmp_cursor = {}
        with engine.connect() as con:
            rs = con.execute(sql)
            for row in rs:
                # row[0] -> datetime, row[1] -> value, row[2] -> value3
                tmp_cursor[row[0]] = [row[1], row[2]]
        return tmp_cursor

    def _get_yellow_diapozones(self) -> Dict[diapozonesType, Period]:
        next_day = self.date_shift + timedelta(days=1)
        date_shift = str(self.date_shift) + " "
        next_day = str(next_day) + " "
        format = '%Y-%m-%d %H:%M'
        if self.shift == 1:
            # diapozones = {
            #     'START':        [date_shift + '08:00', date_shift + '08:20'],
            #     'WORK_1':       [date_shift + '08:20', date_shift + '12:00'],
            #     'LANCH':  [date_shift + '12:00', date_shift + '13:00'],
            #     # 'LANCH_START':  [date_shift + '11:59', date_shift + '12:00'],
            #     # 'LANCH_FINISH': [date_shift + '13:00', date_shift + '13:01'],
            #     'WORK_2':       [date_shift + '13:00', date_shift + '16:30'],
            #     'TEA':    [date_shift + '16:30', date_shift + '17:00'],
            #     # 'TEA_START':    [date_shift + '16:29', date_shift + '16:30'],
            #     # 'TEA_FINISH':   [date_shift + '17:00', date_shift + '17:01'],
            #     'WORK_3':       [date_shift + '17:00', date_shift + '19:40'],
            #     'FINISH':       [date_shift + '19:40', date_shift + '20:00'],
            # }
            diapozones = {
                'WORK_1':       [date_shift + '08:00', date_shift + '12:30'],
                'WORK_2':       [date_shift + '12:31', date_shift + '16:45'],
                'WORK_3':       [date_shift + '16:46', date_shift + '20:00'],
            }
        elif self.shift == 2:
            diapozones = {
                'START':        [date_shift + '20:00',  date_shift + '20:20'],
                'WORK_1':       [date_shift + '20:20',  next_day + '00:59'],
                'LANCH_START':  [next_day + '00:59',    next_day + '01:00'],
                'LANCH_FINISH': [next_day + '02:00',    next_day + '02:01'],
                'WORK_2':       [next_day + '02:01',    next_day + '04:29'],
                'TEA_START':    [next_day + '04:29',    next_day + '04:30'],
                'TEA_FINISH':   [next_day + '05:00',    next_day + '05:01'],
                'WORK_3':       [next_day + '05:01',    next_day + '07:40'],
                'FINISH':       [next_day + '07:40',    next_day + '08:00'],
            }
        else:
            raise AttributeError
        formated_diapozones = {}
        for k, v in diapozones.items():
            formated_diapozones[k] = Period(
                datetime.strptime(v[0], format),
                datetime.strptime(v[1], format)
            )
        return formated_diapozones

    def _split_by_periods(self, data_period) -> Dict[Period, List[PostKran]]:
        split_periods: Dict[Period, List[PostKran]] = {}
        for work_period in self.work_periods:
            split_periods[work_period] = []

        for timestamp, value in data_period.items():
            for work_period in self.work_periods:
                if timestamp > work_period.begin and timestamp < work_period.stop:
                    split_periods[work_period].append(
                        PostKran(timestamp, value))
        return split_periods

    # def _get_red_zones(self) -> List[datetime]:
    #     zones = [
    #         self.diapozones['START'].stop + timedelta(minutes=10),  # >
    #         self.diapozones['LANCH_START'].begin - timedelta(minutes=5),  # <
    #         self.diapozones['LANCH_FINISH'].stop + timedelta(minutes=5),  # >
    #         self.diapozones['TEA_START'].begin - timedelta(minutes=5),  # <
    #         self.diapozones['TEA_FINISH'].stop + timedelta(minutes=5),  # >
    #         self.diapozones['FINISH'].begin - timedelta(minutes=10),  # <
    #     ]
    #     return zones

    def delta_minutes(self, side_time_periods: List[datetime | None]):
        # return 
        # self.work_periods[0].begin 
        # self.work_periods[0].stop)
        print(self.get_delta_minutes(side_time_periods[0], self.work_periods[0].begin))
        print(self.get_delta_minutes(self.work_periods[0].stop, side_time_periods[1]))
        print(self.get_delta_minutes(side_time_periods[2], self.work_periods[1].begin))
        print(self.get_delta_minutes(self.work_periods[1].stop, side_time_periods[3]))
        print(self.get_delta_minutes(side_time_periods[4], self.work_periods[2].begin))
        print(self.get_delta_minutes(self.work_periods[2].stop, side_time_periods[5]))

    def get_delta_minutes(self, a, b):
        print(a,"    ", b)
        if a is None or b is None:
            return 0
        if isinstance(a, datetime)\
            and isinstance(b, datetime):
            return (a - b).total_seconds()/60

class Kran(Mechanism):
    itemsKran = Dict[datetime, int]
    data_period: itemsKran = {}
    split_periods: Dict[Period, List[PostKran]] = {}
    sum_split_periods: Dict[Period, int] = {}
    TOTAL_PERIOD = 20


    def __init__(self, mech_id, date, shift):
        super().__init__(mech_id, date, shift)
        self.data_period = self._convert_cursor_to_kran()
        # self.red_zones = self._get_red_zones()
        self.split_periods = self._split_by_periods(self.data_period)

        side_time_periods: List[datetime | None] = []
        for i in self.split_periods.values():
            tmp = self._get_side_time_periods(i)
            side_time_periods.append(tmp[0])
            side_time_periods.append(tmp[1])


        self.delta_minutes(side_time_periods)


    def _convert_cursor_to_kran(self) -> itemsKran:
        return {k: int(v[0]) for k, v in self.cursor.items()}

    def _get_side_time_periods(self, period_values: List[PostKran]):
        if self._sum_period(period_values) > self.TOTAL_PERIOD:  # TODO if only move ?
            return [
                self._get_first_not_empty_value(period_values),
                self._get_last_not_empty_value(period_values)
            ]
        else:
            return [None, None]


    def _get_first_not_empty_value(self, period_values: List[PostKran]):
        for i in period_values:
            if i.value > 0:
                return i.timestamp
        return None


    def _get_last_not_empty_value(self, period_values: List[PostKran]):
        for i in period_values[::-1]:
            if i.value > 0:
                return i.timestamp
        return None


    def _sum_period(self, items):
        return sum([1 for i in items if i.value > 0])


if __name__ == "__main__":
    from list_mechanisms import kran
    date_shift: date = datetime.now().date() - timedelta(days=5)
    num = 47
    print(date_shift, f"{num=}")
    print("____________________")
    shift: int = 1
    k = Kran(kran[num], date_shift, shift)
