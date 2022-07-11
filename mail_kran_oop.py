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
FILTER_MORE = 40
ServerName = "192.168.99.106"
Database = "nmtport"
UserPwd = "ubuntu:Port2020"
Driver = "driver=ODBC Driver 17 for SQL Server"


class Period(NamedTuple):
    begin: datetime
    stop: datetime


class PostKran(NamedTuple):
    timestamp: datetime
    value: int


diapozonesType = Literal[
                         "WORK_1",
                         "WORK_2",
                         "WORK_3",
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
        self.diapozones = self._get_diapozones()
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

    def _get_diapozones(self) -> Dict[diapozonesType, Period]:
        next_day = self.date_shift + timedelta(days=1)
        date_shift = str(self.date_shift) + " "
        next_day = str(next_day) + " "
        format = '%Y-%m-%d %H:%M'
        if self.shift == 1:
            diapozones = {
                'WORK_1':       [date_shift + '08:00', date_shift + '12:30'],
                'WORK_2':       [date_shift + '12:31', date_shift + '16:45'],
                'WORK_3':       [date_shift + '16:46', date_shift + '20:00'],
            }
        elif self.shift == 2:
            diapozones = {
                'WORK_1':       [date_shift + '20:00',  next_day + '01:30'],
                'WORK_2':       [next_day + '01:31',    next_day + '04:45'],
                'WORK_3':       [next_day + '04:45',    next_day + '08:00'],
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

    def _get_delta_ideal_minutes(self, side_time_periods: List[datetime | None]):
        permited_deviation_minutes = [20, 30, 30, 15, 15, 20]
        list_delta_minutes: List[float | None] = [
            self.get_delta_minutes(side_time_periods[0], self.work_periods[0].begin),
            self.get_delta_minutes(self.work_periods[0].stop, side_time_periods[1]),
            self.get_delta_minutes(side_time_periods[2], self.work_periods[1].begin),
            self.get_delta_minutes(self.work_periods[1].stop, side_time_periods[3]),
            self.get_delta_minutes(side_time_periods[4], self.work_periods[2].begin),
            self.get_delta_minutes(self.work_periods[2].stop, side_time_periods[5])
        ]
        print(list_delta_minutes)
        result = []
        for i in range(6):
            result.append(self.get_delta_minutes(list_delta_minutes[i],permited_deviation_minutes[i]))
        return result

    def get_delta_minutes(self, a, b):
        if a is None or b is None:
            return None
        if isinstance(a, datetime)\
            and isinstance(b, datetime):
            return int((a - b).total_seconds()/60)
        if isinstance(a, int)\
            and isinstance(b, int):
            return a-b
        assert "ERR"
        return 0

    def _filter_if_more(self, items, border):
        new_items = []
        for i in items:
            if i is None:
                new_items.append(None)
            elif i > border:
                new_items.append(0)
            else:
                new_items.append(i)
        return new_items


class Kran(Mechanism):
    itemsKran = Dict[datetime, int]
    data_period: itemsKran = {}
    split_periods: Dict[Period, List[PostKran]] = {}
    sum_split_periods: Dict[Period, int] = {}
    side_time_periods: List[datetime | None] = []
    TOTAL_PERIOD = 20
    delta_ideal_minutes: List[int | None] = []


    def __init__(self, mech_id, date, shift):
        super().__init__(mech_id, date, shift)
        self.data_period = self._convert_cursor_to_kran()
        self.split_periods = self._split_by_periods(self.data_period)
        self.side_time_periods=self._get_all_side_time_periods()
        self.delta_ideal_minutes = self._get_delta_ideal_minutes(self.side_time_periods) 
        # self.delta_ideal_minutes = self._filter_if_more(self.delta_ideal_minutes, FILTER_MORE)
        print(self.delta_ideal_minutes)

    def _get_all_side_time_periods(self):
        result = []
        for i in self.split_periods.values():
            tmp = self._get_side_time_periods(i)
            result.append(tmp[0])
            result.append(tmp[1])
        return result


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
    date_shift: date = datetime.now().date() - timedelta(days=4)
    shift: int = 2
    num = 13
    print(date_shift, f"{shift=} {num=}")
    print("_________________________")
    k = Kran(kran[num], date_shift, shift)
