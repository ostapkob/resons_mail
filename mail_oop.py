#!/usr/bin/env python
import os
from sqlalchemy import create_engine
# from middleware.list_mechanisms import kran as list_kran  # TODO request to db
from rich import print
from datetime import datetime, timedelta, date
import sys
from typing import List, Dict,  NamedTuple
from enum import Enum
sys.path.insert(0, '/home/admin/nmtport')
# from dataclasses import dataclass
# from collections import defaultdict
# mail_pass = os.environ['YANDEX_MAIL']

krans_UT = [45, 34, 53, 69, 21, 37, 4, 41, 5, 36, 40, 32,
            25, 11, 33, 20, 8, 22, 12, 13, 6, 26, 47, 54, 14, 16, 82]
krans_GUT = [28, 18, 1, 35, 31, 17, 58, 60, 49, 38, 39, 23, 48, 72, 65, 10]
HOURS = 10
FILTER_MINUTS_MORE = 40
ServerName = "192.168.99.106"
Database = "nmtport"
UserPwd = "ubuntu:Port2020"
Driver = "driver=ODBC Driver 17 for SQL Server"


class Period(NamedTuple):
    begin: datetime
    stop: datetime


class Post(NamedTuple):
    timestamp: datetime
    value: int


class Color(Enum):
    red = "red"
    yellow = "yellow"
    black = "black"
    blue = "blue"
    orange = "orange"


itemsMech = List[Post]


class Mechanism:
    cursor: Dict[datetime, List[float]] = {}
    data_period: itemsMech = []
    split_periods: Dict[Period, itemsMech] = {}
    side_time_periods: List[datetime | None] = []
    delta_allowable_range: List[int | None] = []
    colors_periods = []
    TOTAL_PERIOD = 20  # if work more than
    work_periods = []
    resons: List[int | None] = []

    def __init__(self, mech_id: int, date_shift: date, shift: int):
        assert date_shift <= datetime.now().date()
        self.mech_id = mech_id
        self.date_shift = date_shift
        self.shift = shift
        self.cursor = self._get_cursor()
        self.cursor_resons = self._get_resons_from_db()
        self.time_lanch = self._get_time_lanch()
        self.time_tea = self._get_time_tea()
        self.time_shift = self._get_time_shift()

    def _get_cursor(self) -> Dict[datetime, List[float]]:
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

    def _get_time_lanch(self) -> Period:
        next_day = self.date_shift + timedelta(days=1)
        date_shift = str(self.date_shift) + " "
        next_day = str(next_day) + " "
        format = '%Y-%m-%d %H:%M'
        if self.shift == 1:
            v = [date_shift + '12:00', date_shift + '13:00']
        elif self.shift == 2:
            v = [next_day + '01:00',    next_day + '02:00']
        else:
            raise AttributeError
        return Period(datetime.strptime(v[0], format),
                      datetime.strptime(v[1], format))

    def _get_time_tea(self) -> Period:
        next_day = self.date_shift + timedelta(days=1)
        date_shift = str(self.date_shift) + " "
        next_day = str(next_day) + " "
        format = '%Y-%m-%d %H:%M'
        if self.shift == 1:
            v = [date_shift + '16:30', date_shift + '17:00']
        elif self.shift == 2:
            v = [next_day + '04:30',    next_day + '05:00']
        else:
            raise AttributeError
        return Period(datetime.strptime(v[0], format),
                      datetime.strptime(v[1], format))

    def _get_time_shift(self) -> Period:
        next_day = self.date_shift + timedelta(days=1)
        date_shift = str(self.date_shift) + " "
        next_day = str(next_day) + " "
        format = '%Y-%m-%d %H:%M'
        if self.shift == 1:
            v = [date_shift + '08:00', date_shift + '20:00']
        elif self.shift == 2:
            v = [date_shift + '20:00',  next_day + '08:00']
        else:
            raise AttributeError
        return Period(datetime.strptime(v[0], format),
                      datetime.strptime(v[1], format))

    def _get_delta_minutes(self, a, b) -> None | int:
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

    def _find_max_empty_period(self, all_period, break_period: Period) -> Period:
        "return period from start_shift from atrt to stop"
        dt = self._get_delta_minutes(break_period.stop, break_period.begin)
        if dt is None:
            return break_period
        dt /= 2
        begin = break_period.begin + timedelta(minutes=dt)
        stop = break_period.stop - timedelta(minutes=dt)
        break_begin = break_period.begin  # - timedelta(minutes=5)
        break_stop = break_period.stop + timedelta(minutes=5)
        max_period = 15
        my_period = [x for x in all_period if x.timestamp >
                     break_begin and x.timestamp < break_stop]
        if len(my_period) < 2:
            return break_period
        tmp = my_period[0]
        for i in my_period[0:]:
            period = self._get_delta_float_minutes(i.timestamp, tmp.timestamp)
            if period > max_period:
                max_period = period
                begin = tmp.timestamp
                stop = i.timestamp
            if i.value > 0:
                tmp = i
        return Period(begin, stop)

    def _get_delta_float_minutes(self, a: datetime, b: datetime) -> float:
        if isinstance(a, datetime)\
                and isinstance(b, datetime):
            return (a - b).total_seconds()/60
        assert "types not datetime"
        return 0.0

    def _filter_if_more(self, items: List[int | None], border: int) -> List[int | None]:
        new_items = []
        for i in items:
            if i is None:
                new_items.append(None)
            elif i > border:
                new_items.append(0)
            else:
                new_items.append(i)
        return new_items

    def _convert_to_allowable_range(self, delta_minutes: List[int | None]) -> List[int | None]:
        allowable_range = [20, 0, 0, 0, 0, 20]
        result = []
        for i in range(6):
            result.append(self._get_delta_minutes(
                delta_minutes[i], allowable_range[i]))
        return result

    def _total_minuts_work(self, data_period) -> int:
        "if brek more 15 minutes then count how not work"
        BREAK = 15
        total_work = 0
        work_values = [x for x in data_period if x.value > 0]
        if len(work_values) < 2:
            return 0
        tmp = work_values[0]
        for i in work_values[0:]:
            dt = self._get_delta_float_minutes(i.timestamp, tmp.timestamp)
            if dt < BREAK:
                total_work += dt
            tmp = i
        return int(total_work)

    def _get_delta_allowable_range(self, side_time_periods: List[datetime | None]) -> List[int | None]:
        list_delta_minutes: List[int | None] = [
            self._get_delta_minutes(
                side_time_periods[0], self.time_shift.begin),
            self._get_delta_minutes(
                self.time_lanch.begin, side_time_periods[1]),
            self._get_delta_minutes(
                side_time_periods[2], self.time_lanch.stop),
            self._get_delta_minutes(self.time_tea.begin, side_time_periods[3]),
            self._get_delta_minutes(side_time_periods[4], self.time_tea.stop),
            self._get_delta_minutes(self.time_shift.stop, side_time_periods[5])
        ]
        return list_delta_minutes

    def _get_all_side_time_periods(self, periods) -> List[datetime | None]:
        result = []
        for i in periods.values():
            tmp = self._get_side_time_periods(i)
            result.append(tmp[0])
            result.append(tmp[1])
        return result

    def _get_side_time_periods(self, period_values: List[Post]) -> List[datetime | None]:
        if self._sum_period(period_values) > self.TOTAL_PERIOD:  # TODO if only move ?
            return [
                self._get_first_not_empty_value(period_values),
                self._get_last_not_empty_value(period_values)
            ]
        else:
            return [None, None]

    def _get_first_not_empty_value(self, period_values: List[Post]) -> datetime | None:
        for i in period_values:
            if i.value > 0:
                return i.timestamp
        return None

    def _get_last_not_empty_value(self, period_values: List[Post]) -> datetime | None:
        for i in period_values[::-1]:
            if i.value > 0:
                return i.timestamp
        return None

    def _sum_period(self, period_values: List[Post]) -> int:
        return sum([1 for i in period_values if i.value > 0])

    def _split_by_periods(self, data_period, work_periods) -> Dict[Period, List[Post]]:
        split_periods: Dict[Period, List[Post]] = {}
        for work_period in work_periods:
            split_periods[work_period] = []

        for timestamp, value in data_period:
            for work_period in work_periods:
                if timestamp > work_period.begin and timestamp < work_period.stop:
                    split_periods[work_period].append(
                        Post(timestamp, value))
        return split_periods

    def _get_resons_from_db(self) -> List[List]:
        engine = create_engine('mssql+pyodbc://' + UserPwd + '@' +
                               ServerName + '/' + Database + "?" + Driver)
        sql = """
        SELECT TOP (1000) [id]
              ,[inv_num]
              ,[data_smen]
              ,[smena]
              ,[data_nach]
              ,[data_kon]
              ,[id_downtime]
              ,[ID_DOK_1C]
          FROM [nmtport].[dbo].[mechanism_downtime_1C]
            where 
          inv_num=""" + str(self.mech_id) + """
          and data_smen=CONVERT(datetime, '""" + str(self.date_shift) + """ 00:00:00', 120) 
          and smena=""" + str(self.shift) + """

          order by data_nach
        """
        tmp_cursor = []
        with engine.connect() as con:
            rs = con.execute(sql)
            for row in rs:
                tmp_cursor.append(
                    [Period(row.data_nach, row.data_kon), row.id_downtime])
        return tmp_cursor

    def _check_exist_resons(self, timestamp: datetime | None) -> int | None:
        if timestamp is None:
            return None
        for [begin, stop], reson in self.cursor_resons:
            begin -= timedelta(minutes=2)
            stop += timedelta(minutes=2)
            if timestamp > begin and timestamp < stop:
                return reson
        return None

    def _get_resons(self) -> List[int | None]:
        return [self._check_exist_resons(i) for i in self.side_time_periods]

    def _get_bg_cells(self):
        border = [10, 5, 5, 5, 5, 10]
        result = []
        for i in range(6):
            if self.resons[i]:
                result.append('white')
                continue
            if self.delta_allowable_range[i] is None:
                result.append('white')
                continue
            if self.delta_allowable_range[i] <= 0:
                result.append('white')
                continue
            if self.delta_allowable_range[i] > border[i]:
                result.append('red')
                continue
            if self.delta_allowable_range[i] < border[i]:
                result.append('yellow')
                continue
            result.append('white')
        return result

    def show(self):
        print(self.colors_periods)
        print(f"{self.delta_allowable_range=}")
        print(f"{self.resons=}")
        print(f"{self._total_minuts_work(self.data_period)=}")
        print(f"{self._get_bg_cells()=}")


class Kran(Mechanism):
    def __init__(self, mech_id, date, shift):
        super().__init__(mech_id, date, shift)
        self.data_period = self._convert_cursor_to_kran()
        call_methods(self)
        self.colors_periods = [self._get_color_period(
            period) for period in self.split_periods.values()]

    def _convert_cursor_to_kran(self) -> itemsMech:
        return [Post(k, int(v[0])) for k, v in self.cursor.items()]

    def _get_color_period(self, period_values: List[Post]) -> Color:
        yellow = sum([1 for i in period_values if i.value == 0])
        blue = sum([1 for i in period_values if i.value == 2])
        black = sum([1 for i in period_values if i.value in (1, 3)])
        orange = sum([1 for i in period_values if i.value == 5])
        if blue > 20 and blue > black:
            return Color.blue
        if black > 20 and black > blue:
            return Color.black
        if orange > 12:
            return Color.orange
        if orange < 12 and yellow > 12:
            return Color.yellow
        return Color.red


class Usm(Mechanism):
    def __init__(self, mech_id, date, shift):
        super().__init__(mech_id, date, shift)
        self.data_period = self._convert_cursor_to_usm()
        call_methods(self)
        self.colors_periods = [self._get_color_period(
            period) for period in self.split_periods.values()]

    def _convert_cursor_to_usm(self) -> itemsMech:
        result = []
        for timestamp, [lever, roll] in self.cursor.items():
            if lever > 0.1 and roll > 4:
                result.append(Post(timestamp, 1))
            else:
                result.append(Post(timestamp, 0))
        return result

    def _get_color_period(self, period_values: List[Post]) -> Color:
        yellow = sum([1 for i in period_values if i.value == 0])
        blue = sum([1 for i in period_values if i.value > 0])
        if blue > 40:
            return Color.blue
        if yellow > 40:
            return Color.yellow
        return Color.red


def call_methods(obj: Mechanism):
    break_lanch = obj._find_max_empty_period(
        obj.data_period, obj.time_lanch)
    break_tea = obj._find_max_empty_period(
        obj.data_period, obj.time_tea)
    obj.work_periods = [
        Period(obj.time_shift.begin, break_lanch.begin),
        Period(break_lanch.stop, break_tea.begin),
        Period(break_tea.stop, obj.time_shift.stop)
    ]
    obj.split_periods = obj._split_by_periods(
        obj.data_period, obj.work_periods)
    obj.side_time_periods = obj._get_all_side_time_periods(
        obj.split_periods)
    obj.delta_allowable_range = obj._get_delta_allowable_range(
        obj.side_time_periods)
    obj.delta_allowable_range = obj._convert_to_allowable_range(
        obj.delta_allowable_range)
    obj.delta_allowable_range = obj._filter_if_more(
        obj.delta_allowable_range, FILTER_MINUTS_MORE)
    obj.resons = obj._get_resons()


if __name__ == "__main__":
    from list_mechanisms import kran, usm
    date_shift: date = datetime.now().date() - timedelta(days=1)
    shift: int = 1
    num = 31
    print(date_shift, f"{shift=} {num=}")
    print("_________________________")

    k = Kran(kran[num], date_shift, shift)
    k.show()

    # u = Usm(usm[num], date_shift, shift)
    # u.show()
