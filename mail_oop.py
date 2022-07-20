#!/usr/bin/env python
from sqlalchemy import create_engine
from rich import print
from datetime import datetime, timedelta, date
# import sys
# sys.path.insert(0, '/home/admin/nmtport')
from typing import List, Dict,  NamedTuple
from enum import Enum
from list_mechanisms import krans as dict_krans, usms as dict_usms # TODO request tot DB
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
import smtplib
import time

krans_UT = [82, 47, 54, 14, 16, 11, 33, 20, 8, 22, 12, 13, 6, 26 ]
krans_GUT = [28, 18, 1, 35, 31, 17, 39, 23, 48, 72, 65, 10]
HOURS = 10
FILTER_MINUTS_MORE = 50
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
    

class BgColor(Enum):
    red = "bg-red"
    yellow = "bg-yellow"
    white = "bg-white"
    gray = "bg-gray"


class FontColor(Enum):
    red = "font-red"
    green = "font-green"
    white = "font-white"


class PeriodColor(Enum):
    red = "line-red"
    yellow = "line-yellow"
    blue = "line-blue"
    orange = "line-orange"
    black = "line-black"

itemsMech = List[Post]


class Mechanism:
    cursor: Dict[datetime, List[float]] = {}
    data_period: itemsMech = []
    split_periods: Dict[Period, itemsMech] = {}
    side_time_periods: List[datetime | None] = []
    dt_minutes: List[int | None] = []
    str_dt_minutes: List[str] = []
    colors_periods = []
    TOTAL_PERIOD = 20  # if work more than
    work_periods = []
    resons: List[int | None] = []
    str_resons: List[str] = []
    bg_cells: List[BgColor] = []
    times: List[str] = []
    font_cells: List[FontColor] = []
    sum_dt_minutes: int
    total_work_time: float
    allowable_range: List[int]
    red_border: List[int]

    def __init__(self, mech_id: int, date_shift: date, shift: int):
        assert date_shift <= datetime.now().date()
        self.mech_id = mech_id
        self.date_shift = date_shift
        self.shift = shift
        self.cursor = self._get_cursor()
        self.cursor_resons = self._get_resons_from_db()
        #can't use one function because condition different 
        self.time_lanch = self._get_time_lanch()
        self.time_tea = self._get_time_tea()
        self.time_shift = self._get_time_shift()

    def _get_cursor(self) -> Dict[datetime, List[float]]:
        "in kran need only value in USM need value is lever, value3 is roll"
        engine = create_engine('mssql+pyodbc://' + UserPwd + '@' +
                               ServerName + '/' + Database + "?" + Driver)
        # not use f'string because bad sea values
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
        """start and stop lanch"""
        next_day = self.date_shift + timedelta(days=1)
        date_shift = str(self.date_shift) + " "
        next_day = str(next_day) + " "
        format = '%Y-%m-%d %H:%M'
        if self.shift == 1:
            v = [date_shift + '12:01', date_shift + '13:00']
        elif self.shift == 2:
            v = [next_day + '01:01',    next_day + '02:00']
        else:
            raise AttributeError
        return Period(datetime.strptime(v[0], format),
                      datetime.strptime(v[1], format))

    def _get_time_tea(self) -> Period:
        """start and stop tea"""
        next_day = self.date_shift + timedelta(days=1)
        date_shift = str(self.date_shift) + " "
        next_day = str(next_day) + " "
        format = '%Y-%m-%d %H:%M'
        if self.shift == 1:
            v = [date_shift + '16:31', date_shift + '17:00']
        elif self.shift == 2:
            v = [next_day + '04:31',    next_day + '05:00']
        else:
            raise AttributeError
        return Period(datetime.strptime(v[0], format),
                      datetime.strptime(v[1], format))

    def _get_time_shift(self) -> Period:
        """start and stop shift"""
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
        """a-b"""
        if a is None or b is None:
            return None
        if isinstance(a, datetime)\
                and isinstance(b, datetime):
            return int((a - b).total_seconds()/60) 
        if isinstance(a, int)\
                and isinstance(b, int):
            return a-b
        raise TypeError

    def _find_max_empty_period(self, all_period, break_period: Period) -> Period:
        """function starting from middle break period and find first not empty values right and left"""
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
        """need all number"""
        if isinstance(a, datetime)\
                and isinstance(b, datetime):
            return (a - b).total_seconds()/60
        assert "types not datetime"
        return 0.0

    def _filter_if_more(self, items: List[int | None], border: int) -> List[int | None]:
        """if mechanism start/stop earlier"""
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
        result = []
        for i in range(6):
            result.append(self._get_delta_minutes(
                delta_minutes[i], self.allowable_range[i]))
        return result

    def _get_total_minuts_work(self, data_period) -> int:
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
        """return delta time start/stop mechanism"""
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
        """return 6 timestamp when mech stop and start by periods"""
        result = []
        for i in periods.values():
            tmp = self._get_side_time_periods(i)
            result.append(tmp[0])
            result.append(tmp[1])
        return result

    def _get_side_time_periods(self, period_values: List[Post]) -> List[datetime | None]:
        "return [first, last] not empty values"
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
        """return sum turns for kran and work minutes fo usm"""
        return sum([1 for i in period_values if i.value > 0])

    def _split_by_periods(self, data_period, work_periods) -> Dict[Period, List[Post]]:
        """return 3 periods start-lanch lanch-tea tea-stop"""
        split_periods: Dict[Period, List[Post]] = {}
        for work_period in work_periods:
            split_periods[work_period] = []

        for timestamp, value in data_period:
            for work_period in work_periods:
                if timestamp > work_period.begin and timestamp <= work_period.stop:
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
        """if timestamp in reson's period"""
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
        result = []
        for i in range(6):
            if self.dt_minutes[i] is None:
                result.append(BgColor.white)
                continue
            if self.dt_minutes[i] <= 0: #normal
                result.append(BgColor.white)
                continue
            if self.resons[i]:
                result.append(BgColor.gray)
                continue
            if self.dt_minutes[i] > self.red_border[i]:
                result.append(BgColor.red)
                continue
            if self.dt_minutes[i] <= self.red_border[i]:
                result.append(BgColor.yellow)
                continue
            result.append(BgColor.white)
        return result
    
    def _get_font_cells(self):
        """if value<0 red if value>0 green"""
        result = []
        for i in self.dt_minutes:
            if i is None:
                result.append(FontColor.white)
            elif i<0:
                result.append(FontColor.red)
            elif i>0:
                result.append(FontColor.green)
            else:
                result.append(FontColor.white)
        return result


    def _get_hour_and_minutes(self, time):
        "convert 8,1 to 08:01"
        if time:
            h = time.hour
            m = time.minute
            if h < 10:
                h = '0' + str(h)
            if m < 10:
                m = '0' + str(m)
            return f'{h}:{m}'
        return ''


    def _convert_time_to_str(self, side_times):
        return [self._get_hour_and_minutes(time) for time in side_times]

    def plus_minute(self, dt_minutes) -> List[int | None]:
        """because minutes have adding second"""
        diff = [0,1,0,1,0,1]
        result = []
        for i in range(6):
            if dt_minutes[i]:
                result.append(dt_minutes[i]+diff[i])
            else:
                result.append(None)
        return result

    def show(self):
        print(f"{self.times=}")
        print(f"{self.dt_minutes=}")
        print(self.colors_periods)
        # print(f"{self.resons=}")
        # print(f"{self._get_total_minuts_work(self.data_period)=}")
        # print(f"{self.bg_cells}")
        # print(f"{self.font_cells}")


class Kran(Mechanism):
    def __init__(self, number, date, shift):
        self.mech_id = dict_krans[number]
        self.allowable_range = [20, 0, 0, 0, 0, 20]
        self.red_border = [10, 5, 5, 5, 5, 10]
        super().__init__(self.mech_id, date, shift)
        self.number = number
        self.data_period = self._convert_cursor_to_kran()
        call_methods(self)
        self.colors_periods = [self._get_color_period(
            period) for period in self.split_periods.values()]

    def _convert_cursor_to_kran(self) -> itemsMech:
        return [Post(k, int(v[0])) for k, v in self.cursor.items()]

    def _get_color_period(self, period_values: List[Post]) -> PeriodColor:
        yellow = sum([1 for i in period_values if i.value == 0])
        blue = sum([1 for i in period_values if i.value == 2])
        black = sum([1 for i in period_values if i.value in (1, 3)])
        orange = sum([1 for i in period_values if i.value == 5])
        if blue > 20 and blue > black:
            return PeriodColor.blue
        if black > 20 and black > blue:
            return PeriodColor.black
        if orange > 12:
            return PeriodColor.orange
        if orange < 12 and yellow > 12:
            return PeriodColor.yellow
        return PeriodColor.red


class Usm(Mechanism):
    def __init__(self, number, date, shift):
        self.mech_id = dict_usms[number]
        self.allowable_range = [20, 0, 0, 0, 0, 30]
        self.red_border = [10, 5, 5, 5, 5, 10] # after
        super().__init__(self.mech_id, date, shift)
        self.number = number
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

    def _get_color_period(self, period_values: List[Post]) -> PeriodColor:
        yellow = sum([1 for i in period_values if i.value == 0])
        blue = sum([1 for i in period_values if i.value > 0])
        if blue > 40:
            return PeriodColor.blue
        if yellow > 40:
            return PeriodColor.yellow
        return PeriodColor.red


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
    obj.dt_minutes = obj._get_delta_allowable_range(
        obj.side_time_periods)
    obj.dt_minutes = obj.plus_minute(obj.dt_minutes)
    obj.dt_minutes = obj._convert_to_allowable_range(
        obj.dt_minutes)
    obj.dt_minutes = obj._filter_if_more(
        obj.dt_minutes, FILTER_MINUTS_MORE)
    obj.total_work_time = round(obj._get_total_minuts_work(obj.data_period)/60, 1)
    obj.times = obj._convert_time_to_str(obj.side_time_periods)
    obj.resons = obj._get_resons()
    obj.bg_cells = obj._get_bg_cells()
    obj.dt_minutes = [0-x if x else x for x in obj.dt_minutes] # change +/-
    obj.sum_dt_minutes = sum([x if x else 0 for x in obj.dt_minutes])
    obj.str_dt_minutes = [str(x) if x else "0" for x in obj.dt_minutes] # convert to str
    obj.str_resons = [str(x) if x else " " for x in obj._get_resons()] # convert to str
    obj.font_cells = obj._get_font_cells()

class Table:
    titles = [ 
        "номер и статус",
        "начало смены",
        "окончание перед обедом",
        "начало после обеда",
        "окончание перед тех. перерывом",
        "начало после тех. перерыва",
        "окончание смены",
        "общие потери и отработанно",
    ]
    def __init__(self, mechanisms, shift, LIST_RESONS):
        self.LIST_RESONS=LIST_RESONS
        self.mechanisms = mechanisms
        self.shift = shift
        self.total_sum_dt_minutes = sum([m.sum_dt_minutes for m in self.mechanisms])
        # table = self.make_table(Mechanisms)

    def make_table(self) -> str:
        if not self.mechanisms:
            return ""
        table = "<table>"
        table += '<tr class="titles">'
        for title in self.titles:
            table += f'<td> {title} </td>'
        table += '</tr>'

        table += '<tr>'
        for mech in self.mechanisms:
            table += f"""<td class="number">  {mech.number} 
                <table class="box-line">
                    <tr>
                        <td class={mech.colors_periods[0].value}>.</td>
                        <td class={mech.colors_periods[1].value}>.</td>
                        <td class={mech.colors_periods[2].value}>.</td>
                    </tr>
                </table>
            </td>"""
            for i in range(6):
                table += '<td>'
                table += f'<p class={mech.font_cells[i].value}> { mech.str_dt_minutes[i] } </p> '
                table += f'<div class={mech.bg_cells[i].value}> {mech.times[i]} </div>'
                table += f'<div class=reson> {self.LIST_RESONS[mech.resons[i]] } </div>'
                table += '</td>'
            table += f'''<td class="sum"> 
                <p class="total-dt">
                    {mech.sum_dt_minutes} мин.
                </p>
                <div class="total-work"> 
                    {mech.total_work_time}ч. 
                </div> 
            </td>'''
            table += '</tr>'
        table += '<tr>' + '<td class=empty></td>'*7 
        table += f'<td class=total> {self.total_sum_dt_minutes} </td></tr>'
        table += '</table>'
        return table


class Form:
    links = {1: "krans", 2: "krans", 3: "usm"}
    def __init__(self, data1, data2, date_shift, division, LIST_RESONS):
        self.date_shift = date_shift
        self.division = division
        table1 = Table(data1, 1, LIST_RESONS).make_table()
        table2 = Table(data2, 2, LIST_RESONS).make_table()
        self.html = self.make_html(table1, table2)
        self.save_to_file()

    def save_to_file(self):
        with open(str(self.division)+'mail.html', 'w') as f:
            f.write(self.html)
            f.close()

    def get_html(self):
        return self.html

    def make_html(self, table1, table2):
        date = self.date_shift.strftime("%d.%m.%Y")
        link1 = f'<a href="https://m1.nmtport.ru/{self.links[self.division]}/{date}/1"> Подробнее </a>'
        link2 = f'<a href="https://m1.nmtport.ru/{self.links[self.division]}/{date}/2"> Подробнее </a>'
        styles = """
        <html>
            <head>
            <style> 
              table, th, td { 
                border: 1px solid #AAA; 
                border-collapse: collapse; 
                vertical-align: top;
              }
              th, td { 
                width: 125px;
                padding: 2px; 
              }
              
              .number {
                background: #F9F9F9;
                text-align: center;
                font-weight: bold;
                vertical-align: bottom;
                width: 95px;
                margin-bottom: 2em;
              }
              .box-line {
                color: #FFF;
                font-size: 2px;
                margin-top: 2px;
                height: 1em;
                padding: 0px;

              }
              .line-blue {
                background: #00B0F0;
                color: #00B0F0;
              }
              .line-red {
                background: #FF0000;
                color: #FF0000;
              }
              .line-yellow {
                background: #FFFF00;
                color: #FFFF00;
              }
              .line-orange {
                background: #F2C400;
                color: #F2C400;
              }
              .line-black {
                background: #404040;
                color: #404040;
              }
              .bg-red {
                color: #555;
                background: #FBC3C3;
              }
              .bg-yellow {
                color: #555;
                background: #FFFFA4;
              }
              .bg-white {
                color: #FFF;
                background: #FFFFFF;
              }
              .bg-gray {
                color: #999;
                background: #DBDBDB;
              }
              .font-green {
                color: #00BD39;
                background: #D4FDE0;
                padding-top: 0;
                font-size: 12px;
                margin-left: 90px;
                margin-bottom: 5px;
                text-align: right;
              }
              .font-red {
                color: #ED002F;
                background: #FDE6EB;
                padding-top: 0;
                font-size: 12px;
                margin-left: 90px;
                margin-bottom: 5px;
                text-align: right;
              }
              .font-white {
                font-size: 12px;
                color: #FFFFFF;
                padding-top: 0;
                font-size: 12px;
                margin-left: 90px;
                margin-bottom: 5px;
                text-align: right;
              }
              .reson {
                color: #666;
                font-size: 10px;
              }
              .titles {
                background: #F5F5F5;
                color: #444;
                font-size: 12px;
              }
              .sum {
                color: #666;
              }
              .empty {
                text-align: right;
                border: 0px solid #fff;
              }
              .total {
                text-align: right;
                color: #111;
              }
              .total-dt {
                text-align: right;
                margin: 0px 0px 5px 0px;
              }
              .total-work {
                font-weight: bold;
              }
            </style>
            """
        body = f"""</head> <body>
                <p class="title"> {self.date_shift}  </p>
                <p>Позднее начало, ранее окончание по 
                производственным периодам</p>
                1 смена
                """ + table1 + link1 + """ 
                <div> &nbsp; </div>
                <div> 2 смена </div>
                """ + table2 + link2 +""" 
                <div> </div>
            </body>
        </html>
        """
        return styles + body

class Mail:
    HOST = 'smtp.yandex.ru'
    FROM = 'smartportdaly@yandex.ru'
    mail_pass = os.environ['YANDEX_MAIL']
    cc = [
        # 'Vladimir.Grigoriev@nmtport.ru',
        # 'Radion.Bespalov@nmtport.ru',
        # 'Disp.Smen@nmtport.ru',
        # 'Disp1.Smen@nmtport.ru',
        # 'Oleg.Evsyukov@nmtport.ru',
        'Alexander.Ostapchenko@nmtport.ru',
    ]

    nameTerminal = {1: "УТ-1", 2: "ГУТ-2", 3: "УОУ"}
    addresses = {
        1: [
            'ostap666@yandex.ru',
            # 'Vadim.Evsyukov@nmtport.ru',
            # 'Maxim.Anufriev@nmtport.ru',
            # 'Konstantin.Nikitenko@nmtport.ru',
            # 'Petr.Gerasimenko@nmtport.ru',
        ],
        2: [
            'ostap666@yandex.ru',
            # 'Dmitry.Golynsky@nmtport.ru',
            # 'Vyacheslav.Gaz@nmtport.ru',
            # 'Vladimir.Speransky@nmtport.ru',
            # 'Denis.Medvedev@nmtport.ru',
            # 'Petr.Gerasimenko@nmtport.ru',
        ],
        3: [
            'ostap666@yandex.ru',
            # 'Petr.Gerasimenko@nmtport.ru',
            # 'Fedor.Tormasov@nmtport.ru',
            # 'Aleksey.Makogon@nmtport.ru',
            # 'shift.engineer@nmtport.ru'
        ]
    }
    def __init__(self, html: str, division: int):
        self.html = html
        self.division = division

    def sent(self):
        text = "У Вас отключена поддержка HTML таблиц"
        part1 = MIMEText(text, 'plain')
        part2 = MIMEText(self.html, 'html')

        message = MIMEMultipart('alternative')
        message['Subject'] = "Простои по " + self.nameTerminal[self.division]
        message['From'] = self.FROM
        message['To'] = ", ".join(self.addresses[self.division])
        message['Cc'] = ', '.join(self.cc)

        message.attach(part1)
        message.attach(part2)
        recipients = self.addresses[self.division] + self.cc

        server = smtplib.SMTP_SSL(self.HOST, 465)
        server.ehlo()
        server.login(self.FROM, self.mail_pass)
        server.sendmail(self.FROM, recipients, message.as_string())
        server.quit()



def get_list_resons_from_db() -> Dict[int, str]:
    engine = create_engine('mssql+pyodbc://' + UserPwd + '@' +
                           ServerName + '/' + Database + "?" + Driver)
    sql = """
      SELECT  [id] ,[name]
      FROM [nmtport].[dbo].[Downtime]
    """
    tmp_cursor = {}
    tmp_cursor[None] = "   "
    with engine.connect() as con:
        rs = con.execute(sql)
        for row in rs:
            tmp_cursor[row.id] = row.name
    return tmp_cursor

def every_day(date_shift):
    LIST_RESONS = get_list_resons_from_db()
    LIST_RESONS = {k: v.lower() for k,v in LIST_RESONS.items()}
    mech_divisions = {
        1: krans_UT,
        2: krans_GUT,
        3: list(range(5,14)),
    }

    mech_class = {
        1: Kran,
        2: Kran,
        3: Usm,
    }
    for i in range(1,4):
        mechanisms = [mech_class[i](num, date_shift, 1) for num in  (mech_divisions[i])]
        data1 = [m for m in mechanisms if m.sum_dt_minutes != 0]
        mechanisms = [mech_class[i](num, date_shift, 2) for num in  (mech_divisions[i])]
        data2 = [m for m in mechanisms if m.sum_dt_minutes != 0]
        form = Form(data1, data2, date_shift, i, LIST_RESONS).get_html()
        mailer = Mail(form, i)
        mailer.sent()


if __name__ == "__main__":

    while True:
        hour = datetime.now().hour
        minute = datetime.now().minute
        if hour==16 and minute==55:
            date_shift = datetime.now().date() - timedelta(days=1)
            print(datetime.now())
            every_day(date_shift)
        time.sleep(30)

    # date_shift = datetime.now().date() - timedelta(days=1)
    # every_day(date_shift)

    # k = Kran(num, date_shift, shift)
    # k.show()

    # u = Usm(num, date_shift, shift)
    # u.show()
