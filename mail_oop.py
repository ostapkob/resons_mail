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
from tabulate import tabulate
from list_mechanisms import krans as dict_krans, usms as dict_usms
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
    colors_periods = []
    TOTAL_PERIOD = 20  # if work more than
    work_periods = []
    resons: List[int | None] = []
    str_resons: List[str] = []
    bg_cells: List[BgColor] = []
    times: List[str] = []
    font_cells: List[FontColor] = []
    sum_dt_minutes: int
    total_work_time: int

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
            v = [date_shift + '12:01', date_shift + '13:00']
        elif self.shift == 2:
            v = [next_day + '01:01',    next_day + '02:00']
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
            v = [date_shift + '16:31', date_shift + '17:00']
        elif self.shift == 2:
            v = [next_day + '04:31',    next_day + '05:00']
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
        raise TypeError

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
                result.append(BgColor.gray)
                continue
            if self.dt_minutes[i] is None:
                result.append(BgColor.white)
                continue
            if self.dt_minutes[i] <= 0:
                result.append(BgColor.white)
                continue
            if self.dt_minutes[i] > border[i]:
                result.append(BgColor.red)
                continue
            if self.dt_minutes[i] <= border[i]:
                result.append(BgColor.yellow)
                continue
            result.append(BgColor.white)
        return result
    
    def _get_font_cells(self):
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

    def show(self):
        print(self.colors_periods)
        print(f"{self.times=}")
        print(f"{self.dt_minutes=}")
        print(f"{self.resons=}")
        print(f"{self._get_total_minuts_work(self.data_period)=}")
        print(f"{self.bg_cells}")
        print(f"{self.font_cells}")


class Kran(Mechanism):
    def __init__(self, number, date, shift):
        self.mech_id = dict_krans[number]
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
    obj.dt_minutes = obj._convert_to_allowable_range(
        obj.dt_minutes)
    obj.dt_minutes = obj._filter_if_more(
        obj.dt_minutes, FILTER_MINUTS_MORE)
    obj.total_work_time = obj._get_total_minuts_work(obj.data_period)
    obj.times = obj._convert_time_to_str(obj.side_time_periods)
    obj.resons = obj._get_resons()
    obj.bg_cells = obj._get_bg_cells()
    obj.dt_minutes = [0-x if x else x for x in obj.dt_minutes] # change +/-
    obj.sum_dt_minutes = sum([x if x else 0 for x in obj.dt_minutes])
    obj.str_resons = [str(x) if x else " " for x in obj._get_resons()] # convert to str
    obj.font_cells = obj._get_font_cells()



class Table:
    text =  "SmartPort"
    def __init__(self, Mechanisms, date_shift, shift):
        self.date_shift = date_shift
        self.shift = shift
        table = self.make_table(Mechanisms)
        html = self.make_html(table)

        text = self.text.format(table=tabulate(
            table, headers="firstrow", tablefmt="grid"))
        html = html.format(table=tabulate(
            table, headers="firstrow", tablefmt="html"))

        # message = MIMEMultipart(
        #     "alternative", None, [MIMEText(text), MIMEText(html, 'html')])
        self.save_to_html(html)

    def make_table(self, mechanisms: List):
        if not mechanisms:
            return ""
        table = "<table>"
        table += '<tr>'
        for mech in mechanisms:
            table += f"""<td class="number">  {mech.number} 
                </br>  
                <div class="box-line">
                    <div class={mech.colors_periods[0].value}> . </div>
                    <div class={mech.colors_periods[1].value}> . </div>
                    <div class={mech.colors_periods[2].value}> . </div>
                </div>
            </td>"""
            for i in range(6):
                table += '<td>'
                table += f'<p class={mech.font_cells[i].value}> { mech.dt_minutes[i] } </p>'
                table += f'<div class={mech.bg_cells[i].value}> {mech.times[i]} </div>'
                table += f'<div class=reson> {LIST_RESONS[mech.resons[i]] } </div>'
                table += '</td>'
            table += f'''<td class="sum"> 
                <p class="total-dt">
                    {mech.sum_dt_minutes} 
                </p>
                <div class="total-work"> 
                    {mech.total_work_time} 
                </div> 
            </td>'''

            table += '</tr>'
        table += '<tr>' + '<td class=empty></td>'*7 + \
            '<td class=total> Total </td></tr>'
        table += '</table>'
        return table

    def save_to_html(self, table):
        with open('mail.html', 'w') as f:
            f.write(table)
            f.close()

    def make_html(self, table1):
        styles = """
        <html>
            <head>
            <style> 
              table, th, td {{ 
                border: 1px solid #999; 
                border-collapse: collapse; 
                vertical-align: top;
              }}
              th, td {{ 
                width: 45px;
                padding: 5px; 

              }}
              .box-line {{
                margin-top: 10px;
                display: flex;
                }}

              .line-blue {{
               width: calc(100% / 3);
                color: #104BA9;
                background: #6E86D6;
              }}
              .line-red {{
               width: calc(100% / 3);
                color: #EF002A;
                background: #FE7276;
              }}
              .line-yellow {{
               width: calc(100% / 3);
                color: #FFEE00;
                background: #FFFA73;
              }}
              .line-orange {{
               width: calc(100% / 3);
                color: #FFEE00;
                background: #FFC773;
              }}
              .line-black {{
               width: calc(100% / 3);
                color: #333;
                background: #333;
              }}

              .bg-red {{
                color: #555;
                background: #F4A9A9;
              }}
              .bg-yellow {{
                color: #555;
                background: #FFFFA4;
              }}
              .bg-white {{
                color: #FFF;
                background: #FFFFFF;
              }}
              .bg-gray {{
                color: #555;
                background: #D4D4D4;
              }}
              .font-green {{
                text-align: right
                padding-top: 0;
                padding-right: 1px;
                text-align: right;
                font-size: 12px;
                color: #0ACF00;
              }}
              .font-red {{
                text-align: right
                padding-top: 0;
                padding-right: 1px;
                text-align: right;
                font-size: 12px;
                color: #FD0006;
              }}
              .font-white {{
                text-align: right
                padding-top: 0;
                padding-right: 1px;
                text-align: right;
                font-size: 12px;
                color: #FFFFFF;
              }}
              .reson {{
                color: #666;
                font-size: 10px;
              }}

              .titles {{
                background: #F5F5F5;
                color: #444;
              }}
              .number {{
                background: #F9F9F9;
                text-align: center;
                font-weight: bold;
                padding: 0px;
                vertical-align: bottom;
              }}
              .sum {{
                font-weight: bold;
                text-align: right;
                color: #666;
              }}
              .empty {{
                border: 0px solid #fff;
              }}
              .total {{
                font-weight: bold;
                text-align: right;
                color: #111;
              }}
              .total-dt {{
                margin: 0px;
              }}
              .total-work {{
                margin: 8px;
              }}
            </style>
            """
        body = f"""</head> <body>
                <p class="title"> {self.date_shift}  {self.shift} </p>
                <p>Позднее начало, ранее окончание по 
                производственным периодам</p>
                """ + table1 + """ 
                </br>
                <a href="https://m1.nmtport.ru/krans"> SmartPort </a>
            </body>
        </html>
        """
        return styles + body

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

if __name__ == "__main__":
    LIST_RESONS = get_list_resons_from_db()
    date_shift: date = datetime.now().date() - timedelta(days=7)
    shift: int = 1
    num = 9
    print(date_shift, f"{shift=} {num=}")
    print("_________________________")


    # krans = [Kran(num, date_shift, shift) for num in (31, 35, 48, 65, 72)]
    # Table(krans)

    usms = [Usm(num, date_shift, shift) for num in range(5,14)]
    usms = [u for u in usms if u.sum_dt_minutes != 0]
    Table(usms, date_shift, shift)

    # k = Kran(num, date_shift, shift)
    # k.show()

    # u = Usm(num, date_shift, shift)
    # u.show()
