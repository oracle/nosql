select timestamp_floor(t3, 'year') as t3_to_year,
       timestamp_floor(t3, 'iyear') as t3_to_iyear,
       timestamp_floor(t3, 'QUARTER') as t3_to_quarter,
       timestamp_floor(t3, 'Month') as t3_to_month,
       timestamp_floor(t3, 'weeK') as t3_to_week,
       timestamp_floor(t3, 'iweeK') as t3_to_iweek,
       timestamp_floor(s9, 'day') as t9_to_day,
       timestamp_floor(s9) as t9_to_day1,         
       timestamp_floor(s9, 'hour') as t9_to_hour,
       timestamp_floor(s9, 'minute') as t9_to_minute,
       timestamp_floor(t3, 'second') as t3_to_second
from roundtest t
where id = 2
