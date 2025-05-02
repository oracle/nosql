select timestamp_ceil(t0, 'year') as t0_to_year,
       timestamp_ceil(t0, 'iyear') as t0_to_iyear,
       timestamp_ceil(t0, 'QUARTER') as t0_to_quarter,
       timestamp_ceil(t0, 'Month') as tm0_to_month,
       timestamp_ceil(t0, 'weeK') as t0_to_week,
       timestamp_ceil(t0, 'iweeK') as t0_to_iweek,
       timestamp_ceil(t0, 'day') as t0_to_day1,
       timestamp_ceil(t0) as t0_to_day2,
       timestamp_ceil(t0, 'hour') as t0_to_hour,
       timestamp_ceil(t0, 'minute') as t0_to_minute,       
       timestamp_ceil(t0, 'second') as t0_to_second
from roundtest t
where id = 2