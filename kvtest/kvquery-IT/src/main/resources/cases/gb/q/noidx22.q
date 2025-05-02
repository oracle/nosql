select case when id < 16 then number else decimal end as number,
       count(*) as cnt
from numbers
group by case when id < 16 then number else decimal end
