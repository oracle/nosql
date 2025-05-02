select cast(number as double) as number, count(*) as cnt
from numbers
group by number

