#Using Math functions on aggregate functions

select trunc(sin(count(value1)),7) as sincount,
        trunc(cos(count(value2)),7) as coscount,
        trunc(atan(count(value3)),7) as atancount
from aggregate_test
