#Using Math functions on aggregate functions

select trunc(sqrt(min(value1)),7) as sqrtmin,
        trunc(min(sqrt(value1)),7) as minsqrt,
        trunc(log10(max(value3)),7) as logtenmax,
        trunc(max(log10(value2)),7) as maxlogten
from aggregate_test
