#Using Math functions on aggregate functions

select round(abs(avg(value2)),2) as absavg,
        round(avg(abs(value2)),2) as avgabs,
        round(round(avg(value3),1),2) as roundavg,
        round(avg(round(value3,1)),2) as avground
from aggregate_test
