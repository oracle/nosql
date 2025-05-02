#Using Math functions on aggregate functions

select floor(sum(value1)) as floorsum ,
        sum(floor(value1)) as sumfloor,
        ceil(sum(value3)) as ceilsum,
        sum(ceil(value3)) as sumceil
from aggregate_test
