#sqrt of standard values
select
    trunc(sqrt(4),7) as sqrt4,
    trunc(sqrt(9),7) as sqrt9,
    trunc(sqrt(16),7) as sqrt16,
    trunc(sqrt(25),7) as sqrt25,
    trunc(sqrt(100),7) as sqrt100,
    trunc(sqrt(216),7) as sqrt216,
    trunc(sqrt(625),7) as sqrt625,
    trunc(sqrt(49),7) as sqrt49
from functional_test where id =1