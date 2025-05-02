#exp of standard values
select
    trunc(exp(0),7) as exp0,
    trunc(exp(1),7) as exp1,
    trunc(exp(2),7) as exp2,
    trunc(exp(-0),7) as expneg0,
    trunc(exp(-1),7) as expneg1,
    trunc(exp(-2),7) as expneg2
from functional_test where id=1