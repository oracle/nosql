# Test Description: Float column operand compared with long literal using = operator.

select id1, flt = 9223372036854775807
from foo
order by id1