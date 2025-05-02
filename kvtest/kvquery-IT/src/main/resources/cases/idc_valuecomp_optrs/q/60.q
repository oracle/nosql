# Test Description: Integer column operand compared with double numeric literal using >= operator.

select id1, int >= 1.7976931348623157E308
from foo
order by id1