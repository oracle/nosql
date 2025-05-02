# Test Description: Long column operand compared with double literal using >= operator.

select id1, dbl, lng >= 1.7976931348623157E308
from Foo
order by id1