# Test Description: Integer column operand compared with long numeric literal using >= operator.

select id1, int, int >= 9223372036854775807
from Foo
order by id1