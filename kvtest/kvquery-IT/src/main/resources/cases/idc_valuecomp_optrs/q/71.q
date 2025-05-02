# Test Description: Long column operand compared with float literal using = operator.

select id1, lng = 3.4028235E38
from Foo
order by id1