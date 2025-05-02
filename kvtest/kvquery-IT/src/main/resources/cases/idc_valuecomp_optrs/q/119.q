# Test Description: Enum column operand compared with string literal using < operator.

select id1, enm < "tok1"
from foo
order by id1