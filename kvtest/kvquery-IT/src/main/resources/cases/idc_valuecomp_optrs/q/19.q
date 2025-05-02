# Test Description: record operand1 >= map operand2.

select id1
from Foo
where Foo.rec >= Foo.map
order by id1
