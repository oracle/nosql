# Test Description: ANY_ATOMIC numerical operands and > operator.

select id1 
from Foo 
where int > flt
order by id1
