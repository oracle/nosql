# Test Description: Variable table alias with valid characters (alias starts with $).

select id 
from Users AS $TRUE_FALSE_INT_FLOAT
order by id