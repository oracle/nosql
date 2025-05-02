# Test Description: Literal table alias with valid characters.

select firstName 
from Users AS a_012_UUU_34 where lastName = "Scully"
order by id