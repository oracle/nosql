# Test Description: Literal table alias with invalid characters.

select id 
from Users AS &u4r where age > 13
order by id