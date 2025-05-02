# Test Description: Literal table alias with valid characters.

select id, long.select from select long 
order by id