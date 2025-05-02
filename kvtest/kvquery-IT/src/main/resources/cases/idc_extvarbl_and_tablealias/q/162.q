# Test Description: Literal table alias with valid characters.

select id, enum.select from select enum
order by id 