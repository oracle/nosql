# Test Description: Literal alias with valid characters.

select id, firstName as F_NAME from Users where lastName = "Smith"
order by id