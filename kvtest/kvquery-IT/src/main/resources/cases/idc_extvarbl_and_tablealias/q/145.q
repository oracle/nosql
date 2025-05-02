# Test Description: Type of item bound to external variable is of the same type as the declared variable.

declare $v3 integer;
select id, age from Users where age = $v3
order by id