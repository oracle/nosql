# Test Description: Type of item bound to external variable is sub-type of the declared variable.

declare $v4 long;
select id, age from Users where age = $v4
order by id