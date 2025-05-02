# Test Description: Type of item bound to external variable is not of the type of the declared variable.

declare $v4 long;
select * from Users where lastname = $v4
order by id