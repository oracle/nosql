# Test Description: Variable table alias (alias starts with $). Name matches declared external variable.

declare $v1 integer; 
select id from Users AS $v1
order by id