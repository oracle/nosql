# Test Description: Reference to external un-defined variable.

declare $v2 integer;
select * from Users where id = $v2
order by id