# Test Description: Reference to external defined variable.

declare $v1 integer;
select id from Users where income= $v1
order by id