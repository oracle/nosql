# Test Description: variable names cannot be used as names for external variables: $pos.

declare $pos integer;
select id, age from Users where age = $pos
order by id