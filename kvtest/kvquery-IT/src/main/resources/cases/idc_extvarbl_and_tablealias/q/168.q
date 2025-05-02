# Test Description: variable names cannot be used as names for external variables: $value.

declare $value integer;
select id, age from Users where age = $value
order by id