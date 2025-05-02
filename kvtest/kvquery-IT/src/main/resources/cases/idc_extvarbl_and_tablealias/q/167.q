# Test Description: variable names cannot be used as names for external variables: $key.

declare $key integer;
select id, age from Users where age = $key
order by id