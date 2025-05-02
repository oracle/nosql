# Test Description: variable names cannot be used as names for external variables: $element.

declare $element integer;
select id, age from Users where age = $element
order by id