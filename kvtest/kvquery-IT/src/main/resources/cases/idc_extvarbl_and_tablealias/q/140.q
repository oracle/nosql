# Test Description: Variable table alias with valid characters (alias starts with $).

select id from Users AS $col#
order by id