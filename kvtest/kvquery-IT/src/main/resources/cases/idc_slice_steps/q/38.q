# Test Description: Context item is an parenthesized_expr in a array.

declare $phones string;
select id, c.address.$phones[0:2]
from complex c
order by id