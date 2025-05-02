# Test Description: Context item is a map parenthesized_expr.

declare $map string;
select id, $C.$map[0:1]
from Complex $C
order by id