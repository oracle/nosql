# Array filter step
# Test Description: Context item is an atomic var_ref in a array - with true expression.

declare $work string;
select id, c.address.phones.$work[true]
from complex c
order by id