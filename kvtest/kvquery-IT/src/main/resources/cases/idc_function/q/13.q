# Test Description: Pass array as argument to size() function using map var_ref.

declare $phone string;
select id1, size(f.address.$phone)
from Foo f
order by id1