# Test Description: omparison with multiple AND and OR with parenthesis.

select id1
from Foo
where (int = 0 or lng = 1) and (str = "tok1" and enm = "tok3" or flt = 0)