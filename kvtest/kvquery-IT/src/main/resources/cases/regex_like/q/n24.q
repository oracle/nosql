#Test Description: regex_like function in predicate. Source is float type.

select id1, str
from foo f
where regex_like(flt, "a.*")
