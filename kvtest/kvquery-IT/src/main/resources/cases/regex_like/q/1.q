#Test Description: regex_like function in predicate. Pattern parameter a constant.

select id1, str
from foo f
where regex_like(str, "a.*")
