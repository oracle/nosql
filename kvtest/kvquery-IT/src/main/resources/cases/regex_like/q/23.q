#Test Description: regex_like function in predicate with and clause. Pattern parameter a constant.

select id1, str
from foo f
where regex_like(str, "a.*") and id1 > 0
