#Test Description: regex_like function in predicate with empty flags string.

select id1, str
from foo f
where regex_like(str, "a.*", "")
