#Test Description: regex_like function in predicate with all flag options set.

select id1, str
from foo f
where regex_like(str, "a.*","dixlsucU")
