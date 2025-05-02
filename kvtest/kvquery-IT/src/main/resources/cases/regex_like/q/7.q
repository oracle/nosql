#Test Description: regex_like function in predicate with all flag options set except literal. 

select id1, str
from foo f
where regex_like(str, "a.*","dixsucU")
