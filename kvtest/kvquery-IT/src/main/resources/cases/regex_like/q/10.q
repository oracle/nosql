#Test Description: regex_like function in predicate prefix pattern match.

select id1, str
from foo f
where regex_like(str, "1 Earth.*", "")
