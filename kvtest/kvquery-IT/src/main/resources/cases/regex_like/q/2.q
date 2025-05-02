#Test Description: regex_like function in predicate. Pattern and flags parameters are constants. Tests the case insensitive flag.

select id1, str
from foo f
where regex_like(str, "A.*", "i")
