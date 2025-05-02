#Test Description: regex_like function in predicate. Invalid parameter type.
# Type: Negative


select id1, str
from foo f
where regex_like(str, 1)
