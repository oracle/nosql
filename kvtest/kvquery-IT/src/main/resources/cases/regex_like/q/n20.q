#Test Description: regex_like function in predicate. Invalid number of parameters.
# Type: Negative


select id1, str
from foo f
where regex_like(str, "a.*", "i", "bad")
