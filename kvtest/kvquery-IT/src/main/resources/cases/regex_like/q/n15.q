#Test Description: regex_like function in predicate. Flags parameter is an invalid constant.
# Type: Negative

select id1, str
from foo f
where regex_like(str, "a.*", "z")
