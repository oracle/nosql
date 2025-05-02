#Test Description: regex_like function in predicate. Invalid flags parameter datatype.
# Type: Negative

select id1, str
from foo f
where regex_like(str, "a.*", 4)
