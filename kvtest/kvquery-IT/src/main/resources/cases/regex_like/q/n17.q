#Test Description: regex_like function in predicate. Flags parameter is an invalid null valued variable.
# Type: Negative

declare $invalidflags string;

select id1, str
from foo f
where regex_like(str, "a.*", $invalidflags)
