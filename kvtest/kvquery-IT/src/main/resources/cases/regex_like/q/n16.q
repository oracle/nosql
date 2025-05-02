#Test Description: regex_like function in predicate. Pattern parameter is an invalid null valued variable.
# Type: Negative

declare $invalidpattern string;

select id1, str
from foo f
where regex_like(str, $invalidpattern)
