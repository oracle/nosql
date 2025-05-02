#Test Description: regex_like function in predicate with backslash d (digit) pattern match. The digit is not supported
# Type: Negative

select id1, str
from foo f
where regex_like(str, "\\d.*", "")
