#Test Description: regex_like function in predicate with invalid regex.
# Type: Negative

select id1, str
from foo f
where regex_like(str, "*")
