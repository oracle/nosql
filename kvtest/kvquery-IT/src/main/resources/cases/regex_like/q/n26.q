#Test Description: regex_like function in predicate. Invalid source parameter, type not a string.
## Type: Negative

select id1, str
from foo f
where regex_like(enm, "a.*")
