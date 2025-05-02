#Test Description: regex_like function in projection.

select id1, str, regex_like(str, "a.*")
from foo f
