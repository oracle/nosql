#Test Description: regex_like function in predicate. Source is a variable with invalid null value.

declare $nullvalue string;

select id1, str
from foo f
where regex_like($nullvalue, "a.*)
