#Test Description: regex_like function in predicate. Flags parameter is a variable with invalid json null value.
# Type: Negative

declare $nullvalue string;

select id1, str
from foo f
where regex_like(str, "a.*", $nullvalue)
