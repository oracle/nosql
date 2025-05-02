#Test Description: regex_like function in predicate. Pattern parameter is a variable. 

declare $pattern1 string;

select id1, str
from foo f
where regex_like(str, $pattern1)
