#Test Description: regex_like function in predicate. Pattern and flags parameters are variables.

declare 
$pattern2 string;
$flags string;

select id1, str
from foo f
where regex_like(str, $pattern2, $flags)
