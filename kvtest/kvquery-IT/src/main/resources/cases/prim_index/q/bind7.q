#
# range only and always true pred
#
declare
$ext7_1 integer; // 4

select id1
from foo
where 4 <= id1 and $ext7_1 < id1
