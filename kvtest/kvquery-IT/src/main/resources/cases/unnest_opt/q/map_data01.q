select id, $child_info.friends
from foo as $t, $t.children.values() as $child_info
where $child_info.age > 10 and $t.children.Anna.school > "sch_1"
