select /*+ FORCE_PRIMARY_INDEX(User) */ id, $child_info.friends
from User as $u, $u.children.values() as $child_info
where $child_info.age > 10 and $u.children.Anna.school > "sch_1"
