#
# TODO: eliminate the $child var
#
select /* FORCE_PRIMARY_INDEX(Foo) */id, $child_info.friends
from foo as $t, $t.children.keys() as $child, $t.children.Anna $child_info
where $child = "Anna" and $child_info.age > 5
