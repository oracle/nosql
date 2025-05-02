select /* FORCE_INDEX(Foo idx_children_both) */id
from foo f
where f.info.children.keys($value.age <= 10) =any "Anna"
