select /*+ FORCE_INDEX(Foo idx_kids_anna_friends) */id
from foo f
where f.info.children.Anna.friends[] =any "Bobby"
