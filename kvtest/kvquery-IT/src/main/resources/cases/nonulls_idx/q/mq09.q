select /*+ PREFER_INDEXES(Foo idx_anna_areacode) */id
from foo f
where f.info.children.Anna.age > 9
