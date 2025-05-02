select /*+ PREFER_INDEXES(Foo idx_children_both) */ id
from foo f
where f.info.children.values().age =any 10 and
      f.info[].children[].keys() =any "Anna"
