#
# Because table and/or index is not existing the hint will be ignored
#
select /*+ FORCE_INDEX(table_not_existing index_not_existing)  */ id1
from Foo