#
# Because index is not existing the hint will be ignored
#
select /*+ FORCE_INDEX(Foo index_not_existing) 'no index'
        */
       id1
from Foo