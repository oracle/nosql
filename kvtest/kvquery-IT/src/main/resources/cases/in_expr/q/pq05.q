declare $arr1 array(json);
select /*+ FORCE_PRIMARY_INDEX(foo) */ 
       id
from foo f
where (f.info.bar1, f.info.bar2) in $arr1[]
