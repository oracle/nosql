declare $arr2 array(json);
select /*+ FORCE_PRIMARY_INDEX(foo) */ 
       id
from foo f
where f.info.bar1 in $arr2[]
