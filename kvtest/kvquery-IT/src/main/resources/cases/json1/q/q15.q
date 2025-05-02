#
# arithm op with json data
# comparison with jnull
#
select { "id" : id, 
         "areacode" : case when $areacode = null then 3.5
                                                 else $areacode
                      end + 10.0
       }
from foo $f, $f.info.address.phones.areacode as $areacode
where $f.info.address.phones.areacode =any 408



