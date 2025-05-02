select {
         "id" : t.id,
         "phones" : [seq_transform(t.info.addresses[],
                                   {"num_phones": size($.phones)})]
       }
from foo t
where t.info.addresses.phones is of type (array(any)+)
