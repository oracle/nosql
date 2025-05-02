select seq_transform(t.doc.bucket_arr[], 
                     timestamp_bucket($sq1.time, 
                                      $sq1.interval, 
                                      $sq1.origin)) as buckets 
from roundtest t 
where id = 0
