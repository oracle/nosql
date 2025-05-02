select timestamp_bucket(t3, '5 minutes') as t3_5mins, 
       count(*) as count 
from roundtest 
group by timestamp_bucket(t3, '5 minutes')
