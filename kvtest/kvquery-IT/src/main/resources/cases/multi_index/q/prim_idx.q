select * 
from usage 
where tenantId="acme" AND 
      tableName="customers" AND
      startSeconds >= 1508259960
order by tenantId, tableName, startSeconds
limit 1

