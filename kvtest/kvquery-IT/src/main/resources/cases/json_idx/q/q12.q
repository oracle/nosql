declare $jnull string; // json null
select id, t.info.age, $jnull
from foo t
where t.info.address.state = $jnull
