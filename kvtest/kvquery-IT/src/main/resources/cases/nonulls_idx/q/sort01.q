#
# nothing pushed
# TODO: use type info from typed json indexes during query compilation
#       For this query, such info would result in no conditional array
#       constructor being placed around the t.info.age expr in the SELECT
#       clause, which would also make the t.info.age from the SELECT match
#       with the t.info.age from the ORDERBY.
#
select id, t.info.age
from foo t
where t.info.age > 10
order by t.info.address.state, t.info.address.city, t.info.age
