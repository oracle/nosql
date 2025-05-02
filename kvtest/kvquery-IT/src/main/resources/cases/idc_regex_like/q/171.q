# Test for regex_like function with case expression
select
{
"age" : p.info.id.age,
"phones" : case
when exists p.info.id.phones then p.info.id.phones
else "Phone info absent or not at the expected place"
end,
"high_expenses" : [ p.info.id.expenses.keys($value > 5000) ]
}
from playerinfo p