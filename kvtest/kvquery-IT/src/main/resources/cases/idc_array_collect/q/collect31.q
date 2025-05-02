#For each user create a map with 3 fields recording the user's last name, their phone
#information, and the expense categories in which more than $5000 was spent

SELECT u.country, array_collect(
{
"last_name" : u.lastName,
"phones" : CASE
WHEN exists u.address.phones
THEN u.address.phones
ELSE "Phone info absent or not at the expected place"
END,
"high_expenses" : [ u.expenses.keys($value > 5000) ]
}
) as info FROM users u group by u.country
