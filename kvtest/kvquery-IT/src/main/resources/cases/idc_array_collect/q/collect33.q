#For each user select their id and the expense categories in which they spent more than they
#spent on clothes

select u.country,array_collect([u.id,  u.expenses.keys($value > $.clothes)]) as highExpense from users u group by u.country

