#For each user select their id and the expense categories in which the user spent more
#than $1000.

select u.country,array_collect(u.expenses.keys($value > 1000)) as highExpense from users u group by u.country


