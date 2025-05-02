select u.country,array_collect(distinct u.expenses.keys($value > 1000)) as highExpense from users u group by u.country
