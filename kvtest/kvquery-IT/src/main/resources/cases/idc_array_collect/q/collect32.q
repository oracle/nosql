#Select the id and amount spent on books for all users who live in India.

select array_collect({"id" : u.id, "bookExpense" : case when EXISTS u.expenses.books THEN u.expenses.books else 0 end}) as userDetails from users u where u.country='IND'
