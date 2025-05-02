select u.country, array_collect({"id":u.id, "oldIncome" : u.income, "newIncome" : (u.income*1.10/0*1)+1-1}) as increments from users u group by country

