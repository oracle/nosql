select id, firstName, lastName, age from Users
where (age > 13 and age < 17) or
      lastName = "last9" and firstName > "first"
