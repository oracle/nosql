#Select the id, lastName for users who are connected with the user with id 3. Group by state
select u.address.state,array_collect({"id" : u.id, "lastName" : u.lastName}) from users u WHERE u.connections[] =any 3 group by u.address.state
