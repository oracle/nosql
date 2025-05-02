#For each user, select their last name and phone numbers having the same area code
#as the first phone number of that user
 
SELECT u.country, array_collect([u.lastName,[ u.address.phones[$element.area = $[0].area].number ]]) as userInfo  FROM users u group by u.country
