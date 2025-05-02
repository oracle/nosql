#Using Math function on JSON collection field that does not exists and a positive result

select trunc(abs(t.address.grid),7) as grid, trunc(sqrt(t.numbers.num2),7) as sqrt1o001 from jsonCollection_test t where id =1