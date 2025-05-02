select distinct count(f.lastname) as lastname,f.isEmp,f.lastname from SimpleDatatype f group by f.isEmp,f.lastname,f.bin
