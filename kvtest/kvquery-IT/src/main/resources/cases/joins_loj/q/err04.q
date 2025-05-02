# Error: The reference to column c1 is ambiguous because more than one tables has a column with this name

select a.ida as a_ida, c1
from A a left outer join A.G g on a.ida = g.ida
order by a.ida
