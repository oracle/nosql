#TestDescription: Use of Logical operators should be flagged as an error. X|Y
#Expected result: return error

select regex_like(name,"Virat Kohli|test1") from playerinfo