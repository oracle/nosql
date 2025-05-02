#TestDescription: insert Integer.MAX_VALUE+1 and Integer.MIN_VALUE-1 in integer column
#Expected result: insert successful

insert into playerinformation (id,id1,name,age,ballsbowled,ballsplayed,strikerate,tier1rated,avg,fbin,bin,century,country,type,time,stats1) values (
100,
21,
"Integer.MAX_VALUE and MIN_VALUE test",
2147483647+1,
2147483647-1,
50432,
83.5,
true,
48,
"SGVsbG8gSG93IGFyZSBZb3U/",
"Tm9TcWw=",
100,
"India",
"international",
"1987-04-30T07:45:00",
{
		"Tests": {
			"matches": 2147483647-1,
			"inns": 131,
			"notout": 8,
			"runs": 2147483647+1,
			"hs": 243,
			"avg": 53.76,
			"bf": 11549,
			"sr": 57.26,
			"century": 25,
			"fifty": 20,
			"fours": 731,
			"sixes": 19
		}	
}
)

select id1, name, age, playerinformation.stats1.Tests.matches
from 
playerinformation 
where id1=21