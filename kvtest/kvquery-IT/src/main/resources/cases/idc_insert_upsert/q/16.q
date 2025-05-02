#TestDescription: insert Float.MAX_VALUE+1 and Float.MIN_VALUE-1 in float column
#Expected result: insert successful

insert into playerinformation (id,id1,name,age,ballsbowled,ballsplayed,strikerate,tier1rated,avg,fbin,bin,century,country,type,time,stats1) values (
100,
22,
"Float.MAX_VALUE and MIN_VALUE",
2147483647+1,
2147483647-1,
50432,
83.5,
true,
3.4028235E38+1,
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
			"avg": 1.4E-45-1,
			"bf": 11549,
			"sr": 57.26,
			"century": 25,
			"fifty": 20,
			"fours": 731,
			"sixes": 19
		}
	}
)

select id1, avg,playerinformation.stats1.Tests.avg
from playerinformation 
where id1=22