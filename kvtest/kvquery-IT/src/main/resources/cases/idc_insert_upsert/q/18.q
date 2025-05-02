#TestDescription: insert Double.MAX_VALUE+1 and Double.MIN_VALUE-1 in long column
#Expected result: insert successful

insert into playerinformation (id,id1,name,age,ballsbowled,ballsplayed,strikerate,tier1rated,avg,fbin,bin,century,country,type,time,stats1) values (
100,
24,
"DOUBLE.MAX and MIN test",
2147483647+1,
9223372036854775807+1,
50432,
1.7976931348623157E308+1,
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
			"runs": -9223372036854775808-1,
			"hs": 243,
			"avg": 1.4E-45-1,
			"bf": 11549,
			"sr": 4.9E-324-1,
			"century": 25,
			"fifty": 20,
			"fours": 731,
			"sixes": 19
		}
	}
)

select id1, name, strikerate, playerinformation.stats1.Tests.sr 
from playerinformation 
where id1=24