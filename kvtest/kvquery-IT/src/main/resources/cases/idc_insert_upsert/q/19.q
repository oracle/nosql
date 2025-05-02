#TestDescription: insert numeric value in E format in integer/long/float/double
#Expected result: insert successful

insert into playerinformation (id,id1,name,age,ballsbowled,ballsplayed,strikerate,tier1rated,avg,fbin,bin,century,country,type,time,stats1) values (
100,
25,
"E value test",
12E1,
3.4875E38,
50432,
1.79769313457E308,
true,
3.4025E8+1,
"SGVsbG8gSG93IGFyZSBZb3U/",
"Tm9TcWw=",
100,
"India",
"international",
"1987-04-30T07:45:00",
{
		"Tests": {
			"matches": 12E12,
			"inns": 131,
			"notout": 8,
			"runs": 233232E4,
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

select *
from playerinformation 
where id1=25