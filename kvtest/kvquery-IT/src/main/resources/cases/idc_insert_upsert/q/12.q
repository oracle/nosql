#TestDescription: column list specified including columns with default not null in create table and default clause in value list
#Expected result: insert successful

insert into playerinformation (id,id1,name,age,ballsbowled,ballsplayed,strikerate,tier1rated,avg,fbin,bin,century,country,type,time,stats1,stats2,stats3,json) values (
100,
7,
"Rishabh Pant",
31,
5266,
50432,
83.5,
true,
48,
"SGVsbG8gSG93IGFyZSBZb3U/",
"Tm9TcWw=",
default,
"India",
"international",
"1987-04-30T07:45:00",
{
		"Tests": {
			"matches": 77,
			"inns": 131,
			"notout": 8,
			"runs": 6613,
			"hs": 243,
			"avg": 53.76,
			"bf": 11549,
			"sr": 57.26,
			"century": 25,
			"fifty": 20,
			"fours": 731,
			"sixes": 19
		},
		"Odi": {
			"matches": 77,
			"inns": 131,
			"notout": 8,
			"runs": 6613,
			"hs": 243,
			"avg": 53.76,
			"bf": 11549,
			"sr": 57.26,
			"century": 25,
			"fifty": 20,
			"fours": 731,
			"sixes": 19
		},
		"T20": {
			"matches": 77,
			"inns": 131,
			"notout": 8,
			"runs": 6613,
			"hs": 243,
			"avg": 53.76,
			"bf": 11549,
			"sr": 57.26,
			"century": 25,
			"fifty": 20,
			"fours": 731,
			"sixes": 19
		}
	},
[{
		"last5intest": [12, 23, 33, 56, 98]
	}, {
		"last5inodi": [123, 98, 34, 22, 88]
	}, {
		"last5int20": [22, 98, 67, 10, 76]
	}],
{
		"city": "Adelaide",
		"country": "AUS",
		"runs": [{
			"test": 100,
			"odi": 43,
			"t20": 41
		}, {
			"test": 112,
			"odi": 51,
			"t20": 19
		}, {
			"test": 58,
			"odi": 52,
			"t20": 81
		}, {
			"test": 38,
			"odi": 53,
			"t20": 11
		}, {
			"test": 58,
			"odi": 4,
		    "t20": 1
		}],
		"ptr": "city"
	},
{
		"Virat": {
			"age": 30,
			"ballsbowled": 5266,
			"ballsplayed": 50432,
			"strikerate": 83.5,
			"tier1rated": true,
			"avg": 56.87,
			"fbin": "SGVsbG8gSG93IGFyZSBZb3U/",
			"bin": "Tm9TcWw=",
			"century": 100,
			"country": "India",
			"type": "international",
			"dob": "1988-11-05T10:45:00",
			"stats1": {
				"Tests": {
					"matches": 77,
					"inns": 131,
					"notout": 8,
					"runs": 6613,
					"hs": 243,
					"avg": 53.76,
					"bf": 11549,
					"sr": 57.26,
					"century": 25,
					"fifty": 20,
					"fours": 731,
					"sixes": 19
				},
				"Odi": {
					"matches": 77,
					"inns": 131,
					"notout": 8,
					"runs": 6613,
					"hs": 243,
					"avg": 53.76,
					"bf": 11549,
					"sr": 57.26,
					"century": 25,
					"fifty": 20,
					"fours": 731,
					"sixes": 19
				},
				"T20": {
					"matches": 77,
					"inns": 131,
					"notout": 8,
					"runs": 6613,
					"hs": 243,
					"avg": 53.76,
					"bf": 11549,
					"sr": 57.26,
					"century": 25,
					"fifty": 20,
					"fours": 731,
					"sixes": 19
				}
			},
			"stats2": [{
				"last5intest": [12, 23, 33, 56, 98]
			}, {
				"last5inodi": [123, 98, 34, 22, 88]
			}, {
				"last5int20": [22, 98, 67, 10, 76]
			}],
			"stats3": {
				"city": "Adelaide oval",
				"country": "AUS",
				"century": [{"test": 4},{ "odi": 2},{ "t20": 1}]
			}
}
}
)
select century
from playerinformation 
where id1 = 7