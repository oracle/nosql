#Test discription: update an existing row with upsert
#Expected result: upsert successful

upsert into playerinformation values (
100,
3,
"Shikhar Dhawan",
33,
23233,
5045432,
73.5,
true,
44,
"SGVsbG8gSG93IGFyZSBZb3U/",
"Tm9TcWw=",
100,
"India",
"international",
"1985-11-05T012:45:00",
{
		"Tests": {
			"matches": 34,
			"inns": 58,
			"notout": 1,
			"runs": 2315,
			"hs": 190,
			"avg": 40.61,
			"bf": 3458,
			"sr": 66.95,
			"century": 7,
			"fifty": 5,
			"fours": 316,
			"sixes": 12
		},
		"Odi": {
			"matches": 123,
			"inns": 122,
			"notout": 7,
			"runs": 5178,
			"hs": 137,
			"avg": 45.03,
			"bf": 5539,
			"sr": 93.48,
			"century": 15,
			"fifty": 27,
			"fours": 642,
			"sixes": 64
		},
		"T20": {
			"matches": 47,
			"inns": 46,
			"notout": 3,
			"runs": 1261,
			"hs": 92,
			"avg": 29.33,
			"bf": 945,
			"sr": 133.44,
			"century": 0,
			"fifty": 9,
			"fours": 140,
			"sixes": 42
		}
	},
[{
		"last5intest": [10, 3, 39, 23, 98]
	}, {
		"last5inodi": [13, 48, 34, 12, 18]
	}, {
		"last5int20": [45, 98, 67, 13, 86]
	}],
{
		"city": "Wellington",
		"country": "NZ",
		"runs": [{
			"test": 10,
			"odi": 43,
			"t20": 41
		}, {
			"test": 12,
			"odi": 51,
			"t20": 19
		}, {
			"test": 58,
			"odi": 51,
			"t20": 89
		}, {
			"test": 138,
			"odi": 5,
			"t20": 110
		}, {
			"test": 58,
			"odi": 4,
		    "t20": 1
		}],
		"ptr": "city"
	},
{
		"Shikhar": {
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
				"city": "Wellington",
				"country": "NZ",
				"century": [{"test": 4},{ "odi": 2},{ "t20": 1}]
			}
}
}
)
select stats1, stats2, stats3 
from playerinformation 
where id1 = 3
