#Test discription: no column list is specified and value list does not match (data type mis-match) columns in create table
#Expected result : syntax error

insert into playerinformation values (
100,
4,
"MS Dhoni",
33,
2334433,
504543432,
91.5,
88,
59,
"SGVsbG8gSG93IGFyZSBZb3U/",
"Tm9TcWw=",
100,
"India",
"international",
"1981-07-07T010:25:09",
{
		"Tests": {
			"matches": 90,
			"inns": 144,
			"notout": 16,
			"runs": 4876,
			"hs": 224,
			"avg": 38.09,
			"bf": 8248,
			"sr": 59.12,
			"century": 6,
			"fifty": 33,
			"fours": 544,
			"sixes": 78
		},
		"Odi": {
			"matches": 338,
			"inns": 286,
			"notout": 81,
			"runs": 10415,
			"hs": 183,
			"avg": 50.08,
			"bf": 11877,
			"sr": 87.69,
			"century": 10,
			"fifty": 70,
			"fours": 798,
			"sixes": 222
		},
		"T20": {
			"matches": 94,
			"inns": 81,
			"notout": 40,
			"runs": 1526,
			"hs": 56,
			"avg": 37.22,
			"bf": 1201,
			"sr": 127.06,
			"century": 0,
			"fifty": 2,
			"fours": 112,
			"sixes": 48
		}
	},
[{
		"last5intest": [130, 32, 39, 45, 98]
	}, {
		"last5inodi": [123, 48, 34, 23, 8]
	}, {
		"last5int20": [98, 98, 67, 13, 86]
	}],
{
		"city": "Cheenai",
		"country": "IND",
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
		"Dhoni": {
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
				"city": "Chennai",
				"country": "IND",
				"century": [{"test": 4},{ "odi": 2},{ "t20": 1}]
			}
}
}
)