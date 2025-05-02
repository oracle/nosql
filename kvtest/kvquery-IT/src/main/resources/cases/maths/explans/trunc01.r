compiled-query-plan

{
"query file" : "maths/q/trunc01.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "math_test",
      "row variable" : "$$math_test",
      "index used" : "primary index",
      "covering index" : true,
      "index scans" : [
        {
          "equality conditions" : {"id":1},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$math_test",
    "SELECT expressions" : [
      {
        "field name" : "trunc_3o14149",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 3.14149
            }
          ]
        }
      },
      {
        "field name" : "trunc_5o678",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 5.678
            }
          ]
        }
      },
      {
        "field name" : "trunc_1o2345",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 1.2345
            }
          ]
        }
      },
      {
        "field name" : "trunc_9o90",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 9.9
            }
          ]
        }
      },
      {
        "field name" : "trunc_1o5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 1.5
            }
          ]
        }
      },
      {
        "field name" : "trunc_2o5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 2.5
            }
          ]
        }
      },
      {
        "field name" : "trunc_9o5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 9.5
            }
          ]
        }
      },
      {
        "field name" : "trunc_o5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 0.5
            }
          ]
        }
      },
      {
        "field name" : "trunc_123456789o987654321",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 123456789.987654321
            }
          ]
        }
      },
      {
        "field name" : "trunc_Neg3o14149",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -3.14149
            }
          ]
        }
      },
      {
        "field name" : "trunc_Neg5o678",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -5.678
            }
          ]
        }
      },
      {
        "field name" : "trunc_Neg1o2345",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -1.2345
            }
          ]
        }
      },
      {
        "field name" : "trunc_Neg9o90",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -9.9
            }
          ]
        }
      },
      {
        "field name" : "trunc_Neg1o5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -1.5
            }
          ]
        }
      },
      {
        "field name" : "trunc_Neg2o5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -2.5
            }
          ]
        }
      },
      {
        "field name" : "trunc_Neg9o5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -9.5
            }
          ]
        }
      },
      {
        "field name" : "trunc_Nego5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -0.5
            }
          ]
        }
      },
      {
        "field name" : "trunc_Neg123456789o987654321",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -123456789.987654321
            }
          ]
        }
      },
      {
        "field name" : "trunc_3o15259_3",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 3.15259
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "trunc_5o678_2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 5.678
            },
            {
              "iterator kind" : "CONST",
              "value" : 2
            }
          ]
        }
      },
      {
        "field name" : "trunc_1o2345_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 1.2345
            },
            {
              "iterator kind" : "CONST",
              "value" : 1
            }
          ]
        }
      },
      {
        "field name" : "trunc_15o193_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 15.193
            },
            {
              "iterator kind" : "CONST",
              "value" : 1
            }
          ]
        }
      },
      {
        "field name" : "trunc__9o90_2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 9.9
            },
            {
              "iterator kind" : "CONST",
              "value" : 2
            }
          ]
        }
      },
      {
        "field name" : "trunc_1o5_3",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 1.5
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "trunc_2o5_0",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 2.5
            },
            {
              "iterator kind" : "CONST",
              "value" : 0
            }
          ]
        }
      },
      {
        "field name" : "trunc_9o5_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 9.5
            },
            {
              "iterator kind" : "CONST",
              "value" : 1
            }
          ]
        }
      },
      {
        "field name" : "trunc_o5_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 0.5
            },
            {
              "iterator kind" : "CONST",
              "value" : 1
            }
          ]
        }
      },
      {
        "field name" : "trunc_123456789o987654321_4",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 123456789.987654321
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "trunc_95o654535334545_2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 95.654535334545
            },
            {
              "iterator kind" : "CONST",
              "value" : 2
            }
          ]
        }
      },
      {
        "field name" : "trunc_123o456_10",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 123.456
            },
            {
              "iterator kind" : "CONST",
              "value" : 10
            }
          ]
        }
      },
      {
        "field name" : "trunc_19o25_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 19.25
            },
            {
              "iterator kind" : "CONST",
              "value" : 1
            }
          ]
        }
      },
      {
        "field name" : "trunc_28o73_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 28.73
            },
            {
              "iterator kind" : "CONST",
              "value" : 1
            }
          ]
        }
      },
      {
        "field name" : "trunc_34o32_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 34.32
            },
            {
              "iterator kind" : "CONST",
              "value" : 1
            }
          ]
        }
      },
      {
        "field name" : "trunc_45o39_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 45.39
            },
            {
              "iterator kind" : "CONST",
              "value" : 1
            }
          ]
        }
      },
      {
        "field name" : "trunc_45o39_o512",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 45.39
            },
            {
              "iterator kind" : "CONST",
              "value" : 0.512
            }
          ]
        }
      },
      {
        "field name" : "trunc_Neg3o15259_3",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -3.15259
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "trunc_Neg5o678_2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -5.678
            },
            {
              "iterator kind" : "CONST",
              "value" : 2
            }
          ]
        }
      },
      {
        "field name" : "trunc_Neg1o2345_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -1.2345
            },
            {
              "iterator kind" : "CONST",
              "value" : 1
            }
          ]
        }
      },
      {
        "field name" : "trunc_Neg15o193_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -15.193
            },
            {
              "iterator kind" : "CONST",
              "value" : 1
            }
          ]
        }
      },
      {
        "field name" : "trunc_Neg9o90_2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -9.9
            },
            {
              "iterator kind" : "CONST",
              "value" : 2
            }
          ]
        }
      },
      {
        "field name" : "trunc_Neg1o5_3",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -1.5
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "trunc_Neg2o5_0",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -2.5
            },
            {
              "iterator kind" : "CONST",
              "value" : 0
            }
          ]
        }
      },
      {
        "field name" : "trunc_Neg9o5_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -9.5
            },
            {
              "iterator kind" : "CONST",
              "value" : 1
            }
          ]
        }
      },
      {
        "field name" : "trunc_Nego5_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -0.5
            },
            {
              "iterator kind" : "CONST",
              "value" : 1
            }
          ]
        }
      },
      {
        "field name" : "trunc_Neg123456789o987654321_4",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -123456789.987654321
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "trunc_Neg95o654535334545_2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -95.654535334545
            },
            {
              "iterator kind" : "CONST",
              "value" : 2
            }
          ]
        }
      },
      {
        "field name" : "trunc_Neg123o456_10",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -123.456
            },
            {
              "iterator kind" : "CONST",
              "value" : 10
            }
          ]
        }
      },
      {
        "field name" : "trunc_Neg19o25_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -19.25
            },
            {
              "iterator kind" : "CONST",
              "value" : 1
            }
          ]
        }
      },
      {
        "field name" : "trunc_Neg28o73_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -28.73
            },
            {
              "iterator kind" : "CONST",
              "value" : 1
            }
          ]
        }
      },
      {
        "field name" : "trunc_Neg34o32_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -34.32
            },
            {
              "iterator kind" : "CONST",
              "value" : 1
            }
          ]
        }
      },
      {
        "field name" : "trunc_Neg45o39_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -45.39
            },
            {
              "iterator kind" : "CONST",
              "value" : 1
            }
          ]
        }
      },
      {
        "field name" : "trunc_3o14159_Neg3",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 3.15259
            },
            {
              "iterator kind" : "CONST",
              "value" : -3
            }
          ]
        }
      },
      {
        "field name" : "trunc_5o768_Neg2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 5.678
            },
            {
              "iterator kind" : "CONST",
              "value" : -2
            }
          ]
        }
      },
      {
        "field name" : "trunc_1o2345_Neg1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 1.2345
            },
            {
              "iterator kind" : "CONST",
              "value" : -1
            }
          ]
        }
      },
      {
        "field name" : "trunc_5o768_Neg1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 15.193
            },
            {
              "iterator kind" : "CONST",
              "value" : -1
            }
          ]
        }
      },
      {
        "field name" : "trunc__9o90_Neg2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 9.9
            },
            {
              "iterator kind" : "CONST",
              "value" : -2
            }
          ]
        }
      },
      {
        "field name" : "trunc_1o5_Neg3",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 1.5
            },
            {
              "iterator kind" : "CONST",
              "value" : -3
            }
          ]
        }
      },
      {
        "field name" : "trunc_2o5_Neg0",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 2.5
            },
            {
              "iterator kind" : "CONST",
              "value" : 0
            }
          ]
        }
      },
      {
        "field name" : "trunc_9o5_Neg1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 9.5
            },
            {
              "iterator kind" : "CONST",
              "value" : -1
            }
          ]
        }
      },
      {
        "field name" : "trunc_o5_Neg1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 0.5
            },
            {
              "iterator kind" : "CONST",
              "value" : -1
            }
          ]
        }
      },
      {
        "field name" : "trunc_123456789o987654321_Neg4",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 123456789.987654321
            },
            {
              "iterator kind" : "CONST",
              "value" : -4
            }
          ]
        }
      },
      {
        "field name" : "trunc_95o654535334545_Neg2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 95.654535334545
            },
            {
              "iterator kind" : "CONST",
              "value" : -2
            }
          ]
        }
      },
      {
        "field name" : "trunc_123o456_Neg10",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 123.456
            },
            {
              "iterator kind" : "CONST",
              "value" : -10
            }
          ]
        }
      },
      {
        "field name" : "trunc_19o25_Neg1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 19.25
            },
            {
              "iterator kind" : "CONST",
              "value" : -1
            }
          ]
        }
      },
      {
        "field name" : "trunc_28o73_Neg1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 28.73
            },
            {
              "iterator kind" : "CONST",
              "value" : -1
            }
          ]
        }
      },
      {
        "field name" : "trunc_34o32_Neg1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 34.32
            },
            {
              "iterator kind" : "CONST",
              "value" : -1
            }
          ]
        }
      },
      {
        "field name" : "trunc_45o39_Neg1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 45.39
            },
            {
              "iterator kind" : "CONST",
              "value" : -1
            }
          ]
        }
      },
      {
        "field name" : "trunc_Neg3o14159_Neg2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -3.15259
            },
            {
              "iterator kind" : "CONST",
              "value" : -3
            }
          ]
        }
      },
      {
        "field name" : "trunc_Neg5o768_Neg2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -5.678
            },
            {
              "iterator kind" : "CONST",
              "value" : -2
            }
          ]
        }
      },
      {
        "field name" : "trunc_Neg1o2345_Neg1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -1.2345
            },
            {
              "iterator kind" : "CONST",
              "value" : -1
            }
          ]
        }
      },
      {
        "field name" : "trunc_Neg5o768_Neg1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -15.193
            },
            {
              "iterator kind" : "CONST",
              "value" : -1
            }
          ]
        }
      },
      {
        "field name" : "trunc_Neg9o90_Neg2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -9.9
            },
            {
              "iterator kind" : "CONST",
              "value" : -2
            }
          ]
        }
      },
      {
        "field name" : "trunc_Neg1o5_Neg3",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -1.5
            },
            {
              "iterator kind" : "CONST",
              "value" : -3
            }
          ]
        }
      },
      {
        "field name" : "trunc_Neg2o5_Neg0",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -2.5
            },
            {
              "iterator kind" : "CONST",
              "value" : 0
            }
          ]
        }
      },
      {
        "field name" : "trunc_Neg9o5_Neg1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -9.5
            },
            {
              "iterator kind" : "CONST",
              "value" : -1
            }
          ]
        }
      },
      {
        "field name" : "trunc_Nego5_Neg1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -0.5
            },
            {
              "iterator kind" : "CONST",
              "value" : -1
            }
          ]
        }
      },
      {
        "field name" : "trunc_Neg123456789o987654321_Neg4",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -123456789.987654321
            },
            {
              "iterator kind" : "CONST",
              "value" : -4
            }
          ]
        }
      },
      {
        "field name" : "trunc_Neg95o654535334545_Neg2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -95.654535334545
            },
            {
              "iterator kind" : "CONST",
              "value" : -2
            }
          ]
        }
      },
      {
        "field name" : "trunc_Neg123o456_Neg10",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : -123.456
            },
            {
              "iterator kind" : "CONST",
              "value" : -10
            }
          ]
        }
      },
      {
        "field name" : "trunc_null",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : null
            }
          ]
        }
      },
      {
        "field name" : "trunc_null_null",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : null
            },
            {
              "iterator kind" : "CONST",
              "value" : null
            }
          ]
        }
      },
      {
        "field name" : "trunc_15o192_null",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 15.192
            },
            {
              "iterator kind" : "CONST",
              "value" : null
            }
          ]
        }
      },
      {
        "field name" : "trunc_null_2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : null
            },
            {
              "iterator kind" : "CONST",
              "value" : 2
            }
          ]
        }
      },
      {
        "field name" : "trunc_123o456_NaN",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 123.456
            },
            {
              "iterator kind" : "ACOS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 10
                }
              ]
            }
          ]
        }
      },
      {
        "field name" : "trunc_NaN",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ACOS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 10
                }
              ]
            }
          ]
        }
      },
      {
        "field name" : "trunc_Infinity",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "COT",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                }
              ]
            }
          ]
        }
      },
      {
        "field name" : "trunc_123o456_Infinity",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 123.456
            },
            {
              "iterator kind" : "COT",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                }
              ]
            }
          ]
        }
      },
      {
        "field name" : "trunc_NegInfinity",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LN",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                }
              ]
            }
          ]
        }
      },
      {
        "field name" : "trunc_123o456_NegInfinity",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 123.456
            },
            {
              "iterator kind" : "LN",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                }
              ]
            }
          ]
        }
      },
      {
        "field name" : "trunc_123o4567890_30",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 123.456789
            },
            {
              "iterator kind" : "CONST",
              "value" : 30
            }
          ]
        }
      },
      {
        "field name" : "test_trunc_123o4567890_10000",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : 123.456789
              },
              {
                "iterator kind" : "CONST",
                "value" : 10000
              }
            ]
          },
          "right operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : 123.456789
              },
              {
                "iterator kind" : "CONST",
                "value" : 30
              }
            ]
          }
        }
      },
      {
        "field name" : "trunc_123o4567890_Neg30",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 123.456789
            },
            {
              "iterator kind" : "CONST",
              "value" : -30
            }
          ]
        }
      },
      {
        "field name" : "test_trunc_123o4567890_Neg10000o25",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : 123.456789
              },
              {
                "iterator kind" : "CONST",
                "value" : -10000.25
              }
            ]
          },
          "right operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : 123.456789
              },
              {
                "iterator kind" : "CONST",
                "value" : -30
              }
            ]
          }
        }
      },
      {
        "field name" : "test_trunc_123o4567890_1o5",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : 123.456789
              },
              {
                "iterator kind" : "CONST",
                "value" : 1.5
              }
            ]
          },
          "right operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : 123.456789
              },
              {
                "iterator kind" : "CONST",
                "value" : 1
              }
            ]
          }
        }
      },
      {
        "field name" : "test_trunc_123o4567890_E",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : 123.456789
              },
              {
                "iterator kind" : "E",
                "input iterators" : [

                ]
              }
            ]
          },
          "right operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : 123.456789
              },
              {
                "iterator kind" : "CONST",
                "value" : 2
              }
            ]
          }
        }
      },
      {
        "field name" : "test_trunc_123o4567890_NegPI",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : 123.456789
              },
              {
                "iterator kind" : "ARITHMETIC_NEGATION",
                "input iterator" :
                {
                  "iterator kind" : "PI",
                  "input iterators" : [

                  ]
                }
              }
            ]
          },
          "right operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : 123.456789
              },
              {
                "iterator kind" : "CONST",
                "value" : -3
              }
            ]
          }
        }
      }
    ]
  }
}
}