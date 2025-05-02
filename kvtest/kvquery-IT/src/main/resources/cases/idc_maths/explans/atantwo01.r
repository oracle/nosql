compiled-query-plan

{
"query file" : "idc_maths/q/atantwo01.q",
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
      "target table" : "functional_test",
      "row variable" : "$$functional_test",
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
    "FROM variable" : "$$functional_test",
    "SELECT expressions" : [
      {
        "field name" : "atan2_3o15259_3",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_5o678_2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_1o2345_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_15o193_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2__9o90_2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_1o5_3",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_9o5_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_o5_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_123456789o987654321_4",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_95o654535334545_2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_123o456_10",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_19o25_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_28o73_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_34o32_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_45o39_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_45o39_o512",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_longmax_5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 9223372036854775807
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 5
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_inf_5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 3.7976931348623157E+308
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 5
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_doublemin_5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 1.7976931348623157E-308
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 5
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_Neg3o15259_3",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_Neg5o678_2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_Neg1o2345_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_Neg15o193_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_Neg9o90_2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_Neg1o5_3",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_Neg9o5_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_Nego5_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_Neg123456789o987654321_4",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_Neg95o654535334545_2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_Neg123o456_10",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_Neg19o25_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_Neg28o73_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_Neg34o32_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_Neg45o39_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_longmin_5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -9223372036854775808
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 5
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_neginf_5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -5.7976931348623157E+308
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 5
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_negdoublemin_5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -1.7976931348623157E-308
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 5
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_3o14159_Neg3",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_5o768_Neg2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_1o2345_Neg1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_15o193_Neg1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2__9o90_Neg2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_1o5_Neg3",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_9o5_Neg1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_o5_Neg1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_123456789o987654321_Neg4",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_95o654535334545_Neg2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_123o456_Neg10",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_19o25_Neg1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_28o73_Neg1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_34o32_Neg1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_45o39_Neg1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_longmax_neg5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 9223372036854775807
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -5
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_inf_neg5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 3.7976931348623157E+308
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -5
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_doublemin_neg5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 1.7976931348623157E-308
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -5
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_Neg3o14159_Neg2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_Neg5o768_Neg2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_Neg1o2345_Neg1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_Neg15o193_Neg1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_Neg9o90_Neg2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_Neg1o5_Neg3",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_Neg9o5_Neg1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_Nego5_Neg1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_Neg123456789o987654321_Neg4",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_Neg95o654535334545_Neg2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_Neg123o456_Neg10",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_longmin_neg5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -9223372036854775808
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -5
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_neginf_neg5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -5.7976931348623157E+308
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -5
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_negdoublemin_neg5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -1.7976931348623157E-308
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -5
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_0_2o5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 2.5
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_0_neg2o5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -2.5
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_2o5_0",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_neg2o5_0",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_0_neg0",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_0_0",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_neg0_0",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_neg0_neg0",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_null_null",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_15o192_null",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_null_2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_123o456_12",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 123.456
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 12
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_123o456_neg12",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 123.456
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -12
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_longmin_neg20",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -9223372036854775808
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -20
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_longmax_20",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 9223372036854775807
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 20
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_negdoublemin_neg500",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -1.7976931348623157E-308
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -500
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_doublemin_500",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 1.7976931348623157E-308
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 500
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_100_neginf",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 100
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -5.7976931348623157E+308
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_100_inf",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 100
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 3.7976931348623157E+308
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_123o456_1o2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 123.456
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 1.2
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_123o456_neg1o2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 123.456
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -1.2
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_123_neg1o5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 123
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -1.5
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_123_pi",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 123
                },
                {
                  "iterator kind" : "PI",
                  "input iterators" : [

                  ]
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "atan2_123_negpi",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ATAN2",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 123
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
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      }
    ]
  }
}
}