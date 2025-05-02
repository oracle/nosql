compiled-query-plan

{
"query file" : "idc_maths/q/trunc01.q",
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
        "field name" : "trunc_longmax_5_divide_int_max",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "MULTIPLY_DIVIDE",
              "operations and operands" : [
                {
                  "operation" : "*",
                  "operand" :
                  {
                    "iterator kind" : "TRUNC",
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
                  }
                },
                {
                  "operation" : "/",
                  "operand" :
                  {
                    "iterator kind" : "CONST",
                    "value" : 2.147483647E9
                  }
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 2
            }
          ]
        }
      },
      {
        "field name" : "trunc_inf_5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "TRUNC",
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
        "field name" : "trunc_doublemin_5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "TRUNC",
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
        "field name" : "trunc_longmin_5_divide_int_max",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "MULTIPLY_DIVIDE",
              "operations and operands" : [
                {
                  "operation" : "*",
                  "operand" :
                  {
                    "iterator kind" : "TRUNC",
                    "input iterators" : [
                      {
                        "iterator kind" : "CONST",
                        "value" : -9223372036854775807
                      },
                      {
                        "iterator kind" : "CONST",
                        "value" : 5
                      }
                    ]
                  }
                },
                {
                  "operation" : "/",
                  "operand" :
                  {
                    "iterator kind" : "CONST",
                    "value" : 2.147483647E9
                  }
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 2
            }
          ]
        }
      },
      {
        "field name" : "trunc_neginf_5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "TRUNC",
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
        "field name" : "trunc_negdoublemin_5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "TRUNC",
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
        "field name" : "trunc_15o193_Neg1",
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
        "field name" : "trunc_longmax_neg5_divide_int_max",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "MULTIPLY_DIVIDE",
              "operations and operands" : [
                {
                  "operation" : "*",
                  "operand" :
                  {
                    "iterator kind" : "TRUNC",
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
                  }
                },
                {
                  "operation" : "/",
                  "operand" :
                  {
                    "iterator kind" : "CONST",
                    "value" : 2.147483647E9
                  }
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 2
            }
          ]
        }
      },
      {
        "field name" : "trunc_inf_neg5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "TRUNC",
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
        "field name" : "trunc_doublemin_neg5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "TRUNC",
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
        "field name" : "trunc_Neg15o193_Neg1",
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
        "field name" : "trunc_longmin_neg5_divide_int_max",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "MULTIPLY_DIVIDE",
              "operations and operands" : [
                {
                  "operation" : "*",
                  "operand" :
                  {
                    "iterator kind" : "TRUNC",
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
                  }
                },
                {
                  "operation" : "/",
                  "operand" :
                  {
                    "iterator kind" : "CONST",
                    "value" : 2.147483647E9
                  }
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 2
            }
          ]
        }
      },
      {
        "field name" : "trunc_neginf_neg5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "TRUNC",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -3.7976931348623157E+308
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
        "field name" : "trunc_negdoublemin_neg5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "TRUNC",
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
        "field name" : "trunc_acos5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ACOS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 5
                }
              ]
            }
          ]
        }
      },
      {
        "field name" : "trunc_acos5_asin5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ACOS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 5
                }
              ]
            },
            {
              "iterator kind" : "ASIN",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 5
                }
              ]
            }
          ]
        }
      },
      {
        "field name" : "trunc_15o192_acosneg5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 15.192
            },
            {
              "iterator kind" : "ACOS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -5
                }
              ]
            }
          ]
        }
      },
      {
        "field name" : "trunc_asinneg5_2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "ASIN",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -5
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 2
            }
          ]
        }
      },
      {
        "field name" : "trunc_123456789o987654321_10000_30",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : 123456789.987654321
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
                "value" : 123456789.987654321
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
        "field name" : "trunc_123456789o987654321_neg10000_30",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : 123456789.987654321
              },
              {
                "iterator kind" : "CONST",
                "value" : -10000
              }
            ]
          },
          "right operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : 123456789.987654321
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
        "field name" : "trunc_neg123456789o987654321_neg10000_30",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : -123456789.987654321
              },
              {
                "iterator kind" : "CONST",
                "value" : -10000
              }
            ]
          },
          "right operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : -123456789.987654321
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
        "field name" : "trunc_neg123456789o987654321_10000_30",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : -123456789.987654321
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
                "value" : -123456789.987654321
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
        "field name" : "trunc_123456789o987654321_10000o99999_30",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : 123456789.987654321
              },
              {
                "iterator kind" : "CONST",
                "value" : 10000.99999
              }
            ]
          },
          "right operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : 123456789.987654321
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
        "field name" : "trunc_123456789o987654321_neg10000o99999_30",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : 123456789.987654321
              },
              {
                "iterator kind" : "CONST",
                "value" : -10000.99999
              }
            ]
          },
          "right operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : 123456789.987654321
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
        "field name" : "trunc_neg123456789o987654321_neg10000o99999_30",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : -123456789.987654321
              },
              {
                "iterator kind" : "CONST",
                "value" : -10000.99999
              }
            ]
          },
          "right operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : -123456789.987654321
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
        "field name" : "trunc_neg123456789o987654321_10000o99999_30",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : -123456789.987654321
              },
              {
                "iterator kind" : "CONST",
                "value" : 10000.99999
              }
            ]
          },
          "right operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : -123456789.987654321
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
        "field name" : "trunc_123456789o987654321_pi",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : 123456789.987654321
              },
              {
                "iterator kind" : "PI",
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
                "value" : 123456789.987654321
              },
              {
                "iterator kind" : "CONST",
                "value" : 3
              }
            ]
          }
        }
      },
      {
        "field name" : "trunc_123456789o987654321_negpi",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : 123456789.987654321
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
                "value" : 123456789.987654321
              },
              {
                "iterator kind" : "CONST",
                "value" : -3
              }
            ]
          }
        }
      },
      {
        "field name" : "trunc_123456789o987654321_1o99999",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : 123456789.987654321
              },
              {
                "iterator kind" : "CONST",
                "value" : 1.999999
              }
            ]
          },
          "right operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : 123456789.987654321
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
        "field name" : "trunc_123456789o987654321_neg1o99999",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : 123456789.987654321
              },
              {
                "iterator kind" : "CONST",
                "value" : -1.999999
              }
            ]
          },
          "right operand" :
          {
            "iterator kind" : "TRUNC",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : 123456789.987654321
              },
              {
                "iterator kind" : "CONST",
                "value" : -1
              }
            ]
          }
        }
      },
      {
        "field name" : "trunc_123o456_12",
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
              "value" : 12
            }
          ]
        }
      },
      {
        "field name" : "trunc_123o456_neg12",
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
              "value" : -12
            }
          ]
        }
      },
      {
        "field name" : "trunc_longmin_neg20",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "TRUNC",
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
        "field name" : "trunc_longmax_20",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "TRUNC",
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
        "field name" : "trunc_negdoublemin_neg500",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "TRUNC",
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
        "field name" : "trunc_doublemin_500",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "TRUNC",
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
        "field name" : "trunc_100_neginf",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
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
        }
      },
      {
        "field name" : "trunc_100_inf",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
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
        }
      },
      {
        "field name" : "trunc_123o456_1o2",
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
              "value" : 1.2
            }
          ]
        }
      },
      {
        "field name" : "trunc_123o456_neg1o2",
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
              "value" : -1.2
            }
          ]
        }
      },
      {
        "field name" : "trunc_123_neg1o5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
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
        }
      },
      {
        "field name" : "trunc_123_pi",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
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
        }
      },
      {
        "field name" : "trunc_123_negpi",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
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
        }
      }
    ]
  }
}
}