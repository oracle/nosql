compiled-query-plan

{
"query file" : "idc_maths/q/power01.q",
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
        "field name" : "power_0_0",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
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
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_0_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 1
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_1_0",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 1
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_1_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 1
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 1
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_2_4",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 2
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 4
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_2_64",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 2
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 64
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_2_128_2",
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
                    "iterator kind" : "POWER",
                    "input iterators" : [
                      {
                        "iterator kind" : "CONST",
                        "value" : 2
                      },
                      {
                        "iterator kind" : "CONST",
                        "value" : 128
                      }
                    ]
                  }
                },
                {
                  "operation" : "/",
                  "operand" :
                  {
                    "iterator kind" : "CONST",
                    "value" : 2
                  }
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_2_256",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 2
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 256
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "pow_3_9",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 3
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 9
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_100000000_2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 100000000
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 2
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_100000000_10",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 100000000
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 10
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_100E24_100E24",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 1E+26
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 1E+26
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg0_100E24",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 1E+26
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_0_100E24",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 1E+26
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_100E24_neg0",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 1E+26
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_100E24_0",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 1E+26
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_5_25",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 5.0
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 25.0
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_05_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
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
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_1_05",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 1
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 0.5
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_2_05",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 2
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 0.5
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_3_03",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 3
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 0.3
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_4_05",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 4
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 0.5
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_1000000000_05",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 1000000000
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 0.5
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_001_001",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 0.01
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 0.01
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_0_neg0",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
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
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_0_neg1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -1
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_1_neg0",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 1
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_1_neg1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 1
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -1
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_2_neg4",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 2
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -4
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_2_neg64",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 2
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -64
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_2_neg128_2",
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
                    "iterator kind" : "POWER",
                    "input iterators" : [
                      {
                        "iterator kind" : "CONST",
                        "value" : 2
                      },
                      {
                        "iterator kind" : "CONST",
                        "value" : -128
                      }
                    ]
                  }
                },
                {
                  "operation" : "/",
                  "operand" :
                  {
                    "iterator kind" : "CONST",
                    "value" : 2
                  }
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_2_neg256",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 2
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -256
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "pow_3_neg9",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 3
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -9
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_100000000_neg2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 100000000
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -2
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_100000000_neg10",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 100000000
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -10
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_100E24_neg100E24",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 1E+26
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -1E+26
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_5_neg25",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 5.0
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -25.0
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_05_neg1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
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
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_1_neg05",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 1
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -0.5
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_2_neg05",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 2
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -0.5
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_3_neg03",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 3
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -0.3
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_4_neg05",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 4
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -0.5
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_1000000000_neg05",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 1000000000
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -0.5
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_001_neg001",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 0.01
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -0.01
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg0_0",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
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
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg0_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 1
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg1_0",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -1
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg1_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -1
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 1
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg2_4",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -2
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 4
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg2_64",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -2
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 64
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg2_128_2",
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
                    "iterator kind" : "POWER",
                    "input iterators" : [
                      {
                        "iterator kind" : "CONST",
                        "value" : -2
                      },
                      {
                        "iterator kind" : "CONST",
                        "value" : 128
                      }
                    ]
                  }
                },
                {
                  "operation" : "/",
                  "operand" :
                  {
                    "iterator kind" : "CONST",
                    "value" : 2
                  }
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg2_256",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -2
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 256
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "pow_neg3_9",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -3
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 9
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg100000000_2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -100000000
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 2
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg100000000_10",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -100000000
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 10
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg100E24_100E24",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -1E+26
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 1E+26
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg5_25",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -5.0
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 25.0
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg05_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
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
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg1_05",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -1
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 0.5
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg2_05",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -2
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 0.5
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg3_03",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -3
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 0.3
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg4_05",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -4
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 0.5
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg1000000000_05",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -1000000000
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 0.5
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg001_001",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -0.01
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 0.01
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg0_neg0",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
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
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg0_neg1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -1
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg1_neg0",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -1
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg1_neg1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -1
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -1
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg2_neg4",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -2
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -4
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg2_neg64",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -2
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -64
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg2_neg128_2",
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
                    "iterator kind" : "POWER",
                    "input iterators" : [
                      {
                        "iterator kind" : "CONST",
                        "value" : -2
                      },
                      {
                        "iterator kind" : "CONST",
                        "value" : -128
                      }
                    ]
                  }
                },
                {
                  "operation" : "/",
                  "operand" :
                  {
                    "iterator kind" : "CONST",
                    "value" : 2
                  }
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg2_neg256",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -2
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -256
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "pow_neg3_neg9",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -3
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -9
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg100000000_neg2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -100000000
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -2
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg100000000_neg10",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -100000000
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -10
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg100E24_neg100E24",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -1E+26
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -1E+26
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg0_neg100E24",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -1E+26
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_0_neg100E24",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -1E+26
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg100E24_neg0",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -1E+26
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg100E24_0",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -1E+26
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg5_neg25",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -5.0
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -25.0
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg05_neg1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
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
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg1_neg05",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -1
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -0.5
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg2_neg05",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -2
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -0.5
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg3_neg03",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -3
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -0.3
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg4_neg05",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -4
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -0.5
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg1000000000_neg05",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -1000000000
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -0.5
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      },
      {
        "field name" : "power_neg001_neg001",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "POWER",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -0.01
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -0.01
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 3
            }
          ]
        }
      }
    ]
  }
}
}