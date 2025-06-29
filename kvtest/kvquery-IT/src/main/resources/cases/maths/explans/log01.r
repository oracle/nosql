compiled-query-plan

{
"query file" : "maths/q/log01.q",
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
        "field name" : "log_1000_10",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 1000
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 10
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_64_2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 64
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 2
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_27_3",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 27
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 3
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_10E100_10",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "POWER",
                  "input iterators" : [
                    {
                      "iterator kind" : "CONST",
                      "value" : 10
                    },
                    {
                      "iterator kind" : "CONST",
                      "value" : 100
                    }
                  ]
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 10
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_1_10",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 1
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 10
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_1_2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 1
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 2
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_1_20",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 1
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 20
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_1_16",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 1
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 16
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_1_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 2
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 1
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_10_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 10
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 1
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_20_1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 20
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 1
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_neg1_10",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -100
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 10
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_neg1024_2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -1024
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 2
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_100_0",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 100
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_2o5_2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 2.5
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 2
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_o5_o75",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 0.5
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 0.75
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_e_pi",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "E",
                  "input iterators" : [

                  ]
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
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_100_neg10",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 100
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -10
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_256_neg2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 256
                },
                {
                  "iterator kind" : "CONST",
                  "value" : -2
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_null_10",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : null
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 10
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_10_null",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 10
                },
                {
                  "iterator kind" : "CONST",
                  "value" : null
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 4
            }
          ]
        }
      },
      {
        "field name" : "log_null_null",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LOG",
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
              "value" : 4
            }
          ]
        }
      }
    ]
  }
}
}