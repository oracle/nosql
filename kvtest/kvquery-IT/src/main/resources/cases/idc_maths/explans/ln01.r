compiled-query-plan

{
"query file" : "idc_maths/q/ln01.q",
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
        "field name" : "ln0",
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "ln1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LN",
              "input iterators" : [
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
        "field name" : "ln2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LN",
              "input iterators" : [
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
        "field name" : "lnexp5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LN",
              "input iterators" : [
                {
                  "iterator kind" : "EXP",
                  "input iterators" : [
                    {
                      "iterator kind" : "CONST",
                      "value" : 5
                    }
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
        "field name" : "ln10",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LN",
              "input iterators" : [
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
        "field name" : "ln100",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LN",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 100
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
        "field name" : "ln10000",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LN",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 10000
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
        "field name" : "lnexp1000000000000",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LN",
              "input iterators" : [
                {
                  "iterator kind" : "EXP",
                  "input iterators" : [
                    {
                      "iterator kind" : "CONST",
                      "value" : 1000000000000
                    }
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
        "field name" : "ln01",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LN",
              "input iterators" : [
                {
                  "iterator kind" : "MULTIPLY_DIVIDE",
                  "operations and operands" : [
                    {
                      "operation" : "*",
                      "operand" :
                      {
                        "iterator kind" : "CONST",
                        "value" : 1
                      }
                    },
                    {
                      "operation" : "/",
                      "operand" :
                      {
                        "iterator kind" : "CONST",
                        "value" : 10.0
                      }
                    }
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
        "field name" : "ln001",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LN",
              "input iterators" : [
                {
                  "iterator kind" : "MULTIPLY_DIVIDE",
                  "operations and operands" : [
                    {
                      "operation" : "*",
                      "operand" :
                      {
                        "iterator kind" : "CONST",
                        "value" : 1
                      }
                    },
                    {
                      "operation" : "/",
                      "operand" :
                      {
                        "iterator kind" : "CONST",
                        "value" : 100.0
                      }
                    }
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
        "field name" : "ln0001",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LN",
              "input iterators" : [
                {
                  "iterator kind" : "MULTIPLY_DIVIDE",
                  "operations and operands" : [
                    {
                      "operation" : "*",
                      "operand" :
                      {
                        "iterator kind" : "CONST",
                        "value" : 1
                      }
                    },
                    {
                      "operation" : "/",
                      "operand" :
                      {
                        "iterator kind" : "CONST",
                        "value" : 1000.0
                      }
                    }
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
        "field name" : "lnexp0o00001",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LN",
              "input iterators" : [
                {
                  "iterator kind" : "EXP",
                  "input iterators" : [
                    {
                      "iterator kind" : "CONST",
                      "value" : 1.0E-5
                    }
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
        "field name" : "ln123o456",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LN",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 123.456
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
        "field name" : "lnneg1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LN",
              "input iterators" : [
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
        "field name" : "lnneg05",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LN",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -0.5
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
        "field name" : "lnneg1000",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LN",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -1000
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
        "field name" : "lnneg01",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LN",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -0.1
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
        "field name" : "lnpow",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "LN",
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
                      "value" : 1000000
                    }
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
      }
    ]
  }
}
}