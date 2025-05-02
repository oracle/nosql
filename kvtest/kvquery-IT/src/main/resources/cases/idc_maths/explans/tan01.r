compiled-query-plan

{
"query file" : "idc_maths/q/tan01.q",
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
        "field name" : "tan0",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "TAN",
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
        "field name" : "tan30",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "TAN",
              "input iterators" : [
                {
                  "iterator kind" : "MULTIPLY_DIVIDE",
                  "operations and operands" : [
                    {
                      "operation" : "*",
                      "operand" :
                      {
                        "iterator kind" : "PI",
                        "input iterators" : [

                        ]
                      }
                    },
                    {
                      "operation" : "/",
                      "operand" :
                      {
                        "iterator kind" : "CONST",
                        "value" : 6
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
        "field name" : "tan45",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "TAN",
              "input iterators" : [
                {
                  "iterator kind" : "MULTIPLY_DIVIDE",
                  "operations and operands" : [
                    {
                      "operation" : "*",
                      "operand" :
                      {
                        "iterator kind" : "PI",
                        "input iterators" : [

                        ]
                      }
                    },
                    {
                      "operation" : "/",
                      "operand" :
                      {
                        "iterator kind" : "CONST",
                        "value" : 4
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
        "field name" : "tan60",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "TAN",
              "input iterators" : [
                {
                  "iterator kind" : "MULTIPLY_DIVIDE",
                  "operations and operands" : [
                    {
                      "operation" : "*",
                      "operand" :
                      {
                        "iterator kind" : "PI",
                        "input iterators" : [

                        ]
                      }
                    },
                    {
                      "operation" : "/",
                      "operand" :
                      {
                        "iterator kind" : "CONST",
                        "value" : 3
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
        "field name" : "tan90",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "TAN",
              "input iterators" : [
                {
                  "iterator kind" : "MULTIPLY_DIVIDE",
                  "operations and operands" : [
                    {
                      "operation" : "*",
                      "operand" :
                      {
                        "iterator kind" : "PI",
                        "input iterators" : [

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
        "field name" : "tan180",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "TAN",
              "input iterators" : [
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
        "field name" : "tan270",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "TAN",
              "input iterators" : [
                {
                  "iterator kind" : "MULTIPLY_DIVIDE",
                  "operations and operands" : [
                    {
                      "operation" : "*",
                      "operand" :
                      {
                        "iterator kind" : "CONST",
                        "value" : 3
                      }
                    },
                    {
                      "operation" : "*",
                      "operand" :
                      {
                        "iterator kind" : "PI",
                        "input iterators" : [

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
        "field name" : "tan360",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "TAN",
              "input iterators" : [
                {
                  "iterator kind" : "MULTIPLY_DIVIDE",
                  "operations and operands" : [
                    {
                      "operation" : "*",
                      "operand" :
                      {
                        "iterator kind" : "CONST",
                        "value" : 2
                      }
                    },
                    {
                      "operation" : "*",
                      "operand" :
                      {
                        "iterator kind" : "PI",
                        "input iterators" : [

                        ]
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
        "field name" : "tan0neg",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "TAN",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 0.0
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
        "field name" : "tan30neg",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "TAN",
              "input iterators" : [
                {
                  "iterator kind" : "MULTIPLY_DIVIDE",
                  "operations and operands" : [
                    {
                      "operation" : "*",
                      "operand" :
                      {
                        "iterator kind" : "ARITHMETIC_NEGATION",
                        "input iterator" :
                        {
                          "iterator kind" : "PI",
                          "input iterators" : [

                          ]
                        }
                      }
                    },
                    {
                      "operation" : "/",
                      "operand" :
                      {
                        "iterator kind" : "CONST",
                        "value" : 6
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
        "field name" : "tan45neg",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "TAN",
              "input iterators" : [
                {
                  "iterator kind" : "MULTIPLY_DIVIDE",
                  "operations and operands" : [
                    {
                      "operation" : "*",
                      "operand" :
                      {
                        "iterator kind" : "ARITHMETIC_NEGATION",
                        "input iterator" :
                        {
                          "iterator kind" : "PI",
                          "input iterators" : [

                          ]
                        }
                      }
                    },
                    {
                      "operation" : "/",
                      "operand" :
                      {
                        "iterator kind" : "CONST",
                        "value" : 4
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
        "field name" : "tan60neg",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "TAN",
              "input iterators" : [
                {
                  "iterator kind" : "MULTIPLY_DIVIDE",
                  "operations and operands" : [
                    {
                      "operation" : "*",
                      "operand" :
                      {
                        "iterator kind" : "ARITHMETIC_NEGATION",
                        "input iterator" :
                        {
                          "iterator kind" : "PI",
                          "input iterators" : [

                          ]
                        }
                      }
                    },
                    {
                      "operation" : "/",
                      "operand" :
                      {
                        "iterator kind" : "CONST",
                        "value" : 3
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
        "field name" : "tan90neg",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "TAN",
              "input iterators" : [
                {
                  "iterator kind" : "MULTIPLY_DIVIDE",
                  "operations and operands" : [
                    {
                      "operation" : "*",
                      "operand" :
                      {
                        "iterator kind" : "ARITHMETIC_NEGATION",
                        "input iterator" :
                        {
                          "iterator kind" : "PI",
                          "input iterators" : [

                          ]
                        }
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
        "field name" : "tan180neg",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "TAN",
              "input iterators" : [
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
      },
      {
        "field name" : "tan270neg",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "TAN",
              "input iterators" : [
                {
                  "iterator kind" : "MULTIPLY_DIVIDE",
                  "operations and operands" : [
                    {
                      "operation" : "*",
                      "operand" :
                      {
                        "iterator kind" : "CONST",
                        "value" : -3
                      }
                    },
                    {
                      "operation" : "*",
                      "operand" :
                      {
                        "iterator kind" : "PI",
                        "input iterators" : [

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
        "field name" : "tan360neg",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "TAN",
              "input iterators" : [
                {
                  "iterator kind" : "MULTIPLY_DIVIDE",
                  "operations and operands" : [
                    {
                      "operation" : "*",
                      "operand" :
                      {
                        "iterator kind" : "CONST",
                        "value" : -2
                      }
                    },
                    {
                      "operation" : "*",
                      "operand" :
                      {
                        "iterator kind" : "PI",
                        "input iterators" : [

                        ]
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
      }
    ]
  }
}
}