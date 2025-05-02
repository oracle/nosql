compiled-query-plan

{
"query file" : "maths/q/cot01.q",
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
        "field name" : "cot0",
        "field expression" : 
        {
          "iterator kind" : "COT",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 0
            }
          ]
        }
      },
      {
        "field name" : "cot30",
        "field expression" : 
        {
          "iterator kind" : "COT",
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
        }
      },
      {
        "field name" : "cot45",
        "field expression" : 
        {
          "iterator kind" : "COT",
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
        }
      },
      {
        "field name" : "cot60",
        "field expression" : 
        {
          "iterator kind" : "COT",
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
        }
      },
      {
        "field name" : "cot90",
        "field expression" : 
        {
          "iterator kind" : "COT",
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
        }
      },
      {
        "field name" : "cot180",
        "field expression" : 
        {
          "iterator kind" : "COT",
          "input iterators" : [
            {
              "iterator kind" : "PI",
              "input iterators" : [

              ]
            }
          ]
        }
      },
      {
        "field name" : "cot270",
        "field expression" : 
        {
          "iterator kind" : "COT",
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
        }
      },
      {
        "field name" : "cot360",
        "field expression" : 
        {
          "iterator kind" : "COT",
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
        }
      },
      {
        "field name" : "cot0neg",
        "field expression" : 
        {
          "iterator kind" : "COT",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 0.0
            }
          ]
        }
      },
      {
        "field name" : "cot30neg",
        "field expression" : 
        {
          "iterator kind" : "COT",
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
        }
      },
      {
        "field name" : "cot45neg",
        "field expression" : 
        {
          "iterator kind" : "COT",
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
        }
      },
      {
        "field name" : "cot60neg",
        "field expression" : 
        {
          "iterator kind" : "COT",
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
        }
      },
      {
        "field name" : "cot90neg",
        "field expression" : 
        {
          "iterator kind" : "COT",
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
        }
      },
      {
        "field name" : "cot180neg",
        "field expression" : 
        {
          "iterator kind" : "COT",
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
        }
      },
      {
        "field name" : "cot270neg",
        "field expression" : 
        {
          "iterator kind" : "COT",
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
        }
      },
      {
        "field name" : "cot360neg",
        "field expression" : 
        {
          "iterator kind" : "COT",
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
        }
      }
    ]
  }
}
}