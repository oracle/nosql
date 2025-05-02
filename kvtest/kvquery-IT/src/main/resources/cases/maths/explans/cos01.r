compiled-query-plan

{
"query file" : "maths/q/cos01.q",
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
        "field name" : "cos0",
        "field expression" : 
        {
          "iterator kind" : "COS",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 0
            }
          ]
        }
      },
      {
        "field name" : "cos30",
        "field expression" : 
        {
          "iterator kind" : "COS",
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
        "field name" : "cos45",
        "field expression" : 
        {
          "iterator kind" : "COS",
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
        "field name" : "cos60",
        "field expression" : 
        {
          "iterator kind" : "COS",
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
        "field name" : "cos90",
        "field expression" : 
        {
          "iterator kind" : "COS",
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
        "field name" : "cos180",
        "field expression" : 
        {
          "iterator kind" : "COS",
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
        "field name" : "cos270",
        "field expression" : 
        {
          "iterator kind" : "COS",
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
        "field name" : "cos360",
        "field expression" : 
        {
          "iterator kind" : "COS",
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
        "field name" : "cos0neg",
        "field expression" : 
        {
          "iterator kind" : "COS",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 0.0
            }
          ]
        }
      },
      {
        "field name" : "cos30neg",
        "field expression" : 
        {
          "iterator kind" : "COS",
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
        "field name" : "cos45neg",
        "field expression" : 
        {
          "iterator kind" : "COS",
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
        "field name" : "cos60neg",
        "field expression" : 
        {
          "iterator kind" : "COS",
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
        "field name" : "cos90neg",
        "field expression" : 
        {
          "iterator kind" : "COS",
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
        "field name" : "cos180neg",
        "field expression" : 
        {
          "iterator kind" : "COS",
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
        "field name" : "cos270neg",
        "field expression" : 
        {
          "iterator kind" : "COS",
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
        "field name" : "cos360neg",
        "field expression" : 
        {
          "iterator kind" : "COS",
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