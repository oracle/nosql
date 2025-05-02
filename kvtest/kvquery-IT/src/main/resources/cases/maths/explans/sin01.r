compiled-query-plan

{
"query file" : "maths/q/sin01.q",
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
        "field name" : "sin0",
        "field expression" : 
        {
          "iterator kind" : "SIN",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 0
            }
          ]
        }
      },
      {
        "field name" : "sin30",
        "field expression" : 
        {
          "iterator kind" : "SIN",
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
        "field name" : "sin45",
        "field expression" : 
        {
          "iterator kind" : "SIN",
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
        "field name" : "sin60",
        "field expression" : 
        {
          "iterator kind" : "SIN",
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
        "field name" : "sin90",
        "field expression" : 
        {
          "iterator kind" : "SIN",
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
        "field name" : "sin180",
        "field expression" : 
        {
          "iterator kind" : "SIN",
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
        "field name" : "sin270",
        "field expression" : 
        {
          "iterator kind" : "SIN",
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
        "field name" : "sin360",
        "field expression" : 
        {
          "iterator kind" : "SIN",
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
        "field name" : "sin0neg",
        "field expression" : 
        {
          "iterator kind" : "SIN",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 0.0
            }
          ]
        }
      },
      {
        "field name" : "sin30neg",
        "field expression" : 
        {
          "iterator kind" : "SIN",
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
        "field name" : "sin45neg",
        "field expression" : 
        {
          "iterator kind" : "SIN",
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
        "field name" : "sin60neg",
        "field expression" : 
        {
          "iterator kind" : "SIN",
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
        "field name" : "sin90neg",
        "field expression" : 
        {
          "iterator kind" : "SIN",
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
        "field name" : "sin180neg",
        "field expression" : 
        {
          "iterator kind" : "SIN",
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
        "field name" : "sin270neg",
        "field expression" : 
        {
          "iterator kind" : "SIN",
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
        "field name" : "sin360neg",
        "field expression" : 
        {
          "iterator kind" : "SIN",
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