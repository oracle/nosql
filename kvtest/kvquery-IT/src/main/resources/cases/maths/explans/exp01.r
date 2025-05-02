compiled-query-plan

{
"query file" : "maths/q/exp01.q",
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
        "field name" : "exp0",
        "field expression" : 
        {
          "iterator kind" : "EXP",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : 0
            }
          ]
        }
      },
      {
        "field name" : "exp1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "EXP",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 1
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 5
            }
          ]
        }
      },
      {
        "field name" : "exp2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "EXP",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 2
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 5
            }
          ]
        }
      },
      {
        "field name" : "exp5",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "EXP",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 5
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 5
            }
          ]
        }
      },
      {
        "field name" : "exp10",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "EXP",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 10
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 5
            }
          ]
        }
      },
      {
        "field name" : "exp100",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "EXP",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 100
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 5
            }
          ]
        }
      },
      {
        "field name" : "exp1000",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "EXP",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 1000
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 5
            }
          ]
        }
      },
      {
        "field name" : "expneg1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "EXP",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -1
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 5
            }
          ]
        }
      },
      {
        "field name" : "expneg2",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "EXP",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -2
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 5
            }
          ]
        }
      },
      {
        "field name" : "expneg1000",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "EXP",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -1000
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 5
            }
          ]
        }
      },
      {
        "field name" : "exppi",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "EXP",
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
              "value" : 5
            }
          ]
        }
      },
      {
        "field name" : "exp001",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "EXP",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 0.001
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 5
            }
          ]
        }
      },
      {
        "field name" : "exp02",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "EXP",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 0.2
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 5
            }
          ]
        }
      },
      {
        "field name" : "expneg05",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "EXP",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -0.5
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 5
            }
          ]
        }
      },
      {
        "field name" : "exp00125",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "EXP",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 0.0125
                }
              ]
            },
            {
              "iterator kind" : "CONST",
              "value" : 5
            }
          ]
        }
      }
    ]
  }
}
}