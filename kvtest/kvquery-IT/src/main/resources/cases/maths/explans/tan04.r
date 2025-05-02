compiled-query-plan

{
"query file" : "maths/q/tan04.q",
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
        "field name" : "tan0",
        "field expression" : 
        {
          "iterator kind" : "TAN",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                }
              ]
            }
          ]
        }
      },
      {
        "field name" : "tan30",
        "field expression" : 
        {
          "iterator kind" : "TAN",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 30
                }
              ]
            }
          ]
        }
      },
      {
        "field name" : "tan45",
        "field expression" : 
        {
          "iterator kind" : "TAN",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 45
                }
              ]
            }
          ]
        }
      },
      {
        "field name" : "tan60",
        "field expression" : 
        {
          "iterator kind" : "TAN",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 60
                }
              ]
            }
          ]
        }
      },
      {
        "field name" : "tan90",
        "field expression" : 
        {
          "iterator kind" : "TAN",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 90
                }
              ]
            }
          ]
        }
      },
      {
        "field name" : "tan180",
        "field expression" : 
        {
          "iterator kind" : "TAN",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 180
                }
              ]
            }
          ]
        }
      },
      {
        "field name" : "tan270",
        "field expression" : 
        {
          "iterator kind" : "TAN",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 270
                }
              ]
            }
          ]
        }
      },
      {
        "field name" : "tan360",
        "field expression" : 
        {
          "iterator kind" : "TAN",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 360
                }
              ]
            }
          ]
        }
      },
      {
        "field name" : "tan0neg",
        "field expression" : 
        {
          "iterator kind" : "TAN",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 0.0
                }
              ]
            }
          ]
        }
      },
      {
        "field name" : "tan30neg",
        "field expression" : 
        {
          "iterator kind" : "TAN",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -30
                }
              ]
            }
          ]
        }
      },
      {
        "field name" : "tan45neg",
        "field expression" : 
        {
          "iterator kind" : "TAN",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -45
                }
              ]
            }
          ]
        }
      },
      {
        "field name" : "tan60neg",
        "field expression" : 
        {
          "iterator kind" : "TAN",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -60
                }
              ]
            }
          ]
        }
      },
      {
        "field name" : "tan90neg",
        "field expression" : 
        {
          "iterator kind" : "TAN",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -90
                }
              ]
            }
          ]
        }
      },
      {
        "field name" : "tan180neg",
        "field expression" : 
        {
          "iterator kind" : "TAN",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -180
                }
              ]
            }
          ]
        }
      },
      {
        "field name" : "tan270neg",
        "field expression" : 
        {
          "iterator kind" : "TAN",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -270
                }
              ]
            }
          ]
        }
      },
      {
        "field name" : "tan360neg",
        "field expression" : 
        {
          "iterator kind" : "TAN",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -360
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