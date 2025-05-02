compiled-query-plan

{
"query file" : "maths/q/cos04.q",
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
        "field name" : "cos30",
        "field expression" : 
        {
          "iterator kind" : "COS",
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
        "field name" : "cos45",
        "field expression" : 
        {
          "iterator kind" : "COS",
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
        "field name" : "cos60",
        "field expression" : 
        {
          "iterator kind" : "COS",
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
        "field name" : "cos90",
        "field expression" : 
        {
          "iterator kind" : "COS",
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
        "field name" : "cos180",
        "field expression" : 
        {
          "iterator kind" : "COS",
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
        "field name" : "cos270",
        "field expression" : 
        {
          "iterator kind" : "COS",
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
        "field name" : "cos360",
        "field expression" : 
        {
          "iterator kind" : "COS",
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
        "field name" : "cos0neg",
        "field expression" : 
        {
          "iterator kind" : "COS",
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
        "field name" : "cos30neg",
        "field expression" : 
        {
          "iterator kind" : "COS",
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
        "field name" : "cos45neg",
        "field expression" : 
        {
          "iterator kind" : "COS",
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
        "field name" : "cos60neg",
        "field expression" : 
        {
          "iterator kind" : "COS",
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
        "field name" : "cos90neg",
        "field expression" : 
        {
          "iterator kind" : "COS",
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
        "field name" : "cos180neg",
        "field expression" : 
        {
          "iterator kind" : "COS",
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
        "field name" : "cos270neg",
        "field expression" : 
        {
          "iterator kind" : "COS",
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
        "field name" : "cos360neg",
        "field expression" : 
        {
          "iterator kind" : "COS",
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