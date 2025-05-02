compiled-query-plan

{
"query file" : "maths/q/sin04.q",
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
        "field name" : "sin30",
        "field expression" : 
        {
          "iterator kind" : "SIN",
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
        "field name" : "sin45",
        "field expression" : 
        {
          "iterator kind" : "SIN",
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
        "field name" : "sin60",
        "field expression" : 
        {
          "iterator kind" : "SIN",
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
        "field name" : "sin90",
        "field expression" : 
        {
          "iterator kind" : "SIN",
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
        "field name" : "sin180",
        "field expression" : 
        {
          "iterator kind" : "SIN",
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
        "field name" : "sin270",
        "field expression" : 
        {
          "iterator kind" : "SIN",
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
        "field name" : "sin360",
        "field expression" : 
        {
          "iterator kind" : "SIN",
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
        "field name" : "sin0neg",
        "field expression" : 
        {
          "iterator kind" : "SIN",
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
        "field name" : "sin30neg",
        "field expression" : 
        {
          "iterator kind" : "SIN",
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
        "field name" : "sin45neg",
        "field expression" : 
        {
          "iterator kind" : "SIN",
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
        "field name" : "sin60neg",
        "field expression" : 
        {
          "iterator kind" : "SIN",
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
        "field name" : "sin90neg",
        "field expression" : 
        {
          "iterator kind" : "SIN",
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
        "field name" : "sin180neg",
        "field expression" : 
        {
          "iterator kind" : "SIN",
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
        "field name" : "sin270neg",
        "field expression" : 
        {
          "iterator kind" : "SIN",
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
        "field name" : "sin360neg",
        "field expression" : 
        {
          "iterator kind" : "SIN",
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