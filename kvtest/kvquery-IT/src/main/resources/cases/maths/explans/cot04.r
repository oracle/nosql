compiled-query-plan

{
"query file" : "maths/q/cot04.q",
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
        "field name" : "cot30",
        "field expression" : 
        {
          "iterator kind" : "COT",
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
        "field name" : "cot45",
        "field expression" : 
        {
          "iterator kind" : "COT",
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
        "field name" : "cot60",
        "field expression" : 
        {
          "iterator kind" : "COT",
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
        "field name" : "cot90",
        "field expression" : 
        {
          "iterator kind" : "COT",
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
        "field name" : "cot180",
        "field expression" : 
        {
          "iterator kind" : "COT",
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
        "field name" : "cot270",
        "field expression" : 
        {
          "iterator kind" : "COT",
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
        "field name" : "cot360",
        "field expression" : 
        {
          "iterator kind" : "COT",
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
        "field name" : "cot0neg",
        "field expression" : 
        {
          "iterator kind" : "COT",
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
        "field name" : "cot30neg",
        "field expression" : 
        {
          "iterator kind" : "COT",
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
        "field name" : "cot45neg",
        "field expression" : 
        {
          "iterator kind" : "COT",
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
        "field name" : "cot60neg",
        "field expression" : 
        {
          "iterator kind" : "COT",
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
        "field name" : "cot90neg",
        "field expression" : 
        {
          "iterator kind" : "COT",
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
        "field name" : "cot180neg",
        "field expression" : 
        {
          "iterator kind" : "COT",
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
        "field name" : "cot270neg",
        "field expression" : 
        {
          "iterator kind" : "COT",
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
        "field name" : "cot360neg",
        "field expression" : 
        {
          "iterator kind" : "COT",
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