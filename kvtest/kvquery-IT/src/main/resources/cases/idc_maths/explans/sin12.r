compiled-query-plan

{
"query file" : "idc_maths/q/sin12.q",
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
        "field name" : "sin0",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "sin30",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "sin45",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "sin60",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "sin90",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "sin180",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "sin270",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "sin360",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "sin0neg",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "sin30neg",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "sin45neg",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "sin60neg",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "sin90neg",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "sin180neg",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "sin270neg",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
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
            },
            {
              "iterator kind" : "CONST",
              "value" : 7
            }
          ]
        }
      },
      {
        "field name" : "sin360neg",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
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