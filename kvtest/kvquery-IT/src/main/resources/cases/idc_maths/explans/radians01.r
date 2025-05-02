compiled-query-plan

{
"query file" : "idc_maths/q/radians01.q",
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
        "field name" : "degrees0",
        "field expression" : 
        {
          "iterator kind" : "CONST",
          "value" : 0
        }
      },
      {
        "field name" : "radians0",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
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
        "field name" : "degrees1",
        "field expression" : 
        {
          "iterator kind" : "CONST",
          "value" : 1
        }
      },
      {
        "field name" : "radians1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 1
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
        "field name" : "degrees30",
        "field expression" : 
        {
          "iterator kind" : "CONST",
          "value" : 30
        }
      },
      {
        "field name" : "radians30",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 30
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
        "field name" : "degrees45",
        "field expression" : 
        {
          "iterator kind" : "CONST",
          "value" : 45
        }
      },
      {
        "field name" : "radians45",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 45
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
        "field name" : "degrees60",
        "field expression" : 
        {
          "iterator kind" : "CONST",
          "value" : 60
        }
      },
      {
        "field name" : "radians60",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 60
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
        "field name" : "degrees90",
        "field expression" : 
        {
          "iterator kind" : "CONST",
          "value" : 90
        }
      },
      {
        "field name" : "radians90",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 90
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
        "field name" : "degrees120",
        "field expression" : 
        {
          "iterator kind" : "CONST",
          "value" : 120
        }
      },
      {
        "field name" : "radians120",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 120
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
        "field name" : "degrees135",
        "field expression" : 
        {
          "iterator kind" : "CONST",
          "value" : 135
        }
      },
      {
        "field name" : "radians135",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 135
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
        "field name" : "degrees180",
        "field expression" : 
        {
          "iterator kind" : "CONST",
          "value" : 180
        }
      },
      {
        "field name" : "radians180",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 180
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
        "field name" : "degrees270",
        "field expression" : 
        {
          "iterator kind" : "CONST",
          "value" : 270
        }
      },
      {
        "field name" : "radians270",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 270
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
        "field name" : "degrees360",
        "field expression" : 
        {
          "iterator kind" : "CONST",
          "value" : 360
        }
      },
      {
        "field name" : "radians360",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : 360
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
        "field name" : "degreesNeg0",
        "field expression" : 
        {
          "iterator kind" : "CONST",
          "value" : 0
        }
      },
      {
        "field name" : "radiansNeg0",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
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
        "field name" : "degreesNeg1",
        "field expression" : 
        {
          "iterator kind" : "CONST",
          "value" : -1
        }
      },
      {
        "field name" : "radiansNeg1",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -1
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
        "field name" : "degreesNeg30",
        "field expression" : 
        {
          "iterator kind" : "CONST",
          "value" : -30
        }
      },
      {
        "field name" : "radiansNeg30",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -30
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
        "field name" : "degreesNeg45",
        "field expression" : 
        {
          "iterator kind" : "CONST",
          "value" : -45
        }
      },
      {
        "field name" : "radiansNeg45",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -45
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
        "field name" : "degreesNeg60",
        "field expression" : 
        {
          "iterator kind" : "CONST",
          "value" : -60
        }
      },
      {
        "field name" : "radiansNeg60",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -60
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
        "field name" : "degreesNeg90",
        "field expression" : 
        {
          "iterator kind" : "CONST",
          "value" : -90
        }
      },
      {
        "field name" : "radiansNeg90",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -90
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
        "field name" : "degreesNeg120",
        "field expression" : 
        {
          "iterator kind" : "CONST",
          "value" : -120
        }
      },
      {
        "field name" : "radiansNeg120",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -120
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
        "field name" : "degreesNeg135",
        "field expression" : 
        {
          "iterator kind" : "CONST",
          "value" : -135
        }
      },
      {
        "field name" : "radiansNeg135",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -135
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
        "field name" : "degreesNeg180",
        "field expression" : 
        {
          "iterator kind" : "CONST",
          "value" : -180
        }
      },
      {
        "field name" : "radiansNeg180",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -180
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
        "field name" : "degreesNeg270",
        "field expression" : 
        {
          "iterator kind" : "CONST",
          "value" : -270
        }
      },
      {
        "field name" : "radiansNeg270",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -270
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
        "field name" : "degreesNeg360",
        "field expression" : 
        {
          "iterator kind" : "CONST",
          "value" : -360
        }
      },
      {
        "field name" : "radiansNeg360",
        "field expression" : 
        {
          "iterator kind" : "TRUNC",
          "input iterators" : [
            {
              "iterator kind" : "RADIANS",
              "input iterators" : [
                {
                  "iterator kind" : "CONST",
                  "value" : -360
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