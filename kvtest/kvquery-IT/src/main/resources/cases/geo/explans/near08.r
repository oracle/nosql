compiled-query-plan

{
"query file" : "geo/q/near08.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "SORT",
    "order by fields at positions" : [ 2 ],
    "input iterator" :
    {
      "iterator kind" : "RECEIVE",
      "distribution kind" : "ALL_SHARDS",
      "input iterator" :
      {
        "iterator kind" : "SELECT",
        "FROM" :
        {
          "iterator kind" : "TABLE",
          "target table" : "points",
          "row variable" : "$$p",
          "index used" : "idx_ptn",
          "covering index" : false,
          "index scans" : [
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18uurh", "start inclusive" : true, "end value" : "18uurzzzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18uux0", "start inclusive" : true, "end value" : "18uux0", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18uux2", "start inclusive" : true, "end value" : "18uux2", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18uux8", "start inclusive" : true, "end value" : "18uux8", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18uuxb", "start inclusive" : true, "end value" : "18uuxb", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vh2h", "start inclusive" : true, "end value" : "18vh2zzzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vh3h", "start inclusive" : true, "end value" : "18vh3zzzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vh6h", "start inclusive" : true, "end value" : "18vh6zzzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vh7h", "start inclusive" : true, "end value" : "18vh80zzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vh82", "start inclusive" : true, "end value" : "18vh82", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vh88", "start inclusive" : true, "end value" : "18vh88", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vh8b", "start inclusive" : true, "end value" : "18vh8b", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vh90", "start inclusive" : true, "end value" : "18vh90", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vh92", "start inclusive" : true, "end value" : "18vh92", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vh98", "start inclusive" : true, "end value" : "18vh98", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vh9b", "start inclusive" : true, "end value" : "18vh9b", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vhd0", "start inclusive" : true, "end value" : "18vhd0", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vhd2", "start inclusive" : true, "end value" : "18vhd2", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vhd8", "start inclusive" : true, "end value" : "18vhd8", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vhdb", "start inclusive" : true, "end value" : "18vhdb", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vhe0", "start inclusive" : true, "end value" : "18vhe0", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vhe2", "start inclusive" : true, "end value" : "18vhe2", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vhe8", "start inclusive" : true, "end value" : "18vhe8", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vheb", "start inclusive" : true, "end value" : "18vheb", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vhkh", "start inclusive" : true, "end value" : "18vhkzzzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vhmh", "start inclusive" : true, "end value" : "18vhmzzzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vhqh", "start inclusive" : true, "end value" : "18vhqzzzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vhrh", "start inclusive" : true, "end value" : "18vhs0zzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vhs2", "start inclusive" : true, "end value" : "18vhs2", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vhs8", "start inclusive" : true, "end value" : "18vhs8", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vhsb", "start inclusive" : true, "end value" : "18vhsb", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vht0", "start inclusive" : true, "end value" : "18vht0", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vht2", "start inclusive" : true, "end value" : "18vht2", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vht8", "start inclusive" : true, "end value" : "18vht8", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vhtb", "start inclusive" : true, "end value" : "18vhtb", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vhw0", "start inclusive" : true, "end value" : "18vhw0", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vhw2", "start inclusive" : true, "end value" : "18vhw2", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vhw8", "start inclusive" : true, "end value" : "18vhw8", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vhwb", "start inclusive" : true, "end value" : "18vhwb", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vhx0", "start inclusive" : true, "end value" : "18vhx0", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vhx2", "start inclusive" : true, "end value" : "18vhx2", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vhx8", "start inclusive" : true, "end value" : "18vhx8", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vhxb", "start inclusive" : true, "end value" : "18vhxb", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vk2h", "start inclusive" : true, "end value" : "18vk2zzzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vk3h", "start inclusive" : true, "end value" : "18vk3tzzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vk3w", "start inclusive" : true, "end value" : "18vk3xzzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vk80", "start inclusive" : true, "end value" : "18vk80", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vk82", "start inclusive" : true, "end value" : "18vk82", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vk88", "start inclusive" : true, "end value" : "18vk88", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vk8b", "start inclusive" : true, "end value" : "18vk8b", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vk90", "start inclusive" : true, "end value" : "18vk90", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vk92", "start inclusive" : true, "end value" : "18vk92", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "18vk98", "start inclusive" : true, "end value" : "18vk98", "end inclusive" : true } }
            }
          ],
          "position in join" : 0
        },
        "FROM variable" : "$$p",
        "WHERE" : 
        {
          "iterator kind" : "FN_GEO_WITHIN_DISTANCE",
          "search target iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "point",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "info",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$p"
              }
            }
          },
          "search geometry iterator" :
          {
            "iterator kind" : "CONST",
            "value" : {"coordinates":[[-105.372036,-85.0],[-105.174282,-85.0]],"type":"LineString"}
          },
          "distance iterator" :
          {
            "iterator kind" : "CONST",
            "value" : 1300.0
          }
        },
        "SELECT expressions" : [
          {
            "field name" : "id",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "id",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$p"
              }
            }
          },
          {
            "field name" : "point",
            "field expression" : 
            {
              "iterator kind" : "ARRAY_CONSTRUCTOR",
              "conditional" : true,
              "input iterators" : [
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "point",
                  "input iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "info",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$p"
                    }
                  }
                }
              ]
            }
          },
          {
            "field name" : "sort_gen",
            "field expression" : 
            {
              "iterator kind" : "GEO_DISTANCE",
              "first geometry iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "point",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "info",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$p"
                  }
                }
              },
              "second geometry iterator" :
              {
                "iterator kind" : "CONST",
                "value" : {"coordinates":[[-105.372036,-85.0],[-105.174282,-85.0]],"type":"LineString"}
              }
            }
          }
        ]
      }
    }
  },
  "FROM variable" : "$from-0",
  "SELECT expressions" : [
    {
      "field name" : "id",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "id",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    },
    {
      "field name" : "point",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "point",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    }
  ]
}
}