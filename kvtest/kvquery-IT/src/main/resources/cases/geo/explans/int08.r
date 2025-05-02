compiled-query-plan

{
"query file" : "geo/q/int08.q",
"plan" : 
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
          "range conditions" : { "info.point" : { "start value" : "18vh6nn", "start inclusive" : true, "end value" : "18vh6npzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vh6q0", "start inclusive" : true, "end value" : "18vh6q1zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vh6q4", "start inclusive" : true, "end value" : "18vh6q5zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vh6qh", "start inclusive" : true, "end value" : "18vh6qjzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vh6qn", "start inclusive" : true, "end value" : "18vh6qpzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vh6w0", "start inclusive" : true, "end value" : "18vh6w1zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vh6w4", "start inclusive" : true, "end value" : "18vh6w5zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vh6wh", "start inclusive" : true, "end value" : "18vh6wjzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vh6wn", "start inclusive" : true, "end value" : "18vh6wpzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vh6y0", "start inclusive" : true, "end value" : "18vh6y1zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vh6y4", "start inclusive" : true, "end value" : "18vh6y5zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vh6yh", "start inclusive" : true, "end value" : "18vh6yjzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vh6yn", "start inclusive" : true, "end value" : "18vh6ypzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vh7n0", "start inclusive" : true, "end value" : "18vh7n1zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vh7n4", "start inclusive" : true, "end value" : "18vh7n5zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vh7nh", "start inclusive" : true, "end value" : "18vh7njzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vh7nn", "start inclusive" : true, "end value" : "18vh7npzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vh7q0", "start inclusive" : true, "end value" : "18vh7q1zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vh7q4", "start inclusive" : true, "end value" : "18vh7q5zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vh7qh", "start inclusive" : true, "end value" : "18vh7qjzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vh7qn", "start inclusive" : true, "end value" : "18vh7qpzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vh7w0", "start inclusive" : true, "end value" : "18vh7w1zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vh7w4", "start inclusive" : true, "end value" : "18vh7w5zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vh7wh", "start inclusive" : true, "end value" : "18vh7wjzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vh7wn", "start inclusive" : true, "end value" : "18vh7wpzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vh7y0", "start inclusive" : true, "end value" : "18vh7y1zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vh7y4", "start inclusive" : true, "end value" : "18vh7y5zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vh7yh", "start inclusive" : true, "end value" : "18vh7yjzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vh7yn", "start inclusive" : true, "end value" : "18vh7ypzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhkn0", "start inclusive" : true, "end value" : "18vhkn1zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhkn4", "start inclusive" : true, "end value" : "18vhkn5zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhknh", "start inclusive" : true, "end value" : "18vhknjzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhknn", "start inclusive" : true, "end value" : "18vhknpzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhkq0", "start inclusive" : true, "end value" : "18vhkq1zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhkq4", "start inclusive" : true, "end value" : "18vhkq5zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhkqh", "start inclusive" : true, "end value" : "18vhkqjzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhkqn", "start inclusive" : true, "end value" : "18vhkqpzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhkw0", "start inclusive" : true, "end value" : "18vhkw1zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhkw4", "start inclusive" : true, "end value" : "18vhkw5zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhkwh", "start inclusive" : true, "end value" : "18vhkwjzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhkwn", "start inclusive" : true, "end value" : "18vhkwpzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhky0", "start inclusive" : true, "end value" : "18vhky1zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhky4", "start inclusive" : true, "end value" : "18vhky5zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhkyh", "start inclusive" : true, "end value" : "18vhkyjzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhkyn", "start inclusive" : true, "end value" : "18vhkypzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhmn0", "start inclusive" : true, "end value" : "18vhmn1zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhmn4", "start inclusive" : true, "end value" : "18vhmn5zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhmnh", "start inclusive" : true, "end value" : "18vhmnjzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhmnn", "start inclusive" : true, "end value" : "18vhmnpzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhmq0", "start inclusive" : true, "end value" : "18vhmq1zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhmq4", "start inclusive" : true, "end value" : "18vhmq5zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhmqh", "start inclusive" : true, "end value" : "18vhmqjzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhmqn", "start inclusive" : true, "end value" : "18vhmqpzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhmw0", "start inclusive" : true, "end value" : "18vhmw1zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhmw4", "start inclusive" : true, "end value" : "18vhmw5zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhmwh", "start inclusive" : true, "end value" : "18vhmwjzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhmwn", "start inclusive" : true, "end value" : "18vhmwpzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhmy0", "start inclusive" : true, "end value" : "18vhmy1zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhmy4", "start inclusive" : true, "end value" : "18vhmy5zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhmyh", "start inclusive" : true, "end value" : "18vhmyjzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhmyn", "start inclusive" : true, "end value" : "18vhmypzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhqn0", "start inclusive" : true, "end value" : "18vhqn1zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhqn4", "start inclusive" : true, "end value" : "18vhqn5zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhqnh", "start inclusive" : true, "end value" : "18vhqnjzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhqnn", "start inclusive" : true, "end value" : "18vhqnpzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhqq0", "start inclusive" : true, "end value" : "18vhqq1zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhqq4", "start inclusive" : true, "end value" : "18vhqq5zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhqqh", "start inclusive" : true, "end value" : "18vhqqjzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhqqn", "start inclusive" : true, "end value" : "18vhqqpzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhqw0", "start inclusive" : true, "end value" : "18vhqw1zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhqw4", "start inclusive" : true, "end value" : "18vhqw5zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhqwh", "start inclusive" : true, "end value" : "18vhqwjzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "18vhqwn", "start inclusive" : true, "end value" : "18vhqwn", "end inclusive" : true } }
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$p",
    "WHERE" : 
    {
      "iterator kind" : "FN_GEO_INTERSECT",
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
      }
    ]
  }
}
}