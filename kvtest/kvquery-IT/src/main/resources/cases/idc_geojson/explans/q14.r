compiled-query-plan

{
"query file" : "idc_geojson/q/q14.q",
"plan" : 
{
  "iterator kind" : "SORT",
  "order by fields at positions" : [ 0 ],
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
            "range conditions" : { "info.point" : { "start value" : "t9uu", "start inclusive" : true, "end value" : "t9uvzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "t9uy", "start inclusive" : true, "end value" : "t9uzzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "t9vh", "start inclusive" : true, "end value" : "t9vzzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "t9yh", "start inclusive" : true, "end value" : "t9yzzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "t9zh", "start inclusive" : true, "end value" : "t9zzzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tdhb", "start inclusive" : true, "end value" : "tdhczzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tdhf", "start inclusive" : true, "end value" : "tdhgzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tdhu", "start inclusive" : true, "end value" : "tdhvzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tdhy", "start inclusive" : true, "end value" : "tdjzzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tdkb", "start inclusive" : true, "end value" : "tdkczzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tdkf", "start inclusive" : true, "end value" : "tdkgzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tdku", "start inclusive" : true, "end value" : "tdkvzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tdky", "start inclusive" : true, "end value" : "tdrzzzzzzz", "end inclusive" : true } }
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
          "value" : {"coordinates":[[[74.44335937499999,10.703791711680736],[78.44238281249999,10.703791711680736],[78.44238281249999,13.966054081318314],[74.44335937499999,13.966054081318314],[74.44335937499999,10.703791711680736]]],"type":"Polygon"}
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
}