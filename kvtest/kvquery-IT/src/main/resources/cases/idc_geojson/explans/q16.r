compiled-query-plan

{
"query file" : "idc_geojson/q/q16.q",
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
          "range conditions" : { "info.point" : { "start value" : "t9rnhgednn", "start inclusive" : true, "end value" : "t9rnhgednn", "end inclusive" : true } }
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
        "value" : {"coordinates":[77.55755424499512,8.105542704535111],"type":"Point"}
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