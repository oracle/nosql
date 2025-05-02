compiled-query-plan

{
"query file" : "geo/q/ext_near54.q",
"plan" : 
{
  "iterator kind" : "SORT",
  "order by fields at positions" : [ 2 ],
  "input iterator" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_SHARDS",
    "distinct by fields at positions" : [ 0 ],
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "polygons",
        "row variable" : "$$p",
        "index used" : "idx_city_geom",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {"info.city":""},
            "range conditions" : { "info.geom" : { "start value" : "EMPTY", "start inclusive" : false, "end value" : "EMPTY", "end inclusive" : false } }
          }
        ],
        "key bind expressions" : [
          {
            "iterator kind" : "ARRAY_FILTER",
            "input iterator" :
            {
              "iterator kind" : "EXTERNAL_VAR_REF",
              "variable" : "$arr2"
            }
          },
          {
            "iterator kind" : "CONST",
            "value" : {"coordinates":[[24.0262,35.5043],[24.0202,35.5122]],"type":"linestring"}
          },
          {
            "iterator kind" : "PROMOTE",
            "target type" : "Double",
            "input iterator" :
            {
              "iterator kind" : "EXTERNAL_VAR_REF",
              "variable" : "$dist"
            }
          }
        ],
        "map of key bind expressions" : [
          [ 0, 1, 2 ]
        ],
        "bind info for in3 operator" : [
          {
            "theNumComps" : 1,
            "thePushedComps" : [ 0 ],
            "theIndexFieldPositions" : [ 0 ]
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
          "field name" : "geom",
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
          "value" : {"coordinates":[[24.0262,35.5043],[24.0202,35.5122]],"type":"linestring"}
        },
        "distance iterator" :
        {
          "iterator kind" : "PROMOTE",
          "target type" : "Double",
          "input iterator" :
          {
            "iterator kind" : "EXTERNAL_VAR_REF",
            "variable" : "$dist"
          }
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
          "field name" : "geom",
          "field expression" : 
          {
            "iterator kind" : "ARRAY_CONSTRUCTOR",
            "conditional" : true,
            "input iterators" : [
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "geom",
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
          "field name" : "Column_3",
          "field expression" : 
          {
            "iterator kind" : "GEO_DISTANCE",
            "first geometry iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "geom",
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
              "value" : {"coordinates":[[24.0262,35.5043],[24.0202,35.5122]],"type":"linestring"}
            }
          }
        }
      ]
    }
  }
}
}