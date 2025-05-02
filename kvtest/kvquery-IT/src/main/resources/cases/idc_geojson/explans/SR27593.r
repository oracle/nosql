compiled-query-plan

{
"query file" : "idc_geojson/q/SR27593.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_PARTITIONS",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "points",
      "row variable" : "$$p",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$p",
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
        "field name" : "Column_2",
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
            "value" : {"coordinates":[[[-189.31640625,68.46379955520322],[-195.46875,50.736455137010665],[-147.83203125,51.83577752045248],[-142.91015625,68.2042121888185],[-189.31640625,68.46379955520322]]],"type":"Polygon"}
          }
        }
      }
    ]
  }
}
}