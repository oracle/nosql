compiled-query-plan

{
"query file" : "idc_geojson/q/q02.q",
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
      "target table" : "points",
      "row variable" : "$$p",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"id":5},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$p",
    "SELECT expressions" : [
      {
        "field name" : "dist",
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
            "value" : {"coordinates":[[[-71.02150440216064,42.3670263034981],[-71.0214614868164,42.3669311802851],[-71.02244853973389,42.365789690494196],[-71.02244853973389,42.363617631805496],[-71.02135419845581,42.36222240121282],[-71.02006673812866,42.36141378748323],[-71.01792097091675,42.3611601026],[-71.01594686508179,42.36318955298668],[-71.01869344711304,42.36705801120375],[-71.02150440216064,42.3670263034981]]],"type":"Polygon"}
          }
        }
      }
    ]
  }
}
}