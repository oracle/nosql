compiled-query-plan

{
"query file" : "idc_geojson/q/q137.q",
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
      "target table" : "geotypes",
      "row variable" : "$$p",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"id":4},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$p",
    "SELECT expressions" : [
      {
        "field name" : "Column_1",
        "field expression" : 
        {
          "iterator kind" : "FN_GEO_INSIDE",
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
            "value" : {"coordinates":[[[77.59585618972778,12.997323703940266],[77.59327054023743,12.99679055092788],[77.59404301643372,12.995055182213463],[77.59585618972778,12.994563840675227],[77.59606003761292,12.991511228908237],[77.60017991065979,12.991479866263827],[77.60303378105162,12.991908488727931],[77.60230422019958,12.993183897537897],[77.60178923606873,12.994835646752719],[77.59585618972778,12.997323703940266]]],"type":"Polygon"}
          }
        }
      }
    ]
  }
}
}