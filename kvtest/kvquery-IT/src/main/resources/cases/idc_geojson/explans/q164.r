compiled-query-plan

{
"query file" : "idc_geojson/q/q164.q",
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
      "target table" : "testsqlnull",
      "row variable" : "$$t",
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
    "FROM variable" : "$$t",
    "SELECT expressions" : [
      {
        "field name" : "Column_1",
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
                "variable" : "$$t"
              }
            }
          },
          "second geometry iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "info1",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$t"
            }
          }
        }
      }
    ]
  }
}
}