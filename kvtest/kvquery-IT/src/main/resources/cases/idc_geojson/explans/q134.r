compiled-query-plan

{
"query file" : "idc_geojson/q/q134.q",
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
          "equality conditions" : {"id":3},
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
            "value" : {"coordinates":[[[77.61332273483276,12.987988133754438],[77.61310815811157,12.987047239353823],[77.61474967002869,12.985311802556145],[77.61811852455139,12.9870890569584],[77.61782884597778,12.98832267312697],[77.61744260787964,12.989242654078309],[77.6163375377655,12.989242654078309],[77.614985704422,12.9890126591599],[77.61332273483276,12.987988133754438]]],"type":"Polygon"}
          }
        }
      }
    ]
  }
}
}