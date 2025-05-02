compiled-query-plan

{
"query file" : "idc_geojson/q/q47.q",
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
          "equality conditions" : {"id":18},
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
            "value" : {"coordinates":[[[77.49481201171875,12.958724651072012],[77.48331069946289,12.944002741634725],[77.49910354614258,12.938649104401463],[77.5224494934082,12.94316624339459],[77.5521469116211,12.954207794100693],[77.56811141967773,12.973445690117916],[77.56158828735352,12.989671280403403],[77.50133514404297,12.990173085892906],[77.49481201171875,12.958724651072012]]],"type":"Polygon"}
          },
          "distance iterator" :
          {
            "iterator kind" : "CONST",
            "value" : 300000.098
          }
        }
      }
    ]
  }
}
}