compiled-query-plan

{
"query file" : "idc_geojson/q/q119.q",
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
      "row variable" : "$$g",
      "index used" : "primary index",
      "covering index" : true,
      "index scans" : [
        {
          "equality conditions" : {"id":1},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$g",
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
            "variable" : "$$g"
          }
        }
      },
      {
        "field name" : "Column_2",
        "field expression" : 
        {
          "iterator kind" : "GEO_IS_GEOMETRY",
          "input iterator" :
          {
            "iterator kind" : "MAP_CONSTRUCTOR",
            "input iterators" : [
              {
                "iterator kind" : "CONST",
                "value" : "typee"
              },
              {
                "iterator kind" : "CONST",
                "value" : "point"
              },
              {
                "iterator kind" : "CONST",
                "value" : "coordinates"
              },
              {
                "iterator kind" : "ARRAY_CONSTRUCTOR",
                "conditional" : false,
                "input iterators" : [
                  {
                    "iterator kind" : "CONST",
                    "value" : 77.58909702301025
                  },
                  {
                    "iterator kind" : "CONST",
                    "value" : 12.97269293084298
                  }
                ]
              }
            ]
          }
        }
      }
    ]
  }
}
}