compiled-query-plan

{
"query file" : "idc_new_timestamp/q/parse_to_timestamp02.q",
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
      "target table" : "roundFunc",
      "row variable" : "$$roundFunc",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"id":6},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$roundFunc",
    "SELECT expressions" : [
      {
        "field name" : "Column_1",
        "field expression" : 
        {
          "iterator kind" : "FN_PARSE_TO_TIMESTAMP",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "l3",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$roundFunc"
            }
          }
        }
      }
    ]
  }
}
}