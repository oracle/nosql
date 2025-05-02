compiled-query-plan

{
"query file" : "idc_new_timestamp/q/to_last_day_of_month02.q",
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
          "equality conditions" : {"id":0},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$roundFunc",
    "SELECT expressions" : [
      {
        "field name" : "to_last_day_of_month_resolve_long",
        "field expression" : 
        {
          "iterator kind" : "FN_LAST_DAY_OF_MONTH",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "l3",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$roundFunc"
              }
            }
          ]
        }
      }
    ]
  }
}
}