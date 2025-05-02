compiled-query-plan

{
"query file" : "idc_new_timestamp/q/parse_to_timestamp03.q",
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
      "row variable" : "$$t",
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
    "FROM variable" : "$$t",
    "SELECT expressions" : [
      {
        "field name" : "Column_1",
        "field expression" : 
        {
          "iterator kind" : "FN_PARSE_TO_TIMESTAMP",
          "input iterator" :
          {
            "iterator kind" : "PROMOTE",
            "target type" : "Any",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "str4",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "doc",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$t"
                }
              }
            }
          }
        }
      },
      {
        "field name" : "Column_2",
        "field expression" : 
        {
          "iterator kind" : "FN_PARSE_TO_TIMESTAMP",
          "input iterator" :
          {
            "iterator kind" : "CONST",
            "value" : "2100-02-28T21:50:30"
          }
        }
      },
      {
        "field name" : "Column_3",
        "field expression" : 
        {
          "iterator kind" : "FN_PARSE_TO_TIMESTAMP",
          "input iterator" :
          {
            "iterator kind" : "CONST",
            "value" : "9999-12-31T23:59:59.999999999"
          }
        }
      }
    ]
  }
}
}