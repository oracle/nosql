compiled-query-plan

{
"query file" : "idc_new_timestamp/q/parse_to_timestamp07.q",
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
      "target table" : "jsonCollection_test",
      "row variable" : "$$t",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"id":1},
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
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "str3",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            },
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "pattern3",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            }
          ]
        }
      },
      {
        "field name" : "Column_2",
        "field expression" : 
        {
          "iterator kind" : "FN_PARSE_TO_TIMESTAMP",
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "str4",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            },
            {
              "iterator kind" : "CONST",
              "value" : "yyyy-MM-dd'T'HH:mm:ss"
            }
          ]
        }
      }
    ]
  }
}
}