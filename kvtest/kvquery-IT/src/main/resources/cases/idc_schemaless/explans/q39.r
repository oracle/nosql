compiled-query-plan

{
"query file" : "idc_schemaless/q/q39.q",
"plan" : 
{
  "iterator kind" : "SORT",
  "order by fields at positions" : [ 0 ],
  "input iterator" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_SHARDS",
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "jsoncol",
        "row variable" : "$$f",
        "index used" : "idx_index",
        "covering index" : true,
        "index row variable" : "$$f_idx",
        "index scans" : [
          {
            "equality conditions" : {},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$f_idx",
      "SELECT expressions" : [
        {
          "field name" : "majorKey1",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "#majorKey1",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f_idx"
            }
          }
        },
        {
          "field name" : "Column_2",
          "field expression" : 
          {
            "iterator kind" : "FUNC_SIZE",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "index",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$f_idx"
              }
            }
          }
        }
      ]
    }
  }
}
}