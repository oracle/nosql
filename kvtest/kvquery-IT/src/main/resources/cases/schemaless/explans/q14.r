compiled-query-plan

{
"query file" : "schemaless/q/q14.q",
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
      "target table" : "Viewers",
      "row variable" : "$$v",
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
    "FROM variable" : "$$v",
    "WHERE" : 
    {
      "iterator kind" : "OP_EXISTS",
      "input iterator" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "a with space",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$$v"
        }
      }
    },
    "SELECT expressions" : [
      {
        "field name" : "v",
        "field expression" : 
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$$v"
        }
      }
    ]
  }
}
}