compiled-query-plan

{
"query file" : "unnest/q/q2.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "Foo",
      "row variable" : "$t",
      "index used" : "idx_state_city_age",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"address.state":"MA"},
          "range conditions" : { "address.city" : { "start value" : "F", "start inclusive" : false } }
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$t",
    "FROM" :
    {
      "iterator kind" : "ARRAY_FILTER",
      "input iterator" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "phones",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "address",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$t"
          }
        }
      }
    },
    "FROM variable" : "$phone",
    "SELECT expressions" : [
      {
        "field name" : "t",
        "field expression" : 
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$t"
        }
      },
      {
        "field name" : "phone",
        "field expression" : 
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$phone"
        }
      }
    ]
  }
}
}