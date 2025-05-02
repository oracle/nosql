compiled-query-plan

{
"query file" : "sec_index/q/q15.q",
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
      "target table" : "keyOnly",
      "row variable" : "$$keyOnly",
      "index used" : "First",
      "covering index" : true,
      "index row variable" : "$$keyOnly_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : { "firstName" : { "start value" : "first1", "start inclusive" : false } }
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$keyOnly_idx",
    "SELECT expressions" : [
      {
        "field name" : "firstName",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "firstName",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$keyOnly_idx"
          }
        }
      },
      {
        "field name" : "lastName",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "#lastName",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$keyOnly_idx"
          }
        }
      },
      {
        "field name" : "age",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "#age",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$keyOnly_idx"
          }
        }
      },
      {
        "field name" : "id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "#id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$keyOnly_idx"
          }
        }
      }
    ]
  }
}
}