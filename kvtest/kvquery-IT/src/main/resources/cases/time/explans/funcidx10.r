compiled-query-plan

{
"query file" : "time/q/funcidx10.q",
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
      "target table" : "foo",
      "row variable" : "$f",
      "index used" : "idx_modtime",
      "covering index" : true,
      "index row variable" : "$f_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : { "modification_time#" : { "end value" : "-6384-00-00T00:00:00.000Z", "end inclusive" : false } }
        }
      ],
      "key bind expressions" : [
        {
          "iterator kind" : "FUNC_CURRENT_TIME"
        }
      ],
      "map of key bind expressions" : [
        [ -1, 0 ]
      ],
      "position in join" : 0
    },
    "FROM variable" : "$f_idx",
    "SELECT expressions" : [
      {
        "field name" : "id1",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "#id1",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$f_idx"
          }
        }
      },
      {
        "field name" : "Column_2",
        "field expression" : 
        {
          "iterator kind" : "GREATER_THAN",
          "left operand" :
          {
            "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "modification_time#",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$f_idx"
              }
            }
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 2020
          }
        }
      }
    ]
  }
}
}