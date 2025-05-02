compiled-query-plan

{
"query file" : "idc_schemaless/q/q33.q",
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
      "target table" : "jsoncol",
      "row variable" : "$f",
      "index used" : "idx_name",
      "covering index" : true,
      "index row variable" : "$f_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$f_idx",
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
            "variable" : "$f_idx"
          }
        }
      },
      {
        "field name" : "index_size",
        "field expression" : 
        {
          "iterator kind" : "AND",
          "input iterators" : [
            {
              "iterator kind" : "LESS_OR_EQUAL",
              "left operand" :
              {
                "iterator kind" : "CONST",
                "value" : 0
              },
              "right operand" :
              {
                "iterator kind" : "FUNC_INDEX_STORAGE_SIZE",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$f_idx"
                }
              }
            },
            {
              "iterator kind" : "LESS_OR_EQUAL",
              "left operand" :
              {
                "iterator kind" : "FUNC_INDEX_STORAGE_SIZE",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$f_idx"
                }
              },
              "right operand" :
              {
                "iterator kind" : "CONST",
                "value" : 50
              }
            }
          ]
        }
      }
    ]
  }
}
}