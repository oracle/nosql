compiled-query-plan

{
"query file" : "queryspec/q/ufuncidx01.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
  "input iterator" :
  {
    "iterator kind" : "UPDATE_ROW",
    "indexes to update" : [ "idx_areacodes_income", "idx_areacodes_kinds_income", "idx_connections", "idx_exp_time", "idx_expenses_books", "idx_expenses_keys_values", "idx_income", "idx_mod_time", "idx_row_size", "idx_state_city_income", "idx_substring", "idx_year_month" ],
    "update clauses" : [

    ],
    "update TTL" : true,
    "TimeUnit" : "HOURS",
    "TTL iterator" :
    {
      "iterator kind" : "CONST",
      "value" : 1
    },
    "isCompletePrimaryKey" : true,
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "Users",
        "row variable" : "$$users",
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
      "FROM variable" : "$$users",
      "SELECT expressions" : [
        {
          "field name" : "users",
          "field expression" : 
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$users"
          }
        }
      ]
    }
  }
}
}