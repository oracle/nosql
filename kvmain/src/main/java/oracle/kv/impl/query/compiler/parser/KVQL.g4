/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

/*
 * IMPORTANT: the code generation by Antlr using this grammar is not
 * automatically part of the build.  It has its own target:
 *   ant generate-ddl
 * If this file is modified that target must be run.  Once all relevant
 * testing is done the resulting files must be modified to avoid warnings and
 * errors in Eclipse because of unused imports.  At that point the new files can
 * be check in.
 *
 * This file describes the syntax of the Oracle NoSQL Table DDL.  In order to
 * make the syntax as familiar as possible, the following hierarchy of existing
 * languages is used as the model for each operation:
 * 1.  SQL standard
 * 2.  Oracle SQL
 * 3.  MySQL
 * 4.  SQLite or other SQL
 * 5.  Any comparable declarative language
 * 6.  New syntax, specific to Oracle NoSQL.
 *
 * The major commands in this grammar include
 * o create/drop table
 * o alter table
 * o create/drop index
 * o describe
 * o show
 *
 * Grammar notes:
 *  Antlr resolves ambiguity in token recognition by using order of
 *  declaration, so order matters when ambiguity is possible.  This is most
 *  typical with different types of identifiers that have some overlap.
 *
 *  This grammar uses some extra syntax and Antlr actions to generate more
 *  useful error messages (see use of "notifyErrorListeners").  This is Java-
 *  specific code.  In the future it may be useful to parse in other languages,
 *  which is supported by Antlr4.  If done, these errors may have to be handled
 *  elsewhere or we'd have to handle multiple versions of the grammar with minor
 *  changes for language-specific constructs.
 */

/*
 * Parser rules (start with lowercase).
 */

grammar KVQL;

/*
 * This is the starting rule for the parse.  It accepts one of the number of
 * top-level statements.  The EOF indicates that only a single statement can
 * be accepted and any extraneous input after the statement will generate an
 * error.  Allow a semicolon to terminate statements.  In the future a semicolon
 * may act as a statement separator.
 */
parse : statement EOF;

statement :
    (
    query
  | insert_statement
  | update_statement
  | delete_statement
  | create_table_statement
  | create_index_statement
  | create_user_statement
  | create_role_statement
  | create_namespace_statement
  | create_region_statement
  | drop_index_statement
  | drop_namespace_statement
  | drop_region_statement
  | create_text_index_statement
  | drop_role_statement
  | drop_user_statement
  | alter_table_statement
  | alter_user_statement
  | drop_table_statement
  | grant_statement
  | revoke_statement
  | describe_statement
  | set_local_region_statement
  | show_statement
  | index_function_path) ;

/******************************************************************************
 *
 * Query expressions
 *
 ******************************************************************************/

query : prolog? sfw_expr ;

prolog : DECLARE var_decl SEMI (var_decl SEMI)*;

var_decl : VARNAME type_def;

index_function_path : prolog func_call ;

/*
 * TODO : add sfw_expr here when subqueries are supported
 */
expr : or_expr ;


sfw_expr :
    select_clause
    from_clause
    where_clause?
    groupby_clause?
    orderby_clause?
    limit_clause?
    offset_clause? ;

from_clause :
    FROM table_spec (COMMA table_spec)*
         (COMMA ( (expr (AS? VARNAME)) | unnest_clause ) )*;

table_spec : from_table | nested_tables | left_outer_join_tables ;

nested_tables :
    NESTED TABLES
    LP
    from_table
    (ANCESTORS LP ancestor_tables RP) ?
    (DESCENDANTS LP descendant_tables RP) ?
    RP ;

ancestor_tables : from_table (COMMA from_table)* ;

descendant_tables : from_table (COMMA from_table)* ;

left_outer_join_tables :
    from_table left_outer_join_table (left_outer_join_table)* ;

left_outer_join_table : LEFT_OUTER_JOIN from_table ;

/*
 * The ON clause is not allowed for the target table. The grammar allows it,
 * but the translator throws error if present. It is allowed in the grammar
 * for implementation convenience.
 */
from_table : aliased_table_name (ON or_expr)? ;

aliased_table_name : (table_name) (AS? tab_alias)? ;

tab_alias : VARNAME | id ;

unnest_clause :
    UNNEST LP path_expr (AS? VARNAME) (COMMA (path_expr (AS? VARNAME)))* RP ;

where_clause : WHERE expr ;

select_clause : SELECT select_list ;

select_list : hints? DISTINCT? ( STAR | (expr col_alias (COMMA expr col_alias)*) ) ;

hints : '/*+' hint* '*/' ;

hint : ( (PREFER_INDEXES LP table_name index_name* RP) |
         (FORCE_INDEX    LP table_name index_name  RP) |
         (PREFER_PRIMARY_INDEX LP table_name RP)       |
         (FORCE_PRIMARY_INDEX  LP table_name RP) ) STRING?;

col_alias : (AS id)? ;

orderby_clause : ORDER BY expr sort_spec (COMMA expr sort_spec)* ;

sort_spec : (ASC | DESC)? (NULLS (FIRST | LAST))? ;

groupby_clause : GROUP BY expr (COMMA expr)* ;

limit_clause : LIMIT add_expr;

offset_clause : OFFSET add_expr ;

or_expr : and_expr | or_expr OR and_expr ;

and_expr : not_expr | and_expr AND not_expr ;

not_expr : NOT? is_null_expr ;

is_null_expr : cond_expr (IS NOT? NULL)? ;

cond_expr : between_expr | comp_expr | in_expr | exists_expr | is_of_type_expr ;

between_expr : concatenate_expr BETWEEN concatenate_expr AND concatenate_expr ;

comp_expr : (concatenate_expr ((comp_op | any_op) concatenate_expr)?) ;

comp_op : EQ | NEQ | GT | GTE | LT | LTE ;

any_op : (EQ_ANY) | (NEQ_ANY) | (GT_ANY) | (GTE_ANY) | (LT_ANY) | (LTE_ANY);

in_expr : in1_expr | in2_expr | in3_expr ;

in1_expr : in1_left_op IN LP in1_expr_list (COMMA in1_expr_list)+ RP ;

in1_left_op :  LP concatenate_expr (COMMA concatenate_expr)* RP ;

in1_expr_list : LP expr (COMMA expr)* RP ;

in2_expr : concatenate_expr IN LP expr (COMMA expr)+ RP ;

in3_expr : (concatenate_expr |
           (LP concatenate_expr (COMMA concatenate_expr)* RP)) IN path_expr ;

exists_expr : EXISTS concatenate_expr ;

is_of_type_expr : concatenate_expr IS NOT? OF TYPE?
        LP ONLY? quantified_type_def ( COMMA ONLY? quantified_type_def )* RP;

concatenate_expr : add_expr ( CONCAT add_expr)* ;

add_expr : multiply_expr ((PLUS | MINUS) multiply_expr)* ;

multiply_expr : unary_expr ((STAR | IDIV | RDIV) unary_expr)* ;

unary_expr : path_expr | (PLUS | MINUS) unary_expr ;

path_expr : primary_expr (map_step | array_step)* ;

/*
 * It's important that filtering_field_step appears before simple_field_step,
 * because a filtering_field_step looks like a function call, which is also
 * part of simple_field_step. By putting filtering_field_step first, it takes
 * priority over the func_call production.
 */
map_step : DOT ( map_filter_step | map_field_step );

map_field_step : ( id | string | var_ref | parenthesized_expr | func_call );

map_filter_step : (KEYS | VALUES) LP expr? RP ;

array_step : array_filter_step | array_slice_step;

array_slice_step : LBRACK expr? COLON expr? RBRACK ;

array_filter_step : LBRACK expr? RBRACK ;

primary_expr :
    const_expr |
    column_ref |
    var_ref |
    array_constructor |
    map_constructor |
    transform_expr |
    collect |
    func_call |
    count_star |
    count_distinct |
    case_expr |
    cast_expr |
    parenthesized_expr |
    extract_expr ;

/*
 * If there are 2 ids, the first one refers to a table name/alias and the
 * second to a column in that table. A single id refers to a column in some
 * of the table in the FROM clause. If more than one table has a column of
 * that name, an error is thrown. In this case, the user has to rewrite the
 * query to use table aliases to resolve the ambiguity.
 */
column_ref : id (DOT (id | string))? ;

/*
 * INT/FLOAT literals are translated to Long/Double values.
 */
const_expr : number | string | TRUE | FALSE | NULL;

var_ref : VARNAME | DOLLAR | QUESTION_MARK ;

array_constructor : LBRACK expr? (COMMA expr)* RBRACK ;

map_constructor :
    (LBRACE expr COLON expr (COMMA expr COLON expr)* RBRACE) |
    (LBRACE RBRACE) ;

transform_expr : SEQ_TRANSFORM LP transform_input_expr COMMA expr RP;

transform_input_expr : expr ;

collect : ARRAY_COLLECT LP DISTINCT? expr RP;

func_call : id LP (expr (COMMA expr)*)? RP ;

count_star : COUNT LP STAR RP ;

count_distinct : COUNT LP DISTINCT expr RP ;

case_expr : CASE WHEN expr THEN expr (WHEN expr THEN expr)* (ELSE expr)? END;

cast_expr : CAST LP expr AS quantified_type_def RP ;

parenthesized_expr : LP expr RP;

extract_expr : EXTRACT LP id FROM expr RP ;

/******************************************************************************
 *
 * updates
 *
 ******************************************************************************/

insert_statement :
    prolog?
    (INSERT | UPSERT) INTO table_name (AS? tab_alias)?
    (LP insert_label (COMMA insert_label)* RP)?
    VALUES LP insert_clause (COMMA insert_clause)* RP
    (SET TTL insert_ttl_clause)?
    insert_returning_clause? ;

/* JSON collection tables allow top-level strings as field names */
insert_label : id | string ;

insert_returning_clause : RETURNING select_list ;

insert_clause : DEFAULT | expr;

insert_ttl_clause : (add_expr (HOURS | DAYS)) | (USING TABLE DEFAULT) ;

update_statement :
    prolog?
    UPDATE table_name (AS? tab_alias)? update_clause (COMMA update_clause)*
    WHERE expr
    update_returning_clause? ;

update_returning_clause : RETURNING select_list ;

update_clause :
    (SET set_clause (COMMA (update_clause | set_clause))*) |
    (ADD add_clause (COMMA (update_clause | add_clause))*) |
    (PUT put_clause (COMMA (update_clause | put_clause))*) |
    (REMOVE remove_clause (COMMA (update_clause | remove_clause))*) |
    (JSON MERGE json_merge_patch_clause (COMMA (update_clause |
                                               json_merge_patch_clause))*) |
    (SET TTL ttl_clause (COMMA update_clause)*) ;

set_clause : target_expr EQ expr ;

add_clause : INTO? target_expr (('@')? pos_expr)? ELEMENTS? expr ;

put_clause : INTO? target_expr FIELDS? expr ;

remove_clause : target_expr ;

json_merge_patch_clause : target_expr WITH PATCH json_patch_expr ;

json_patch_expr : map_constructor | array_constructor | const_expr | var_ref ;

ttl_clause : (add_expr (HOURS | DAYS)) | (USING TABLE DEFAULT) ;

target_expr : path_expr ;

pos_expr : add_expr ;


delete_statement :
    prolog?
    DELETE FROM table_name (AS? tab_alias)? (WHERE expr)?
    delete_returning_clause? ;

delete_returning_clause : RETURNING select_list ;


/******************************************************************************
 *
 * Types
 *
 ******************************************************************************/

quantified_type_def : type_def ( STAR | PLUS | QUESTION_MARK)? ;

/*
 * All supported type definitions. The # labels on each line cause Antlr to
 * generate events specific to that type, which allows the parser code to more
 * simply discriminate among types.
 */
type_def :
    binary_def         # Binary
  | array_def          # Array
  | boolean_def        # Boolean
  | enum_def           # Enum
  | float_def          # Float        // float, double, number
  | integer_def        # Int          // int, long
  | json_def           # JSON
  | map_def            # Map
  | record_def         # Record
  | string_def         # StringT
  | timestamp_def      # Timestamp
  | any_def            # Any           // not allowed in DDL
  | anyAtomic_def      # AnyAtomic     // not allowed in DDL
  | anyJsonAtomic_def  # AnyJsonAtomic // not allowed in DDL
  | anyRecord_def      # AnyRecord     // not allowed in DDL
  ;

/*
 * A record contains one or more field definitions.
 */
record_def : RECORD_T LP field_def (COMMA field_def)* RP ;

/*
 * A definition of a named, typed field within a record.
 * The field name must be an identifier, rather than the more general
 * field_name syntax. This restriction is placed by AVRO!
 */
field_def : id type_def default_def? comment? ;

/*
 * The translator checks that the value conforms to the associated type.
 * Binary fields have no default value and as a result they are always nullable.
 * This is enforced in code.
 */
default_def : (default_value not_null?) | (not_null default_value?) ;

/*
 * The id alternative is used for enum defaults. The translator
 * checks that the type of the defualt value conforms with the type of
 * the field.
 */
default_value : DEFAULT (number | string | TRUE | FALSE | id) ;

not_null : NOT NULL ;

map_def : MAP_T LP type_def RP ;

array_def : ARRAY_T LP type_def RP ;

integer_def : (INTEGER_T | LONG_T) ;

json_def : JSON ;

float_def : (FLOAT_T | DOUBLE_T | NUMBER_T) ;

string_def : STRING_T ;

/*
 * Enumeration is defined by a list of ID values.
 *   enum (val1, val2, ...)
 */
enum_def : (ENUM_T LP id_list RP) |
           (ENUM_T LP id_list { notifyErrorListeners("Missing closing ')'"); }) ;

boolean_def : BOOLEAN_T ;

binary_def : BINARY_T (LP INT RP)? ;

timestamp_def: TIMESTAMP_T (LP INT RP)? ;

any_def : ANY_T ;

anyAtomic_def : ANYATOMIC_T;

anyJsonAtomic_def : ANYJSONATOMIC_T;

anyRecord_def : ANYRECORD_T;

/******************************************************************************
 *
 * DDL statements
 *
 ******************************************************************************/

/*
 * id_path is used for table and index names. name_path is used for field paths.
 * Both of these may have multiple components. Table names may reference child
 * tables using dot notation and similarly, field paths may reference nested
 * fields using dot notation as well.
 */
id_path : id (DOT id)* ;

table_id_path : table_id (DOT table_id)* ;

table_id : (SYSDOLAR)? id ;

name_path : field_name (DOT field_name)* ;

field_name : id | DSTRING ;

/*
 * Namespace
 */
create_namespace_statement :
    CREATE NAMESPACE (IF NOT EXISTS)? namespace;

drop_namespace_statement :
    DROP NAMESPACE (IF EXISTS)? namespace CASCADE?;

/*
 * Region statements
 */
region_name : id ;

create_region_statement :
    CREATE REGION region_name;

drop_region_statement :
    DROP REGION  region_name;

set_local_region_statement :
    SET LOCAL REGION region_name;

/*
 * CREATE TABLE.
 */
create_table_statement :
    CREATE TABLE (IF NOT EXISTS)? table_name comment? LP table_def RP
    table_options? ;

table_name : (namespace ':' )? table_id_path ;

namespace :  id_path ;

table_def : (column_def | key_def | json_collection_mrcounter_def) (COMMA (column_def | key_def | json_collection_mrcounter_def))* ;

column_def : id type_def (default_def | identity_def | uuid_def |
             mr_counter_def | json_mrcounter_fields)? comment? ;

json_mrcounter_fields : LP json_mrcounter_def (COMMA json_mrcounter_def)* RP ;

json_mrcounter_def : json_mrcounter_path AS (INTEGER_T | LONG_T | NUMBER_T)
                     MR_COUNTER ;

json_collection_mrcounter_def : json_mrcounter_def ;

json_mrcounter_path : (id | string) (DOT (id | string))* ;

key_def : PRIMARY KEY LP (shard_key_def COMMA?)? id_list_with_size? RP ;

shard_key_def :
    (SHARD LP id_list_with_size RP) |
    (LP id_list_with_size { notifyErrorListeners("Missing closing ')'"); }) ;

id_list_with_size : id_with_size (COMMA id_with_size)* ;

id_with_size : id storage_size? ;

storage_size : LP INT RP ;

table_options : 
    (ttl_def |
     regions_def |
     frozen_def |
     json_collection_def |
     enable_before_image)+ ;

ttl_def : USING TTL duration ;

region_names : id_list ;

regions_def : IN REGIONS region_names ;

frozen_def : WITH SCHEMA FROZEN FORCE?;

json_collection_def : AS JSON COLLECTION ;

enable_before_image : ENABLE BEFORE IMAGE before_image_ttl? ;

before_image_ttl : USING TTL duration ;

disable_before_image : DISABLE BEFORE IMAGE ;

/*
 * This is used for setting a field as identity field.
 */
identity_def :
    GENERATED (ALWAYS | (BY DEFAULT (ON NULL)?)) AS IDENTITY
    (LP sequence_options+ RP)?;

sequence_options :
    (START WITH signed_int) |
    (INCREMENT BY signed_int) |
    (MAXVALUE signed_int) | (NO MAXVALUE) |
    (MINVALUE signed_int) | (NO MINVALUE) |
    (CACHE INT) | (NO CACHE) |
    CYCLE | (NO CYCLE);

mr_counter_def :
    AS MR_COUNTER ;

/*
 * This is used for setting a STRING field AS UUID
 */
uuid_def :
    AS UUID (GENERATED BY DEFAULT)?;

/*
 * ALTER TABLE
 */
alter_table_statement : ALTER TABLE table_name alter_def ;

alter_def : alter_field_statements | ttl_def |
            add_region_def | drop_region_def |
            freeze_def | unfreeze_def |
            enable_before_image | disable_before_image ;

freeze_def: FREEZE SCHEMA FORCE?;

unfreeze_def: UNFREEZE SCHEMA ;

add_region_def : ADD REGIONS region_names ;

drop_region_def : DROP REGIONS region_names ;

/*
 * Table modification -- add, drop, modify fields in an existing table.
 * This definition allows multiple changes to be contained in a single
 * alter table statement.
 */
alter_field_statements :
    LP
    (add_field_statement | drop_field_statement | modify_field_statement)
    (COMMA (add_field_statement | drop_field_statement | modify_field_statement))*
    RP ;

add_field_statement : ADD schema_path type_def (default_def | identity_def |
    mr_counter_def | uuid_def | json_mrcounter_fields)? comment? ;

drop_field_statement : DROP schema_path ;

/* MODIFY schema_path type_def - option not actually implemented, error is
   thrown in Translator */
modify_field_statement :
    MODIFY schema_path ((type_def default_def? comment?) |
                         identity_def |  uuid_def | DROP IDENTITY );

schema_path : init_schema_path_step (DOT schema_path_step)*;

init_schema_path_step : id (LBRACK RBRACK)* ;

schema_path_step : id (LBRACK RBRACK)* | VALUES LP RP ;

/*
 * DROP TABLE
 */
drop_table_statement : DROP TABLE (IF EXISTS)? table_name ;

/*
 * CREATE INDEX
 */
create_index_statement :
    CREATE INDEX (IF NOT EXISTS)? index_name ON table_name
    ( (LP index_field_list RP (WITH NO? NULLS)? (WITH UNIQUE KEYS PER ROW)? ) |
      (LP index_field_list { notifyErrorListeners("Missing closing ')'"); }) )
    comment?;

index_name : id ;

/*
 * A comma-separated list of field paths that may or may not reference nested
 * fields. This is used to reference fields in an index or a describe statement.
 */
index_field_list : index_field (COMMA index_field)* ;

index_field : (index_path path_type?) | index_function ;

index_function : id LP index_path? path_type? index_function_args? RP ;

index_function_args : (COMMA const_expr)+ ;

/*
 * index_path handles a basic name_path but adds .keys(), .values(), and []
 * steps to handle addressing in maps and arrays.
 * NOTE: if the syntax of .keys(), .values(), or [] changes the source should
 * be checked for code that reproduces these constants.
 */
index_path :
    (row_metadata? (name_path | multikey_path_prefix multikey_path_suffix? )) |
    old_index_path;

old_index_path :
     ELEMENTOF LP name_path RP multikey_path_suffix? |
     KEYOF LP name_path RP |
     KEYS LP name_path RP ;

row_metadata : 'row_metadata().' ;

multikey_path_prefix :
    field_name
    ((DOT field_name) | (DOT VALUES LP RP) | (LBRACK RBRACK))*
    ((LBRACK RBRACK) | (DOT VALUES LP RP) | (DOT KEYS LP RP));

multikey_path_suffix : (DOT name_path) ;

path_type :
    AS (INTEGER_T | LONG_T | DOUBLE_T | STRING_T | BOOLEAN_T | NUMBER_T |
        ANYATOMIC_T |
        (GEOMETRY_T jsobject ?) | POINT_T);

/*
 * CREATE FULLTEXT INDEX [if not exists] name ON name_path (field [ mapping ], ...)
 */
create_text_index_statement :
    CREATE FULLTEXT INDEX (IF NOT EXISTS)?
    index_name ON table_name fts_field_list es_properties? OVERRIDE? comment?;

/*
 * A list of field names, as above, which may or may not include
 * a text-search mapping specification per field.
 */
fts_field_list :
    LP fts_path_list RP |
    LP fts_path_list {notifyErrorListeners("Missing closing ')'");}
    ;

/*
 * A comma-separated list of paths to field names with optional mapping specs.
 */
fts_path_list : fts_path (COMMA fts_path)* ;

/*
 * A field name with optional mapping spec.
 */
fts_path : index_path jsobject? ;

es_properties: es_property_assignment es_property_assignment* ;

es_property_assignment: ES_SHARDS EQ INT | ES_REPLICAS EQ INT ;

/*
 * DROP INDEX [if exists] index_name ON table_name
 */
drop_index_statement : DROP INDEX (IF EXISTS)? index_name ON table_name
                       OVERRIDE? ;

/*
 * DESC[RIBE] TABLE table_name [field_path[,field_path]]
 * DESC[RIBE] INDEX index_name ON table_name
 */
describe_statement :
    (DESCRIBE | DESC) (AS JSON)?
    (TABLE (table_name) (
             (LP schema_path_list RP) |
             (LP schema_path_list { notifyErrorListeners("Missing closing ')'")
             ; })
           )? |
     INDEX index_name ON table_name) ;

schema_path_list : schema_path (COMMA schema_path)* ;

/*
 * SHOW TABLES
 * SHOW INDEXES ON table_name
 * SHOW TABLE table_name -- lists hierarchy of the table
 * SHOW NAMESPACES   -- lists available namespaces
 * SHOW REGIONS     -- lists active regions
 */
show_statement: SHOW (AS JSON)?
        (TABLES
        | USERS
        | ROLES
        | USER identifier_or_string
        | ROLE id
        | INDEXES ON table_name
        | TABLE table_name
        | NAMESPACES
        | REGIONS )
  ;


/******************************************************************************
 *
 * Parse rules of security commands.
 *
 ******************************************************************************/

/*
 * CREATE USER user (IDENTIFIED BY password [PASSWORD EXPIRE]
 * [PASSWORD LIFETIME duration] | IDENTIFIED EXTERNALLY)
 * [ACCOUNT LOCK|UNLOCK] [ADMIN]
 */
create_user_statement :
    CREATE USER create_user_identified_clause account_lock? ADMIN? ;

/*
 * CREATE ROLE role
 */
create_role_statement : CREATE ROLE id ;

/*
 * ALTER USER user [IDENTIFIED BY password [RETAIN CURRENT PASSWORD]]
 *       [CLEAR RETAINED PASSWORD] [PASSWORD EXPIRE]
 *       [PASSWORD LIFETIME duration] [ACCOUNT UNLOCK|LOCK]
 */
alter_user_statement : ALTER USER identifier_or_string
    reset_password_clause? (CLEAR_RETAINED_PASSWORD)? (PASSWORD_EXPIRE)?
        password_lifetime? account_lock? ;

/*
 * DROP USER user [CASCADE]
 */
drop_user_statement : DROP USER identifier_or_string (CASCADE)?;

/*
 * DROP ROLE role_name
 */
drop_role_statement : DROP ROLE id ;

/*
 * GRANT (grant_roles|grant_system_privileges|grant_object_privileges)
 *     grant_roles ::= role [, role]... TO { USER user | ROLE role }
 *     grant_system_privileges ::=
 *         {system_privilege | ALL PRIVILEGES}
 *             [,{system_privilege | ALL PRIVILEGES}]...
 *         TO role
 *     grant_object_privileges ::=
 *         {object_privileges| ALL [PRIVILEGES]}
 *             [,{object_privileges| ALL [PRIVILEGES]}]...
 *         ON table TO role
 */
grant_statement : GRANT
        (grant_roles
        | grant_system_privileges
        | grant_object_privileges)
    ;

/*
 * REVOKE (revoke_roles | revoke_system_privileges | revoke_object_privileges)
 *     revoke_roles ::= role [, role]... FROM { user | role }
 *     revoke_system_privileges ::=
 *         {system_privilege | ALL PRIVILEGES}
 *             [, {system_privilege | ALL PRIVILEGES}]...
 *         FROM role
 *     revoke_object_privileges ::=
 *         {object_privileges| ALL [PRIVILEGES]}
 *             [, { object_privileges | ALL [PRIVILEGES] }]...
 *         ON object FROM role
 */
revoke_statement : REVOKE
        (revoke_roles
        | revoke_system_privileges
        | revoke_object_privileges)
    ;

/*
 * An identifier or a string
 */
identifier_or_string : (id | string);

/*
 * Identified clause, indicates the authentication method of user.
 */
identified_clause : IDENTIFIED by_password ;

/*
 * Identified clause for create user command, indicates the authentication
 * method of user. If the user is an internal user, we use the extended_id
 * for the user name. If the user is an external user, we use STRING for
 * the user name.
 */
create_user_identified_clause :
    id identified_clause (PASSWORD_EXPIRE)? password_lifetime? |
    string IDENTIFIED_EXTERNALLY ;

/*
 * Rule of authentication by password.
 */
by_password : BY string;

/*
 * Rule of password lifetime definition.
 */
password_lifetime : PASSWORD LIFETIME duration;

/*
 * Rule of defining the reset password clause in the alter user statement.
 */
reset_password_clause : identified_clause RETAIN_CURRENT_PASSWORD? ;

account_lock : ACCOUNT (LOCK | UNLOCK) ;

/*
 * Subrule of granting roles to a user or a role.
 */
grant_roles : id_list TO principal ;

/*
 * Subrule of granting system privileges to a role.
 */
grant_system_privileges : sys_priv_list TO id ;

/*
 * Subrule of granting object privileges to a role.
 */
grant_object_privileges : obj_priv_list ON
    ( object | NAMESPACE namespace ) TO id ;

/*
 * Subrule of revoking roles from a user or a role.
 */
revoke_roles : id_list FROM principal ;

/*
 * Subrule of revoking system privileges from a role.
 */
revoke_system_privileges : sys_priv_list FROM id ;

/*
 * Subrule of revoking object privileges from a role.
 */
revoke_object_privileges : obj_priv_list ON
    ( object | NAMESPACE namespace ) FROM id  ;

/*
 * Parsing a principal of user or role.
 */
principal : (USER identifier_or_string | ROLE id) ;

sys_priv_list : priv_item (COMMA priv_item)* ;

priv_item : (id | ALL_PRIVILEGES) ;

obj_priv_list : (priv_item | ALL) (COMMA (priv_item | ALL))* ;

/*
 * Subrule of parsing the operated object. For now, only table object is
 * available.
 */
object : table_name ;


/******************************************************************************
 *
 * Literals and identifiers
 *
 ******************************************************************************/

/*
 * Simple JSON parser, derived from example in Terence Parr's book,
 * _The Definitive Antlr 4 Reference_.
 */
json_text : jsobject | jsarray | string | number | TRUE | FALSE | NULL;

jsobject
    :   LBRACE jspair (',' jspair)* RBRACE    # JsonObject
    |   LBRACE RBRACE                         # EmptyJsonObject ;

jsarray
    :   LBRACK jsvalue (',' jsvalue)* RBRACK  # ArrayOfJsonValues
    |   LBRACK RBRACK                         # EmptyJsonArray ;

jspair :   DSTRING ':' jsvalue                 # JsonPair ;

jsvalue
    :   jsobject  	# JsonObjectValue
    |   jsarray  	# JsonArrayValue
    |   DSTRING		# JsonAtom
    |   number      # JsonAtom
    |   TRUE		# JsonAtom
    |   FALSE		# JsonAtom
    |   NULL		# JsonAtom ;


comment : COMMENT string ;

duration : INT time_unit ;

time_unit : (SECONDS | MINUTES | HOURS | DAYS) ;

/* this is a parser rule to allow space between '-' and digits */
number : '-'? (INT | FLOAT | NUMBER);

signed_int : ('-'|'+')?INT;

string : STRING | DSTRING ;

/*
 * Identifiers
 */

id_list : id (COMMA id)* ;

id :
    (ACCOUNT | ADD | ADMIN | ALL | ALTER | ALWAYS| ANCESTORS | AND | ANY_T |
     ANYATOMIC_T | ANYJSONATOMIC_T | ANYRECORD_T | ARRAY_COLLECT | AS | ASC |
     BEFORE | BETWEEN | BY | CACHE | CASE | CAST | COLLECTION | COMMENT | COUNT |
     CREATE | CYCLE | DAYS | DECLARE | DEFAULT | DELETE | DESC | DESCENDANTS |
     DESCRIBE | DISABLE | DISTINCT | DROP |
     ELEMENTOF | ELEMENTS | ELSE | ENABLE | END | ES_SHARDS | ES_REPLICAS |
     EXISTS | EXTRACT |
     FIELDS | FIRST | FREEZE | FROM | FROZEN | FULLTEXT |
     GENERATED | GRANT | GROUP | HOURS |
     IDENTIFIED | IDENTITY | IF | INCREMENT | IMAGE | INDEX | INDEXES | INSERT |
     INTO | IN | IS | JSON | KEY | KEYOF | KEYS |
     LIFETIME | LAST | LIMIT | LOCAL | LOCK | MERGE | MINUTES | MODIFY | MR_COUNTER
     NAMESPACE | NAMESPACES | NESTED | NO | NOT | NULLS |
     OF | OFFSET | ON | OR | ORDER | OVERRIDE |
     PER | PASSWORD | PATCH | PRIMARY | PUT |
     RDIV | REGION | REGIONS | REMOVE | RETURNING | ROW | ROLE | ROLES |
     REVOKE | SCHEMA | SECONDS | SELECT | SEQ_TRANSFORM| SET |
     SHARD | SHOW | START | TABLE | TABLES | THEN | TO | TTL | TYPE |
     UNFREEZE | UNLOCK | UNIQUE | UNNEST | UPDATE | UPSERT | USER | USERS |
     USING | VALUES | WHEN | WHERE | WITH |
     ARRAY_T |  BINARY_T | BOOLEAN_T | DOUBLE_T | ENUM_T | FLOAT_T |
     GEOMETRY_T | LONG_T | INTEGER_T | MAP_T | NUMBER_T | POINT_T | RECORD_T |
     STRING_T | TIMESTAMP_T | SCALAR_T |
     ID) |
     BAD_ID
     {
        notifyErrorListeners("Identifiers must start with a letter: " + $text);
     }
  ;


/******************************************************************************
 * Lexical rules (start with uppercase)
 *
 * Keywords need to be case-insensitive, which makes their lexical rules a bit
 * more complicated than simple strings.
 ******************************************************************************/

VARNAME : DOLLAR ALPHA (ALPHA | DIGIT | UNDER)* ;

/*
 * Keywords
 */

ACCOUNT : [Aa][Cc][Cc][Oo][Uu][Nn][Tt] ;

ADD : [Aa][Dd][Dd] ;

ADMIN : [Aa][Dd][Mm][Ii][Nn] ;

ALL : [Aa][Ll][Ll] ;

ALTER : [Aa][Ll][Tt][Ee][Rr] ;

ALWAYS : [Aa][Ll][Ww][Aa][Yy][Ss] ;

ANCESTORS : [Aa][Nn][Cc][Ee][Ss][Tt][Oo][Rr][Ss] ;

AND : [Aa][Nn][Dd] ;

AS : [Aa][Ss] ;

ASC : [Aa][Ss][Cc];

ARRAY_COLLECT : 'array_collect' ;

BEFORE : [Bb][Ee][Ff][Oo][Rr][Ee] ;

BETWEEN : [Bb][Ee][Tt][Ww][Ee][Ee][Nn] ;

BY : [Bb][Yy] ;

CACHE : [Cc][Aa][Cc][Hh][Ee] ;

CASE : [Cc][Aa][Ss][Ee] ;

CASCADE : [Cc][Aa][Ss][Cc][Aa][Dd][Ee] ;

CAST : [Cc][Aa][Ss][Tt] ;

COLLECTION : [Cc][Oo][Ll][Ll][Ee][Cc][Tt][Ii][Oo][Nn] ;

COMMENT : [Cc][Oo][Mm][Mm][Ee][Nn][Tt] ;

COUNT : 'count' ;

CREATE : [Cc][Rr][Ee][Aa][Tt][Ee] ;

CYCLE : [Cc][Yy][Cc][Ll][Ee] ;

DAYS : ([Dd] | [Dd][Aa][Yy][Ss]) ;

DECLARE : [Dd][Ee][Cc][Ll][Aa][Rr][Ee] ;

DEFAULT : [Dd][Ee][Ff][Aa][Uu][Ll][Tt] ;

DELETE : [Dd][Ee][Ll][Ee][Tt][Ee] ;

DESC : [Dd][Ee][Ss][Cc] ;

DESCENDANTS : [Dd][Ee][Ss][Cc][Ee][Nn][Dd][Aa][Nn][Tt][Ss] ;

DESCRIBE : [Dd][Ee][Ss][Cc][Rr][Ii][Bb][Ee] ;

DISABLE : [Dd][Ii][Ss][Aa][Bb][Ll][Ee] ;

DISTINCT : [Dd][Ii][Ss][Tt][Ii][Nn][Cc][Tt] ;

DROP : [Dd][Rr][Oo][Pp] ;

ELEMENTOF : [Ee][Ll][Ee][Mm][Ee][Nn][Tt][Oo][Ff] ;

ELEMENTS : [Ee][Ll][Ee][Mm][Ee][Nn][Tt][Ss] ;

ELSE : [Ee][Ll][Ss][Ee] ;

ENABLE : [Ee][Nn][Aa][Bb][Ll][Ee] ;

END : [Ee][Nn][Dd] ;

ES_SHARDS : [Ee][Ss] UNDER [Ss][Hh][Aa][Rr][Dd][Ss] ;

ES_REPLICAS : [Ee][Ss] UNDER [Rr][Ee][Pp][Ll][Ii][Cc][Aa][Ss] ;

EXISTS : [Ee][Xx][Ii][Ss][Tt][Ss] ;

EXTRACT: [Ee][Xx][Tt][Rr][Aa][Cc][Tt] ;

FIELDS : [Ff][Ii][Ee][Ll][Dd][Ss] ;

FIRST : [Ff][Ii][Rr][Ss][Tt] ;

FORCE : [Ff][Oo][Rr][Cc][Ee] ;

FORCE_INDEX : FORCE UNDER INDEX;

FORCE_PRIMARY_INDEX : FORCE UNDER PRIMARY UNDER INDEX;

FREEZE : [Ff][Rr][Ee][Ee][Zz][Ee] ;

FROM : [Ff][Rr][Oo][Mm] ;

FROZEN : [Ff][Rr][Oo][Zz][Ee][Nn] ;

FULLTEXT : [Ff][Uu][Ll][Ll][Tt][Ee][Xx][Tt] ;

GENERATED : [Gg][Ee][Nn][Ee][Rr][Aa][Tt][Ee][Dd] ;

GRANT : [Gg][Rr][Aa][Nn][Tt] ;

GROUP : [Gg][Rr][Oo][Uu][Pp] ;

HOURS : ([Hh] | [Hh][Oo][Uu][Rr][Ss]) ;

IDENTIFIED : [Ii][Dd][Ee][Nn][Tt][Ii][Ff][Ii][Ee][Dd] ;

IDENTITY : [Ii][Dd][Ee][Nn][Tt][Ii][Tt][Yy] ;

IF : [Ii][Ff] ;

IMAGE : [Ii][Mm][Aa][Gg][Ee] ;

IN : [Ii][Nn] ;

INCREMENT : [Ii][Nn][Cc][Rr][Ee][Mm][Ee][Nn][Tt] ;

INDEX : [Ii][Nn][Dd][Ee][Xx] ;

INDEXES : [Ii][Nn][Dd][Ee][Xx][Ee][Ss] ;

INSERT : [Ii][Nn][Ss][Ee][Rr][Tt];

INTO : [Ii][Nn][Tt][Oo];

IS : [Ii][Ss];

JSON : [Jj][Ss][Oo][Nn] ;

JOIN : [Jj][Oo][Ii][Nn] ;

KEY : [Kk][Ee][Yy] ;

KEYOF : [Kk][Ee][Yy][Oo][Ff] ;

KEYS : [Kk][Ee][Yy][Ss] ;

LAST : [Ll][Aa][Ss][Tt] ;

LEFT : [Ll][Ee][Ff][Tt] ;

LIFETIME : [Ll][Ii][Ff][Ee][Tt][Ii][Mm][Ee] ;

LIMIT : [Ll][Ii][Mm][Ii][Tt] ;

LOCAL : [Ll][Oo][Cc][Aa] [Ll] ;

LOCK : [Ll][Oo][Cc][Kk] ;

MAXVALUE : [Mm][Aa][Xx][Vv][Aa][Ll][Uu][Ee] ;

MERGE : [Mm][Ee][Rr][Gg][Ee] ;

MINUTES : ([Mm] | [Mm][Ii][Nn][Uu][Tt][Ee][Ss]) ;

MINVALUE : [Mm][Ii][Nn][Vv][Aa][Ll][Uu][Ee] ;

MODIFY : [Mm][Oo][Dd][Ii][Ff][Yy] ;

MR_COUNTER : [Mm][Rr] UNDER [Cc][Oo][Uu][Nn][Tt][Ee][Rr] ;

NAMESPACE : [Nn][Aa][Mm][Ee][Ss][Pp][Aa][Cc][Ee];

NAMESPACES : [Nn][Aa][Mm][Ee][Ss][Pp][Aa][Cc][Ee][Ss];

NESTED : [Nn][Ee][Ss][Tt][Ee][Dd] ;

NO : [Nn][Oo] ;

NOT : [Nn][Oo][Tt] ;

NULLS : [Nn][Uu][Ll][Ll][Ss] ;

OFFSET : [Oo][Ff][Ff][Ss][Ee][Tt] ;

OF : [Oo][Ff] ;

ON : [Oo][Nn] ;

ONLY : [Oo][Nn][Ll][Yy] ;

OR : [Oo][Rr] ;

ORDER : [Oo][Rr][Dd][Ee][Rr];

OUTER : [Oo][Uu][Tt][Ee][Rr];

OVERRIDE : [Oo][Vv][Ee][Rr][Rr][Ii][Dd][Ee] ;

PASSWORD : [Pp][Aa][Ss][Ss][Ww][Oo][Rr][Dd] ;

PATCH : [Pp][Aa][Tt][Cc][Hh] ;

PER : [Pp][Ee][Rr] ;

PREFER_INDEXES: PREFER UNDER INDEXES;

PREFER_PRIMARY_INDEX : PREFER UNDER PRIMARY UNDER INDEX;

PRIMARY : [Pp][Rr][Ii][Mm][Aa][Rr][Yy] ;

PUT : [Pp][Uu][Tt] ;

REGION : [Rr][Ee][Gg][Ii][Oo][Nn] ;

REGIONS : [Rr][Ee][Gg][Ii][Oo][Nn][Ss] ;

REMOVE : [Rr][Ee][Mm][Oo][Vv][Ee] ;

RETURNING : [Rr][Ee][Tt][Uu][Rr][Nn][Ii][Nn][Gg] ;

REVOKE : [Rr][Ee][Vv][Oo][Kk][Ee] ;

ROLE : [Rr][Oo][Ll][Ee] ;

ROLES : [Rr][Oo][Ll][Ee][Ss] ;

ROW : [Rr][Oo][Ww] ;

SCHEMA : [Ss][Cc][Hh][Ee][Mm][Aa] ;

SECONDS : ([Ss] | [Ss][Ee][Cc][Oo][Nn][Dd][Ss]) ;

SELECT : [Ss][Ee][Ll][Ee][Cc][Tt] ;

SEQ_TRANSFORM : 'seq_transform' ;

SET : [Ss][Ee][Tt] ;

SHARD : [Ss][Hh][Aa][Rr][Dd] ;

SHOW : [Ss][Hh][Oo][Ww] ;

START : [Ss][Tt][Aa][Rr][Tt] ;

TABLE : [Tt][Aa][Bb][Ll][Ee] ;

TABLES : [Tt][Aa][Bb][Ll][Ee][Ss] ;

THEN : [Tt][Hh][Ee][Nn] ;

TO : [Tt][Oo] ;

TTL : [Tt][Tt][Ll];

TYPE : [Tt][Yy][Pp][Ee] ;

UNFREEZE : [Uu][Nn][Ff][Rr][Ee][Ee][Zz][Ee] ;

UNLOCK : [Uu][Nn][Ll][Oo][Cc][Kk] ;

UPDATE : [Uu][Pp][Dd][Aa][Tt][Ee] ;

UPSERT : [Uu][Pp][Ss][Ee][Rr][Tt] ;

USER : [Uu][Ss][Ee][Rr] ;

USERS : [Uu][Ss][Ee][Rr][Ss] ;

USING: [Uu][Ss][Ii][Nn][Gg];

VALUES : [Vv][Aa][Ll][Uu][Ee][Ss] ;

WHEN : [Ww][Hh][Ee][Nn] ;

WHERE : [Ww][Hh][Ee][Rr][Ee] ;

WITH : [Ww][Ii][Tt][Hh] ;

UNIQUE : [Uu][Nn][Ii][Qq][Uu][Ee];

UNNEST : [Uu][Nn][Nn][Ee][Ss][Tt] ;

UUID: [Uu][Uu][Ii][Dd] ;

/* multi-word tokens */

ALL_PRIVILEGES : ALL WS+ PRIVILEGES ;

IDENTIFIED_EXTERNALLY : IDENTIFIED WS+ EXTERNALLY ;

PASSWORD_EXPIRE : PASSWORD WS+ EXPIRE ;

RETAIN_CURRENT_PASSWORD : RETAIN WS+ CURRENT WS+ PASSWORD ;

CLEAR_RETAINED_PASSWORD : CLEAR WS+ RETAINED WS+ PASSWORD;

LEFT_OUTER_JOIN : LEFT WS+ OUTER WS+ JOIN ;

/* types */
ARRAY_T : [Aa][Rr][Rr][Aa][Yy] ;

BINARY_T : [Bb][Ii][Nn][Aa][Rr][Yy] ;

BOOLEAN_T : [Bb][Oo][Oo][Ll][Ee][Aa][Nn] ;

DOUBLE_T : [Dd][Oo][Uu][Bb][Ll][Ee] ;

ENUM_T : [Ee][Nn][Uu][Mm] ;

FLOAT_T : [Ff][Ll][Oo][Aa][Tt] ;

GEOMETRY_T : [Gg][Ee][Oo][Mm][Ee][Tt][Rr][Yy] ;

INTEGER_T : [Ii][Nn][Tt][Ee][Gg][Ee][Rr] ;

LONG_T : [Ll][Oo][Nn][Gg] ;

MAP_T : [Mm][Aa][Pp] ;

NUMBER_T : [Nn][Uu][Mm][Bb][Ee][Rr] ;

POINT_T : [Pp][Oo][Ii][Nn][Tt] ;

RECORD_T : [Rr][Ee][Cc][Oo][Rr][Dd] ;

STRING_T : [Ss][Tt][Rr][Ii][Nn][Gg] ;

TIMESTAMP_T : [Tt][Ii][Mm][Ee][Ss][Tt][Aa][Mm][Pp] ;

ANY_T : [Aa][Nn][Yy] ;

ANYATOMIC_T : [Aa][Nn][Yy][Aa][Tt][Oo][Mm][Ii][Cc] ;

ANYJSONATOMIC_T : [Aa][Nn][Yy][Jj][Ss][Oo][Nn][Aa][Tt][Oo][Mm][Ii][Cc] ;

ANYRECORD_T : [Aa][Nn][Yy][Rr][Ee][Cc][Oo][Rr][Dd] ;

/* used to define a scalar (atomic type) index */
SCALAR_T : [Ss][Cc][Aa][Ll][Aa][Rr] ;

/*
 * Punctuation marks
 */
SEMI : ';' ;
COMMA : ',' ;
COLON : ':' ;
LP : '(' ;
RP : ')' ;
LBRACK : '[' ;
RBRACK : ']' ;
LBRACE : '{' ;
RBRACE : '}' ;
STAR : '*' ;
DOT : '.' ;
DOLLAR : '$' ;
QUESTION_MARK : '?' ;

/*
 * Operators
 */
LT : '<' ;
LTE : '<=' ;
GT : '>' ;
GTE : '>=' ;
EQ : '=' ;
NEQ : '!=' ;

LT_ANY : '<'[Aa][Nn][Yy] ;
LTE_ANY : '<='[Aa][Nn][Yy] ;
GT_ANY : '>'[Aa][Nn][Yy] ;
GTE_ANY : '>='[Aa][Nn][Yy] ;
EQ_ANY : '='[Aa][Nn][Yy] ;
NEQ_ANY : '!='[Aa][Nn][Yy] ;

PLUS : '+' ;
MINUS : '-' ;
//MULT : '*' ; STAR already defined
IDIV : '/' ;  // performs integer division if args are integers
RDIV : [Dd][Ii][Vv]; // always performs real-number division

CONCAT : '||' ;  // string concatenation

/*
 * LITERALS
 */

NULL : [Nn][Uu][Ll][Ll] ;

FALSE : [Ff][Aa][Ll][Ss][Ee] ;

TRUE : [Tt][Rr][Uu][Ee] ;

INT : DIGIT+ ; // translated to Integer or Long item

FLOAT : ( DIGIT* '.' DIGIT+ ([Ee] [+-]? DIGIT+)? ) |
        ( DIGIT+ [Ee] [+-]? DIGIT+ ) ;

// For number use number parser rule instead of this lexer rule.
NUMBER : (INT | FLOAT) ('n'|'N');

DSTRING : '"' (DSTR_ESC | .)*? '"' ;

STRING : '\'' (ESC | .)*? '\'' ;

SYSDOLAR : [S][Y][S][$];

/*
 * Identifiers (MUST come after all the keywords and literals defined above)
 */

ID : ALPHA (ALPHA | DIGIT | UNDER)* ;

/* A special token to catch badly-formed identifiers. */
BAD_ID : (DIGIT | UNDER) (ALPHA | DIGIT | UNDER)* ;


/*
 * Skip whitespace, don't pass to parser.
 */
WS : (' ' | '\t' | '\r' | '\n')+ -> skip ;

/*
 * Comments.  3 styles.
 */
C_COMMENT : '/*' ~[+] .*? '*/' -> skip ;

LINE_COMMENT : '//' ~[\r\n]* -> skip ;

LINE_COMMENT1 : '#' ~[\r\n]* -> skip ;

/*
 * Add a token that will match anything.  The resulting error will be
 * more usable this way.
 */
UnrecognizedToken : . ;

/*
 * fragments can only be used in other lexical rules and are not tokens
 */

fragment ALPHA : 'a'..'z'|'A'..'Z' ;

fragment DIGIT : '0'..'9' ;

fragment DSTR_ESC : '\\' (["\\/bfnrt] | UNICODE) ; /* " */

fragment ESC : '\\' (['\\/bfnrt] | UNICODE) ;

fragment HEX : [0-9a-fA-F] ;

fragment UNDER : '_';

fragment UNICODE : 'u' HEX HEX HEX HEX ;

fragment CLEAR : [Cc][Ll][Ee][Aa][Rr] ;

fragment CURRENT : [Cc][Uu][Rr][Rr][Ee][Nn][Tt] ;

fragment EXPIRE : [Ee][Xx][Pp][Ii][Rr][Ee] ;

fragment EXTERNALLY : [Ee][Xx][Tt][Ee][Rr][Nn][Aa][Ll][Ll][Yy] ;

fragment PREFER : [Pp][Rr][Ee][Ff][Ee][Rr] ;

fragment PRIVILEGES : [Pp][Rr][Ii][Vv][Ii][Ll][Ee][Gg][Ee][Ss] ;

fragment RETAIN : [Rr][Ee][Tt][Aa][Ii][Nn] ;

fragment RETAINED : [Rr][Ee][Tt][Aa][Ii][Nn][Ee][Dd] ;
