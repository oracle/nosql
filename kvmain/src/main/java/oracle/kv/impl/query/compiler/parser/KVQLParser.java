// Generated from KVQL.g4 by ANTLR 4.13.1
package oracle.kv.impl.query.compiler.parser;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue"})
public class KVQLParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.13.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, VARNAME=5, ACCOUNT=6, ADD=7, ADMIN=8, 
		ALL=9, ALTER=10, ALWAYS=11, ANCESTORS=12, AND=13, AS=14, ASC=15, ARRAY_COLLECT=16, 
		BEFORE=17, BETWEEN=18, BY=19, CACHE=20, CASE=21, CASCADE=22, CAST=23, 
		COLLECTION=24, COMMENT=25, COUNT=26, CREATE=27, CYCLE=28, DAYS=29, DECLARE=30, 
		DEFAULT=31, DELETE=32, DESC=33, DESCENDANTS=34, DESCRIBE=35, DISABLE=36, 
		DISTINCT=37, DROP=38, ELEMENTOF=39, ELEMENTS=40, ELSE=41, ENABLE=42, END=43, 
		ES_SHARDS=44, ES_REPLICAS=45, EXISTS=46, EXTRACT=47, FIELDS=48, FIRST=49, 
		FORCE=50, FORCE_INDEX=51, FORCE_PRIMARY_INDEX=52, FREEZE=53, FROM=54, 
		FROZEN=55, FULLTEXT=56, GENERATED=57, GRANT=58, GROUP=59, HOURS=60, IDENTIFIED=61, 
		IDENTITY=62, IF=63, IMAGE=64, IN=65, INCREMENT=66, INDEX=67, INDEXES=68, 
		INSERT=69, INTO=70, IS=71, JSON=72, JOIN=73, KEY=74, KEYOF=75, KEYS=76, 
		LAST=77, LEFT=78, LIFETIME=79, LIMIT=80, LOCAL=81, LOCK=82, MAXVALUE=83, 
		MERGE=84, MINUTES=85, MINVALUE=86, MODIFY=87, MR_COUNTER=88, NAMESPACE=89, 
		NAMESPACES=90, NESTED=91, NO=92, NOT=93, NULLS=94, OFFSET=95, OF=96, ON=97, 
		ONLY=98, OR=99, ORDER=100, OUTER=101, OVERRIDE=102, PASSWORD=103, PATCH=104, 
		PER=105, PREFER_INDEXES=106, PREFER_PRIMARY_INDEX=107, PRIMARY=108, PUT=109, 
		REGION=110, REGIONS=111, REMOVE=112, RETURNING=113, REVOKE=114, ROLE=115, 
		ROLES=116, ROW=117, SCHEMA=118, SECONDS=119, SELECT=120, SEQ_TRANSFORM=121, 
		SET=122, SHARD=123, SHOW=124, START=125, TABLE=126, TABLES=127, THEN=128, 
		TO=129, TTL=130, TYPE=131, UNFREEZE=132, UNLOCK=133, UPDATE=134, UPSERT=135, 
		USER=136, USERS=137, USING=138, VALUES=139, WHEN=140, WHERE=141, WITH=142, 
		UNIQUE=143, UNNEST=144, UUID=145, ALL_PRIVILEGES=146, IDENTIFIED_EXTERNALLY=147, 
		PASSWORD_EXPIRE=148, RETAIN_CURRENT_PASSWORD=149, CLEAR_RETAINED_PASSWORD=150, 
		LEFT_OUTER_JOIN=151, ARRAY_T=152, BINARY_T=153, BOOLEAN_T=154, DOUBLE_T=155, 
		ENUM_T=156, FLOAT_T=157, GEOMETRY_T=158, INTEGER_T=159, LONG_T=160, MAP_T=161, 
		NUMBER_T=162, POINT_T=163, RECORD_T=164, STRING_T=165, TIMESTAMP_T=166, 
		ANY_T=167, ANYATOMIC_T=168, ANYJSONATOMIC_T=169, ANYRECORD_T=170, SCALAR_T=171, 
		SEMI=172, COMMA=173, COLON=174, LP=175, RP=176, LBRACK=177, RBRACK=178, 
		LBRACE=179, RBRACE=180, STAR=181, DOT=182, DOLLAR=183, QUESTION_MARK=184, 
		LT=185, LTE=186, GT=187, GTE=188, EQ=189, NEQ=190, LT_ANY=191, LTE_ANY=192, 
		GT_ANY=193, GTE_ANY=194, EQ_ANY=195, NEQ_ANY=196, PLUS=197, MINUS=198, 
		IDIV=199, RDIV=200, CONCAT=201, NULL=202, FALSE=203, TRUE=204, INT=205, 
		FLOAT=206, NUMBER=207, DSTRING=208, STRING=209, SYSDOLAR=210, ID=211, 
		BAD_ID=212, WS=213, C_COMMENT=214, LINE_COMMENT=215, LINE_COMMENT1=216, 
		UnrecognizedToken=217;
	public static final int
		RULE_parse = 0, RULE_statement = 1, RULE_query = 2, RULE_prolog = 3, RULE_var_decl = 4, 
		RULE_index_function_path = 5, RULE_expr = 6, RULE_sfw_expr = 7, RULE_from_clause = 8, 
		RULE_table_spec = 9, RULE_nested_tables = 10, RULE_ancestor_tables = 11, 
		RULE_descendant_tables = 12, RULE_left_outer_join_tables = 13, RULE_left_outer_join_table = 14, 
		RULE_from_table = 15, RULE_aliased_table_name = 16, RULE_tab_alias = 17, 
		RULE_unnest_clause = 18, RULE_where_clause = 19, RULE_select_clause = 20, 
		RULE_select_list = 21, RULE_hints = 22, RULE_hint = 23, RULE_col_alias = 24, 
		RULE_orderby_clause = 25, RULE_sort_spec = 26, RULE_groupby_clause = 27, 
		RULE_limit_clause = 28, RULE_offset_clause = 29, RULE_or_expr = 30, RULE_and_expr = 31, 
		RULE_not_expr = 32, RULE_is_null_expr = 33, RULE_cond_expr = 34, RULE_between_expr = 35, 
		RULE_comp_expr = 36, RULE_comp_op = 37, RULE_any_op = 38, RULE_in_expr = 39, 
		RULE_in1_expr = 40, RULE_in1_left_op = 41, RULE_in1_expr_list = 42, RULE_in2_expr = 43, 
		RULE_in3_expr = 44, RULE_exists_expr = 45, RULE_is_of_type_expr = 46, 
		RULE_concatenate_expr = 47, RULE_add_expr = 48, RULE_multiply_expr = 49, 
		RULE_unary_expr = 50, RULE_path_expr = 51, RULE_map_step = 52, RULE_map_field_step = 53, 
		RULE_map_filter_step = 54, RULE_array_step = 55, RULE_array_slice_step = 56, 
		RULE_array_filter_step = 57, RULE_primary_expr = 58, RULE_column_ref = 59, 
		RULE_const_expr = 60, RULE_var_ref = 61, RULE_array_constructor = 62, 
		RULE_map_constructor = 63, RULE_transform_expr = 64, RULE_transform_input_expr = 65, 
		RULE_collect = 66, RULE_func_call = 67, RULE_count_star = 68, RULE_count_distinct = 69, 
		RULE_case_expr = 70, RULE_cast_expr = 71, RULE_parenthesized_expr = 72, 
		RULE_extract_expr = 73, RULE_insert_statement = 74, RULE_insert_label = 75, 
		RULE_insert_returning_clause = 76, RULE_insert_clause = 77, RULE_insert_ttl_clause = 78, 
		RULE_update_statement = 79, RULE_update_returning_clause = 80, RULE_update_clause = 81, 
		RULE_set_clause = 82, RULE_add_clause = 83, RULE_put_clause = 84, RULE_remove_clause = 85, 
		RULE_json_merge_patch_clause = 86, RULE_json_patch_expr = 87, RULE_ttl_clause = 88, 
		RULE_target_expr = 89, RULE_pos_expr = 90, RULE_delete_statement = 91, 
		RULE_delete_returning_clause = 92, RULE_quantified_type_def = 93, RULE_type_def = 94, 
		RULE_record_def = 95, RULE_field_def = 96, RULE_default_def = 97, RULE_default_value = 98, 
		RULE_not_null = 99, RULE_map_def = 100, RULE_array_def = 101, RULE_integer_def = 102, 
		RULE_json_def = 103, RULE_float_def = 104, RULE_string_def = 105, RULE_enum_def = 106, 
		RULE_boolean_def = 107, RULE_binary_def = 108, RULE_timestamp_def = 109, 
		RULE_any_def = 110, RULE_anyAtomic_def = 111, RULE_anyJsonAtomic_def = 112, 
		RULE_anyRecord_def = 113, RULE_id_path = 114, RULE_table_id_path = 115, 
		RULE_table_id = 116, RULE_name_path = 117, RULE_field_name = 118, RULE_create_namespace_statement = 119, 
		RULE_drop_namespace_statement = 120, RULE_region_name = 121, RULE_create_region_statement = 122, 
		RULE_drop_region_statement = 123, RULE_set_local_region_statement = 124, 
		RULE_create_table_statement = 125, RULE_table_name = 126, RULE_namespace = 127, 
		RULE_table_def = 128, RULE_column_def = 129, RULE_json_mrcounter_fields = 130, 
		RULE_json_mrcounter_def = 131, RULE_json_collection_mrcounter_def = 132, 
		RULE_json_mrcounter_path = 133, RULE_key_def = 134, RULE_shard_key_def = 135, 
		RULE_id_list_with_size = 136, RULE_id_with_size = 137, RULE_storage_size = 138, 
		RULE_table_options = 139, RULE_ttl_def = 140, RULE_region_names = 141, 
		RULE_regions_def = 142, RULE_frozen_def = 143, RULE_json_collection_def = 144, 
		RULE_enable_before_image = 145, RULE_before_image_ttl = 146, RULE_disable_before_image = 147, 
		RULE_identity_def = 148, RULE_sequence_options = 149, RULE_mr_counter_def = 150, 
		RULE_uuid_def = 151, RULE_alter_table_statement = 152, RULE_alter_def = 153, 
		RULE_freeze_def = 154, RULE_unfreeze_def = 155, RULE_add_region_def = 156, 
		RULE_drop_region_def = 157, RULE_alter_field_statements = 158, RULE_add_field_statement = 159, 
		RULE_drop_field_statement = 160, RULE_modify_field_statement = 161, RULE_schema_path = 162, 
		RULE_init_schema_path_step = 163, RULE_schema_path_step = 164, RULE_drop_table_statement = 165, 
		RULE_create_index_statement = 166, RULE_index_name = 167, RULE_index_field_list = 168, 
		RULE_index_field = 169, RULE_index_function = 170, RULE_index_function_args = 171, 
		RULE_index_path = 172, RULE_old_index_path = 173, RULE_row_metadata = 174, 
		RULE_multikey_path_prefix = 175, RULE_multikey_path_suffix = 176, RULE_path_type = 177, 
		RULE_create_text_index_statement = 178, RULE_fts_field_list = 179, RULE_fts_path_list = 180, 
		RULE_fts_path = 181, RULE_es_properties = 182, RULE_es_property_assignment = 183, 
		RULE_drop_index_statement = 184, RULE_describe_statement = 185, RULE_schema_path_list = 186, 
		RULE_show_statement = 187, RULE_create_user_statement = 188, RULE_create_role_statement = 189, 
		RULE_alter_user_statement = 190, RULE_drop_user_statement = 191, RULE_drop_role_statement = 192, 
		RULE_grant_statement = 193, RULE_revoke_statement = 194, RULE_identifier_or_string = 195, 
		RULE_identified_clause = 196, RULE_create_user_identified_clause = 197, 
		RULE_by_password = 198, RULE_password_lifetime = 199, RULE_reset_password_clause = 200, 
		RULE_account_lock = 201, RULE_grant_roles = 202, RULE_grant_system_privileges = 203, 
		RULE_grant_object_privileges = 204, RULE_revoke_roles = 205, RULE_revoke_system_privileges = 206, 
		RULE_revoke_object_privileges = 207, RULE_principal = 208, RULE_sys_priv_list = 209, 
		RULE_priv_item = 210, RULE_obj_priv_list = 211, RULE_object = 212, RULE_json_text = 213, 
		RULE_jsobject = 214, RULE_jsarray = 215, RULE_jspair = 216, RULE_jsvalue = 217, 
		RULE_comment = 218, RULE_duration = 219, RULE_time_unit = 220, RULE_number = 221, 
		RULE_signed_int = 222, RULE_string = 223, RULE_id_list = 224, RULE_id = 225;
	private static String[] makeRuleNames() {
		return new String[] {
			"parse", "statement", "query", "prolog", "var_decl", "index_function_path", 
			"expr", "sfw_expr", "from_clause", "table_spec", "nested_tables", "ancestor_tables", 
			"descendant_tables", "left_outer_join_tables", "left_outer_join_table", 
			"from_table", "aliased_table_name", "tab_alias", "unnest_clause", "where_clause", 
			"select_clause", "select_list", "hints", "hint", "col_alias", "orderby_clause", 
			"sort_spec", "groupby_clause", "limit_clause", "offset_clause", "or_expr", 
			"and_expr", "not_expr", "is_null_expr", "cond_expr", "between_expr", 
			"comp_expr", "comp_op", "any_op", "in_expr", "in1_expr", "in1_left_op", 
			"in1_expr_list", "in2_expr", "in3_expr", "exists_expr", "is_of_type_expr", 
			"concatenate_expr", "add_expr", "multiply_expr", "unary_expr", "path_expr", 
			"map_step", "map_field_step", "map_filter_step", "array_step", "array_slice_step", 
			"array_filter_step", "primary_expr", "column_ref", "const_expr", "var_ref", 
			"array_constructor", "map_constructor", "transform_expr", "transform_input_expr", 
			"collect", "func_call", "count_star", "count_distinct", "case_expr", 
			"cast_expr", "parenthesized_expr", "extract_expr", "insert_statement", 
			"insert_label", "insert_returning_clause", "insert_clause", "insert_ttl_clause", 
			"update_statement", "update_returning_clause", "update_clause", "set_clause", 
			"add_clause", "put_clause", "remove_clause", "json_merge_patch_clause", 
			"json_patch_expr", "ttl_clause", "target_expr", "pos_expr", "delete_statement", 
			"delete_returning_clause", "quantified_type_def", "type_def", "record_def", 
			"field_def", "default_def", "default_value", "not_null", "map_def", "array_def", 
			"integer_def", "json_def", "float_def", "string_def", "enum_def", "boolean_def", 
			"binary_def", "timestamp_def", "any_def", "anyAtomic_def", "anyJsonAtomic_def", 
			"anyRecord_def", "id_path", "table_id_path", "table_id", "name_path", 
			"field_name", "create_namespace_statement", "drop_namespace_statement", 
			"region_name", "create_region_statement", "drop_region_statement", "set_local_region_statement", 
			"create_table_statement", "table_name", "namespace", "table_def", "column_def", 
			"json_mrcounter_fields", "json_mrcounter_def", "json_collection_mrcounter_def", 
			"json_mrcounter_path", "key_def", "shard_key_def", "id_list_with_size", 
			"id_with_size", "storage_size", "table_options", "ttl_def", "region_names", 
			"regions_def", "frozen_def", "json_collection_def", "enable_before_image", 
			"before_image_ttl", "disable_before_image", "identity_def", "sequence_options", 
			"mr_counter_def", "uuid_def", "alter_table_statement", "alter_def", "freeze_def", 
			"unfreeze_def", "add_region_def", "drop_region_def", "alter_field_statements", 
			"add_field_statement", "drop_field_statement", "modify_field_statement", 
			"schema_path", "init_schema_path_step", "schema_path_step", "drop_table_statement", 
			"create_index_statement", "index_name", "index_field_list", "index_field", 
			"index_function", "index_function_args", "index_path", "old_index_path", 
			"row_metadata", "multikey_path_prefix", "multikey_path_suffix", "path_type", 
			"create_text_index_statement", "fts_field_list", "fts_path_list", "fts_path", 
			"es_properties", "es_property_assignment", "drop_index_statement", "describe_statement", 
			"schema_path_list", "show_statement", "create_user_statement", "create_role_statement", 
			"alter_user_statement", "drop_user_statement", "drop_role_statement", 
			"grant_statement", "revoke_statement", "identifier_or_string", "identified_clause", 
			"create_user_identified_clause", "by_password", "password_lifetime", 
			"reset_password_clause", "account_lock", "grant_roles", "grant_system_privileges", 
			"grant_object_privileges", "revoke_roles", "revoke_system_privileges", 
			"revoke_object_privileges", "principal", "sys_priv_list", "priv_item", 
			"obj_priv_list", "object", "json_text", "jsobject", "jsarray", "jspair", 
			"jsvalue", "comment", "duration", "time_unit", "number", "signed_int", 
			"string", "id_list", "id"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "'/*+'", "'*/'", "'@'", "'row_metadata().'", null, null, null, 
			null, null, null, null, null, null, null, null, "'array_collect'", null, 
			null, null, null, null, null, null, null, null, "'count'", null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, "'seq_transform'", null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, null, null, null, null, null, null, null, null, null, null, null, 
			null, "';'", "','", "':'", "'('", "')'", "'['", "']'", "'{'", "'}'", 
			"'*'", "'.'", "'$'", "'?'", "'<'", "'<='", "'>'", "'>='", "'='", "'!='", 
			null, null, null, null, null, null, "'+'", "'-'", "'/'", null, "'||'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, null, "VARNAME", "ACCOUNT", "ADD", "ADMIN", "ALL", 
			"ALTER", "ALWAYS", "ANCESTORS", "AND", "AS", "ASC", "ARRAY_COLLECT", 
			"BEFORE", "BETWEEN", "BY", "CACHE", "CASE", "CASCADE", "CAST", "COLLECTION", 
			"COMMENT", "COUNT", "CREATE", "CYCLE", "DAYS", "DECLARE", "DEFAULT", 
			"DELETE", "DESC", "DESCENDANTS", "DESCRIBE", "DISABLE", "DISTINCT", "DROP", 
			"ELEMENTOF", "ELEMENTS", "ELSE", "ENABLE", "END", "ES_SHARDS", "ES_REPLICAS", 
			"EXISTS", "EXTRACT", "FIELDS", "FIRST", "FORCE", "FORCE_INDEX", "FORCE_PRIMARY_INDEX", 
			"FREEZE", "FROM", "FROZEN", "FULLTEXT", "GENERATED", "GRANT", "GROUP", 
			"HOURS", "IDENTIFIED", "IDENTITY", "IF", "IMAGE", "IN", "INCREMENT", 
			"INDEX", "INDEXES", "INSERT", "INTO", "IS", "JSON", "JOIN", "KEY", "KEYOF", 
			"KEYS", "LAST", "LEFT", "LIFETIME", "LIMIT", "LOCAL", "LOCK", "MAXVALUE", 
			"MERGE", "MINUTES", "MINVALUE", "MODIFY", "MR_COUNTER", "NAMESPACE", 
			"NAMESPACES", "NESTED", "NO", "NOT", "NULLS", "OFFSET", "OF", "ON", "ONLY", 
			"OR", "ORDER", "OUTER", "OVERRIDE", "PASSWORD", "PATCH", "PER", "PREFER_INDEXES", 
			"PREFER_PRIMARY_INDEX", "PRIMARY", "PUT", "REGION", "REGIONS", "REMOVE", 
			"RETURNING", "REVOKE", "ROLE", "ROLES", "ROW", "SCHEMA", "SECONDS", "SELECT", 
			"SEQ_TRANSFORM", "SET", "SHARD", "SHOW", "START", "TABLE", "TABLES", 
			"THEN", "TO", "TTL", "TYPE", "UNFREEZE", "UNLOCK", "UPDATE", "UPSERT", 
			"USER", "USERS", "USING", "VALUES", "WHEN", "WHERE", "WITH", "UNIQUE", 
			"UNNEST", "UUID", "ALL_PRIVILEGES", "IDENTIFIED_EXTERNALLY", "PASSWORD_EXPIRE", 
			"RETAIN_CURRENT_PASSWORD", "CLEAR_RETAINED_PASSWORD", "LEFT_OUTER_JOIN", 
			"ARRAY_T", "BINARY_T", "BOOLEAN_T", "DOUBLE_T", "ENUM_T", "FLOAT_T", 
			"GEOMETRY_T", "INTEGER_T", "LONG_T", "MAP_T", "NUMBER_T", "POINT_T", 
			"RECORD_T", "STRING_T", "TIMESTAMP_T", "ANY_T", "ANYATOMIC_T", "ANYJSONATOMIC_T", 
			"ANYRECORD_T", "SCALAR_T", "SEMI", "COMMA", "COLON", "LP", "RP", "LBRACK", 
			"RBRACK", "LBRACE", "RBRACE", "STAR", "DOT", "DOLLAR", "QUESTION_MARK", 
			"LT", "LTE", "GT", "GTE", "EQ", "NEQ", "LT_ANY", "LTE_ANY", "GT_ANY", 
			"GTE_ANY", "EQ_ANY", "NEQ_ANY", "PLUS", "MINUS", "IDIV", "RDIV", "CONCAT", 
			"NULL", "FALSE", "TRUE", "INT", "FLOAT", "NUMBER", "DSTRING", "STRING", 
			"SYSDOLAR", "ID", "BAD_ID", "WS", "C_COMMENT", "LINE_COMMENT", "LINE_COMMENT1", 
			"UnrecognizedToken"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "KVQL.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public KVQLParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ParseContext extends ParserRuleContext {
		public StatementContext statement() {
			return getRuleContext(StatementContext.class,0);
		}
		public TerminalNode EOF() { return getToken(KVQLParser.EOF, 0); }
		public ParseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_parse; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterParse(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitParse(this);
		}
	}

	public final ParseContext parse() throws RecognitionException {
		ParseContext _localctx = new ParseContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_parse);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(452);
			statement();
			setState(453);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class StatementContext extends ParserRuleContext {
		public QueryContext query() {
			return getRuleContext(QueryContext.class,0);
		}
		public Insert_statementContext insert_statement() {
			return getRuleContext(Insert_statementContext.class,0);
		}
		public Update_statementContext update_statement() {
			return getRuleContext(Update_statementContext.class,0);
		}
		public Delete_statementContext delete_statement() {
			return getRuleContext(Delete_statementContext.class,0);
		}
		public Create_table_statementContext create_table_statement() {
			return getRuleContext(Create_table_statementContext.class,0);
		}
		public Create_index_statementContext create_index_statement() {
			return getRuleContext(Create_index_statementContext.class,0);
		}
		public Create_user_statementContext create_user_statement() {
			return getRuleContext(Create_user_statementContext.class,0);
		}
		public Create_role_statementContext create_role_statement() {
			return getRuleContext(Create_role_statementContext.class,0);
		}
		public Create_namespace_statementContext create_namespace_statement() {
			return getRuleContext(Create_namespace_statementContext.class,0);
		}
		public Create_region_statementContext create_region_statement() {
			return getRuleContext(Create_region_statementContext.class,0);
		}
		public Drop_index_statementContext drop_index_statement() {
			return getRuleContext(Drop_index_statementContext.class,0);
		}
		public Drop_namespace_statementContext drop_namespace_statement() {
			return getRuleContext(Drop_namespace_statementContext.class,0);
		}
		public Drop_region_statementContext drop_region_statement() {
			return getRuleContext(Drop_region_statementContext.class,0);
		}
		public Create_text_index_statementContext create_text_index_statement() {
			return getRuleContext(Create_text_index_statementContext.class,0);
		}
		public Drop_role_statementContext drop_role_statement() {
			return getRuleContext(Drop_role_statementContext.class,0);
		}
		public Drop_user_statementContext drop_user_statement() {
			return getRuleContext(Drop_user_statementContext.class,0);
		}
		public Alter_table_statementContext alter_table_statement() {
			return getRuleContext(Alter_table_statementContext.class,0);
		}
		public Alter_user_statementContext alter_user_statement() {
			return getRuleContext(Alter_user_statementContext.class,0);
		}
		public Drop_table_statementContext drop_table_statement() {
			return getRuleContext(Drop_table_statementContext.class,0);
		}
		public Grant_statementContext grant_statement() {
			return getRuleContext(Grant_statementContext.class,0);
		}
		public Revoke_statementContext revoke_statement() {
			return getRuleContext(Revoke_statementContext.class,0);
		}
		public Describe_statementContext describe_statement() {
			return getRuleContext(Describe_statementContext.class,0);
		}
		public Set_local_region_statementContext set_local_region_statement() {
			return getRuleContext(Set_local_region_statementContext.class,0);
		}
		public Show_statementContext show_statement() {
			return getRuleContext(Show_statementContext.class,0);
		}
		public Index_function_pathContext index_function_path() {
			return getRuleContext(Index_function_pathContext.class,0);
		}
		public StatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitStatement(this);
		}
	}

	public final StatementContext statement() throws RecognitionException {
		StatementContext _localctx = new StatementContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(480);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,0,_ctx) ) {
			case 1:
				{
				setState(455);
				query();
				}
				break;
			case 2:
				{
				setState(456);
				insert_statement();
				}
				break;
			case 3:
				{
				setState(457);
				update_statement();
				}
				break;
			case 4:
				{
				setState(458);
				delete_statement();
				}
				break;
			case 5:
				{
				setState(459);
				create_table_statement();
				}
				break;
			case 6:
				{
				setState(460);
				create_index_statement();
				}
				break;
			case 7:
				{
				setState(461);
				create_user_statement();
				}
				break;
			case 8:
				{
				setState(462);
				create_role_statement();
				}
				break;
			case 9:
				{
				setState(463);
				create_namespace_statement();
				}
				break;
			case 10:
				{
				setState(464);
				create_region_statement();
				}
				break;
			case 11:
				{
				setState(465);
				drop_index_statement();
				}
				break;
			case 12:
				{
				setState(466);
				drop_namespace_statement();
				}
				break;
			case 13:
				{
				setState(467);
				drop_region_statement();
				}
				break;
			case 14:
				{
				setState(468);
				create_text_index_statement();
				}
				break;
			case 15:
				{
				setState(469);
				drop_role_statement();
				}
				break;
			case 16:
				{
				setState(470);
				drop_user_statement();
				}
				break;
			case 17:
				{
				setState(471);
				alter_table_statement();
				}
				break;
			case 18:
				{
				setState(472);
				alter_user_statement();
				}
				break;
			case 19:
				{
				setState(473);
				drop_table_statement();
				}
				break;
			case 20:
				{
				setState(474);
				grant_statement();
				}
				break;
			case 21:
				{
				setState(475);
				revoke_statement();
				}
				break;
			case 22:
				{
				setState(476);
				describe_statement();
				}
				break;
			case 23:
				{
				setState(477);
				set_local_region_statement();
				}
				break;
			case 24:
				{
				setState(478);
				show_statement();
				}
				break;
			case 25:
				{
				setState(479);
				index_function_path();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class QueryContext extends ParserRuleContext {
		public Sfw_exprContext sfw_expr() {
			return getRuleContext(Sfw_exprContext.class,0);
		}
		public PrologContext prolog() {
			return getRuleContext(PrologContext.class,0);
		}
		public QueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_query; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitQuery(this);
		}
	}

	public final QueryContext query() throws RecognitionException {
		QueryContext _localctx = new QueryContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_query);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(483);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DECLARE) {
				{
				setState(482);
				prolog();
				}
			}

			setState(485);
			sfw_expr();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PrologContext extends ParserRuleContext {
		public TerminalNode DECLARE() { return getToken(KVQLParser.DECLARE, 0); }
		public List<Var_declContext> var_decl() {
			return getRuleContexts(Var_declContext.class);
		}
		public Var_declContext var_decl(int i) {
			return getRuleContext(Var_declContext.class,i);
		}
		public List<TerminalNode> SEMI() { return getTokens(KVQLParser.SEMI); }
		public TerminalNode SEMI(int i) {
			return getToken(KVQLParser.SEMI, i);
		}
		public PrologContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_prolog; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterProlog(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitProlog(this);
		}
	}

	public final PrologContext prolog() throws RecognitionException {
		PrologContext _localctx = new PrologContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_prolog);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(487);
			match(DECLARE);
			setState(488);
			var_decl();
			setState(489);
			match(SEMI);
			setState(495);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==VARNAME) {
				{
				{
				setState(490);
				var_decl();
				setState(491);
				match(SEMI);
				}
				}
				setState(497);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Var_declContext extends ParserRuleContext {
		public TerminalNode VARNAME() { return getToken(KVQLParser.VARNAME, 0); }
		public Type_defContext type_def() {
			return getRuleContext(Type_defContext.class,0);
		}
		public Var_declContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_var_decl; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterVar_decl(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitVar_decl(this);
		}
	}

	public final Var_declContext var_decl() throws RecognitionException {
		Var_declContext _localctx = new Var_declContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_var_decl);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(498);
			match(VARNAME);
			setState(499);
			type_def();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Index_function_pathContext extends ParserRuleContext {
		public PrologContext prolog() {
			return getRuleContext(PrologContext.class,0);
		}
		public Func_callContext func_call() {
			return getRuleContext(Func_callContext.class,0);
		}
		public Index_function_pathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_index_function_path; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterIndex_function_path(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitIndex_function_path(this);
		}
	}

	public final Index_function_pathContext index_function_path() throws RecognitionException {
		Index_function_pathContext _localctx = new Index_function_pathContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_index_function_path);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(501);
			prolog();
			setState(502);
			func_call();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ExprContext extends ParserRuleContext {
		public Or_exprContext or_expr() {
			return getRuleContext(Or_exprContext.class,0);
		}
		public ExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterExpr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitExpr(this);
		}
	}

	public final ExprContext expr() throws RecognitionException {
		ExprContext _localctx = new ExprContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_expr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(504);
			or_expr(0);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Sfw_exprContext extends ParserRuleContext {
		public Select_clauseContext select_clause() {
			return getRuleContext(Select_clauseContext.class,0);
		}
		public From_clauseContext from_clause() {
			return getRuleContext(From_clauseContext.class,0);
		}
		public Where_clauseContext where_clause() {
			return getRuleContext(Where_clauseContext.class,0);
		}
		public Groupby_clauseContext groupby_clause() {
			return getRuleContext(Groupby_clauseContext.class,0);
		}
		public Orderby_clauseContext orderby_clause() {
			return getRuleContext(Orderby_clauseContext.class,0);
		}
		public Limit_clauseContext limit_clause() {
			return getRuleContext(Limit_clauseContext.class,0);
		}
		public Offset_clauseContext offset_clause() {
			return getRuleContext(Offset_clauseContext.class,0);
		}
		public Sfw_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sfw_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterSfw_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitSfw_expr(this);
		}
	}

	public final Sfw_exprContext sfw_expr() throws RecognitionException {
		Sfw_exprContext _localctx = new Sfw_exprContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_sfw_expr);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(506);
			select_clause();
			setState(507);
			from_clause();
			setState(509);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WHERE) {
				{
				setState(508);
				where_clause();
				}
			}

			setState(512);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==GROUP) {
				{
				setState(511);
				groupby_clause();
				}
			}

			setState(515);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ORDER) {
				{
				setState(514);
				orderby_clause();
				}
			}

			setState(518);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LIMIT) {
				{
				setState(517);
				limit_clause();
				}
			}

			setState(521);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==OFFSET) {
				{
				setState(520);
				offset_clause();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class From_clauseContext extends ParserRuleContext {
		public TerminalNode FROM() { return getToken(KVQLParser.FROM, 0); }
		public List<Table_specContext> table_spec() {
			return getRuleContexts(Table_specContext.class);
		}
		public Table_specContext table_spec(int i) {
			return getRuleContext(Table_specContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public List<Unnest_clauseContext> unnest_clause() {
			return getRuleContexts(Unnest_clauseContext.class);
		}
		public Unnest_clauseContext unnest_clause(int i) {
			return getRuleContext(Unnest_clauseContext.class,i);
		}
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public List<TerminalNode> VARNAME() { return getTokens(KVQLParser.VARNAME); }
		public TerminalNode VARNAME(int i) {
			return getToken(KVQLParser.VARNAME, i);
		}
		public List<TerminalNode> AS() { return getTokens(KVQLParser.AS); }
		public TerminalNode AS(int i) {
			return getToken(KVQLParser.AS, i);
		}
		public From_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_from_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterFrom_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitFrom_clause(this);
		}
	}

	public final From_clauseContext from_clause() throws RecognitionException {
		From_clauseContext _localctx = new From_clauseContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_from_clause);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(523);
			match(FROM);
			setState(524);
			table_spec();
			setState(529);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,8,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(525);
					match(COMMA);
					setState(526);
					table_spec();
					}
					} 
				}
				setState(531);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,8,_ctx);
			}
			setState(544);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(532);
				match(COMMA);
				setState(540);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,10,_ctx) ) {
				case 1:
					{
					{
					setState(533);
					expr();
					{
					setState(535);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==AS) {
						{
						setState(534);
						match(AS);
						}
					}

					setState(537);
					match(VARNAME);
					}
					}
					}
					break;
				case 2:
					{
					setState(539);
					unnest_clause();
					}
					break;
				}
				}
				}
				setState(546);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Table_specContext extends ParserRuleContext {
		public From_tableContext from_table() {
			return getRuleContext(From_tableContext.class,0);
		}
		public Nested_tablesContext nested_tables() {
			return getRuleContext(Nested_tablesContext.class,0);
		}
		public Left_outer_join_tablesContext left_outer_join_tables() {
			return getRuleContext(Left_outer_join_tablesContext.class,0);
		}
		public Table_specContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table_spec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterTable_spec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitTable_spec(this);
		}
	}

	public final Table_specContext table_spec() throws RecognitionException {
		Table_specContext _localctx = new Table_specContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_table_spec);
		try {
			setState(550);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(547);
				from_table();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(548);
				nested_tables();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(549);
				left_outer_join_tables();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Nested_tablesContext extends ParserRuleContext {
		public TerminalNode NESTED() { return getToken(KVQLParser.NESTED, 0); }
		public TerminalNode TABLES() { return getToken(KVQLParser.TABLES, 0); }
		public List<TerminalNode> LP() { return getTokens(KVQLParser.LP); }
		public TerminalNode LP(int i) {
			return getToken(KVQLParser.LP, i);
		}
		public From_tableContext from_table() {
			return getRuleContext(From_tableContext.class,0);
		}
		public List<TerminalNode> RP() { return getTokens(KVQLParser.RP); }
		public TerminalNode RP(int i) {
			return getToken(KVQLParser.RP, i);
		}
		public TerminalNode ANCESTORS() { return getToken(KVQLParser.ANCESTORS, 0); }
		public Ancestor_tablesContext ancestor_tables() {
			return getRuleContext(Ancestor_tablesContext.class,0);
		}
		public TerminalNode DESCENDANTS() { return getToken(KVQLParser.DESCENDANTS, 0); }
		public Descendant_tablesContext descendant_tables() {
			return getRuleContext(Descendant_tablesContext.class,0);
		}
		public Nested_tablesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nested_tables; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterNested_tables(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitNested_tables(this);
		}
	}

	public final Nested_tablesContext nested_tables() throws RecognitionException {
		Nested_tablesContext _localctx = new Nested_tablesContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_nested_tables);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(552);
			match(NESTED);
			setState(553);
			match(TABLES);
			setState(554);
			match(LP);
			setState(555);
			from_table();
			setState(561);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ANCESTORS) {
				{
				setState(556);
				match(ANCESTORS);
				setState(557);
				match(LP);
				setState(558);
				ancestor_tables();
				setState(559);
				match(RP);
				}
			}

			setState(568);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DESCENDANTS) {
				{
				setState(563);
				match(DESCENDANTS);
				setState(564);
				match(LP);
				setState(565);
				descendant_tables();
				setState(566);
				match(RP);
				}
			}

			setState(570);
			match(RP);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Ancestor_tablesContext extends ParserRuleContext {
		public List<From_tableContext> from_table() {
			return getRuleContexts(From_tableContext.class);
		}
		public From_tableContext from_table(int i) {
			return getRuleContext(From_tableContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Ancestor_tablesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ancestor_tables; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAncestor_tables(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAncestor_tables(this);
		}
	}

	public final Ancestor_tablesContext ancestor_tables() throws RecognitionException {
		Ancestor_tablesContext _localctx = new Ancestor_tablesContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_ancestor_tables);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(572);
			from_table();
			setState(577);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(573);
				match(COMMA);
				setState(574);
				from_table();
				}
				}
				setState(579);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Descendant_tablesContext extends ParserRuleContext {
		public List<From_tableContext> from_table() {
			return getRuleContexts(From_tableContext.class);
		}
		public From_tableContext from_table(int i) {
			return getRuleContext(From_tableContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Descendant_tablesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_descendant_tables; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterDescendant_tables(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitDescendant_tables(this);
		}
	}

	public final Descendant_tablesContext descendant_tables() throws RecognitionException {
		Descendant_tablesContext _localctx = new Descendant_tablesContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_descendant_tables);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(580);
			from_table();
			setState(585);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(581);
				match(COMMA);
				setState(582);
				from_table();
				}
				}
				setState(587);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Left_outer_join_tablesContext extends ParserRuleContext {
		public From_tableContext from_table() {
			return getRuleContext(From_tableContext.class,0);
		}
		public List<Left_outer_join_tableContext> left_outer_join_table() {
			return getRuleContexts(Left_outer_join_tableContext.class);
		}
		public Left_outer_join_tableContext left_outer_join_table(int i) {
			return getRuleContext(Left_outer_join_tableContext.class,i);
		}
		public Left_outer_join_tablesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_left_outer_join_tables; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterLeft_outer_join_tables(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitLeft_outer_join_tables(this);
		}
	}

	public final Left_outer_join_tablesContext left_outer_join_tables() throws RecognitionException {
		Left_outer_join_tablesContext _localctx = new Left_outer_join_tablesContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_left_outer_join_tables);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(588);
			from_table();
			setState(589);
			left_outer_join_table();
			setState(593);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==LEFT_OUTER_JOIN) {
				{
				{
				setState(590);
				left_outer_join_table();
				}
				}
				setState(595);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Left_outer_join_tableContext extends ParserRuleContext {
		public TerminalNode LEFT_OUTER_JOIN() { return getToken(KVQLParser.LEFT_OUTER_JOIN, 0); }
		public From_tableContext from_table() {
			return getRuleContext(From_tableContext.class,0);
		}
		public Left_outer_join_tableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_left_outer_join_table; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterLeft_outer_join_table(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitLeft_outer_join_table(this);
		}
	}

	public final Left_outer_join_tableContext left_outer_join_table() throws RecognitionException {
		Left_outer_join_tableContext _localctx = new Left_outer_join_tableContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_left_outer_join_table);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(596);
			match(LEFT_OUTER_JOIN);
			setState(597);
			from_table();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class From_tableContext extends ParserRuleContext {
		public Aliased_table_nameContext aliased_table_name() {
			return getRuleContext(Aliased_table_nameContext.class,0);
		}
		public TerminalNode ON() { return getToken(KVQLParser.ON, 0); }
		public Or_exprContext or_expr() {
			return getRuleContext(Or_exprContext.class,0);
		}
		public From_tableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_from_table; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterFrom_table(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitFrom_table(this);
		}
	}

	public final From_tableContext from_table() throws RecognitionException {
		From_tableContext _localctx = new From_tableContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_from_table);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(599);
			aliased_table_name();
			setState(602);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ON) {
				{
				setState(600);
				match(ON);
				setState(601);
				or_expr(0);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Aliased_table_nameContext extends ParserRuleContext {
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public Tab_aliasContext tab_alias() {
			return getRuleContext(Tab_aliasContext.class,0);
		}
		public TerminalNode AS() { return getToken(KVQLParser.AS, 0); }
		public Aliased_table_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_aliased_table_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAliased_table_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAliased_table_name(this);
		}
	}

	public final Aliased_table_nameContext aliased_table_name() throws RecognitionException {
		Aliased_table_nameContext _localctx = new Aliased_table_nameContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_aliased_table_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			{
			setState(604);
			table_name();
			}
			setState(609);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,20,_ctx) ) {
			case 1:
				{
				setState(606);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,19,_ctx) ) {
				case 1:
					{
					setState(605);
					match(AS);
					}
					break;
				}
				setState(608);
				tab_alias();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Tab_aliasContext extends ParserRuleContext {
		public TerminalNode VARNAME() { return getToken(KVQLParser.VARNAME, 0); }
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Tab_aliasContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_tab_alias; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterTab_alias(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitTab_alias(this);
		}
	}

	public final Tab_aliasContext tab_alias() throws RecognitionException {
		Tab_aliasContext _localctx = new Tab_aliasContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_tab_alias);
		try {
			setState(613);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case VARNAME:
				enterOuterAlt(_localctx, 1);
				{
				setState(611);
				match(VARNAME);
				}
				break;
			case ACCOUNT:
			case ADD:
			case ADMIN:
			case ALL:
			case ALTER:
			case ALWAYS:
			case ANCESTORS:
			case AND:
			case AS:
			case ASC:
			case ARRAY_COLLECT:
			case BEFORE:
			case BETWEEN:
			case BY:
			case CACHE:
			case CASE:
			case CAST:
			case COLLECTION:
			case COMMENT:
			case COUNT:
			case CREATE:
			case CYCLE:
			case DAYS:
			case DECLARE:
			case DEFAULT:
			case DELETE:
			case DESC:
			case DESCENDANTS:
			case DESCRIBE:
			case DISABLE:
			case DISTINCT:
			case DROP:
			case ELEMENTOF:
			case ELEMENTS:
			case ELSE:
			case ENABLE:
			case END:
			case ES_SHARDS:
			case ES_REPLICAS:
			case EXISTS:
			case EXTRACT:
			case FIELDS:
			case FIRST:
			case FREEZE:
			case FROM:
			case FROZEN:
			case FULLTEXT:
			case GENERATED:
			case GRANT:
			case GROUP:
			case HOURS:
			case IDENTIFIED:
			case IDENTITY:
			case IF:
			case IMAGE:
			case IN:
			case INCREMENT:
			case INDEX:
			case INDEXES:
			case INSERT:
			case INTO:
			case IS:
			case JSON:
			case KEY:
			case KEYOF:
			case KEYS:
			case LAST:
			case LIFETIME:
			case LIMIT:
			case LOCAL:
			case LOCK:
			case MERGE:
			case MINUTES:
			case MODIFY:
			case MR_COUNTER:
			case NAMESPACES:
			case NESTED:
			case NO:
			case NOT:
			case NULLS:
			case OFFSET:
			case OF:
			case ON:
			case OR:
			case ORDER:
			case OVERRIDE:
			case PASSWORD:
			case PATCH:
			case PER:
			case PRIMARY:
			case PUT:
			case REGION:
			case REGIONS:
			case REMOVE:
			case RETURNING:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROW:
			case SCHEMA:
			case SECONDS:
			case SELECT:
			case SEQ_TRANSFORM:
			case SET:
			case SHARD:
			case SHOW:
			case START:
			case TABLE:
			case TABLES:
			case THEN:
			case TO:
			case TTL:
			case TYPE:
			case UNFREEZE:
			case UNLOCK:
			case UPDATE:
			case UPSERT:
			case USER:
			case USERS:
			case USING:
			case VALUES:
			case WHEN:
			case WHERE:
			case WITH:
			case UNIQUE:
			case UNNEST:
			case ARRAY_T:
			case BINARY_T:
			case BOOLEAN_T:
			case DOUBLE_T:
			case ENUM_T:
			case FLOAT_T:
			case GEOMETRY_T:
			case INTEGER_T:
			case LONG_T:
			case MAP_T:
			case NUMBER_T:
			case POINT_T:
			case RECORD_T:
			case STRING_T:
			case TIMESTAMP_T:
			case ANY_T:
			case ANYATOMIC_T:
			case ANYJSONATOMIC_T:
			case ANYRECORD_T:
			case SCALAR_T:
			case RDIV:
			case ID:
			case BAD_ID:
				enterOuterAlt(_localctx, 2);
				{
				setState(612);
				id();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Unnest_clauseContext extends ParserRuleContext {
		public TerminalNode UNNEST() { return getToken(KVQLParser.UNNEST, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public List<Path_exprContext> path_expr() {
			return getRuleContexts(Path_exprContext.class);
		}
		public Path_exprContext path_expr(int i) {
			return getRuleContext(Path_exprContext.class,i);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public List<TerminalNode> VARNAME() { return getTokens(KVQLParser.VARNAME); }
		public TerminalNode VARNAME(int i) {
			return getToken(KVQLParser.VARNAME, i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public List<TerminalNode> AS() { return getTokens(KVQLParser.AS); }
		public TerminalNode AS(int i) {
			return getToken(KVQLParser.AS, i);
		}
		public Unnest_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_unnest_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterUnnest_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitUnnest_clause(this);
		}
	}

	public final Unnest_clauseContext unnest_clause() throws RecognitionException {
		Unnest_clauseContext _localctx = new Unnest_clauseContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_unnest_clause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(615);
			match(UNNEST);
			setState(616);
			match(LP);
			setState(617);
			path_expr();
			{
			setState(619);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AS) {
				{
				setState(618);
				match(AS);
				}
			}

			setState(621);
			match(VARNAME);
			}
			setState(632);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(623);
				match(COMMA);
				{
				setState(624);
				path_expr();
				{
				setState(626);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AS) {
					{
					setState(625);
					match(AS);
					}
				}

				setState(628);
				match(VARNAME);
				}
				}
				}
				}
				setState(634);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(635);
			match(RP);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Where_clauseContext extends ParserRuleContext {
		public TerminalNode WHERE() { return getToken(KVQLParser.WHERE, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public Where_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_where_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterWhere_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitWhere_clause(this);
		}
	}

	public final Where_clauseContext where_clause() throws RecognitionException {
		Where_clauseContext _localctx = new Where_clauseContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_where_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(637);
			match(WHERE);
			setState(638);
			expr();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Select_clauseContext extends ParserRuleContext {
		public TerminalNode SELECT() { return getToken(KVQLParser.SELECT, 0); }
		public Select_listContext select_list() {
			return getRuleContext(Select_listContext.class,0);
		}
		public Select_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_select_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterSelect_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitSelect_clause(this);
		}
	}

	public final Select_clauseContext select_clause() throws RecognitionException {
		Select_clauseContext _localctx = new Select_clauseContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_select_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(640);
			match(SELECT);
			setState(641);
			select_list();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Select_listContext extends ParserRuleContext {
		public TerminalNode STAR() { return getToken(KVQLParser.STAR, 0); }
		public HintsContext hints() {
			return getRuleContext(HintsContext.class,0);
		}
		public TerminalNode DISTINCT() { return getToken(KVQLParser.DISTINCT, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public List<Col_aliasContext> col_alias() {
			return getRuleContexts(Col_aliasContext.class);
		}
		public Col_aliasContext col_alias(int i) {
			return getRuleContext(Col_aliasContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Select_listContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_select_list; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterSelect_list(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitSelect_list(this);
		}
	}

	public final Select_listContext select_list() throws RecognitionException {
		Select_listContext _localctx = new Select_listContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_select_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(644);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__0) {
				{
				setState(643);
				hints();
				}
			}

			setState(647);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,26,_ctx) ) {
			case 1:
				{
				setState(646);
				match(DISTINCT);
				}
				break;
			}
			setState(661);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case STAR:
				{
				setState(649);
				match(STAR);
				}
				break;
			case VARNAME:
			case ACCOUNT:
			case ADD:
			case ADMIN:
			case ALL:
			case ALTER:
			case ALWAYS:
			case ANCESTORS:
			case AND:
			case AS:
			case ASC:
			case ARRAY_COLLECT:
			case BEFORE:
			case BETWEEN:
			case BY:
			case CACHE:
			case CASE:
			case CAST:
			case COLLECTION:
			case COMMENT:
			case COUNT:
			case CREATE:
			case CYCLE:
			case DAYS:
			case DECLARE:
			case DEFAULT:
			case DELETE:
			case DESC:
			case DESCENDANTS:
			case DESCRIBE:
			case DISABLE:
			case DISTINCT:
			case DROP:
			case ELEMENTOF:
			case ELEMENTS:
			case ELSE:
			case ENABLE:
			case END:
			case ES_SHARDS:
			case ES_REPLICAS:
			case EXISTS:
			case EXTRACT:
			case FIELDS:
			case FIRST:
			case FREEZE:
			case FROM:
			case FROZEN:
			case FULLTEXT:
			case GENERATED:
			case GRANT:
			case GROUP:
			case HOURS:
			case IDENTIFIED:
			case IDENTITY:
			case IF:
			case IMAGE:
			case IN:
			case INCREMENT:
			case INDEX:
			case INDEXES:
			case INSERT:
			case INTO:
			case IS:
			case JSON:
			case KEY:
			case KEYOF:
			case KEYS:
			case LAST:
			case LIFETIME:
			case LIMIT:
			case LOCAL:
			case LOCK:
			case MERGE:
			case MINUTES:
			case MODIFY:
			case MR_COUNTER:
			case NAMESPACES:
			case NESTED:
			case NO:
			case NOT:
			case NULLS:
			case OFFSET:
			case OF:
			case ON:
			case OR:
			case ORDER:
			case OVERRIDE:
			case PASSWORD:
			case PATCH:
			case PER:
			case PRIMARY:
			case PUT:
			case REGION:
			case REGIONS:
			case REMOVE:
			case RETURNING:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROW:
			case SCHEMA:
			case SECONDS:
			case SELECT:
			case SEQ_TRANSFORM:
			case SET:
			case SHARD:
			case SHOW:
			case START:
			case TABLE:
			case TABLES:
			case THEN:
			case TO:
			case TTL:
			case TYPE:
			case UNFREEZE:
			case UNLOCK:
			case UPDATE:
			case UPSERT:
			case USER:
			case USERS:
			case USING:
			case VALUES:
			case WHEN:
			case WHERE:
			case WITH:
			case UNIQUE:
			case UNNEST:
			case ARRAY_T:
			case BINARY_T:
			case BOOLEAN_T:
			case DOUBLE_T:
			case ENUM_T:
			case FLOAT_T:
			case GEOMETRY_T:
			case INTEGER_T:
			case LONG_T:
			case MAP_T:
			case NUMBER_T:
			case POINT_T:
			case RECORD_T:
			case STRING_T:
			case TIMESTAMP_T:
			case ANY_T:
			case ANYATOMIC_T:
			case ANYJSONATOMIC_T:
			case ANYRECORD_T:
			case SCALAR_T:
			case LP:
			case LBRACK:
			case LBRACE:
			case DOLLAR:
			case QUESTION_MARK:
			case PLUS:
			case MINUS:
			case RDIV:
			case NULL:
			case FALSE:
			case TRUE:
			case INT:
			case FLOAT:
			case NUMBER:
			case DSTRING:
			case STRING:
			case ID:
			case BAD_ID:
				{
				{
				setState(650);
				expr();
				setState(651);
				col_alias();
				setState(658);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(652);
					match(COMMA);
					setState(653);
					expr();
					setState(654);
					col_alias();
					}
					}
					setState(660);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class HintsContext extends ParserRuleContext {
		public List<HintContext> hint() {
			return getRuleContexts(HintContext.class);
		}
		public HintContext hint(int i) {
			return getRuleContext(HintContext.class,i);
		}
		public HintsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_hints; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterHints(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitHints(this);
		}
	}

	public final HintsContext hints() throws RecognitionException {
		HintsContext _localctx = new HintsContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_hints);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(663);
			match(T__0);
			setState(667);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (((((_la - 51)) & ~0x3f) == 0 && ((1L << (_la - 51)) & 108086391056891907L) != 0)) {
				{
				{
				setState(664);
				hint();
				}
				}
				setState(669);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(670);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class HintContext extends ParserRuleContext {
		public TerminalNode STRING() { return getToken(KVQLParser.STRING, 0); }
		public TerminalNode PREFER_INDEXES() { return getToken(KVQLParser.PREFER_INDEXES, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public TerminalNode FORCE_INDEX() { return getToken(KVQLParser.FORCE_INDEX, 0); }
		public List<Index_nameContext> index_name() {
			return getRuleContexts(Index_nameContext.class);
		}
		public Index_nameContext index_name(int i) {
			return getRuleContext(Index_nameContext.class,i);
		}
		public TerminalNode PREFER_PRIMARY_INDEX() { return getToken(KVQLParser.PREFER_PRIMARY_INDEX, 0); }
		public TerminalNode FORCE_PRIMARY_INDEX() { return getToken(KVQLParser.FORCE_PRIMARY_INDEX, 0); }
		public HintContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_hint; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterHint(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitHint(this);
		}
	}

	public final HintContext hint() throws RecognitionException {
		HintContext _localctx = new HintContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_hint);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(699);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case PREFER_INDEXES:
				{
				{
				setState(672);
				match(PREFER_INDEXES);
				setState(673);
				match(LP);
				setState(674);
				table_name();
				setState(678);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while ((((_la) & ~0x3f) == 0 && ((1L << _la) & -7881299352092736L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & -13348796645889L) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & 17592169398271L) != 0) || ((((_la - 200)) & ~0x3f) == 0 && ((1L << (_la - 200)) & 6145L) != 0)) {
					{
					{
					setState(675);
					index_name();
					}
					}
					setState(680);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(681);
				match(RP);
				}
				}
				break;
			case FORCE_INDEX:
				{
				{
				setState(683);
				match(FORCE_INDEX);
				setState(684);
				match(LP);
				setState(685);
				table_name();
				setState(686);
				index_name();
				setState(687);
				match(RP);
				}
				}
				break;
			case PREFER_PRIMARY_INDEX:
				{
				{
				setState(689);
				match(PREFER_PRIMARY_INDEX);
				setState(690);
				match(LP);
				setState(691);
				table_name();
				setState(692);
				match(RP);
				}
				}
				break;
			case FORCE_PRIMARY_INDEX:
				{
				{
				setState(694);
				match(FORCE_PRIMARY_INDEX);
				setState(695);
				match(LP);
				setState(696);
				table_name();
				setState(697);
				match(RP);
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(702);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==STRING) {
				{
				setState(701);
				match(STRING);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Col_aliasContext extends ParserRuleContext {
		public TerminalNode AS() { return getToken(KVQLParser.AS, 0); }
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Col_aliasContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_col_alias; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterCol_alias(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitCol_alias(this);
		}
	}

	public final Col_aliasContext col_alias() throws RecognitionException {
		Col_aliasContext _localctx = new Col_aliasContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_col_alias);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(706);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AS) {
				{
				setState(704);
				match(AS);
				setState(705);
				id();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Orderby_clauseContext extends ParserRuleContext {
		public TerminalNode ORDER() { return getToken(KVQLParser.ORDER, 0); }
		public TerminalNode BY() { return getToken(KVQLParser.BY, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public List<Sort_specContext> sort_spec() {
			return getRuleContexts(Sort_specContext.class);
		}
		public Sort_specContext sort_spec(int i) {
			return getRuleContext(Sort_specContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Orderby_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_orderby_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterOrderby_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitOrderby_clause(this);
		}
	}

	public final Orderby_clauseContext orderby_clause() throws RecognitionException {
		Orderby_clauseContext _localctx = new Orderby_clauseContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_orderby_clause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(708);
			match(ORDER);
			setState(709);
			match(BY);
			setState(710);
			expr();
			setState(711);
			sort_spec();
			setState(718);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(712);
				match(COMMA);
				setState(713);
				expr();
				setState(714);
				sort_spec();
				}
				}
				setState(720);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Sort_specContext extends ParserRuleContext {
		public TerminalNode NULLS() { return getToken(KVQLParser.NULLS, 0); }
		public TerminalNode ASC() { return getToken(KVQLParser.ASC, 0); }
		public TerminalNode DESC() { return getToken(KVQLParser.DESC, 0); }
		public TerminalNode FIRST() { return getToken(KVQLParser.FIRST, 0); }
		public TerminalNode LAST() { return getToken(KVQLParser.LAST, 0); }
		public Sort_specContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sort_spec; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterSort_spec(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitSort_spec(this);
		}
	}

	public final Sort_specContext sort_spec() throws RecognitionException {
		Sort_specContext _localctx = new Sort_specContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_sort_spec);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(722);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ASC || _la==DESC) {
				{
				setState(721);
				_la = _input.LA(1);
				if ( !(_la==ASC || _la==DESC) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(726);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==NULLS) {
				{
				setState(724);
				match(NULLS);
				setState(725);
				_la = _input.LA(1);
				if ( !(_la==FIRST || _la==LAST) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Groupby_clauseContext extends ParserRuleContext {
		public TerminalNode GROUP() { return getToken(KVQLParser.GROUP, 0); }
		public TerminalNode BY() { return getToken(KVQLParser.BY, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Groupby_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupby_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterGroupby_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitGroupby_clause(this);
		}
	}

	public final Groupby_clauseContext groupby_clause() throws RecognitionException {
		Groupby_clauseContext _localctx = new Groupby_clauseContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_groupby_clause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(728);
			match(GROUP);
			setState(729);
			match(BY);
			setState(730);
			expr();
			setState(735);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(731);
				match(COMMA);
				setState(732);
				expr();
				}
				}
				setState(737);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Limit_clauseContext extends ParserRuleContext {
		public TerminalNode LIMIT() { return getToken(KVQLParser.LIMIT, 0); }
		public Add_exprContext add_expr() {
			return getRuleContext(Add_exprContext.class,0);
		}
		public Limit_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_limit_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterLimit_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitLimit_clause(this);
		}
	}

	public final Limit_clauseContext limit_clause() throws RecognitionException {
		Limit_clauseContext _localctx = new Limit_clauseContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_limit_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(738);
			match(LIMIT);
			setState(739);
			add_expr();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Offset_clauseContext extends ParserRuleContext {
		public TerminalNode OFFSET() { return getToken(KVQLParser.OFFSET, 0); }
		public Add_exprContext add_expr() {
			return getRuleContext(Add_exprContext.class,0);
		}
		public Offset_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_offset_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterOffset_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitOffset_clause(this);
		}
	}

	public final Offset_clauseContext offset_clause() throws RecognitionException {
		Offset_clauseContext _localctx = new Offset_clauseContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_offset_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(741);
			match(OFFSET);
			setState(742);
			add_expr();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Or_exprContext extends ParserRuleContext {
		public And_exprContext and_expr() {
			return getRuleContext(And_exprContext.class,0);
		}
		public Or_exprContext or_expr() {
			return getRuleContext(Or_exprContext.class,0);
		}
		public TerminalNode OR() { return getToken(KVQLParser.OR, 0); }
		public Or_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_or_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterOr_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitOr_expr(this);
		}
	}

	public final Or_exprContext or_expr() throws RecognitionException {
		return or_expr(0);
	}

	private Or_exprContext or_expr(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		Or_exprContext _localctx = new Or_exprContext(_ctx, _parentState);
		Or_exprContext _prevctx = _localctx;
		int _startState = 60;
		enterRecursionRule(_localctx, 60, RULE_or_expr, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			{
			setState(745);
			and_expr(0);
			}
			_ctx.stop = _input.LT(-1);
			setState(752);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,38,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					{
					_localctx = new Or_exprContext(_parentctx, _parentState);
					pushNewRecursionContext(_localctx, _startState, RULE_or_expr);
					setState(747);
					if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
					setState(748);
					match(OR);
					setState(749);
					and_expr(0);
					}
					} 
				}
				setState(754);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,38,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class And_exprContext extends ParserRuleContext {
		public Not_exprContext not_expr() {
			return getRuleContext(Not_exprContext.class,0);
		}
		public And_exprContext and_expr() {
			return getRuleContext(And_exprContext.class,0);
		}
		public TerminalNode AND() { return getToken(KVQLParser.AND, 0); }
		public And_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_and_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAnd_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAnd_expr(this);
		}
	}

	public final And_exprContext and_expr() throws RecognitionException {
		return and_expr(0);
	}

	private And_exprContext and_expr(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		And_exprContext _localctx = new And_exprContext(_ctx, _parentState);
		And_exprContext _prevctx = _localctx;
		int _startState = 62;
		enterRecursionRule(_localctx, 62, RULE_and_expr, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			{
			setState(756);
			not_expr();
			}
			_ctx.stop = _input.LT(-1);
			setState(763);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,39,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					{
					_localctx = new And_exprContext(_parentctx, _parentState);
					pushNewRecursionContext(_localctx, _startState, RULE_and_expr);
					setState(758);
					if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
					setState(759);
					match(AND);
					setState(760);
					not_expr();
					}
					} 
				}
				setState(765);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,39,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Not_exprContext extends ParserRuleContext {
		public Is_null_exprContext is_null_expr() {
			return getRuleContext(Is_null_exprContext.class,0);
		}
		public TerminalNode NOT() { return getToken(KVQLParser.NOT, 0); }
		public Not_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_not_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterNot_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitNot_expr(this);
		}
	}

	public final Not_exprContext not_expr() throws RecognitionException {
		Not_exprContext _localctx = new Not_exprContext(_ctx, getState());
		enterRule(_localctx, 64, RULE_not_expr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(767);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,40,_ctx) ) {
			case 1:
				{
				setState(766);
				match(NOT);
				}
				break;
			}
			setState(769);
			is_null_expr();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Is_null_exprContext extends ParserRuleContext {
		public Cond_exprContext cond_expr() {
			return getRuleContext(Cond_exprContext.class,0);
		}
		public TerminalNode IS() { return getToken(KVQLParser.IS, 0); }
		public TerminalNode NULL() { return getToken(KVQLParser.NULL, 0); }
		public TerminalNode NOT() { return getToken(KVQLParser.NOT, 0); }
		public Is_null_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_is_null_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterIs_null_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitIs_null_expr(this);
		}
	}

	public final Is_null_exprContext is_null_expr() throws RecognitionException {
		Is_null_exprContext _localctx = new Is_null_exprContext(_ctx, getState());
		enterRule(_localctx, 66, RULE_is_null_expr);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(771);
			cond_expr();
			setState(777);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,42,_ctx) ) {
			case 1:
				{
				setState(772);
				match(IS);
				setState(774);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(773);
					match(NOT);
					}
				}

				setState(776);
				match(NULL);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Cond_exprContext extends ParserRuleContext {
		public Between_exprContext between_expr() {
			return getRuleContext(Between_exprContext.class,0);
		}
		public Comp_exprContext comp_expr() {
			return getRuleContext(Comp_exprContext.class,0);
		}
		public In_exprContext in_expr() {
			return getRuleContext(In_exprContext.class,0);
		}
		public Exists_exprContext exists_expr() {
			return getRuleContext(Exists_exprContext.class,0);
		}
		public Is_of_type_exprContext is_of_type_expr() {
			return getRuleContext(Is_of_type_exprContext.class,0);
		}
		public Cond_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_cond_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterCond_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitCond_expr(this);
		}
	}

	public final Cond_exprContext cond_expr() throws RecognitionException {
		Cond_exprContext _localctx = new Cond_exprContext(_ctx, getState());
		enterRule(_localctx, 68, RULE_cond_expr);
		try {
			setState(784);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,43,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(779);
				between_expr();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(780);
				comp_expr();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(781);
				in_expr();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(782);
				exists_expr();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(783);
				is_of_type_expr();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Between_exprContext extends ParserRuleContext {
		public List<Concatenate_exprContext> concatenate_expr() {
			return getRuleContexts(Concatenate_exprContext.class);
		}
		public Concatenate_exprContext concatenate_expr(int i) {
			return getRuleContext(Concatenate_exprContext.class,i);
		}
		public TerminalNode BETWEEN() { return getToken(KVQLParser.BETWEEN, 0); }
		public TerminalNode AND() { return getToken(KVQLParser.AND, 0); }
		public Between_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_between_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterBetween_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitBetween_expr(this);
		}
	}

	public final Between_exprContext between_expr() throws RecognitionException {
		Between_exprContext _localctx = new Between_exprContext(_ctx, getState());
		enterRule(_localctx, 70, RULE_between_expr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(786);
			concatenate_expr();
			setState(787);
			match(BETWEEN);
			setState(788);
			concatenate_expr();
			setState(789);
			match(AND);
			setState(790);
			concatenate_expr();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Comp_exprContext extends ParserRuleContext {
		public List<Concatenate_exprContext> concatenate_expr() {
			return getRuleContexts(Concatenate_exprContext.class);
		}
		public Concatenate_exprContext concatenate_expr(int i) {
			return getRuleContext(Concatenate_exprContext.class,i);
		}
		public Comp_opContext comp_op() {
			return getRuleContext(Comp_opContext.class,0);
		}
		public Any_opContext any_op() {
			return getRuleContext(Any_opContext.class,0);
		}
		public Comp_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comp_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterComp_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitComp_expr(this);
		}
	}

	public final Comp_exprContext comp_expr() throws RecognitionException {
		Comp_exprContext _localctx = new Comp_exprContext(_ctx, getState());
		enterRule(_localctx, 72, RULE_comp_expr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			{
			setState(792);
			concatenate_expr();
			setState(799);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,45,_ctx) ) {
			case 1:
				{
				setState(795);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case LT:
				case LTE:
				case GT:
				case GTE:
				case EQ:
				case NEQ:
					{
					setState(793);
					comp_op();
					}
					break;
				case LT_ANY:
				case LTE_ANY:
				case GT_ANY:
				case GTE_ANY:
				case EQ_ANY:
				case NEQ_ANY:
					{
					setState(794);
					any_op();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(797);
				concatenate_expr();
				}
				break;
			}
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Comp_opContext extends ParserRuleContext {
		public TerminalNode EQ() { return getToken(KVQLParser.EQ, 0); }
		public TerminalNode NEQ() { return getToken(KVQLParser.NEQ, 0); }
		public TerminalNode GT() { return getToken(KVQLParser.GT, 0); }
		public TerminalNode GTE() { return getToken(KVQLParser.GTE, 0); }
		public TerminalNode LT() { return getToken(KVQLParser.LT, 0); }
		public TerminalNode LTE() { return getToken(KVQLParser.LTE, 0); }
		public Comp_opContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comp_op; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterComp_op(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitComp_op(this);
		}
	}

	public final Comp_opContext comp_op() throws RecognitionException {
		Comp_opContext _localctx = new Comp_opContext(_ctx, getState());
		enterRule(_localctx, 74, RULE_comp_op);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(801);
			_la = _input.LA(1);
			if ( !(((((_la - 185)) & ~0x3f) == 0 && ((1L << (_la - 185)) & 63L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Any_opContext extends ParserRuleContext {
		public TerminalNode EQ_ANY() { return getToken(KVQLParser.EQ_ANY, 0); }
		public TerminalNode NEQ_ANY() { return getToken(KVQLParser.NEQ_ANY, 0); }
		public TerminalNode GT_ANY() { return getToken(KVQLParser.GT_ANY, 0); }
		public TerminalNode GTE_ANY() { return getToken(KVQLParser.GTE_ANY, 0); }
		public TerminalNode LT_ANY() { return getToken(KVQLParser.LT_ANY, 0); }
		public TerminalNode LTE_ANY() { return getToken(KVQLParser.LTE_ANY, 0); }
		public Any_opContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_any_op; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAny_op(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAny_op(this);
		}
	}

	public final Any_opContext any_op() throws RecognitionException {
		Any_opContext _localctx = new Any_opContext(_ctx, getState());
		enterRule(_localctx, 76, RULE_any_op);
		try {
			setState(809);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case EQ_ANY:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(803);
				match(EQ_ANY);
				}
				}
				break;
			case NEQ_ANY:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(804);
				match(NEQ_ANY);
				}
				}
				break;
			case GT_ANY:
				enterOuterAlt(_localctx, 3);
				{
				{
				setState(805);
				match(GT_ANY);
				}
				}
				break;
			case GTE_ANY:
				enterOuterAlt(_localctx, 4);
				{
				{
				setState(806);
				match(GTE_ANY);
				}
				}
				break;
			case LT_ANY:
				enterOuterAlt(_localctx, 5);
				{
				{
				setState(807);
				match(LT_ANY);
				}
				}
				break;
			case LTE_ANY:
				enterOuterAlt(_localctx, 6);
				{
				{
				setState(808);
				match(LTE_ANY);
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class In_exprContext extends ParserRuleContext {
		public In1_exprContext in1_expr() {
			return getRuleContext(In1_exprContext.class,0);
		}
		public In2_exprContext in2_expr() {
			return getRuleContext(In2_exprContext.class,0);
		}
		public In3_exprContext in3_expr() {
			return getRuleContext(In3_exprContext.class,0);
		}
		public In_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_in_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterIn_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitIn_expr(this);
		}
	}

	public final In_exprContext in_expr() throws RecognitionException {
		In_exprContext _localctx = new In_exprContext(_ctx, getState());
		enterRule(_localctx, 78, RULE_in_expr);
		try {
			setState(814);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,47,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(811);
				in1_expr();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(812);
				in2_expr();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(813);
				in3_expr();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class In1_exprContext extends ParserRuleContext {
		public In1_left_opContext in1_left_op() {
			return getRuleContext(In1_left_opContext.class,0);
		}
		public TerminalNode IN() { return getToken(KVQLParser.IN, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public List<In1_expr_listContext> in1_expr_list() {
			return getRuleContexts(In1_expr_listContext.class);
		}
		public In1_expr_listContext in1_expr_list(int i) {
			return getRuleContext(In1_expr_listContext.class,i);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public In1_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_in1_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterIn1_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitIn1_expr(this);
		}
	}

	public final In1_exprContext in1_expr() throws RecognitionException {
		In1_exprContext _localctx = new In1_exprContext(_ctx, getState());
		enterRule(_localctx, 80, RULE_in1_expr);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(816);
			in1_left_op();
			setState(817);
			match(IN);
			setState(818);
			match(LP);
			setState(819);
			in1_expr_list();
			setState(822); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(820);
				match(COMMA);
				setState(821);
				in1_expr_list();
				}
				}
				setState(824); 
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( _la==COMMA );
			setState(826);
			match(RP);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class In1_left_opContext extends ParserRuleContext {
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public List<Concatenate_exprContext> concatenate_expr() {
			return getRuleContexts(Concatenate_exprContext.class);
		}
		public Concatenate_exprContext concatenate_expr(int i) {
			return getRuleContext(Concatenate_exprContext.class,i);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public In1_left_opContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_in1_left_op; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterIn1_left_op(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitIn1_left_op(this);
		}
	}

	public final In1_left_opContext in1_left_op() throws RecognitionException {
		In1_left_opContext _localctx = new In1_left_opContext(_ctx, getState());
		enterRule(_localctx, 82, RULE_in1_left_op);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(828);
			match(LP);
			setState(829);
			concatenate_expr();
			setState(834);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(830);
				match(COMMA);
				setState(831);
				concatenate_expr();
				}
				}
				setState(836);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(837);
			match(RP);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class In1_expr_listContext extends ParserRuleContext {
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public In1_expr_listContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_in1_expr_list; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterIn1_expr_list(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitIn1_expr_list(this);
		}
	}

	public final In1_expr_listContext in1_expr_list() throws RecognitionException {
		In1_expr_listContext _localctx = new In1_expr_listContext(_ctx, getState());
		enterRule(_localctx, 84, RULE_in1_expr_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(839);
			match(LP);
			setState(840);
			expr();
			setState(845);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(841);
				match(COMMA);
				setState(842);
				expr();
				}
				}
				setState(847);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(848);
			match(RP);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class In2_exprContext extends ParserRuleContext {
		public Concatenate_exprContext concatenate_expr() {
			return getRuleContext(Concatenate_exprContext.class,0);
		}
		public TerminalNode IN() { return getToken(KVQLParser.IN, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public In2_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_in2_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterIn2_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitIn2_expr(this);
		}
	}

	public final In2_exprContext in2_expr() throws RecognitionException {
		In2_exprContext _localctx = new In2_exprContext(_ctx, getState());
		enterRule(_localctx, 86, RULE_in2_expr);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(850);
			concatenate_expr();
			setState(851);
			match(IN);
			setState(852);
			match(LP);
			setState(853);
			expr();
			setState(856); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(854);
				match(COMMA);
				setState(855);
				expr();
				}
				}
				setState(858); 
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( _la==COMMA );
			setState(860);
			match(RP);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class In3_exprContext extends ParserRuleContext {
		public TerminalNode IN() { return getToken(KVQLParser.IN, 0); }
		public Path_exprContext path_expr() {
			return getRuleContext(Path_exprContext.class,0);
		}
		public List<Concatenate_exprContext> concatenate_expr() {
			return getRuleContexts(Concatenate_exprContext.class);
		}
		public Concatenate_exprContext concatenate_expr(int i) {
			return getRuleContext(Concatenate_exprContext.class,i);
		}
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public In3_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_in3_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterIn3_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitIn3_expr(this);
		}
	}

	public final In3_exprContext in3_expr() throws RecognitionException {
		In3_exprContext _localctx = new In3_exprContext(_ctx, getState());
		enterRule(_localctx, 88, RULE_in3_expr);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(874);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,53,_ctx) ) {
			case 1:
				{
				setState(862);
				concatenate_expr();
				}
				break;
			case 2:
				{
				{
				setState(863);
				match(LP);
				setState(864);
				concatenate_expr();
				setState(869);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(865);
					match(COMMA);
					setState(866);
					concatenate_expr();
					}
					}
					setState(871);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(872);
				match(RP);
				}
				}
				break;
			}
			setState(876);
			match(IN);
			setState(877);
			path_expr();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Exists_exprContext extends ParserRuleContext {
		public TerminalNode EXISTS() { return getToken(KVQLParser.EXISTS, 0); }
		public Concatenate_exprContext concatenate_expr() {
			return getRuleContext(Concatenate_exprContext.class,0);
		}
		public Exists_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_exists_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterExists_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitExists_expr(this);
		}
	}

	public final Exists_exprContext exists_expr() throws RecognitionException {
		Exists_exprContext _localctx = new Exists_exprContext(_ctx, getState());
		enterRule(_localctx, 90, RULE_exists_expr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(879);
			match(EXISTS);
			setState(880);
			concatenate_expr();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Is_of_type_exprContext extends ParserRuleContext {
		public Concatenate_exprContext concatenate_expr() {
			return getRuleContext(Concatenate_exprContext.class,0);
		}
		public TerminalNode IS() { return getToken(KVQLParser.IS, 0); }
		public TerminalNode OF() { return getToken(KVQLParser.OF, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public List<Quantified_type_defContext> quantified_type_def() {
			return getRuleContexts(Quantified_type_defContext.class);
		}
		public Quantified_type_defContext quantified_type_def(int i) {
			return getRuleContext(Quantified_type_defContext.class,i);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public TerminalNode NOT() { return getToken(KVQLParser.NOT, 0); }
		public TerminalNode TYPE() { return getToken(KVQLParser.TYPE, 0); }
		public List<TerminalNode> ONLY() { return getTokens(KVQLParser.ONLY); }
		public TerminalNode ONLY(int i) {
			return getToken(KVQLParser.ONLY, i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Is_of_type_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_is_of_type_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterIs_of_type_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitIs_of_type_expr(this);
		}
	}

	public final Is_of_type_exprContext is_of_type_expr() throws RecognitionException {
		Is_of_type_exprContext _localctx = new Is_of_type_exprContext(_ctx, getState());
		enterRule(_localctx, 92, RULE_is_of_type_expr);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(882);
			concatenate_expr();
			setState(883);
			match(IS);
			setState(885);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==NOT) {
				{
				setState(884);
				match(NOT);
				}
			}

			setState(887);
			match(OF);
			setState(889);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==TYPE) {
				{
				setState(888);
				match(TYPE);
				}
			}

			setState(891);
			match(LP);
			setState(893);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ONLY) {
				{
				setState(892);
				match(ONLY);
				}
			}

			setState(895);
			quantified_type_def();
			setState(903);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(896);
				match(COMMA);
				setState(898);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ONLY) {
					{
					setState(897);
					match(ONLY);
					}
				}

				setState(900);
				quantified_type_def();
				}
				}
				setState(905);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(906);
			match(RP);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Concatenate_exprContext extends ParserRuleContext {
		public List<Add_exprContext> add_expr() {
			return getRuleContexts(Add_exprContext.class);
		}
		public Add_exprContext add_expr(int i) {
			return getRuleContext(Add_exprContext.class,i);
		}
		public List<TerminalNode> CONCAT() { return getTokens(KVQLParser.CONCAT); }
		public TerminalNode CONCAT(int i) {
			return getToken(KVQLParser.CONCAT, i);
		}
		public Concatenate_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_concatenate_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterConcatenate_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitConcatenate_expr(this);
		}
	}

	public final Concatenate_exprContext concatenate_expr() throws RecognitionException {
		Concatenate_exprContext _localctx = new Concatenate_exprContext(_ctx, getState());
		enterRule(_localctx, 94, RULE_concatenate_expr);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(908);
			add_expr();
			setState(913);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,59,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(909);
					match(CONCAT);
					setState(910);
					add_expr();
					}
					} 
				}
				setState(915);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,59,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Add_exprContext extends ParserRuleContext {
		public List<Multiply_exprContext> multiply_expr() {
			return getRuleContexts(Multiply_exprContext.class);
		}
		public Multiply_exprContext multiply_expr(int i) {
			return getRuleContext(Multiply_exprContext.class,i);
		}
		public List<TerminalNode> PLUS() { return getTokens(KVQLParser.PLUS); }
		public TerminalNode PLUS(int i) {
			return getToken(KVQLParser.PLUS, i);
		}
		public List<TerminalNode> MINUS() { return getTokens(KVQLParser.MINUS); }
		public TerminalNode MINUS(int i) {
			return getToken(KVQLParser.MINUS, i);
		}
		public Add_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_add_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAdd_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAdd_expr(this);
		}
	}

	public final Add_exprContext add_expr() throws RecognitionException {
		Add_exprContext _localctx = new Add_exprContext(_ctx, getState());
		enterRule(_localctx, 96, RULE_add_expr);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(916);
			multiply_expr();
			setState(921);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,60,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(917);
					_la = _input.LA(1);
					if ( !(_la==PLUS || _la==MINUS) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(918);
					multiply_expr();
					}
					} 
				}
				setState(923);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,60,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Multiply_exprContext extends ParserRuleContext {
		public List<Unary_exprContext> unary_expr() {
			return getRuleContexts(Unary_exprContext.class);
		}
		public Unary_exprContext unary_expr(int i) {
			return getRuleContext(Unary_exprContext.class,i);
		}
		public List<TerminalNode> STAR() { return getTokens(KVQLParser.STAR); }
		public TerminalNode STAR(int i) {
			return getToken(KVQLParser.STAR, i);
		}
		public List<TerminalNode> IDIV() { return getTokens(KVQLParser.IDIV); }
		public TerminalNode IDIV(int i) {
			return getToken(KVQLParser.IDIV, i);
		}
		public List<TerminalNode> RDIV() { return getTokens(KVQLParser.RDIV); }
		public TerminalNode RDIV(int i) {
			return getToken(KVQLParser.RDIV, i);
		}
		public Multiply_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multiply_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterMultiply_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitMultiply_expr(this);
		}
	}

	public final Multiply_exprContext multiply_expr() throws RecognitionException {
		Multiply_exprContext _localctx = new Multiply_exprContext(_ctx, getState());
		enterRule(_localctx, 98, RULE_multiply_expr);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(924);
			unary_expr();
			setState(929);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,61,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(925);
					_la = _input.LA(1);
					if ( !(((((_la - 181)) & ~0x3f) == 0 && ((1L << (_la - 181)) & 786433L) != 0)) ) {
					_errHandler.recoverInline(this);
					}
					else {
						if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
						_errHandler.reportMatch(this);
						consume();
					}
					setState(926);
					unary_expr();
					}
					} 
				}
				setState(931);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,61,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Unary_exprContext extends ParserRuleContext {
		public Path_exprContext path_expr() {
			return getRuleContext(Path_exprContext.class,0);
		}
		public Unary_exprContext unary_expr() {
			return getRuleContext(Unary_exprContext.class,0);
		}
		public TerminalNode PLUS() { return getToken(KVQLParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(KVQLParser.MINUS, 0); }
		public Unary_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_unary_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterUnary_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitUnary_expr(this);
		}
	}

	public final Unary_exprContext unary_expr() throws RecognitionException {
		Unary_exprContext _localctx = new Unary_exprContext(_ctx, getState());
		enterRule(_localctx, 100, RULE_unary_expr);
		int _la;
		try {
			setState(935);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,62,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(932);
				path_expr();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(933);
				_la = _input.LA(1);
				if ( !(_la==PLUS || _la==MINUS) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(934);
				unary_expr();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Path_exprContext extends ParserRuleContext {
		public Primary_exprContext primary_expr() {
			return getRuleContext(Primary_exprContext.class,0);
		}
		public List<Map_stepContext> map_step() {
			return getRuleContexts(Map_stepContext.class);
		}
		public Map_stepContext map_step(int i) {
			return getRuleContext(Map_stepContext.class,i);
		}
		public List<Array_stepContext> array_step() {
			return getRuleContexts(Array_stepContext.class);
		}
		public Array_stepContext array_step(int i) {
			return getRuleContext(Array_stepContext.class,i);
		}
		public Path_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_path_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterPath_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitPath_expr(this);
		}
	}

	public final Path_exprContext path_expr() throws RecognitionException {
		Path_exprContext _localctx = new Path_exprContext(_ctx, getState());
		enterRule(_localctx, 102, RULE_path_expr);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(937);
			primary_expr();
			setState(942);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,64,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					setState(940);
					_errHandler.sync(this);
					switch (_input.LA(1)) {
					case DOT:
						{
						setState(938);
						map_step();
						}
						break;
					case LBRACK:
						{
						setState(939);
						array_step();
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					} 
				}
				setState(944);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,64,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Map_stepContext extends ParserRuleContext {
		public TerminalNode DOT() { return getToken(KVQLParser.DOT, 0); }
		public Map_filter_stepContext map_filter_step() {
			return getRuleContext(Map_filter_stepContext.class,0);
		}
		public Map_field_stepContext map_field_step() {
			return getRuleContext(Map_field_stepContext.class,0);
		}
		public Map_stepContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_map_step; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterMap_step(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitMap_step(this);
		}
	}

	public final Map_stepContext map_step() throws RecognitionException {
		Map_stepContext _localctx = new Map_stepContext(_ctx, getState());
		enterRule(_localctx, 104, RULE_map_step);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(945);
			match(DOT);
			setState(948);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,65,_ctx) ) {
			case 1:
				{
				setState(946);
				map_filter_step();
				}
				break;
			case 2:
				{
				setState(947);
				map_field_step();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Map_field_stepContext extends ParserRuleContext {
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public Var_refContext var_ref() {
			return getRuleContext(Var_refContext.class,0);
		}
		public Parenthesized_exprContext parenthesized_expr() {
			return getRuleContext(Parenthesized_exprContext.class,0);
		}
		public Func_callContext func_call() {
			return getRuleContext(Func_callContext.class,0);
		}
		public Map_field_stepContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_map_field_step; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterMap_field_step(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitMap_field_step(this);
		}
	}

	public final Map_field_stepContext map_field_step() throws RecognitionException {
		Map_field_stepContext _localctx = new Map_field_stepContext(_ctx, getState());
		enterRule(_localctx, 106, RULE_map_field_step);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(955);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,66,_ctx) ) {
			case 1:
				{
				setState(950);
				id();
				}
				break;
			case 2:
				{
				setState(951);
				string();
				}
				break;
			case 3:
				{
				setState(952);
				var_ref();
				}
				break;
			case 4:
				{
				setState(953);
				parenthesized_expr();
				}
				break;
			case 5:
				{
				setState(954);
				func_call();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Map_filter_stepContext extends ParserRuleContext {
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public TerminalNode KEYS() { return getToken(KVQLParser.KEYS, 0); }
		public TerminalNode VALUES() { return getToken(KVQLParser.VALUES, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public Map_filter_stepContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_map_filter_step; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterMap_filter_step(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitMap_filter_step(this);
		}
	}

	public final Map_filter_stepContext map_filter_step() throws RecognitionException {
		Map_filter_stepContext _localctx = new Map_filter_stepContext(_ctx, getState());
		enterRule(_localctx, 108, RULE_map_filter_step);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(957);
			_la = _input.LA(1);
			if ( !(_la==KEYS || _la==VALUES) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(958);
			match(LP);
			setState(960);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -7881299352092704L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & -13348796645889L) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & 111059470481752063L) != 0) || ((((_la - 197)) & ~0x3f) == 0 && ((1L << (_la - 197)) & 57323L) != 0)) {
				{
				setState(959);
				expr();
				}
			}

			setState(962);
			match(RP);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Array_stepContext extends ParserRuleContext {
		public Array_filter_stepContext array_filter_step() {
			return getRuleContext(Array_filter_stepContext.class,0);
		}
		public Array_slice_stepContext array_slice_step() {
			return getRuleContext(Array_slice_stepContext.class,0);
		}
		public Array_stepContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_array_step; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterArray_step(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitArray_step(this);
		}
	}

	public final Array_stepContext array_step() throws RecognitionException {
		Array_stepContext _localctx = new Array_stepContext(_ctx, getState());
		enterRule(_localctx, 110, RULE_array_step);
		try {
			setState(966);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,68,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(964);
				array_filter_step();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(965);
				array_slice_step();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Array_slice_stepContext extends ParserRuleContext {
		public TerminalNode LBRACK() { return getToken(KVQLParser.LBRACK, 0); }
		public TerminalNode COLON() { return getToken(KVQLParser.COLON, 0); }
		public TerminalNode RBRACK() { return getToken(KVQLParser.RBRACK, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public Array_slice_stepContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_array_slice_step; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterArray_slice_step(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitArray_slice_step(this);
		}
	}

	public final Array_slice_stepContext array_slice_step() throws RecognitionException {
		Array_slice_stepContext _localctx = new Array_slice_stepContext(_ctx, getState());
		enterRule(_localctx, 112, RULE_array_slice_step);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(968);
			match(LBRACK);
			setState(970);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -7881299352092704L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & -13348796645889L) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & 111059470481752063L) != 0) || ((((_la - 197)) & ~0x3f) == 0 && ((1L << (_la - 197)) & 57323L) != 0)) {
				{
				setState(969);
				expr();
				}
			}

			setState(972);
			match(COLON);
			setState(974);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -7881299352092704L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & -13348796645889L) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & 111059470481752063L) != 0) || ((((_la - 197)) & ~0x3f) == 0 && ((1L << (_la - 197)) & 57323L) != 0)) {
				{
				setState(973);
				expr();
				}
			}

			setState(976);
			match(RBRACK);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Array_filter_stepContext extends ParserRuleContext {
		public TerminalNode LBRACK() { return getToken(KVQLParser.LBRACK, 0); }
		public TerminalNode RBRACK() { return getToken(KVQLParser.RBRACK, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public Array_filter_stepContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_array_filter_step; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterArray_filter_step(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitArray_filter_step(this);
		}
	}

	public final Array_filter_stepContext array_filter_step() throws RecognitionException {
		Array_filter_stepContext _localctx = new Array_filter_stepContext(_ctx, getState());
		enterRule(_localctx, 114, RULE_array_filter_step);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(978);
			match(LBRACK);
			setState(980);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -7881299352092704L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & -13348796645889L) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & 111059470481752063L) != 0) || ((((_la - 197)) & ~0x3f) == 0 && ((1L << (_la - 197)) & 57323L) != 0)) {
				{
				setState(979);
				expr();
				}
			}

			setState(982);
			match(RBRACK);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Primary_exprContext extends ParserRuleContext {
		public Const_exprContext const_expr() {
			return getRuleContext(Const_exprContext.class,0);
		}
		public Column_refContext column_ref() {
			return getRuleContext(Column_refContext.class,0);
		}
		public Var_refContext var_ref() {
			return getRuleContext(Var_refContext.class,0);
		}
		public Array_constructorContext array_constructor() {
			return getRuleContext(Array_constructorContext.class,0);
		}
		public Map_constructorContext map_constructor() {
			return getRuleContext(Map_constructorContext.class,0);
		}
		public Transform_exprContext transform_expr() {
			return getRuleContext(Transform_exprContext.class,0);
		}
		public CollectContext collect() {
			return getRuleContext(CollectContext.class,0);
		}
		public Func_callContext func_call() {
			return getRuleContext(Func_callContext.class,0);
		}
		public Count_starContext count_star() {
			return getRuleContext(Count_starContext.class,0);
		}
		public Count_distinctContext count_distinct() {
			return getRuleContext(Count_distinctContext.class,0);
		}
		public Case_exprContext case_expr() {
			return getRuleContext(Case_exprContext.class,0);
		}
		public Cast_exprContext cast_expr() {
			return getRuleContext(Cast_exprContext.class,0);
		}
		public Parenthesized_exprContext parenthesized_expr() {
			return getRuleContext(Parenthesized_exprContext.class,0);
		}
		public Extract_exprContext extract_expr() {
			return getRuleContext(Extract_exprContext.class,0);
		}
		public Primary_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_primary_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterPrimary_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitPrimary_expr(this);
		}
	}

	public final Primary_exprContext primary_expr() throws RecognitionException {
		Primary_exprContext _localctx = new Primary_exprContext(_ctx, getState());
		enterRule(_localctx, 116, RULE_primary_expr);
		try {
			setState(998);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,72,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(984);
				const_expr();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(985);
				column_ref();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(986);
				var_ref();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(987);
				array_constructor();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(988);
				map_constructor();
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(989);
				transform_expr();
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(990);
				collect();
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(991);
				func_call();
				}
				break;
			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(992);
				count_star();
				}
				break;
			case 10:
				enterOuterAlt(_localctx, 10);
				{
				setState(993);
				count_distinct();
				}
				break;
			case 11:
				enterOuterAlt(_localctx, 11);
				{
				setState(994);
				case_expr();
				}
				break;
			case 12:
				enterOuterAlt(_localctx, 12);
				{
				setState(995);
				cast_expr();
				}
				break;
			case 13:
				enterOuterAlt(_localctx, 13);
				{
				setState(996);
				parenthesized_expr();
				}
				break;
			case 14:
				enterOuterAlt(_localctx, 14);
				{
				setState(997);
				extract_expr();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Column_refContext extends ParserRuleContext {
		public List<IdContext> id() {
			return getRuleContexts(IdContext.class);
		}
		public IdContext id(int i) {
			return getRuleContext(IdContext.class,i);
		}
		public TerminalNode DOT() { return getToken(KVQLParser.DOT, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public Column_refContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_column_ref; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterColumn_ref(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitColumn_ref(this);
		}
	}

	public final Column_refContext column_ref() throws RecognitionException {
		Column_refContext _localctx = new Column_refContext(_ctx, getState());
		enterRule(_localctx, 118, RULE_column_ref);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1000);
			id();
			setState(1006);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,74,_ctx) ) {
			case 1:
				{
				setState(1001);
				match(DOT);
				setState(1004);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case ACCOUNT:
				case ADD:
				case ADMIN:
				case ALL:
				case ALTER:
				case ALWAYS:
				case ANCESTORS:
				case AND:
				case AS:
				case ASC:
				case ARRAY_COLLECT:
				case BEFORE:
				case BETWEEN:
				case BY:
				case CACHE:
				case CASE:
				case CAST:
				case COLLECTION:
				case COMMENT:
				case COUNT:
				case CREATE:
				case CYCLE:
				case DAYS:
				case DECLARE:
				case DEFAULT:
				case DELETE:
				case DESC:
				case DESCENDANTS:
				case DESCRIBE:
				case DISABLE:
				case DISTINCT:
				case DROP:
				case ELEMENTOF:
				case ELEMENTS:
				case ELSE:
				case ENABLE:
				case END:
				case ES_SHARDS:
				case ES_REPLICAS:
				case EXISTS:
				case EXTRACT:
				case FIELDS:
				case FIRST:
				case FREEZE:
				case FROM:
				case FROZEN:
				case FULLTEXT:
				case GENERATED:
				case GRANT:
				case GROUP:
				case HOURS:
				case IDENTIFIED:
				case IDENTITY:
				case IF:
				case IMAGE:
				case IN:
				case INCREMENT:
				case INDEX:
				case INDEXES:
				case INSERT:
				case INTO:
				case IS:
				case JSON:
				case KEY:
				case KEYOF:
				case KEYS:
				case LAST:
				case LIFETIME:
				case LIMIT:
				case LOCAL:
				case LOCK:
				case MERGE:
				case MINUTES:
				case MODIFY:
				case MR_COUNTER:
				case NAMESPACES:
				case NESTED:
				case NO:
				case NOT:
				case NULLS:
				case OFFSET:
				case OF:
				case ON:
				case OR:
				case ORDER:
				case OVERRIDE:
				case PASSWORD:
				case PATCH:
				case PER:
				case PRIMARY:
				case PUT:
				case REGION:
				case REGIONS:
				case REMOVE:
				case RETURNING:
				case REVOKE:
				case ROLE:
				case ROLES:
				case ROW:
				case SCHEMA:
				case SECONDS:
				case SELECT:
				case SEQ_TRANSFORM:
				case SET:
				case SHARD:
				case SHOW:
				case START:
				case TABLE:
				case TABLES:
				case THEN:
				case TO:
				case TTL:
				case TYPE:
				case UNFREEZE:
				case UNLOCK:
				case UPDATE:
				case UPSERT:
				case USER:
				case USERS:
				case USING:
				case VALUES:
				case WHEN:
				case WHERE:
				case WITH:
				case UNIQUE:
				case UNNEST:
				case ARRAY_T:
				case BINARY_T:
				case BOOLEAN_T:
				case DOUBLE_T:
				case ENUM_T:
				case FLOAT_T:
				case GEOMETRY_T:
				case INTEGER_T:
				case LONG_T:
				case MAP_T:
				case NUMBER_T:
				case POINT_T:
				case RECORD_T:
				case STRING_T:
				case TIMESTAMP_T:
				case ANY_T:
				case ANYATOMIC_T:
				case ANYJSONATOMIC_T:
				case ANYRECORD_T:
				case SCALAR_T:
				case RDIV:
				case ID:
				case BAD_ID:
					{
					setState(1002);
					id();
					}
					break;
				case DSTRING:
				case STRING:
					{
					setState(1003);
					string();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Const_exprContext extends ParserRuleContext {
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode TRUE() { return getToken(KVQLParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(KVQLParser.FALSE, 0); }
		public TerminalNode NULL() { return getToken(KVQLParser.NULL, 0); }
		public Const_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_const_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterConst_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitConst_expr(this);
		}
	}

	public final Const_exprContext const_expr() throws RecognitionException {
		Const_exprContext _localctx = new Const_exprContext(_ctx, getState());
		enterRule(_localctx, 120, RULE_const_expr);
		try {
			setState(1013);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case MINUS:
			case INT:
			case FLOAT:
			case NUMBER:
				enterOuterAlt(_localctx, 1);
				{
				setState(1008);
				number();
				}
				break;
			case DSTRING:
			case STRING:
				enterOuterAlt(_localctx, 2);
				{
				setState(1009);
				string();
				}
				break;
			case TRUE:
				enterOuterAlt(_localctx, 3);
				{
				setState(1010);
				match(TRUE);
				}
				break;
			case FALSE:
				enterOuterAlt(_localctx, 4);
				{
				setState(1011);
				match(FALSE);
				}
				break;
			case NULL:
				enterOuterAlt(_localctx, 5);
				{
				setState(1012);
				match(NULL);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Var_refContext extends ParserRuleContext {
		public TerminalNode VARNAME() { return getToken(KVQLParser.VARNAME, 0); }
		public TerminalNode DOLLAR() { return getToken(KVQLParser.DOLLAR, 0); }
		public TerminalNode QUESTION_MARK() { return getToken(KVQLParser.QUESTION_MARK, 0); }
		public Var_refContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_var_ref; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterVar_ref(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitVar_ref(this);
		}
	}

	public final Var_refContext var_ref() throws RecognitionException {
		Var_refContext _localctx = new Var_refContext(_ctx, getState());
		enterRule(_localctx, 122, RULE_var_ref);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1015);
			_la = _input.LA(1);
			if ( !(_la==VARNAME || _la==DOLLAR || _la==QUESTION_MARK) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Array_constructorContext extends ParserRuleContext {
		public TerminalNode LBRACK() { return getToken(KVQLParser.LBRACK, 0); }
		public TerminalNode RBRACK() { return getToken(KVQLParser.RBRACK, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Array_constructorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_array_constructor; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterArray_constructor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitArray_constructor(this);
		}
	}

	public final Array_constructorContext array_constructor() throws RecognitionException {
		Array_constructorContext _localctx = new Array_constructorContext(_ctx, getState());
		enterRule(_localctx, 124, RULE_array_constructor);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1017);
			match(LBRACK);
			setState(1019);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -7881299352092704L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & -13348796645889L) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & 111059470481752063L) != 0) || ((((_la - 197)) & ~0x3f) == 0 && ((1L << (_la - 197)) & 57323L) != 0)) {
				{
				setState(1018);
				expr();
				}
			}

			setState(1025);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1021);
				match(COMMA);
				setState(1022);
				expr();
				}
				}
				setState(1027);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1028);
			match(RBRACK);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Map_constructorContext extends ParserRuleContext {
		public TerminalNode LBRACE() { return getToken(KVQLParser.LBRACE, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public List<TerminalNode> COLON() { return getTokens(KVQLParser.COLON); }
		public TerminalNode COLON(int i) {
			return getToken(KVQLParser.COLON, i);
		}
		public TerminalNode RBRACE() { return getToken(KVQLParser.RBRACE, 0); }
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Map_constructorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_map_constructor; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterMap_constructor(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitMap_constructor(this);
		}
	}

	public final Map_constructorContext map_constructor() throws RecognitionException {
		Map_constructorContext _localctx = new Map_constructorContext(_ctx, getState());
		enterRule(_localctx, 126, RULE_map_constructor);
		int _la;
		try {
			setState(1048);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,79,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(1030);
				match(LBRACE);
				setState(1031);
				expr();
				setState(1032);
				match(COLON);
				setState(1033);
				expr();
				setState(1041);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(1034);
					match(COMMA);
					setState(1035);
					expr();
					setState(1036);
					match(COLON);
					setState(1037);
					expr();
					}
					}
					setState(1043);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1044);
				match(RBRACE);
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(1046);
				match(LBRACE);
				setState(1047);
				match(RBRACE);
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Transform_exprContext extends ParserRuleContext {
		public TerminalNode SEQ_TRANSFORM() { return getToken(KVQLParser.SEQ_TRANSFORM, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public Transform_input_exprContext transform_input_expr() {
			return getRuleContext(Transform_input_exprContext.class,0);
		}
		public TerminalNode COMMA() { return getToken(KVQLParser.COMMA, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Transform_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_transform_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterTransform_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitTransform_expr(this);
		}
	}

	public final Transform_exprContext transform_expr() throws RecognitionException {
		Transform_exprContext _localctx = new Transform_exprContext(_ctx, getState());
		enterRule(_localctx, 128, RULE_transform_expr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1050);
			match(SEQ_TRANSFORM);
			setState(1051);
			match(LP);
			setState(1052);
			transform_input_expr();
			setState(1053);
			match(COMMA);
			setState(1054);
			expr();
			setState(1055);
			match(RP);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Transform_input_exprContext extends ParserRuleContext {
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public Transform_input_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_transform_input_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterTransform_input_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitTransform_input_expr(this);
		}
	}

	public final Transform_input_exprContext transform_input_expr() throws RecognitionException {
		Transform_input_exprContext _localctx = new Transform_input_exprContext(_ctx, getState());
		enterRule(_localctx, 130, RULE_transform_input_expr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1057);
			expr();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class CollectContext extends ParserRuleContext {
		public TerminalNode ARRAY_COLLECT() { return getToken(KVQLParser.ARRAY_COLLECT, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public TerminalNode DISTINCT() { return getToken(KVQLParser.DISTINCT, 0); }
		public CollectContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_collect; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterCollect(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitCollect(this);
		}
	}

	public final CollectContext collect() throws RecognitionException {
		CollectContext _localctx = new CollectContext(_ctx, getState());
		enterRule(_localctx, 132, RULE_collect);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1059);
			match(ARRAY_COLLECT);
			setState(1060);
			match(LP);
			setState(1062);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,80,_ctx) ) {
			case 1:
				{
				setState(1061);
				match(DISTINCT);
				}
				break;
			}
			setState(1064);
			expr();
			setState(1065);
			match(RP);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Func_callContext extends ParserRuleContext {
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Func_callContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_func_call; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterFunc_call(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitFunc_call(this);
		}
	}

	public final Func_callContext func_call() throws RecognitionException {
		Func_callContext _localctx = new Func_callContext(_ctx, getState());
		enterRule(_localctx, 134, RULE_func_call);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1067);
			id();
			setState(1068);
			match(LP);
			setState(1077);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -7881299352092704L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & -13348796645889L) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & 111059470481752063L) != 0) || ((((_la - 197)) & ~0x3f) == 0 && ((1L << (_la - 197)) & 57323L) != 0)) {
				{
				setState(1069);
				expr();
				setState(1074);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(1070);
					match(COMMA);
					setState(1071);
					expr();
					}
					}
					setState(1076);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1079);
			match(RP);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Count_starContext extends ParserRuleContext {
		public TerminalNode COUNT() { return getToken(KVQLParser.COUNT, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public TerminalNode STAR() { return getToken(KVQLParser.STAR, 0); }
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Count_starContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_count_star; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterCount_star(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitCount_star(this);
		}
	}

	public final Count_starContext count_star() throws RecognitionException {
		Count_starContext _localctx = new Count_starContext(_ctx, getState());
		enterRule(_localctx, 136, RULE_count_star);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1081);
			match(COUNT);
			setState(1082);
			match(LP);
			setState(1083);
			match(STAR);
			setState(1084);
			match(RP);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Count_distinctContext extends ParserRuleContext {
		public TerminalNode COUNT() { return getToken(KVQLParser.COUNT, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public TerminalNode DISTINCT() { return getToken(KVQLParser.DISTINCT, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Count_distinctContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_count_distinct; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterCount_distinct(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitCount_distinct(this);
		}
	}

	public final Count_distinctContext count_distinct() throws RecognitionException {
		Count_distinctContext _localctx = new Count_distinctContext(_ctx, getState());
		enterRule(_localctx, 138, RULE_count_distinct);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1086);
			match(COUNT);
			setState(1087);
			match(LP);
			setState(1088);
			match(DISTINCT);
			setState(1089);
			expr();
			setState(1090);
			match(RP);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Case_exprContext extends ParserRuleContext {
		public TerminalNode CASE() { return getToken(KVQLParser.CASE, 0); }
		public List<TerminalNode> WHEN() { return getTokens(KVQLParser.WHEN); }
		public TerminalNode WHEN(int i) {
			return getToken(KVQLParser.WHEN, i);
		}
		public List<ExprContext> expr() {
			return getRuleContexts(ExprContext.class);
		}
		public ExprContext expr(int i) {
			return getRuleContext(ExprContext.class,i);
		}
		public List<TerminalNode> THEN() { return getTokens(KVQLParser.THEN); }
		public TerminalNode THEN(int i) {
			return getToken(KVQLParser.THEN, i);
		}
		public TerminalNode END() { return getToken(KVQLParser.END, 0); }
		public TerminalNode ELSE() { return getToken(KVQLParser.ELSE, 0); }
		public Case_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_case_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterCase_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitCase_expr(this);
		}
	}

	public final Case_exprContext case_expr() throws RecognitionException {
		Case_exprContext _localctx = new Case_exprContext(_ctx, getState());
		enterRule(_localctx, 140, RULE_case_expr);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1092);
			match(CASE);
			setState(1093);
			match(WHEN);
			setState(1094);
			expr();
			setState(1095);
			match(THEN);
			setState(1096);
			expr();
			setState(1104);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==WHEN) {
				{
				{
				setState(1097);
				match(WHEN);
				setState(1098);
				expr();
				setState(1099);
				match(THEN);
				setState(1100);
				expr();
				}
				}
				setState(1106);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1109);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ELSE) {
				{
				setState(1107);
				match(ELSE);
				setState(1108);
				expr();
				}
			}

			setState(1111);
			match(END);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Cast_exprContext extends ParserRuleContext {
		public TerminalNode CAST() { return getToken(KVQLParser.CAST, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode AS() { return getToken(KVQLParser.AS, 0); }
		public Quantified_type_defContext quantified_type_def() {
			return getRuleContext(Quantified_type_defContext.class,0);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Cast_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_cast_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterCast_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitCast_expr(this);
		}
	}

	public final Cast_exprContext cast_expr() throws RecognitionException {
		Cast_exprContext _localctx = new Cast_exprContext(_ctx, getState());
		enterRule(_localctx, 142, RULE_cast_expr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1113);
			match(CAST);
			setState(1114);
			match(LP);
			setState(1115);
			expr();
			setState(1116);
			match(AS);
			setState(1117);
			quantified_type_def();
			setState(1118);
			match(RP);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Parenthesized_exprContext extends ParserRuleContext {
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Parenthesized_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_parenthesized_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterParenthesized_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitParenthesized_expr(this);
		}
	}

	public final Parenthesized_exprContext parenthesized_expr() throws RecognitionException {
		Parenthesized_exprContext _localctx = new Parenthesized_exprContext(_ctx, getState());
		enterRule(_localctx, 144, RULE_parenthesized_expr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1120);
			match(LP);
			setState(1121);
			expr();
			setState(1122);
			match(RP);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Extract_exprContext extends ParserRuleContext {
		public TerminalNode EXTRACT() { return getToken(KVQLParser.EXTRACT, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public TerminalNode FROM() { return getToken(KVQLParser.FROM, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Extract_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_extract_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterExtract_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitExtract_expr(this);
		}
	}

	public final Extract_exprContext extract_expr() throws RecognitionException {
		Extract_exprContext _localctx = new Extract_exprContext(_ctx, getState());
		enterRule(_localctx, 146, RULE_extract_expr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1124);
			match(EXTRACT);
			setState(1125);
			match(LP);
			setState(1126);
			id();
			setState(1127);
			match(FROM);
			setState(1128);
			expr();
			setState(1129);
			match(RP);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Insert_statementContext extends ParserRuleContext {
		public TerminalNode INTO() { return getToken(KVQLParser.INTO, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode VALUES() { return getToken(KVQLParser.VALUES, 0); }
		public List<TerminalNode> LP() { return getTokens(KVQLParser.LP); }
		public TerminalNode LP(int i) {
			return getToken(KVQLParser.LP, i);
		}
		public List<Insert_clauseContext> insert_clause() {
			return getRuleContexts(Insert_clauseContext.class);
		}
		public Insert_clauseContext insert_clause(int i) {
			return getRuleContext(Insert_clauseContext.class,i);
		}
		public List<TerminalNode> RP() { return getTokens(KVQLParser.RP); }
		public TerminalNode RP(int i) {
			return getToken(KVQLParser.RP, i);
		}
		public TerminalNode INSERT() { return getToken(KVQLParser.INSERT, 0); }
		public TerminalNode UPSERT() { return getToken(KVQLParser.UPSERT, 0); }
		public PrologContext prolog() {
			return getRuleContext(PrologContext.class,0);
		}
		public Tab_aliasContext tab_alias() {
			return getRuleContext(Tab_aliasContext.class,0);
		}
		public List<Insert_labelContext> insert_label() {
			return getRuleContexts(Insert_labelContext.class);
		}
		public Insert_labelContext insert_label(int i) {
			return getRuleContext(Insert_labelContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public TerminalNode SET() { return getToken(KVQLParser.SET, 0); }
		public TerminalNode TTL() { return getToken(KVQLParser.TTL, 0); }
		public Insert_ttl_clauseContext insert_ttl_clause() {
			return getRuleContext(Insert_ttl_clauseContext.class,0);
		}
		public Insert_returning_clauseContext insert_returning_clause() {
			return getRuleContext(Insert_returning_clauseContext.class,0);
		}
		public TerminalNode AS() { return getToken(KVQLParser.AS, 0); }
		public Insert_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insert_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterInsert_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitInsert_statement(this);
		}
	}

	public final Insert_statementContext insert_statement() throws RecognitionException {
		Insert_statementContext _localctx = new Insert_statementContext(_ctx, getState());
		enterRule(_localctx, 148, RULE_insert_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1132);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DECLARE) {
				{
				setState(1131);
				prolog();
				}
			}

			setState(1134);
			_la = _input.LA(1);
			if ( !(_la==INSERT || _la==UPSERT) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(1135);
			match(INTO);
			setState(1136);
			table_name();
			setState(1141);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,87,_ctx) ) {
			case 1:
				{
				setState(1138);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,86,_ctx) ) {
				case 1:
					{
					setState(1137);
					match(AS);
					}
					break;
				}
				setState(1140);
				tab_alias();
				}
				break;
			}
			setState(1154);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LP) {
				{
				setState(1143);
				match(LP);
				setState(1144);
				insert_label();
				setState(1149);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(1145);
					match(COMMA);
					setState(1146);
					insert_label();
					}
					}
					setState(1151);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(1152);
				match(RP);
				}
			}

			setState(1156);
			match(VALUES);
			setState(1157);
			match(LP);
			setState(1158);
			insert_clause();
			setState(1163);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1159);
				match(COMMA);
				setState(1160);
				insert_clause();
				}
				}
				setState(1165);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1166);
			match(RP);
			setState(1170);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SET) {
				{
				setState(1167);
				match(SET);
				setState(1168);
				match(TTL);
				setState(1169);
				insert_ttl_clause();
				}
			}

			setState(1173);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==RETURNING) {
				{
				setState(1172);
				insert_returning_clause();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Insert_labelContext extends ParserRuleContext {
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public Insert_labelContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insert_label; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterInsert_label(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitInsert_label(this);
		}
	}

	public final Insert_labelContext insert_label() throws RecognitionException {
		Insert_labelContext _localctx = new Insert_labelContext(_ctx, getState());
		enterRule(_localctx, 150, RULE_insert_label);
		try {
			setState(1177);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ACCOUNT:
			case ADD:
			case ADMIN:
			case ALL:
			case ALTER:
			case ALWAYS:
			case ANCESTORS:
			case AND:
			case AS:
			case ASC:
			case ARRAY_COLLECT:
			case BEFORE:
			case BETWEEN:
			case BY:
			case CACHE:
			case CASE:
			case CAST:
			case COLLECTION:
			case COMMENT:
			case COUNT:
			case CREATE:
			case CYCLE:
			case DAYS:
			case DECLARE:
			case DEFAULT:
			case DELETE:
			case DESC:
			case DESCENDANTS:
			case DESCRIBE:
			case DISABLE:
			case DISTINCT:
			case DROP:
			case ELEMENTOF:
			case ELEMENTS:
			case ELSE:
			case ENABLE:
			case END:
			case ES_SHARDS:
			case ES_REPLICAS:
			case EXISTS:
			case EXTRACT:
			case FIELDS:
			case FIRST:
			case FREEZE:
			case FROM:
			case FROZEN:
			case FULLTEXT:
			case GENERATED:
			case GRANT:
			case GROUP:
			case HOURS:
			case IDENTIFIED:
			case IDENTITY:
			case IF:
			case IMAGE:
			case IN:
			case INCREMENT:
			case INDEX:
			case INDEXES:
			case INSERT:
			case INTO:
			case IS:
			case JSON:
			case KEY:
			case KEYOF:
			case KEYS:
			case LAST:
			case LIFETIME:
			case LIMIT:
			case LOCAL:
			case LOCK:
			case MERGE:
			case MINUTES:
			case MODIFY:
			case MR_COUNTER:
			case NAMESPACES:
			case NESTED:
			case NO:
			case NOT:
			case NULLS:
			case OFFSET:
			case OF:
			case ON:
			case OR:
			case ORDER:
			case OVERRIDE:
			case PASSWORD:
			case PATCH:
			case PER:
			case PRIMARY:
			case PUT:
			case REGION:
			case REGIONS:
			case REMOVE:
			case RETURNING:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROW:
			case SCHEMA:
			case SECONDS:
			case SELECT:
			case SEQ_TRANSFORM:
			case SET:
			case SHARD:
			case SHOW:
			case START:
			case TABLE:
			case TABLES:
			case THEN:
			case TO:
			case TTL:
			case TYPE:
			case UNFREEZE:
			case UNLOCK:
			case UPDATE:
			case UPSERT:
			case USER:
			case USERS:
			case USING:
			case VALUES:
			case WHEN:
			case WHERE:
			case WITH:
			case UNIQUE:
			case UNNEST:
			case ARRAY_T:
			case BINARY_T:
			case BOOLEAN_T:
			case DOUBLE_T:
			case ENUM_T:
			case FLOAT_T:
			case GEOMETRY_T:
			case INTEGER_T:
			case LONG_T:
			case MAP_T:
			case NUMBER_T:
			case POINT_T:
			case RECORD_T:
			case STRING_T:
			case TIMESTAMP_T:
			case ANY_T:
			case ANYATOMIC_T:
			case ANYJSONATOMIC_T:
			case ANYRECORD_T:
			case SCALAR_T:
			case RDIV:
			case ID:
			case BAD_ID:
				enterOuterAlt(_localctx, 1);
				{
				setState(1175);
				id();
				}
				break;
			case DSTRING:
			case STRING:
				enterOuterAlt(_localctx, 2);
				{
				setState(1176);
				string();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Insert_returning_clauseContext extends ParserRuleContext {
		public TerminalNode RETURNING() { return getToken(KVQLParser.RETURNING, 0); }
		public Select_listContext select_list() {
			return getRuleContext(Select_listContext.class,0);
		}
		public Insert_returning_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insert_returning_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterInsert_returning_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitInsert_returning_clause(this);
		}
	}

	public final Insert_returning_clauseContext insert_returning_clause() throws RecognitionException {
		Insert_returning_clauseContext _localctx = new Insert_returning_clauseContext(_ctx, getState());
		enterRule(_localctx, 152, RULE_insert_returning_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1179);
			match(RETURNING);
			setState(1180);
			select_list();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Insert_clauseContext extends ParserRuleContext {
		public TerminalNode DEFAULT() { return getToken(KVQLParser.DEFAULT, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public Insert_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insert_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterInsert_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitInsert_clause(this);
		}
	}

	public final Insert_clauseContext insert_clause() throws RecognitionException {
		Insert_clauseContext _localctx = new Insert_clauseContext(_ctx, getState());
		enterRule(_localctx, 154, RULE_insert_clause);
		try {
			setState(1184);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,94,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1182);
				match(DEFAULT);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1183);
				expr();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Insert_ttl_clauseContext extends ParserRuleContext {
		public Add_exprContext add_expr() {
			return getRuleContext(Add_exprContext.class,0);
		}
		public TerminalNode HOURS() { return getToken(KVQLParser.HOURS, 0); }
		public TerminalNode DAYS() { return getToken(KVQLParser.DAYS, 0); }
		public TerminalNode USING() { return getToken(KVQLParser.USING, 0); }
		public TerminalNode TABLE() { return getToken(KVQLParser.TABLE, 0); }
		public TerminalNode DEFAULT() { return getToken(KVQLParser.DEFAULT, 0); }
		public Insert_ttl_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insert_ttl_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterInsert_ttl_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitInsert_ttl_clause(this);
		}
	}

	public final Insert_ttl_clauseContext insert_ttl_clause() throws RecognitionException {
		Insert_ttl_clauseContext _localctx = new Insert_ttl_clauseContext(_ctx, getState());
		enterRule(_localctx, 156, RULE_insert_ttl_clause);
		int _la;
		try {
			setState(1192);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,95,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(1186);
				add_expr();
				setState(1187);
				_la = _input.LA(1);
				if ( !(_la==DAYS || _la==HOURS) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(1189);
				match(USING);
				setState(1190);
				match(TABLE);
				setState(1191);
				match(DEFAULT);
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Update_statementContext extends ParserRuleContext {
		public TerminalNode UPDATE() { return getToken(KVQLParser.UPDATE, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public List<Update_clauseContext> update_clause() {
			return getRuleContexts(Update_clauseContext.class);
		}
		public Update_clauseContext update_clause(int i) {
			return getRuleContext(Update_clauseContext.class,i);
		}
		public TerminalNode WHERE() { return getToken(KVQLParser.WHERE, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public PrologContext prolog() {
			return getRuleContext(PrologContext.class,0);
		}
		public Tab_aliasContext tab_alias() {
			return getRuleContext(Tab_aliasContext.class,0);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Update_returning_clauseContext update_returning_clause() {
			return getRuleContext(Update_returning_clauseContext.class,0);
		}
		public TerminalNode AS() { return getToken(KVQLParser.AS, 0); }
		public Update_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_update_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterUpdate_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitUpdate_statement(this);
		}
	}

	public final Update_statementContext update_statement() throws RecognitionException {
		Update_statementContext _localctx = new Update_statementContext(_ctx, getState());
		enterRule(_localctx, 158, RULE_update_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1195);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DECLARE) {
				{
				setState(1194);
				prolog();
				}
			}

			setState(1197);
			match(UPDATE);
			setState(1198);
			table_name();
			setState(1203);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,98,_ctx) ) {
			case 1:
				{
				setState(1200);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,97,_ctx) ) {
				case 1:
					{
					setState(1199);
					match(AS);
					}
					break;
				}
				setState(1202);
				tab_alias();
				}
				break;
			}
			setState(1205);
			update_clause();
			setState(1210);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1206);
				match(COMMA);
				setState(1207);
				update_clause();
				}
				}
				setState(1212);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1213);
			match(WHERE);
			setState(1214);
			expr();
			setState(1216);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==RETURNING) {
				{
				setState(1215);
				update_returning_clause();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Update_returning_clauseContext extends ParserRuleContext {
		public TerminalNode RETURNING() { return getToken(KVQLParser.RETURNING, 0); }
		public Select_listContext select_list() {
			return getRuleContext(Select_listContext.class,0);
		}
		public Update_returning_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_update_returning_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterUpdate_returning_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitUpdate_returning_clause(this);
		}
	}

	public final Update_returning_clauseContext update_returning_clause() throws RecognitionException {
		Update_returning_clauseContext _localctx = new Update_returning_clauseContext(_ctx, getState());
		enterRule(_localctx, 160, RULE_update_returning_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1218);
			match(RETURNING);
			setState(1219);
			select_list();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Update_clauseContext extends ParserRuleContext {
		public TerminalNode SET() { return getToken(KVQLParser.SET, 0); }
		public List<Set_clauseContext> set_clause() {
			return getRuleContexts(Set_clauseContext.class);
		}
		public Set_clauseContext set_clause(int i) {
			return getRuleContext(Set_clauseContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public List<Update_clauseContext> update_clause() {
			return getRuleContexts(Update_clauseContext.class);
		}
		public Update_clauseContext update_clause(int i) {
			return getRuleContext(Update_clauseContext.class,i);
		}
		public TerminalNode ADD() { return getToken(KVQLParser.ADD, 0); }
		public List<Add_clauseContext> add_clause() {
			return getRuleContexts(Add_clauseContext.class);
		}
		public Add_clauseContext add_clause(int i) {
			return getRuleContext(Add_clauseContext.class,i);
		}
		public TerminalNode PUT() { return getToken(KVQLParser.PUT, 0); }
		public List<Put_clauseContext> put_clause() {
			return getRuleContexts(Put_clauseContext.class);
		}
		public Put_clauseContext put_clause(int i) {
			return getRuleContext(Put_clauseContext.class,i);
		}
		public TerminalNode REMOVE() { return getToken(KVQLParser.REMOVE, 0); }
		public List<Remove_clauseContext> remove_clause() {
			return getRuleContexts(Remove_clauseContext.class);
		}
		public Remove_clauseContext remove_clause(int i) {
			return getRuleContext(Remove_clauseContext.class,i);
		}
		public TerminalNode JSON() { return getToken(KVQLParser.JSON, 0); }
		public TerminalNode MERGE() { return getToken(KVQLParser.MERGE, 0); }
		public List<Json_merge_patch_clauseContext> json_merge_patch_clause() {
			return getRuleContexts(Json_merge_patch_clauseContext.class);
		}
		public Json_merge_patch_clauseContext json_merge_patch_clause(int i) {
			return getRuleContext(Json_merge_patch_clauseContext.class,i);
		}
		public TerminalNode TTL() { return getToken(KVQLParser.TTL, 0); }
		public Ttl_clauseContext ttl_clause() {
			return getRuleContext(Ttl_clauseContext.class,0);
		}
		public Update_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_update_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterUpdate_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitUpdate_clause(this);
		}
	}

	public final Update_clauseContext update_clause() throws RecognitionException {
		Update_clauseContext _localctx = new Update_clauseContext(_ctx, getState());
		enterRule(_localctx, 162, RULE_update_clause);
		try {
			int _alt;
			setState(1292);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,112,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(1221);
				match(SET);
				setState(1222);
				set_clause();
				setState(1230);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,102,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1223);
						match(COMMA);
						setState(1226);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,101,_ctx) ) {
						case 1:
							{
							setState(1224);
							update_clause();
							}
							break;
						case 2:
							{
							setState(1225);
							set_clause();
							}
							break;
						}
						}
						} 
					}
					setState(1232);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,102,_ctx);
				}
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(1233);
				match(ADD);
				setState(1234);
				add_clause();
				setState(1242);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,104,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1235);
						match(COMMA);
						setState(1238);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,103,_ctx) ) {
						case 1:
							{
							setState(1236);
							update_clause();
							}
							break;
						case 2:
							{
							setState(1237);
							add_clause();
							}
							break;
						}
						}
						} 
					}
					setState(1244);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,104,_ctx);
				}
				}
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				{
				setState(1245);
				match(PUT);
				setState(1246);
				put_clause();
				setState(1254);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,106,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1247);
						match(COMMA);
						setState(1250);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,105,_ctx) ) {
						case 1:
							{
							setState(1248);
							update_clause();
							}
							break;
						case 2:
							{
							setState(1249);
							put_clause();
							}
							break;
						}
						}
						} 
					}
					setState(1256);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,106,_ctx);
				}
				}
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				{
				setState(1257);
				match(REMOVE);
				setState(1258);
				remove_clause();
				setState(1266);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,108,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1259);
						match(COMMA);
						setState(1262);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,107,_ctx) ) {
						case 1:
							{
							setState(1260);
							update_clause();
							}
							break;
						case 2:
							{
							setState(1261);
							remove_clause();
							}
							break;
						}
						}
						} 
					}
					setState(1268);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,108,_ctx);
				}
				}
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				{
				setState(1269);
				match(JSON);
				setState(1270);
				match(MERGE);
				setState(1271);
				json_merge_patch_clause();
				setState(1279);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,110,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1272);
						match(COMMA);
						setState(1275);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,109,_ctx) ) {
						case 1:
							{
							setState(1273);
							update_clause();
							}
							break;
						case 2:
							{
							setState(1274);
							json_merge_patch_clause();
							}
							break;
						}
						}
						} 
					}
					setState(1281);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,110,_ctx);
				}
				}
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				{
				setState(1282);
				match(SET);
				setState(1283);
				match(TTL);
				setState(1284);
				ttl_clause();
				setState(1289);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,111,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(1285);
						match(COMMA);
						setState(1286);
						update_clause();
						}
						} 
					}
					setState(1291);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,111,_ctx);
				}
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Set_clauseContext extends ParserRuleContext {
		public Target_exprContext target_expr() {
			return getRuleContext(Target_exprContext.class,0);
		}
		public TerminalNode EQ() { return getToken(KVQLParser.EQ, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public Set_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_set_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterSet_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitSet_clause(this);
		}
	}

	public final Set_clauseContext set_clause() throws RecognitionException {
		Set_clauseContext _localctx = new Set_clauseContext(_ctx, getState());
		enterRule(_localctx, 164, RULE_set_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1294);
			target_expr();
			setState(1295);
			match(EQ);
			setState(1296);
			expr();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Add_clauseContext extends ParserRuleContext {
		public Target_exprContext target_expr() {
			return getRuleContext(Target_exprContext.class,0);
		}
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode INTO() { return getToken(KVQLParser.INTO, 0); }
		public Pos_exprContext pos_expr() {
			return getRuleContext(Pos_exprContext.class,0);
		}
		public TerminalNode ELEMENTS() { return getToken(KVQLParser.ELEMENTS, 0); }
		public Add_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_add_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAdd_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAdd_clause(this);
		}
	}

	public final Add_clauseContext add_clause() throws RecognitionException {
		Add_clauseContext _localctx = new Add_clauseContext(_ctx, getState());
		enterRule(_localctx, 166, RULE_add_clause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1299);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,113,_ctx) ) {
			case 1:
				{
				setState(1298);
				match(INTO);
				}
				break;
			}
			setState(1301);
			target_expr();
			setState(1306);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,115,_ctx) ) {
			case 1:
				{
				setState(1303);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__2) {
					{
					setState(1302);
					match(T__2);
					}
				}

				setState(1305);
				pos_expr();
				}
				break;
			}
			setState(1309);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,116,_ctx) ) {
			case 1:
				{
				setState(1308);
				match(ELEMENTS);
				}
				break;
			}
			setState(1311);
			expr();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Put_clauseContext extends ParserRuleContext {
		public Target_exprContext target_expr() {
			return getRuleContext(Target_exprContext.class,0);
		}
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode INTO() { return getToken(KVQLParser.INTO, 0); }
		public TerminalNode FIELDS() { return getToken(KVQLParser.FIELDS, 0); }
		public Put_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_put_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterPut_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitPut_clause(this);
		}
	}

	public final Put_clauseContext put_clause() throws RecognitionException {
		Put_clauseContext _localctx = new Put_clauseContext(_ctx, getState());
		enterRule(_localctx, 168, RULE_put_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1314);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,117,_ctx) ) {
			case 1:
				{
				setState(1313);
				match(INTO);
				}
				break;
			}
			setState(1316);
			target_expr();
			setState(1318);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,118,_ctx) ) {
			case 1:
				{
				setState(1317);
				match(FIELDS);
				}
				break;
			}
			setState(1320);
			expr();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Remove_clauseContext extends ParserRuleContext {
		public Target_exprContext target_expr() {
			return getRuleContext(Target_exprContext.class,0);
		}
		public Remove_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_remove_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterRemove_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitRemove_clause(this);
		}
	}

	public final Remove_clauseContext remove_clause() throws RecognitionException {
		Remove_clauseContext _localctx = new Remove_clauseContext(_ctx, getState());
		enterRule(_localctx, 170, RULE_remove_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1322);
			target_expr();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Json_merge_patch_clauseContext extends ParserRuleContext {
		public Target_exprContext target_expr() {
			return getRuleContext(Target_exprContext.class,0);
		}
		public TerminalNode WITH() { return getToken(KVQLParser.WITH, 0); }
		public TerminalNode PATCH() { return getToken(KVQLParser.PATCH, 0); }
		public Json_patch_exprContext json_patch_expr() {
			return getRuleContext(Json_patch_exprContext.class,0);
		}
		public Json_merge_patch_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_json_merge_patch_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterJson_merge_patch_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitJson_merge_patch_clause(this);
		}
	}

	public final Json_merge_patch_clauseContext json_merge_patch_clause() throws RecognitionException {
		Json_merge_patch_clauseContext _localctx = new Json_merge_patch_clauseContext(_ctx, getState());
		enterRule(_localctx, 172, RULE_json_merge_patch_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1324);
			target_expr();
			setState(1325);
			match(WITH);
			setState(1326);
			match(PATCH);
			setState(1327);
			json_patch_expr();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Json_patch_exprContext extends ParserRuleContext {
		public Map_constructorContext map_constructor() {
			return getRuleContext(Map_constructorContext.class,0);
		}
		public Array_constructorContext array_constructor() {
			return getRuleContext(Array_constructorContext.class,0);
		}
		public Const_exprContext const_expr() {
			return getRuleContext(Const_exprContext.class,0);
		}
		public Var_refContext var_ref() {
			return getRuleContext(Var_refContext.class,0);
		}
		public Json_patch_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_json_patch_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterJson_patch_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitJson_patch_expr(this);
		}
	}

	public final Json_patch_exprContext json_patch_expr() throws RecognitionException {
		Json_patch_exprContext _localctx = new Json_patch_exprContext(_ctx, getState());
		enterRule(_localctx, 174, RULE_json_patch_expr);
		try {
			setState(1333);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case LBRACE:
				enterOuterAlt(_localctx, 1);
				{
				setState(1329);
				map_constructor();
				}
				break;
			case LBRACK:
				enterOuterAlt(_localctx, 2);
				{
				setState(1330);
				array_constructor();
				}
				break;
			case MINUS:
			case NULL:
			case FALSE:
			case TRUE:
			case INT:
			case FLOAT:
			case NUMBER:
			case DSTRING:
			case STRING:
				enterOuterAlt(_localctx, 3);
				{
				setState(1331);
				const_expr();
				}
				break;
			case VARNAME:
			case DOLLAR:
			case QUESTION_MARK:
				enterOuterAlt(_localctx, 4);
				{
				setState(1332);
				var_ref();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Ttl_clauseContext extends ParserRuleContext {
		public Add_exprContext add_expr() {
			return getRuleContext(Add_exprContext.class,0);
		}
		public TerminalNode HOURS() { return getToken(KVQLParser.HOURS, 0); }
		public TerminalNode DAYS() { return getToken(KVQLParser.DAYS, 0); }
		public TerminalNode USING() { return getToken(KVQLParser.USING, 0); }
		public TerminalNode TABLE() { return getToken(KVQLParser.TABLE, 0); }
		public TerminalNode DEFAULT() { return getToken(KVQLParser.DEFAULT, 0); }
		public Ttl_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ttl_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterTtl_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitTtl_clause(this);
		}
	}

	public final Ttl_clauseContext ttl_clause() throws RecognitionException {
		Ttl_clauseContext _localctx = new Ttl_clauseContext(_ctx, getState());
		enterRule(_localctx, 176, RULE_ttl_clause);
		int _la;
		try {
			setState(1341);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,120,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(1335);
				add_expr();
				setState(1336);
				_la = _input.LA(1);
				if ( !(_la==DAYS || _la==HOURS) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(1338);
				match(USING);
				setState(1339);
				match(TABLE);
				setState(1340);
				match(DEFAULT);
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Target_exprContext extends ParserRuleContext {
		public Path_exprContext path_expr() {
			return getRuleContext(Path_exprContext.class,0);
		}
		public Target_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_target_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterTarget_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitTarget_expr(this);
		}
	}

	public final Target_exprContext target_expr() throws RecognitionException {
		Target_exprContext _localctx = new Target_exprContext(_ctx, getState());
		enterRule(_localctx, 178, RULE_target_expr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1343);
			path_expr();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Pos_exprContext extends ParserRuleContext {
		public Add_exprContext add_expr() {
			return getRuleContext(Add_exprContext.class,0);
		}
		public Pos_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pos_expr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterPos_expr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitPos_expr(this);
		}
	}

	public final Pos_exprContext pos_expr() throws RecognitionException {
		Pos_exprContext _localctx = new Pos_exprContext(_ctx, getState());
		enterRule(_localctx, 180, RULE_pos_expr);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1345);
			add_expr();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Delete_statementContext extends ParserRuleContext {
		public TerminalNode DELETE() { return getToken(KVQLParser.DELETE, 0); }
		public TerminalNode FROM() { return getToken(KVQLParser.FROM, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public PrologContext prolog() {
			return getRuleContext(PrologContext.class,0);
		}
		public Tab_aliasContext tab_alias() {
			return getRuleContext(Tab_aliasContext.class,0);
		}
		public TerminalNode WHERE() { return getToken(KVQLParser.WHERE, 0); }
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public Delete_returning_clauseContext delete_returning_clause() {
			return getRuleContext(Delete_returning_clauseContext.class,0);
		}
		public TerminalNode AS() { return getToken(KVQLParser.AS, 0); }
		public Delete_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_delete_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterDelete_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitDelete_statement(this);
		}
	}

	public final Delete_statementContext delete_statement() throws RecognitionException {
		Delete_statementContext _localctx = new Delete_statementContext(_ctx, getState());
		enterRule(_localctx, 182, RULE_delete_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1348);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DECLARE) {
				{
				setState(1347);
				prolog();
				}
			}

			setState(1350);
			match(DELETE);
			setState(1351);
			match(FROM);
			setState(1352);
			table_name();
			setState(1357);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,123,_ctx) ) {
			case 1:
				{
				setState(1354);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,122,_ctx) ) {
				case 1:
					{
					setState(1353);
					match(AS);
					}
					break;
				}
				setState(1356);
				tab_alias();
				}
				break;
			}
			setState(1361);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WHERE) {
				{
				setState(1359);
				match(WHERE);
				setState(1360);
				expr();
				}
			}

			setState(1364);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==RETURNING) {
				{
				setState(1363);
				delete_returning_clause();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Delete_returning_clauseContext extends ParserRuleContext {
		public TerminalNode RETURNING() { return getToken(KVQLParser.RETURNING, 0); }
		public Select_listContext select_list() {
			return getRuleContext(Select_listContext.class,0);
		}
		public Delete_returning_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_delete_returning_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterDelete_returning_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitDelete_returning_clause(this);
		}
	}

	public final Delete_returning_clauseContext delete_returning_clause() throws RecognitionException {
		Delete_returning_clauseContext _localctx = new Delete_returning_clauseContext(_ctx, getState());
		enterRule(_localctx, 184, RULE_delete_returning_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1366);
			match(RETURNING);
			setState(1367);
			select_list();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Quantified_type_defContext extends ParserRuleContext {
		public Type_defContext type_def() {
			return getRuleContext(Type_defContext.class,0);
		}
		public TerminalNode STAR() { return getToken(KVQLParser.STAR, 0); }
		public TerminalNode PLUS() { return getToken(KVQLParser.PLUS, 0); }
		public TerminalNode QUESTION_MARK() { return getToken(KVQLParser.QUESTION_MARK, 0); }
		public Quantified_type_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_quantified_type_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterQuantified_type_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitQuantified_type_def(this);
		}
	}

	public final Quantified_type_defContext quantified_type_def() throws RecognitionException {
		Quantified_type_defContext _localctx = new Quantified_type_defContext(_ctx, getState());
		enterRule(_localctx, 186, RULE_quantified_type_def);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1369);
			type_def();
			setState(1371);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (((((_la - 181)) & ~0x3f) == 0 && ((1L << (_la - 181)) & 65545L) != 0)) {
				{
				setState(1370);
				_la = _input.LA(1);
				if ( !(((((_la - 181)) & ~0x3f) == 0 && ((1L << (_la - 181)) & 65545L) != 0)) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Type_defContext extends ParserRuleContext {
		public Type_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_type_def; }
	 
		public Type_defContext() { }
		public void copyFrom(Type_defContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class EnumContext extends Type_defContext {
		public Enum_defContext enum_def() {
			return getRuleContext(Enum_defContext.class,0);
		}
		public EnumContext(Type_defContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterEnum(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitEnum(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AnyAtomicContext extends Type_defContext {
		public AnyAtomic_defContext anyAtomic_def() {
			return getRuleContext(AnyAtomic_defContext.class,0);
		}
		public AnyAtomicContext(Type_defContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAnyAtomic(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAnyAtomic(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AnyJsonAtomicContext extends Type_defContext {
		public AnyJsonAtomic_defContext anyJsonAtomic_def() {
			return getRuleContext(AnyJsonAtomic_defContext.class,0);
		}
		public AnyJsonAtomicContext(Type_defContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAnyJsonAtomic(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAnyJsonAtomic(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AnyRecordContext extends Type_defContext {
		public AnyRecord_defContext anyRecord_def() {
			return getRuleContext(AnyRecord_defContext.class,0);
		}
		public AnyRecordContext(Type_defContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAnyRecord(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAnyRecord(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class JSONContext extends Type_defContext {
		public Json_defContext json_def() {
			return getRuleContext(Json_defContext.class,0);
		}
		public JSONContext(Type_defContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterJSON(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitJSON(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class StringTContext extends Type_defContext {
		public String_defContext string_def() {
			return getRuleContext(String_defContext.class,0);
		}
		public StringTContext(Type_defContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterStringT(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitStringT(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class TimestampContext extends Type_defContext {
		public Timestamp_defContext timestamp_def() {
			return getRuleContext(Timestamp_defContext.class,0);
		}
		public TimestampContext(Type_defContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterTimestamp(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitTimestamp(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class AnyContext extends Type_defContext {
		public Any_defContext any_def() {
			return getRuleContext(Any_defContext.class,0);
		}
		public AnyContext(Type_defContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAny(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAny(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class IntContext extends Type_defContext {
		public Integer_defContext integer_def() {
			return getRuleContext(Integer_defContext.class,0);
		}
		public IntContext(Type_defContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterInt(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitInt(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ArrayContext extends Type_defContext {
		public Array_defContext array_def() {
			return getRuleContext(Array_defContext.class,0);
		}
		public ArrayContext(Type_defContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterArray(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitArray(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class FloatContext extends Type_defContext {
		public Float_defContext float_def() {
			return getRuleContext(Float_defContext.class,0);
		}
		public FloatContext(Type_defContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterFloat(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitFloat(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class RecordContext extends Type_defContext {
		public Record_defContext record_def() {
			return getRuleContext(Record_defContext.class,0);
		}
		public RecordContext(Type_defContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterRecord(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitRecord(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BinaryContext extends Type_defContext {
		public Binary_defContext binary_def() {
			return getRuleContext(Binary_defContext.class,0);
		}
		public BinaryContext(Type_defContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterBinary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitBinary(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class BooleanContext extends Type_defContext {
		public Boolean_defContext boolean_def() {
			return getRuleContext(Boolean_defContext.class,0);
		}
		public BooleanContext(Type_defContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterBoolean(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitBoolean(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class MapContext extends Type_defContext {
		public Map_defContext map_def() {
			return getRuleContext(Map_defContext.class,0);
		}
		public MapContext(Type_defContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterMap(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitMap(this);
		}
	}

	public final Type_defContext type_def() throws RecognitionException {
		Type_defContext _localctx = new Type_defContext(_ctx, getState());
		enterRule(_localctx, 188, RULE_type_def);
		try {
			setState(1388);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case BINARY_T:
				_localctx = new BinaryContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(1373);
				binary_def();
				}
				break;
			case ARRAY_T:
				_localctx = new ArrayContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(1374);
				array_def();
				}
				break;
			case BOOLEAN_T:
				_localctx = new BooleanContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(1375);
				boolean_def();
				}
				break;
			case ENUM_T:
				_localctx = new EnumContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(1376);
				enum_def();
				}
				break;
			case DOUBLE_T:
			case FLOAT_T:
			case NUMBER_T:
				_localctx = new FloatContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(1377);
				float_def();
				}
				break;
			case INTEGER_T:
			case LONG_T:
				_localctx = new IntContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(1378);
				integer_def();
				}
				break;
			case JSON:
				_localctx = new JSONContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(1379);
				json_def();
				}
				break;
			case MAP_T:
				_localctx = new MapContext(_localctx);
				enterOuterAlt(_localctx, 8);
				{
				setState(1380);
				map_def();
				}
				break;
			case RECORD_T:
				_localctx = new RecordContext(_localctx);
				enterOuterAlt(_localctx, 9);
				{
				setState(1381);
				record_def();
				}
				break;
			case STRING_T:
				_localctx = new StringTContext(_localctx);
				enterOuterAlt(_localctx, 10);
				{
				setState(1382);
				string_def();
				}
				break;
			case TIMESTAMP_T:
				_localctx = new TimestampContext(_localctx);
				enterOuterAlt(_localctx, 11);
				{
				setState(1383);
				timestamp_def();
				}
				break;
			case ANY_T:
				_localctx = new AnyContext(_localctx);
				enterOuterAlt(_localctx, 12);
				{
				setState(1384);
				any_def();
				}
				break;
			case ANYATOMIC_T:
				_localctx = new AnyAtomicContext(_localctx);
				enterOuterAlt(_localctx, 13);
				{
				setState(1385);
				anyAtomic_def();
				}
				break;
			case ANYJSONATOMIC_T:
				_localctx = new AnyJsonAtomicContext(_localctx);
				enterOuterAlt(_localctx, 14);
				{
				setState(1386);
				anyJsonAtomic_def();
				}
				break;
			case ANYRECORD_T:
				_localctx = new AnyRecordContext(_localctx);
				enterOuterAlt(_localctx, 15);
				{
				setState(1387);
				anyRecord_def();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Record_defContext extends ParserRuleContext {
		public TerminalNode RECORD_T() { return getToken(KVQLParser.RECORD_T, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public List<Field_defContext> field_def() {
			return getRuleContexts(Field_defContext.class);
		}
		public Field_defContext field_def(int i) {
			return getRuleContext(Field_defContext.class,i);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Record_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_record_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterRecord_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitRecord_def(this);
		}
	}

	public final Record_defContext record_def() throws RecognitionException {
		Record_defContext _localctx = new Record_defContext(_ctx, getState());
		enterRule(_localctx, 190, RULE_record_def);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1390);
			match(RECORD_T);
			setState(1391);
			match(LP);
			setState(1392);
			field_def();
			setState(1397);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1393);
				match(COMMA);
				setState(1394);
				field_def();
				}
				}
				setState(1399);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1400);
			match(RP);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Field_defContext extends ParserRuleContext {
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Type_defContext type_def() {
			return getRuleContext(Type_defContext.class,0);
		}
		public Default_defContext default_def() {
			return getRuleContext(Default_defContext.class,0);
		}
		public CommentContext comment() {
			return getRuleContext(CommentContext.class,0);
		}
		public Field_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_field_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterField_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitField_def(this);
		}
	}

	public final Field_defContext field_def() throws RecognitionException {
		Field_defContext _localctx = new Field_defContext(_ctx, getState());
		enterRule(_localctx, 192, RULE_field_def);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1402);
			id();
			setState(1403);
			type_def();
			setState(1405);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DEFAULT || _la==NOT) {
				{
				setState(1404);
				default_def();
				}
			}

			setState(1408);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMENT) {
				{
				setState(1407);
				comment();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Default_defContext extends ParserRuleContext {
		public Default_valueContext default_value() {
			return getRuleContext(Default_valueContext.class,0);
		}
		public Not_nullContext not_null() {
			return getRuleContext(Not_nullContext.class,0);
		}
		public Default_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_default_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterDefault_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitDefault_def(this);
		}
	}

	public final Default_defContext default_def() throws RecognitionException {
		Default_defContext _localctx = new Default_defContext(_ctx, getState());
		enterRule(_localctx, 194, RULE_default_def);
		int _la;
		try {
			setState(1418);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case DEFAULT:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(1410);
				default_value();
				setState(1412);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==NOT) {
					{
					setState(1411);
					not_null();
					}
				}

				}
				}
				break;
			case NOT:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(1414);
				not_null();
				setState(1416);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==DEFAULT) {
					{
					setState(1415);
					default_value();
					}
				}

				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Default_valueContext extends ParserRuleContext {
		public TerminalNode DEFAULT() { return getToken(KVQLParser.DEFAULT, 0); }
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode TRUE() { return getToken(KVQLParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(KVQLParser.FALSE, 0); }
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Default_valueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_default_value; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterDefault_value(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitDefault_value(this);
		}
	}

	public final Default_valueContext default_value() throws RecognitionException {
		Default_valueContext _localctx = new Default_valueContext(_ctx, getState());
		enterRule(_localctx, 196, RULE_default_value);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1420);
			match(DEFAULT);
			setState(1426);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case MINUS:
			case INT:
			case FLOAT:
			case NUMBER:
				{
				setState(1421);
				number();
				}
				break;
			case DSTRING:
			case STRING:
				{
				setState(1422);
				string();
				}
				break;
			case TRUE:
				{
				setState(1423);
				match(TRUE);
				}
				break;
			case FALSE:
				{
				setState(1424);
				match(FALSE);
				}
				break;
			case ACCOUNT:
			case ADD:
			case ADMIN:
			case ALL:
			case ALTER:
			case ALWAYS:
			case ANCESTORS:
			case AND:
			case AS:
			case ASC:
			case ARRAY_COLLECT:
			case BEFORE:
			case BETWEEN:
			case BY:
			case CACHE:
			case CASE:
			case CAST:
			case COLLECTION:
			case COMMENT:
			case COUNT:
			case CREATE:
			case CYCLE:
			case DAYS:
			case DECLARE:
			case DEFAULT:
			case DELETE:
			case DESC:
			case DESCENDANTS:
			case DESCRIBE:
			case DISABLE:
			case DISTINCT:
			case DROP:
			case ELEMENTOF:
			case ELEMENTS:
			case ELSE:
			case ENABLE:
			case END:
			case ES_SHARDS:
			case ES_REPLICAS:
			case EXISTS:
			case EXTRACT:
			case FIELDS:
			case FIRST:
			case FREEZE:
			case FROM:
			case FROZEN:
			case FULLTEXT:
			case GENERATED:
			case GRANT:
			case GROUP:
			case HOURS:
			case IDENTIFIED:
			case IDENTITY:
			case IF:
			case IMAGE:
			case IN:
			case INCREMENT:
			case INDEX:
			case INDEXES:
			case INSERT:
			case INTO:
			case IS:
			case JSON:
			case KEY:
			case KEYOF:
			case KEYS:
			case LAST:
			case LIFETIME:
			case LIMIT:
			case LOCAL:
			case LOCK:
			case MERGE:
			case MINUTES:
			case MODIFY:
			case MR_COUNTER:
			case NAMESPACES:
			case NESTED:
			case NO:
			case NOT:
			case NULLS:
			case OFFSET:
			case OF:
			case ON:
			case OR:
			case ORDER:
			case OVERRIDE:
			case PASSWORD:
			case PATCH:
			case PER:
			case PRIMARY:
			case PUT:
			case REGION:
			case REGIONS:
			case REMOVE:
			case RETURNING:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROW:
			case SCHEMA:
			case SECONDS:
			case SELECT:
			case SEQ_TRANSFORM:
			case SET:
			case SHARD:
			case SHOW:
			case START:
			case TABLE:
			case TABLES:
			case THEN:
			case TO:
			case TTL:
			case TYPE:
			case UNFREEZE:
			case UNLOCK:
			case UPDATE:
			case UPSERT:
			case USER:
			case USERS:
			case USING:
			case VALUES:
			case WHEN:
			case WHERE:
			case WITH:
			case UNIQUE:
			case UNNEST:
			case ARRAY_T:
			case BINARY_T:
			case BOOLEAN_T:
			case DOUBLE_T:
			case ENUM_T:
			case FLOAT_T:
			case GEOMETRY_T:
			case INTEGER_T:
			case LONG_T:
			case MAP_T:
			case NUMBER_T:
			case POINT_T:
			case RECORD_T:
			case STRING_T:
			case TIMESTAMP_T:
			case ANY_T:
			case ANYATOMIC_T:
			case ANYJSONATOMIC_T:
			case ANYRECORD_T:
			case SCALAR_T:
			case RDIV:
			case ID:
			case BAD_ID:
				{
				setState(1425);
				id();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Not_nullContext extends ParserRuleContext {
		public TerminalNode NOT() { return getToken(KVQLParser.NOT, 0); }
		public TerminalNode NULL() { return getToken(KVQLParser.NULL, 0); }
		public Not_nullContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_not_null; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterNot_null(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitNot_null(this);
		}
	}

	public final Not_nullContext not_null() throws RecognitionException {
		Not_nullContext _localctx = new Not_nullContext(_ctx, getState());
		enterRule(_localctx, 198, RULE_not_null);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1428);
			match(NOT);
			setState(1429);
			match(NULL);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Map_defContext extends ParserRuleContext {
		public TerminalNode MAP_T() { return getToken(KVQLParser.MAP_T, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public Type_defContext type_def() {
			return getRuleContext(Type_defContext.class,0);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Map_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_map_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterMap_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitMap_def(this);
		}
	}

	public final Map_defContext map_def() throws RecognitionException {
		Map_defContext _localctx = new Map_defContext(_ctx, getState());
		enterRule(_localctx, 200, RULE_map_def);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1431);
			match(MAP_T);
			setState(1432);
			match(LP);
			setState(1433);
			type_def();
			setState(1434);
			match(RP);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Array_defContext extends ParserRuleContext {
		public TerminalNode ARRAY_T() { return getToken(KVQLParser.ARRAY_T, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public Type_defContext type_def() {
			return getRuleContext(Type_defContext.class,0);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Array_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_array_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterArray_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitArray_def(this);
		}
	}

	public final Array_defContext array_def() throws RecognitionException {
		Array_defContext _localctx = new Array_defContext(_ctx, getState());
		enterRule(_localctx, 202, RULE_array_def);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1436);
			match(ARRAY_T);
			setState(1437);
			match(LP);
			setState(1438);
			type_def();
			setState(1439);
			match(RP);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Integer_defContext extends ParserRuleContext {
		public TerminalNode INTEGER_T() { return getToken(KVQLParser.INTEGER_T, 0); }
		public TerminalNode LONG_T() { return getToken(KVQLParser.LONG_T, 0); }
		public Integer_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_integer_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterInteger_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitInteger_def(this);
		}
	}

	public final Integer_defContext integer_def() throws RecognitionException {
		Integer_defContext _localctx = new Integer_defContext(_ctx, getState());
		enterRule(_localctx, 204, RULE_integer_def);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1441);
			_la = _input.LA(1);
			if ( !(_la==INTEGER_T || _la==LONG_T) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Json_defContext extends ParserRuleContext {
		public TerminalNode JSON() { return getToken(KVQLParser.JSON, 0); }
		public Json_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_json_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterJson_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitJson_def(this);
		}
	}

	public final Json_defContext json_def() throws RecognitionException {
		Json_defContext _localctx = new Json_defContext(_ctx, getState());
		enterRule(_localctx, 206, RULE_json_def);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1443);
			match(JSON);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Float_defContext extends ParserRuleContext {
		public TerminalNode FLOAT_T() { return getToken(KVQLParser.FLOAT_T, 0); }
		public TerminalNode DOUBLE_T() { return getToken(KVQLParser.DOUBLE_T, 0); }
		public TerminalNode NUMBER_T() { return getToken(KVQLParser.NUMBER_T, 0); }
		public Float_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_float_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterFloat_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitFloat_def(this);
		}
	}

	public final Float_defContext float_def() throws RecognitionException {
		Float_defContext _localctx = new Float_defContext(_ctx, getState());
		enterRule(_localctx, 208, RULE_float_def);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1445);
			_la = _input.LA(1);
			if ( !(((((_la - 155)) & ~0x3f) == 0 && ((1L << (_la - 155)) & 133L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class String_defContext extends ParserRuleContext {
		public TerminalNode STRING_T() { return getToken(KVQLParser.STRING_T, 0); }
		public String_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_string_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterString_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitString_def(this);
		}
	}

	public final String_defContext string_def() throws RecognitionException {
		String_defContext _localctx = new String_defContext(_ctx, getState());
		enterRule(_localctx, 210, RULE_string_def);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1447);
			match(STRING_T);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Enum_defContext extends ParserRuleContext {
		public TerminalNode ENUM_T() { return getToken(KVQLParser.ENUM_T, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public Id_listContext id_list() {
			return getRuleContext(Id_listContext.class,0);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Enum_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_enum_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterEnum_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitEnum_def(this);
		}
	}

	public final Enum_defContext enum_def() throws RecognitionException {
		Enum_defContext _localctx = new Enum_defContext(_ctx, getState());
		enterRule(_localctx, 212, RULE_enum_def);
		try {
			setState(1459);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,135,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(1449);
				match(ENUM_T);
				setState(1450);
				match(LP);
				setState(1451);
				id_list();
				setState(1452);
				match(RP);
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(1454);
				match(ENUM_T);
				setState(1455);
				match(LP);
				setState(1456);
				id_list();
				 notifyErrorListeners("Missing closing ')'"); 
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Boolean_defContext extends ParserRuleContext {
		public TerminalNode BOOLEAN_T() { return getToken(KVQLParser.BOOLEAN_T, 0); }
		public Boolean_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_boolean_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterBoolean_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitBoolean_def(this);
		}
	}

	public final Boolean_defContext boolean_def() throws RecognitionException {
		Boolean_defContext _localctx = new Boolean_defContext(_ctx, getState());
		enterRule(_localctx, 214, RULE_boolean_def);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1461);
			match(BOOLEAN_T);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Binary_defContext extends ParserRuleContext {
		public TerminalNode BINARY_T() { return getToken(KVQLParser.BINARY_T, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public TerminalNode INT() { return getToken(KVQLParser.INT, 0); }
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Binary_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_binary_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterBinary_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitBinary_def(this);
		}
	}

	public final Binary_defContext binary_def() throws RecognitionException {
		Binary_defContext _localctx = new Binary_defContext(_ctx, getState());
		enterRule(_localctx, 216, RULE_binary_def);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1463);
			match(BINARY_T);
			setState(1467);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,136,_ctx) ) {
			case 1:
				{
				setState(1464);
				match(LP);
				setState(1465);
				match(INT);
				setState(1466);
				match(RP);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Timestamp_defContext extends ParserRuleContext {
		public TerminalNode TIMESTAMP_T() { return getToken(KVQLParser.TIMESTAMP_T, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public TerminalNode INT() { return getToken(KVQLParser.INT, 0); }
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Timestamp_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_timestamp_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterTimestamp_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitTimestamp_def(this);
		}
	}

	public final Timestamp_defContext timestamp_def() throws RecognitionException {
		Timestamp_defContext _localctx = new Timestamp_defContext(_ctx, getState());
		enterRule(_localctx, 218, RULE_timestamp_def);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1469);
			match(TIMESTAMP_T);
			setState(1473);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,137,_ctx) ) {
			case 1:
				{
				setState(1470);
				match(LP);
				setState(1471);
				match(INT);
				setState(1472);
				match(RP);
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Any_defContext extends ParserRuleContext {
		public TerminalNode ANY_T() { return getToken(KVQLParser.ANY_T, 0); }
		public Any_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_any_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAny_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAny_def(this);
		}
	}

	public final Any_defContext any_def() throws RecognitionException {
		Any_defContext _localctx = new Any_defContext(_ctx, getState());
		enterRule(_localctx, 220, RULE_any_def);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1475);
			match(ANY_T);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class AnyAtomic_defContext extends ParserRuleContext {
		public TerminalNode ANYATOMIC_T() { return getToken(KVQLParser.ANYATOMIC_T, 0); }
		public AnyAtomic_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_anyAtomic_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAnyAtomic_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAnyAtomic_def(this);
		}
	}

	public final AnyAtomic_defContext anyAtomic_def() throws RecognitionException {
		AnyAtomic_defContext _localctx = new AnyAtomic_defContext(_ctx, getState());
		enterRule(_localctx, 222, RULE_anyAtomic_def);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1477);
			match(ANYATOMIC_T);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class AnyJsonAtomic_defContext extends ParserRuleContext {
		public TerminalNode ANYJSONATOMIC_T() { return getToken(KVQLParser.ANYJSONATOMIC_T, 0); }
		public AnyJsonAtomic_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_anyJsonAtomic_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAnyJsonAtomic_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAnyJsonAtomic_def(this);
		}
	}

	public final AnyJsonAtomic_defContext anyJsonAtomic_def() throws RecognitionException {
		AnyJsonAtomic_defContext _localctx = new AnyJsonAtomic_defContext(_ctx, getState());
		enterRule(_localctx, 224, RULE_anyJsonAtomic_def);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1479);
			match(ANYJSONATOMIC_T);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class AnyRecord_defContext extends ParserRuleContext {
		public TerminalNode ANYRECORD_T() { return getToken(KVQLParser.ANYRECORD_T, 0); }
		public AnyRecord_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_anyRecord_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAnyRecord_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAnyRecord_def(this);
		}
	}

	public final AnyRecord_defContext anyRecord_def() throws RecognitionException {
		AnyRecord_defContext _localctx = new AnyRecord_defContext(_ctx, getState());
		enterRule(_localctx, 226, RULE_anyRecord_def);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1481);
			match(ANYRECORD_T);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Id_pathContext extends ParserRuleContext {
		public List<IdContext> id() {
			return getRuleContexts(IdContext.class);
		}
		public IdContext id(int i) {
			return getRuleContext(IdContext.class,i);
		}
		public List<TerminalNode> DOT() { return getTokens(KVQLParser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(KVQLParser.DOT, i);
		}
		public Id_pathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_id_path; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterId_path(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitId_path(this);
		}
	}

	public final Id_pathContext id_path() throws RecognitionException {
		Id_pathContext _localctx = new Id_pathContext(_ctx, getState());
		enterRule(_localctx, 228, RULE_id_path);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1483);
			id();
			setState(1488);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==DOT) {
				{
				{
				setState(1484);
				match(DOT);
				setState(1485);
				id();
				}
				}
				setState(1490);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Table_id_pathContext extends ParserRuleContext {
		public List<Table_idContext> table_id() {
			return getRuleContexts(Table_idContext.class);
		}
		public Table_idContext table_id(int i) {
			return getRuleContext(Table_idContext.class,i);
		}
		public List<TerminalNode> DOT() { return getTokens(KVQLParser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(KVQLParser.DOT, i);
		}
		public Table_id_pathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table_id_path; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterTable_id_path(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitTable_id_path(this);
		}
	}

	public final Table_id_pathContext table_id_path() throws RecognitionException {
		Table_id_pathContext _localctx = new Table_id_pathContext(_ctx, getState());
		enterRule(_localctx, 230, RULE_table_id_path);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1491);
			table_id();
			setState(1496);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==DOT) {
				{
				{
				setState(1492);
				match(DOT);
				setState(1493);
				table_id();
				}
				}
				setState(1498);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Table_idContext extends ParserRuleContext {
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public TerminalNode SYSDOLAR() { return getToken(KVQLParser.SYSDOLAR, 0); }
		public Table_idContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table_id; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterTable_id(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitTable_id(this);
		}
	}

	public final Table_idContext table_id() throws RecognitionException {
		Table_idContext _localctx = new Table_idContext(_ctx, getState());
		enterRule(_localctx, 232, RULE_table_id);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1500);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SYSDOLAR) {
				{
				setState(1499);
				match(SYSDOLAR);
				}
			}

			setState(1502);
			id();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Name_pathContext extends ParserRuleContext {
		public List<Field_nameContext> field_name() {
			return getRuleContexts(Field_nameContext.class);
		}
		public Field_nameContext field_name(int i) {
			return getRuleContext(Field_nameContext.class,i);
		}
		public List<TerminalNode> DOT() { return getTokens(KVQLParser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(KVQLParser.DOT, i);
		}
		public Name_pathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_name_path; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterName_path(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitName_path(this);
		}
	}

	public final Name_pathContext name_path() throws RecognitionException {
		Name_pathContext _localctx = new Name_pathContext(_ctx, getState());
		enterRule(_localctx, 234, RULE_name_path);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1504);
			field_name();
			setState(1509);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==DOT) {
				{
				{
				setState(1505);
				match(DOT);
				setState(1506);
				field_name();
				}
				}
				setState(1511);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Field_nameContext extends ParserRuleContext {
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public TerminalNode DSTRING() { return getToken(KVQLParser.DSTRING, 0); }
		public Field_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_field_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterField_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitField_name(this);
		}
	}

	public final Field_nameContext field_name() throws RecognitionException {
		Field_nameContext _localctx = new Field_nameContext(_ctx, getState());
		enterRule(_localctx, 236, RULE_field_name);
		try {
			setState(1514);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ACCOUNT:
			case ADD:
			case ADMIN:
			case ALL:
			case ALTER:
			case ALWAYS:
			case ANCESTORS:
			case AND:
			case AS:
			case ASC:
			case ARRAY_COLLECT:
			case BEFORE:
			case BETWEEN:
			case BY:
			case CACHE:
			case CASE:
			case CAST:
			case COLLECTION:
			case COMMENT:
			case COUNT:
			case CREATE:
			case CYCLE:
			case DAYS:
			case DECLARE:
			case DEFAULT:
			case DELETE:
			case DESC:
			case DESCENDANTS:
			case DESCRIBE:
			case DISABLE:
			case DISTINCT:
			case DROP:
			case ELEMENTOF:
			case ELEMENTS:
			case ELSE:
			case ENABLE:
			case END:
			case ES_SHARDS:
			case ES_REPLICAS:
			case EXISTS:
			case EXTRACT:
			case FIELDS:
			case FIRST:
			case FREEZE:
			case FROM:
			case FROZEN:
			case FULLTEXT:
			case GENERATED:
			case GRANT:
			case GROUP:
			case HOURS:
			case IDENTIFIED:
			case IDENTITY:
			case IF:
			case IMAGE:
			case IN:
			case INCREMENT:
			case INDEX:
			case INDEXES:
			case INSERT:
			case INTO:
			case IS:
			case JSON:
			case KEY:
			case KEYOF:
			case KEYS:
			case LAST:
			case LIFETIME:
			case LIMIT:
			case LOCAL:
			case LOCK:
			case MERGE:
			case MINUTES:
			case MODIFY:
			case MR_COUNTER:
			case NAMESPACES:
			case NESTED:
			case NO:
			case NOT:
			case NULLS:
			case OFFSET:
			case OF:
			case ON:
			case OR:
			case ORDER:
			case OVERRIDE:
			case PASSWORD:
			case PATCH:
			case PER:
			case PRIMARY:
			case PUT:
			case REGION:
			case REGIONS:
			case REMOVE:
			case RETURNING:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROW:
			case SCHEMA:
			case SECONDS:
			case SELECT:
			case SEQ_TRANSFORM:
			case SET:
			case SHARD:
			case SHOW:
			case START:
			case TABLE:
			case TABLES:
			case THEN:
			case TO:
			case TTL:
			case TYPE:
			case UNFREEZE:
			case UNLOCK:
			case UPDATE:
			case UPSERT:
			case USER:
			case USERS:
			case USING:
			case VALUES:
			case WHEN:
			case WHERE:
			case WITH:
			case UNIQUE:
			case UNNEST:
			case ARRAY_T:
			case BINARY_T:
			case BOOLEAN_T:
			case DOUBLE_T:
			case ENUM_T:
			case FLOAT_T:
			case GEOMETRY_T:
			case INTEGER_T:
			case LONG_T:
			case MAP_T:
			case NUMBER_T:
			case POINT_T:
			case RECORD_T:
			case STRING_T:
			case TIMESTAMP_T:
			case ANY_T:
			case ANYATOMIC_T:
			case ANYJSONATOMIC_T:
			case ANYRECORD_T:
			case SCALAR_T:
			case RDIV:
			case ID:
			case BAD_ID:
				enterOuterAlt(_localctx, 1);
				{
				setState(1512);
				id();
				}
				break;
			case DSTRING:
				enterOuterAlt(_localctx, 2);
				{
				setState(1513);
				match(DSTRING);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Create_namespace_statementContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(KVQLParser.CREATE, 0); }
		public TerminalNode NAMESPACE() { return getToken(KVQLParser.NAMESPACE, 0); }
		public NamespaceContext namespace() {
			return getRuleContext(NamespaceContext.class,0);
		}
		public TerminalNode IF() { return getToken(KVQLParser.IF, 0); }
		public TerminalNode NOT() { return getToken(KVQLParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(KVQLParser.EXISTS, 0); }
		public Create_namespace_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_namespace_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterCreate_namespace_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitCreate_namespace_statement(this);
		}
	}

	public final Create_namespace_statementContext create_namespace_statement() throws RecognitionException {
		Create_namespace_statementContext _localctx = new Create_namespace_statementContext(_ctx, getState());
		enterRule(_localctx, 238, RULE_create_namespace_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1516);
			match(CREATE);
			setState(1517);
			match(NAMESPACE);
			setState(1521);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,143,_ctx) ) {
			case 1:
				{
				setState(1518);
				match(IF);
				setState(1519);
				match(NOT);
				setState(1520);
				match(EXISTS);
				}
				break;
			}
			setState(1523);
			namespace();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Drop_namespace_statementContext extends ParserRuleContext {
		public TerminalNode DROP() { return getToken(KVQLParser.DROP, 0); }
		public TerminalNode NAMESPACE() { return getToken(KVQLParser.NAMESPACE, 0); }
		public NamespaceContext namespace() {
			return getRuleContext(NamespaceContext.class,0);
		}
		public TerminalNode IF() { return getToken(KVQLParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(KVQLParser.EXISTS, 0); }
		public TerminalNode CASCADE() { return getToken(KVQLParser.CASCADE, 0); }
		public Drop_namespace_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_drop_namespace_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterDrop_namespace_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitDrop_namespace_statement(this);
		}
	}

	public final Drop_namespace_statementContext drop_namespace_statement() throws RecognitionException {
		Drop_namespace_statementContext _localctx = new Drop_namespace_statementContext(_ctx, getState());
		enterRule(_localctx, 240, RULE_drop_namespace_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1525);
			match(DROP);
			setState(1526);
			match(NAMESPACE);
			setState(1529);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,144,_ctx) ) {
			case 1:
				{
				setState(1527);
				match(IF);
				setState(1528);
				match(EXISTS);
				}
				break;
			}
			setState(1531);
			namespace();
			setState(1533);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==CASCADE) {
				{
				setState(1532);
				match(CASCADE);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Region_nameContext extends ParserRuleContext {
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Region_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_region_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterRegion_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitRegion_name(this);
		}
	}

	public final Region_nameContext region_name() throws RecognitionException {
		Region_nameContext _localctx = new Region_nameContext(_ctx, getState());
		enterRule(_localctx, 242, RULE_region_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1535);
			id();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Create_region_statementContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(KVQLParser.CREATE, 0); }
		public TerminalNode REGION() { return getToken(KVQLParser.REGION, 0); }
		public Region_nameContext region_name() {
			return getRuleContext(Region_nameContext.class,0);
		}
		public Create_region_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_region_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterCreate_region_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitCreate_region_statement(this);
		}
	}

	public final Create_region_statementContext create_region_statement() throws RecognitionException {
		Create_region_statementContext _localctx = new Create_region_statementContext(_ctx, getState());
		enterRule(_localctx, 244, RULE_create_region_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1537);
			match(CREATE);
			setState(1538);
			match(REGION);
			setState(1539);
			region_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Drop_region_statementContext extends ParserRuleContext {
		public TerminalNode DROP() { return getToken(KVQLParser.DROP, 0); }
		public TerminalNode REGION() { return getToken(KVQLParser.REGION, 0); }
		public Region_nameContext region_name() {
			return getRuleContext(Region_nameContext.class,0);
		}
		public Drop_region_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_drop_region_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterDrop_region_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitDrop_region_statement(this);
		}
	}

	public final Drop_region_statementContext drop_region_statement() throws RecognitionException {
		Drop_region_statementContext _localctx = new Drop_region_statementContext(_ctx, getState());
		enterRule(_localctx, 246, RULE_drop_region_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1541);
			match(DROP);
			setState(1542);
			match(REGION);
			setState(1543);
			region_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Set_local_region_statementContext extends ParserRuleContext {
		public TerminalNode SET() { return getToken(KVQLParser.SET, 0); }
		public TerminalNode LOCAL() { return getToken(KVQLParser.LOCAL, 0); }
		public TerminalNode REGION() { return getToken(KVQLParser.REGION, 0); }
		public Region_nameContext region_name() {
			return getRuleContext(Region_nameContext.class,0);
		}
		public Set_local_region_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_set_local_region_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterSet_local_region_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitSet_local_region_statement(this);
		}
	}

	public final Set_local_region_statementContext set_local_region_statement() throws RecognitionException {
		Set_local_region_statementContext _localctx = new Set_local_region_statementContext(_ctx, getState());
		enterRule(_localctx, 248, RULE_set_local_region_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1545);
			match(SET);
			setState(1546);
			match(LOCAL);
			setState(1547);
			match(REGION);
			setState(1548);
			region_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Create_table_statementContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(KVQLParser.CREATE, 0); }
		public TerminalNode TABLE() { return getToken(KVQLParser.TABLE, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public Table_defContext table_def() {
			return getRuleContext(Table_defContext.class,0);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public TerminalNode IF() { return getToken(KVQLParser.IF, 0); }
		public TerminalNode NOT() { return getToken(KVQLParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(KVQLParser.EXISTS, 0); }
		public CommentContext comment() {
			return getRuleContext(CommentContext.class,0);
		}
		public Table_optionsContext table_options() {
			return getRuleContext(Table_optionsContext.class,0);
		}
		public Create_table_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_table_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterCreate_table_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitCreate_table_statement(this);
		}
	}

	public final Create_table_statementContext create_table_statement() throws RecognitionException {
		Create_table_statementContext _localctx = new Create_table_statementContext(_ctx, getState());
		enterRule(_localctx, 250, RULE_create_table_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1550);
			match(CREATE);
			setState(1551);
			match(TABLE);
			setState(1555);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,146,_ctx) ) {
			case 1:
				{
				setState(1552);
				match(IF);
				setState(1553);
				match(NOT);
				setState(1554);
				match(EXISTS);
				}
				break;
			}
			setState(1557);
			table_name();
			setState(1559);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMENT) {
				{
				setState(1558);
				comment();
				}
			}

			setState(1561);
			match(LP);
			setState(1562);
			table_def();
			setState(1563);
			match(RP);
			setState(1565);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (((((_la - 14)) & ~0x3f) == 0 && ((1L << (_la - 14)) & 2251800082120705L) != 0) || _la==USING || _la==WITH) {
				{
				setState(1564);
				table_options();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Table_nameContext extends ParserRuleContext {
		public Table_id_pathContext table_id_path() {
			return getRuleContext(Table_id_pathContext.class,0);
		}
		public NamespaceContext namespace() {
			return getRuleContext(NamespaceContext.class,0);
		}
		public TerminalNode COLON() { return getToken(KVQLParser.COLON, 0); }
		public Table_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterTable_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitTable_name(this);
		}
	}

	public final Table_nameContext table_name() throws RecognitionException {
		Table_nameContext _localctx = new Table_nameContext(_ctx, getState());
		enterRule(_localctx, 252, RULE_table_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1570);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,149,_ctx) ) {
			case 1:
				{
				setState(1567);
				namespace();
				setState(1568);
				match(COLON);
				}
				break;
			}
			setState(1572);
			table_id_path();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NamespaceContext extends ParserRuleContext {
		public Id_pathContext id_path() {
			return getRuleContext(Id_pathContext.class,0);
		}
		public NamespaceContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_namespace; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterNamespace(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitNamespace(this);
		}
	}

	public final NamespaceContext namespace() throws RecognitionException {
		NamespaceContext _localctx = new NamespaceContext(_ctx, getState());
		enterRule(_localctx, 254, RULE_namespace);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1574);
			id_path();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Table_defContext extends ParserRuleContext {
		public List<Column_defContext> column_def() {
			return getRuleContexts(Column_defContext.class);
		}
		public Column_defContext column_def(int i) {
			return getRuleContext(Column_defContext.class,i);
		}
		public List<Key_defContext> key_def() {
			return getRuleContexts(Key_defContext.class);
		}
		public Key_defContext key_def(int i) {
			return getRuleContext(Key_defContext.class,i);
		}
		public List<Json_collection_mrcounter_defContext> json_collection_mrcounter_def() {
			return getRuleContexts(Json_collection_mrcounter_defContext.class);
		}
		public Json_collection_mrcounter_defContext json_collection_mrcounter_def(int i) {
			return getRuleContext(Json_collection_mrcounter_defContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Table_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterTable_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitTable_def(this);
		}
	}

	public final Table_defContext table_def() throws RecognitionException {
		Table_defContext _localctx = new Table_defContext(_ctx, getState());
		enterRule(_localctx, 256, RULE_table_def);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1579);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,150,_ctx) ) {
			case 1:
				{
				setState(1576);
				column_def();
				}
				break;
			case 2:
				{
				setState(1577);
				key_def();
				}
				break;
			case 3:
				{
				setState(1578);
				json_collection_mrcounter_def();
				}
				break;
			}
			setState(1589);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1581);
				match(COMMA);
				setState(1585);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,151,_ctx) ) {
				case 1:
					{
					setState(1582);
					column_def();
					}
					break;
				case 2:
					{
					setState(1583);
					key_def();
					}
					break;
				case 3:
					{
					setState(1584);
					json_collection_mrcounter_def();
					}
					break;
				}
				}
				}
				setState(1591);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Column_defContext extends ParserRuleContext {
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Type_defContext type_def() {
			return getRuleContext(Type_defContext.class,0);
		}
		public Default_defContext default_def() {
			return getRuleContext(Default_defContext.class,0);
		}
		public Identity_defContext identity_def() {
			return getRuleContext(Identity_defContext.class,0);
		}
		public Uuid_defContext uuid_def() {
			return getRuleContext(Uuid_defContext.class,0);
		}
		public Mr_counter_defContext mr_counter_def() {
			return getRuleContext(Mr_counter_defContext.class,0);
		}
		public Json_mrcounter_fieldsContext json_mrcounter_fields() {
			return getRuleContext(Json_mrcounter_fieldsContext.class,0);
		}
		public CommentContext comment() {
			return getRuleContext(CommentContext.class,0);
		}
		public Column_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_column_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterColumn_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitColumn_def(this);
		}
	}

	public final Column_defContext column_def() throws RecognitionException {
		Column_defContext _localctx = new Column_defContext(_ctx, getState());
		enterRule(_localctx, 258, RULE_column_def);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1592);
			id();
			setState(1593);
			type_def();
			setState(1599);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,153,_ctx) ) {
			case 1:
				{
				setState(1594);
				default_def();
				}
				break;
			case 2:
				{
				setState(1595);
				identity_def();
				}
				break;
			case 3:
				{
				setState(1596);
				uuid_def();
				}
				break;
			case 4:
				{
				setState(1597);
				mr_counter_def();
				}
				break;
			case 5:
				{
				setState(1598);
				json_mrcounter_fields();
				}
				break;
			}
			setState(1602);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMENT) {
				{
				setState(1601);
				comment();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Json_mrcounter_fieldsContext extends ParserRuleContext {
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public List<Json_mrcounter_defContext> json_mrcounter_def() {
			return getRuleContexts(Json_mrcounter_defContext.class);
		}
		public Json_mrcounter_defContext json_mrcounter_def(int i) {
			return getRuleContext(Json_mrcounter_defContext.class,i);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Json_mrcounter_fieldsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_json_mrcounter_fields; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterJson_mrcounter_fields(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitJson_mrcounter_fields(this);
		}
	}

	public final Json_mrcounter_fieldsContext json_mrcounter_fields() throws RecognitionException {
		Json_mrcounter_fieldsContext _localctx = new Json_mrcounter_fieldsContext(_ctx, getState());
		enterRule(_localctx, 260, RULE_json_mrcounter_fields);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1604);
			match(LP);
			setState(1605);
			json_mrcounter_def();
			setState(1610);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1606);
				match(COMMA);
				setState(1607);
				json_mrcounter_def();
				}
				}
				setState(1612);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1613);
			match(RP);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Json_mrcounter_defContext extends ParserRuleContext {
		public Json_mrcounter_pathContext json_mrcounter_path() {
			return getRuleContext(Json_mrcounter_pathContext.class,0);
		}
		public TerminalNode AS() { return getToken(KVQLParser.AS, 0); }
		public TerminalNode MR_COUNTER() { return getToken(KVQLParser.MR_COUNTER, 0); }
		public TerminalNode INTEGER_T() { return getToken(KVQLParser.INTEGER_T, 0); }
		public TerminalNode LONG_T() { return getToken(KVQLParser.LONG_T, 0); }
		public TerminalNode NUMBER_T() { return getToken(KVQLParser.NUMBER_T, 0); }
		public Json_mrcounter_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_json_mrcounter_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterJson_mrcounter_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitJson_mrcounter_def(this);
		}
	}

	public final Json_mrcounter_defContext json_mrcounter_def() throws RecognitionException {
		Json_mrcounter_defContext _localctx = new Json_mrcounter_defContext(_ctx, getState());
		enterRule(_localctx, 262, RULE_json_mrcounter_def);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1615);
			json_mrcounter_path();
			setState(1616);
			match(AS);
			setState(1617);
			_la = _input.LA(1);
			if ( !(((((_la - 159)) & ~0x3f) == 0 && ((1L << (_la - 159)) & 11L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(1618);
			match(MR_COUNTER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Json_collection_mrcounter_defContext extends ParserRuleContext {
		public Json_mrcounter_defContext json_mrcounter_def() {
			return getRuleContext(Json_mrcounter_defContext.class,0);
		}
		public Json_collection_mrcounter_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_json_collection_mrcounter_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterJson_collection_mrcounter_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitJson_collection_mrcounter_def(this);
		}
	}

	public final Json_collection_mrcounter_defContext json_collection_mrcounter_def() throws RecognitionException {
		Json_collection_mrcounter_defContext _localctx = new Json_collection_mrcounter_defContext(_ctx, getState());
		enterRule(_localctx, 264, RULE_json_collection_mrcounter_def);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1620);
			json_mrcounter_def();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Json_mrcounter_pathContext extends ParserRuleContext {
		public List<IdContext> id() {
			return getRuleContexts(IdContext.class);
		}
		public IdContext id(int i) {
			return getRuleContext(IdContext.class,i);
		}
		public List<StringContext> string() {
			return getRuleContexts(StringContext.class);
		}
		public StringContext string(int i) {
			return getRuleContext(StringContext.class,i);
		}
		public List<TerminalNode> DOT() { return getTokens(KVQLParser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(KVQLParser.DOT, i);
		}
		public Json_mrcounter_pathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_json_mrcounter_path; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterJson_mrcounter_path(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitJson_mrcounter_path(this);
		}
	}

	public final Json_mrcounter_pathContext json_mrcounter_path() throws RecognitionException {
		Json_mrcounter_pathContext _localctx = new Json_mrcounter_pathContext(_ctx, getState());
		enterRule(_localctx, 266, RULE_json_mrcounter_path);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1624);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ACCOUNT:
			case ADD:
			case ADMIN:
			case ALL:
			case ALTER:
			case ALWAYS:
			case ANCESTORS:
			case AND:
			case AS:
			case ASC:
			case ARRAY_COLLECT:
			case BEFORE:
			case BETWEEN:
			case BY:
			case CACHE:
			case CASE:
			case CAST:
			case COLLECTION:
			case COMMENT:
			case COUNT:
			case CREATE:
			case CYCLE:
			case DAYS:
			case DECLARE:
			case DEFAULT:
			case DELETE:
			case DESC:
			case DESCENDANTS:
			case DESCRIBE:
			case DISABLE:
			case DISTINCT:
			case DROP:
			case ELEMENTOF:
			case ELEMENTS:
			case ELSE:
			case ENABLE:
			case END:
			case ES_SHARDS:
			case ES_REPLICAS:
			case EXISTS:
			case EXTRACT:
			case FIELDS:
			case FIRST:
			case FREEZE:
			case FROM:
			case FROZEN:
			case FULLTEXT:
			case GENERATED:
			case GRANT:
			case GROUP:
			case HOURS:
			case IDENTIFIED:
			case IDENTITY:
			case IF:
			case IMAGE:
			case IN:
			case INCREMENT:
			case INDEX:
			case INDEXES:
			case INSERT:
			case INTO:
			case IS:
			case JSON:
			case KEY:
			case KEYOF:
			case KEYS:
			case LAST:
			case LIFETIME:
			case LIMIT:
			case LOCAL:
			case LOCK:
			case MERGE:
			case MINUTES:
			case MODIFY:
			case MR_COUNTER:
			case NAMESPACES:
			case NESTED:
			case NO:
			case NOT:
			case NULLS:
			case OFFSET:
			case OF:
			case ON:
			case OR:
			case ORDER:
			case OVERRIDE:
			case PASSWORD:
			case PATCH:
			case PER:
			case PRIMARY:
			case PUT:
			case REGION:
			case REGIONS:
			case REMOVE:
			case RETURNING:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROW:
			case SCHEMA:
			case SECONDS:
			case SELECT:
			case SEQ_TRANSFORM:
			case SET:
			case SHARD:
			case SHOW:
			case START:
			case TABLE:
			case TABLES:
			case THEN:
			case TO:
			case TTL:
			case TYPE:
			case UNFREEZE:
			case UNLOCK:
			case UPDATE:
			case UPSERT:
			case USER:
			case USERS:
			case USING:
			case VALUES:
			case WHEN:
			case WHERE:
			case WITH:
			case UNIQUE:
			case UNNEST:
			case ARRAY_T:
			case BINARY_T:
			case BOOLEAN_T:
			case DOUBLE_T:
			case ENUM_T:
			case FLOAT_T:
			case GEOMETRY_T:
			case INTEGER_T:
			case LONG_T:
			case MAP_T:
			case NUMBER_T:
			case POINT_T:
			case RECORD_T:
			case STRING_T:
			case TIMESTAMP_T:
			case ANY_T:
			case ANYATOMIC_T:
			case ANYJSONATOMIC_T:
			case ANYRECORD_T:
			case SCALAR_T:
			case RDIV:
			case ID:
			case BAD_ID:
				{
				setState(1622);
				id();
				}
				break;
			case DSTRING:
			case STRING:
				{
				setState(1623);
				string();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(1633);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==DOT) {
				{
				{
				setState(1626);
				match(DOT);
				setState(1629);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case ACCOUNT:
				case ADD:
				case ADMIN:
				case ALL:
				case ALTER:
				case ALWAYS:
				case ANCESTORS:
				case AND:
				case AS:
				case ASC:
				case ARRAY_COLLECT:
				case BEFORE:
				case BETWEEN:
				case BY:
				case CACHE:
				case CASE:
				case CAST:
				case COLLECTION:
				case COMMENT:
				case COUNT:
				case CREATE:
				case CYCLE:
				case DAYS:
				case DECLARE:
				case DEFAULT:
				case DELETE:
				case DESC:
				case DESCENDANTS:
				case DESCRIBE:
				case DISABLE:
				case DISTINCT:
				case DROP:
				case ELEMENTOF:
				case ELEMENTS:
				case ELSE:
				case ENABLE:
				case END:
				case ES_SHARDS:
				case ES_REPLICAS:
				case EXISTS:
				case EXTRACT:
				case FIELDS:
				case FIRST:
				case FREEZE:
				case FROM:
				case FROZEN:
				case FULLTEXT:
				case GENERATED:
				case GRANT:
				case GROUP:
				case HOURS:
				case IDENTIFIED:
				case IDENTITY:
				case IF:
				case IMAGE:
				case IN:
				case INCREMENT:
				case INDEX:
				case INDEXES:
				case INSERT:
				case INTO:
				case IS:
				case JSON:
				case KEY:
				case KEYOF:
				case KEYS:
				case LAST:
				case LIFETIME:
				case LIMIT:
				case LOCAL:
				case LOCK:
				case MERGE:
				case MINUTES:
				case MODIFY:
				case MR_COUNTER:
				case NAMESPACES:
				case NESTED:
				case NO:
				case NOT:
				case NULLS:
				case OFFSET:
				case OF:
				case ON:
				case OR:
				case ORDER:
				case OVERRIDE:
				case PASSWORD:
				case PATCH:
				case PER:
				case PRIMARY:
				case PUT:
				case REGION:
				case REGIONS:
				case REMOVE:
				case RETURNING:
				case REVOKE:
				case ROLE:
				case ROLES:
				case ROW:
				case SCHEMA:
				case SECONDS:
				case SELECT:
				case SEQ_TRANSFORM:
				case SET:
				case SHARD:
				case SHOW:
				case START:
				case TABLE:
				case TABLES:
				case THEN:
				case TO:
				case TTL:
				case TYPE:
				case UNFREEZE:
				case UNLOCK:
				case UPDATE:
				case UPSERT:
				case USER:
				case USERS:
				case USING:
				case VALUES:
				case WHEN:
				case WHERE:
				case WITH:
				case UNIQUE:
				case UNNEST:
				case ARRAY_T:
				case BINARY_T:
				case BOOLEAN_T:
				case DOUBLE_T:
				case ENUM_T:
				case FLOAT_T:
				case GEOMETRY_T:
				case INTEGER_T:
				case LONG_T:
				case MAP_T:
				case NUMBER_T:
				case POINT_T:
				case RECORD_T:
				case STRING_T:
				case TIMESTAMP_T:
				case ANY_T:
				case ANYATOMIC_T:
				case ANYJSONATOMIC_T:
				case ANYRECORD_T:
				case SCALAR_T:
				case RDIV:
				case ID:
				case BAD_ID:
					{
					setState(1627);
					id();
					}
					break;
				case DSTRING:
				case STRING:
					{
					setState(1628);
					string();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				}
				setState(1635);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Key_defContext extends ParserRuleContext {
		public TerminalNode PRIMARY() { return getToken(KVQLParser.PRIMARY, 0); }
		public TerminalNode KEY() { return getToken(KVQLParser.KEY, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Shard_key_defContext shard_key_def() {
			return getRuleContext(Shard_key_defContext.class,0);
		}
		public Id_list_with_sizeContext id_list_with_size() {
			return getRuleContext(Id_list_with_sizeContext.class,0);
		}
		public TerminalNode COMMA() { return getToken(KVQLParser.COMMA, 0); }
		public Key_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_key_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterKey_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitKey_def(this);
		}
	}

	public final Key_defContext key_def() throws RecognitionException {
		Key_defContext _localctx = new Key_defContext(_ctx, getState());
		enterRule(_localctx, 268, RULE_key_def);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1636);
			match(PRIMARY);
			setState(1637);
			match(KEY);
			setState(1638);
			match(LP);
			setState(1643);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,160,_ctx) ) {
			case 1:
				{
				setState(1639);
				shard_key_def();
				setState(1641);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMA) {
					{
					setState(1640);
					match(COMMA);
					}
				}

				}
				break;
			}
			setState(1646);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & -7881299352092736L) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & -13348796645889L) != 0) || ((((_la - 128)) & ~0x3f) == 0 && ((1L << (_la - 128)) & 17592169398271L) != 0) || ((((_la - 200)) & ~0x3f) == 0 && ((1L << (_la - 200)) & 6145L) != 0)) {
				{
				setState(1645);
				id_list_with_size();
				}
			}

			setState(1648);
			match(RP);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Shard_key_defContext extends ParserRuleContext {
		public TerminalNode SHARD() { return getToken(KVQLParser.SHARD, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public Id_list_with_sizeContext id_list_with_size() {
			return getRuleContext(Id_list_with_sizeContext.class,0);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Shard_key_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_shard_key_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterShard_key_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitShard_key_def(this);
		}
	}

	public final Shard_key_defContext shard_key_def() throws RecognitionException {
		Shard_key_defContext _localctx = new Shard_key_defContext(_ctx, getState());
		enterRule(_localctx, 270, RULE_shard_key_def);
		try {
			setState(1659);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case SHARD:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(1650);
				match(SHARD);
				setState(1651);
				match(LP);
				setState(1652);
				id_list_with_size();
				setState(1653);
				match(RP);
				}
				}
				break;
			case LP:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(1655);
				match(LP);
				setState(1656);
				id_list_with_size();
				 notifyErrorListeners("Missing closing ')'"); 
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Id_list_with_sizeContext extends ParserRuleContext {
		public List<Id_with_sizeContext> id_with_size() {
			return getRuleContexts(Id_with_sizeContext.class);
		}
		public Id_with_sizeContext id_with_size(int i) {
			return getRuleContext(Id_with_sizeContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Id_list_with_sizeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_id_list_with_size; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterId_list_with_size(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitId_list_with_size(this);
		}
	}

	public final Id_list_with_sizeContext id_list_with_size() throws RecognitionException {
		Id_list_with_sizeContext _localctx = new Id_list_with_sizeContext(_ctx, getState());
		enterRule(_localctx, 272, RULE_id_list_with_size);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1661);
			id_with_size();
			setState(1666);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,163,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1662);
					match(COMMA);
					setState(1663);
					id_with_size();
					}
					} 
				}
				setState(1668);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,163,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Id_with_sizeContext extends ParserRuleContext {
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Storage_sizeContext storage_size() {
			return getRuleContext(Storage_sizeContext.class,0);
		}
		public Id_with_sizeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_id_with_size; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterId_with_size(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitId_with_size(this);
		}
	}

	public final Id_with_sizeContext id_with_size() throws RecognitionException {
		Id_with_sizeContext _localctx = new Id_with_sizeContext(_ctx, getState());
		enterRule(_localctx, 274, RULE_id_with_size);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1669);
			id();
			setState(1671);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LP) {
				{
				setState(1670);
				storage_size();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Storage_sizeContext extends ParserRuleContext {
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public TerminalNode INT() { return getToken(KVQLParser.INT, 0); }
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Storage_sizeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_storage_size; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterStorage_size(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitStorage_size(this);
		}
	}

	public final Storage_sizeContext storage_size() throws RecognitionException {
		Storage_sizeContext _localctx = new Storage_sizeContext(_ctx, getState());
		enterRule(_localctx, 276, RULE_storage_size);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1673);
			match(LP);
			setState(1674);
			match(INT);
			setState(1675);
			match(RP);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Table_optionsContext extends ParserRuleContext {
		public List<Ttl_defContext> ttl_def() {
			return getRuleContexts(Ttl_defContext.class);
		}
		public Ttl_defContext ttl_def(int i) {
			return getRuleContext(Ttl_defContext.class,i);
		}
		public List<Regions_defContext> regions_def() {
			return getRuleContexts(Regions_defContext.class);
		}
		public Regions_defContext regions_def(int i) {
			return getRuleContext(Regions_defContext.class,i);
		}
		public List<Frozen_defContext> frozen_def() {
			return getRuleContexts(Frozen_defContext.class);
		}
		public Frozen_defContext frozen_def(int i) {
			return getRuleContext(Frozen_defContext.class,i);
		}
		public List<Json_collection_defContext> json_collection_def() {
			return getRuleContexts(Json_collection_defContext.class);
		}
		public Json_collection_defContext json_collection_def(int i) {
			return getRuleContext(Json_collection_defContext.class,i);
		}
		public List<Enable_before_imageContext> enable_before_image() {
			return getRuleContexts(Enable_before_imageContext.class);
		}
		public Enable_before_imageContext enable_before_image(int i) {
			return getRuleContext(Enable_before_imageContext.class,i);
		}
		public Table_optionsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_table_options; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterTable_options(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitTable_options(this);
		}
	}

	public final Table_optionsContext table_options() throws RecognitionException {
		Table_optionsContext _localctx = new Table_optionsContext(_ctx, getState());
		enterRule(_localctx, 278, RULE_table_options);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1682); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				setState(1682);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case USING:
					{
					setState(1677);
					ttl_def();
					}
					break;
				case IN:
					{
					setState(1678);
					regions_def();
					}
					break;
				case WITH:
					{
					setState(1679);
					frozen_def();
					}
					break;
				case AS:
					{
					setState(1680);
					json_collection_def();
					}
					break;
				case ENABLE:
					{
					setState(1681);
					enable_before_image();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				setState(1684); 
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( ((((_la - 14)) & ~0x3f) == 0 && ((1L << (_la - 14)) & 2251800082120705L) != 0) || _la==USING || _la==WITH );
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Ttl_defContext extends ParserRuleContext {
		public TerminalNode USING() { return getToken(KVQLParser.USING, 0); }
		public TerminalNode TTL() { return getToken(KVQLParser.TTL, 0); }
		public DurationContext duration() {
			return getRuleContext(DurationContext.class,0);
		}
		public Ttl_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ttl_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterTtl_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitTtl_def(this);
		}
	}

	public final Ttl_defContext ttl_def() throws RecognitionException {
		Ttl_defContext _localctx = new Ttl_defContext(_ctx, getState());
		enterRule(_localctx, 280, RULE_ttl_def);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1686);
			match(USING);
			setState(1687);
			match(TTL);
			setState(1688);
			duration();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Region_namesContext extends ParserRuleContext {
		public Id_listContext id_list() {
			return getRuleContext(Id_listContext.class,0);
		}
		public Region_namesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_region_names; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterRegion_names(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitRegion_names(this);
		}
	}

	public final Region_namesContext region_names() throws RecognitionException {
		Region_namesContext _localctx = new Region_namesContext(_ctx, getState());
		enterRule(_localctx, 282, RULE_region_names);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1690);
			id_list();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Regions_defContext extends ParserRuleContext {
		public TerminalNode IN() { return getToken(KVQLParser.IN, 0); }
		public TerminalNode REGIONS() { return getToken(KVQLParser.REGIONS, 0); }
		public Region_namesContext region_names() {
			return getRuleContext(Region_namesContext.class,0);
		}
		public Regions_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_regions_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterRegions_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitRegions_def(this);
		}
	}

	public final Regions_defContext regions_def() throws RecognitionException {
		Regions_defContext _localctx = new Regions_defContext(_ctx, getState());
		enterRule(_localctx, 284, RULE_regions_def);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1692);
			match(IN);
			setState(1693);
			match(REGIONS);
			setState(1694);
			region_names();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Frozen_defContext extends ParserRuleContext {
		public TerminalNode WITH() { return getToken(KVQLParser.WITH, 0); }
		public TerminalNode SCHEMA() { return getToken(KVQLParser.SCHEMA, 0); }
		public TerminalNode FROZEN() { return getToken(KVQLParser.FROZEN, 0); }
		public TerminalNode FORCE() { return getToken(KVQLParser.FORCE, 0); }
		public Frozen_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_frozen_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterFrozen_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitFrozen_def(this);
		}
	}

	public final Frozen_defContext frozen_def() throws RecognitionException {
		Frozen_defContext _localctx = new Frozen_defContext(_ctx, getState());
		enterRule(_localctx, 286, RULE_frozen_def);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1696);
			match(WITH);
			setState(1697);
			match(SCHEMA);
			setState(1698);
			match(FROZEN);
			setState(1700);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==FORCE) {
				{
				setState(1699);
				match(FORCE);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Json_collection_defContext extends ParserRuleContext {
		public TerminalNode AS() { return getToken(KVQLParser.AS, 0); }
		public TerminalNode JSON() { return getToken(KVQLParser.JSON, 0); }
		public TerminalNode COLLECTION() { return getToken(KVQLParser.COLLECTION, 0); }
		public Json_collection_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_json_collection_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterJson_collection_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitJson_collection_def(this);
		}
	}

	public final Json_collection_defContext json_collection_def() throws RecognitionException {
		Json_collection_defContext _localctx = new Json_collection_defContext(_ctx, getState());
		enterRule(_localctx, 288, RULE_json_collection_def);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1702);
			match(AS);
			setState(1703);
			match(JSON);
			setState(1704);
			match(COLLECTION);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Enable_before_imageContext extends ParserRuleContext {
		public TerminalNode ENABLE() { return getToken(KVQLParser.ENABLE, 0); }
		public TerminalNode BEFORE() { return getToken(KVQLParser.BEFORE, 0); }
		public TerminalNode IMAGE() { return getToken(KVQLParser.IMAGE, 0); }
		public Before_image_ttlContext before_image_ttl() {
			return getRuleContext(Before_image_ttlContext.class,0);
		}
		public Enable_before_imageContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_enable_before_image; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterEnable_before_image(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitEnable_before_image(this);
		}
	}

	public final Enable_before_imageContext enable_before_image() throws RecognitionException {
		Enable_before_imageContext _localctx = new Enable_before_imageContext(_ctx, getState());
		enterRule(_localctx, 290, RULE_enable_before_image);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1706);
			match(ENABLE);
			setState(1707);
			match(BEFORE);
			setState(1708);
			match(IMAGE);
			setState(1710);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,168,_ctx) ) {
			case 1:
				{
				setState(1709);
				before_image_ttl();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Before_image_ttlContext extends ParserRuleContext {
		public TerminalNode USING() { return getToken(KVQLParser.USING, 0); }
		public TerminalNode TTL() { return getToken(KVQLParser.TTL, 0); }
		public DurationContext duration() {
			return getRuleContext(DurationContext.class,0);
		}
		public Before_image_ttlContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_before_image_ttl; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterBefore_image_ttl(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitBefore_image_ttl(this);
		}
	}

	public final Before_image_ttlContext before_image_ttl() throws RecognitionException {
		Before_image_ttlContext _localctx = new Before_image_ttlContext(_ctx, getState());
		enterRule(_localctx, 292, RULE_before_image_ttl);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1712);
			match(USING);
			setState(1713);
			match(TTL);
			setState(1714);
			duration();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Disable_before_imageContext extends ParserRuleContext {
		public TerminalNode DISABLE() { return getToken(KVQLParser.DISABLE, 0); }
		public TerminalNode BEFORE() { return getToken(KVQLParser.BEFORE, 0); }
		public TerminalNode IMAGE() { return getToken(KVQLParser.IMAGE, 0); }
		public Disable_before_imageContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_disable_before_image; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterDisable_before_image(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitDisable_before_image(this);
		}
	}

	public final Disable_before_imageContext disable_before_image() throws RecognitionException {
		Disable_before_imageContext _localctx = new Disable_before_imageContext(_ctx, getState());
		enterRule(_localctx, 294, RULE_disable_before_image);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1716);
			match(DISABLE);
			setState(1717);
			match(BEFORE);
			setState(1718);
			match(IMAGE);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Identity_defContext extends ParserRuleContext {
		public TerminalNode GENERATED() { return getToken(KVQLParser.GENERATED, 0); }
		public TerminalNode AS() { return getToken(KVQLParser.AS, 0); }
		public TerminalNode IDENTITY() { return getToken(KVQLParser.IDENTITY, 0); }
		public TerminalNode ALWAYS() { return getToken(KVQLParser.ALWAYS, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public TerminalNode BY() { return getToken(KVQLParser.BY, 0); }
		public TerminalNode DEFAULT() { return getToken(KVQLParser.DEFAULT, 0); }
		public List<Sequence_optionsContext> sequence_options() {
			return getRuleContexts(Sequence_optionsContext.class);
		}
		public Sequence_optionsContext sequence_options(int i) {
			return getRuleContext(Sequence_optionsContext.class,i);
		}
		public TerminalNode ON() { return getToken(KVQLParser.ON, 0); }
		public TerminalNode NULL() { return getToken(KVQLParser.NULL, 0); }
		public Identity_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identity_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterIdentity_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitIdentity_def(this);
		}
	}

	public final Identity_defContext identity_def() throws RecognitionException {
		Identity_defContext _localctx = new Identity_defContext(_ctx, getState());
		enterRule(_localctx, 296, RULE_identity_def);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1720);
			match(GENERATED);
			setState(1728);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ALWAYS:
				{
				setState(1721);
				match(ALWAYS);
				}
				break;
			case BY:
				{
				{
				setState(1722);
				match(BY);
				setState(1723);
				match(DEFAULT);
				setState(1726);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==ON) {
					{
					setState(1724);
					match(ON);
					setState(1725);
					match(NULL);
					}
				}

				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(1730);
			match(AS);
			setState(1731);
			match(IDENTITY);
			setState(1740);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LP) {
				{
				setState(1732);
				match(LP);
				setState(1734); 
				_errHandler.sync(this);
				_la = _input.LA(1);
				do {
					{
					{
					setState(1733);
					sequence_options();
					}
					}
					setState(1736); 
					_errHandler.sync(this);
					_la = _input.LA(1);
				} while ( _la==CACHE || _la==CYCLE || ((((_la - 66)) & ~0x3f) == 0 && ((1L << (_la - 66)) & 576460752371712001L) != 0) );
				setState(1738);
				match(RP);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Sequence_optionsContext extends ParserRuleContext {
		public TerminalNode START() { return getToken(KVQLParser.START, 0); }
		public TerminalNode WITH() { return getToken(KVQLParser.WITH, 0); }
		public Signed_intContext signed_int() {
			return getRuleContext(Signed_intContext.class,0);
		}
		public TerminalNode INCREMENT() { return getToken(KVQLParser.INCREMENT, 0); }
		public TerminalNode BY() { return getToken(KVQLParser.BY, 0); }
		public TerminalNode MAXVALUE() { return getToken(KVQLParser.MAXVALUE, 0); }
		public TerminalNode NO() { return getToken(KVQLParser.NO, 0); }
		public TerminalNode MINVALUE() { return getToken(KVQLParser.MINVALUE, 0); }
		public TerminalNode CACHE() { return getToken(KVQLParser.CACHE, 0); }
		public TerminalNode INT() { return getToken(KVQLParser.INT, 0); }
		public TerminalNode CYCLE() { return getToken(KVQLParser.CYCLE, 0); }
		public Sequence_optionsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sequence_options; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterSequence_options(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitSequence_options(this);
		}
	}

	public final Sequence_optionsContext sequence_options() throws RecognitionException {
		Sequence_optionsContext _localctx = new Sequence_optionsContext(_ctx, getState());
		enterRule(_localctx, 298, RULE_sequence_options);
		try {
			setState(1763);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,173,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(1742);
				match(START);
				setState(1743);
				match(WITH);
				setState(1744);
				signed_int();
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(1745);
				match(INCREMENT);
				setState(1746);
				match(BY);
				setState(1747);
				signed_int();
				}
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				{
				setState(1748);
				match(MAXVALUE);
				setState(1749);
				signed_int();
				}
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				{
				setState(1750);
				match(NO);
				setState(1751);
				match(MAXVALUE);
				}
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				{
				setState(1752);
				match(MINVALUE);
				setState(1753);
				signed_int();
				}
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				{
				setState(1754);
				match(NO);
				setState(1755);
				match(MINVALUE);
				}
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				{
				setState(1756);
				match(CACHE);
				setState(1757);
				match(INT);
				}
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				{
				setState(1758);
				match(NO);
				setState(1759);
				match(CACHE);
				}
				}
				break;
			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(1760);
				match(CYCLE);
				}
				break;
			case 10:
				enterOuterAlt(_localctx, 10);
				{
				{
				setState(1761);
				match(NO);
				setState(1762);
				match(CYCLE);
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Mr_counter_defContext extends ParserRuleContext {
		public TerminalNode AS() { return getToken(KVQLParser.AS, 0); }
		public TerminalNode MR_COUNTER() { return getToken(KVQLParser.MR_COUNTER, 0); }
		public Mr_counter_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_mr_counter_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterMr_counter_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitMr_counter_def(this);
		}
	}

	public final Mr_counter_defContext mr_counter_def() throws RecognitionException {
		Mr_counter_defContext _localctx = new Mr_counter_defContext(_ctx, getState());
		enterRule(_localctx, 300, RULE_mr_counter_def);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1765);
			match(AS);
			setState(1766);
			match(MR_COUNTER);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Uuid_defContext extends ParserRuleContext {
		public TerminalNode AS() { return getToken(KVQLParser.AS, 0); }
		public TerminalNode UUID() { return getToken(KVQLParser.UUID, 0); }
		public TerminalNode GENERATED() { return getToken(KVQLParser.GENERATED, 0); }
		public TerminalNode BY() { return getToken(KVQLParser.BY, 0); }
		public TerminalNode DEFAULT() { return getToken(KVQLParser.DEFAULT, 0); }
		public Uuid_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_uuid_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterUuid_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitUuid_def(this);
		}
	}

	public final Uuid_defContext uuid_def() throws RecognitionException {
		Uuid_defContext _localctx = new Uuid_defContext(_ctx, getState());
		enterRule(_localctx, 302, RULE_uuid_def);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1768);
			match(AS);
			setState(1769);
			match(UUID);
			setState(1773);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==GENERATED) {
				{
				setState(1770);
				match(GENERATED);
				setState(1771);
				match(BY);
				setState(1772);
				match(DEFAULT);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Alter_table_statementContext extends ParserRuleContext {
		public TerminalNode ALTER() { return getToken(KVQLParser.ALTER, 0); }
		public TerminalNode TABLE() { return getToken(KVQLParser.TABLE, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public Alter_defContext alter_def() {
			return getRuleContext(Alter_defContext.class,0);
		}
		public Alter_table_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_alter_table_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAlter_table_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAlter_table_statement(this);
		}
	}

	public final Alter_table_statementContext alter_table_statement() throws RecognitionException {
		Alter_table_statementContext _localctx = new Alter_table_statementContext(_ctx, getState());
		enterRule(_localctx, 304, RULE_alter_table_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1775);
			match(ALTER);
			setState(1776);
			match(TABLE);
			setState(1777);
			table_name();
			setState(1778);
			alter_def();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Alter_defContext extends ParserRuleContext {
		public Alter_field_statementsContext alter_field_statements() {
			return getRuleContext(Alter_field_statementsContext.class,0);
		}
		public Ttl_defContext ttl_def() {
			return getRuleContext(Ttl_defContext.class,0);
		}
		public Add_region_defContext add_region_def() {
			return getRuleContext(Add_region_defContext.class,0);
		}
		public Drop_region_defContext drop_region_def() {
			return getRuleContext(Drop_region_defContext.class,0);
		}
		public Freeze_defContext freeze_def() {
			return getRuleContext(Freeze_defContext.class,0);
		}
		public Unfreeze_defContext unfreeze_def() {
			return getRuleContext(Unfreeze_defContext.class,0);
		}
		public Enable_before_imageContext enable_before_image() {
			return getRuleContext(Enable_before_imageContext.class,0);
		}
		public Disable_before_imageContext disable_before_image() {
			return getRuleContext(Disable_before_imageContext.class,0);
		}
		public Alter_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_alter_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAlter_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAlter_def(this);
		}
	}

	public final Alter_defContext alter_def() throws RecognitionException {
		Alter_defContext _localctx = new Alter_defContext(_ctx, getState());
		enterRule(_localctx, 306, RULE_alter_def);
		try {
			setState(1788);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case LP:
				enterOuterAlt(_localctx, 1);
				{
				setState(1780);
				alter_field_statements();
				}
				break;
			case USING:
				enterOuterAlt(_localctx, 2);
				{
				setState(1781);
				ttl_def();
				}
				break;
			case ADD:
				enterOuterAlt(_localctx, 3);
				{
				setState(1782);
				add_region_def();
				}
				break;
			case DROP:
				enterOuterAlt(_localctx, 4);
				{
				setState(1783);
				drop_region_def();
				}
				break;
			case FREEZE:
				enterOuterAlt(_localctx, 5);
				{
				setState(1784);
				freeze_def();
				}
				break;
			case UNFREEZE:
				enterOuterAlt(_localctx, 6);
				{
				setState(1785);
				unfreeze_def();
				}
				break;
			case ENABLE:
				enterOuterAlt(_localctx, 7);
				{
				setState(1786);
				enable_before_image();
				}
				break;
			case DISABLE:
				enterOuterAlt(_localctx, 8);
				{
				setState(1787);
				disable_before_image();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Freeze_defContext extends ParserRuleContext {
		public TerminalNode FREEZE() { return getToken(KVQLParser.FREEZE, 0); }
		public TerminalNode SCHEMA() { return getToken(KVQLParser.SCHEMA, 0); }
		public TerminalNode FORCE() { return getToken(KVQLParser.FORCE, 0); }
		public Freeze_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_freeze_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterFreeze_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitFreeze_def(this);
		}
	}

	public final Freeze_defContext freeze_def() throws RecognitionException {
		Freeze_defContext _localctx = new Freeze_defContext(_ctx, getState());
		enterRule(_localctx, 308, RULE_freeze_def);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1790);
			match(FREEZE);
			setState(1791);
			match(SCHEMA);
			setState(1793);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==FORCE) {
				{
				setState(1792);
				match(FORCE);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Unfreeze_defContext extends ParserRuleContext {
		public TerminalNode UNFREEZE() { return getToken(KVQLParser.UNFREEZE, 0); }
		public TerminalNode SCHEMA() { return getToken(KVQLParser.SCHEMA, 0); }
		public Unfreeze_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_unfreeze_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterUnfreeze_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitUnfreeze_def(this);
		}
	}

	public final Unfreeze_defContext unfreeze_def() throws RecognitionException {
		Unfreeze_defContext _localctx = new Unfreeze_defContext(_ctx, getState());
		enterRule(_localctx, 310, RULE_unfreeze_def);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1795);
			match(UNFREEZE);
			setState(1796);
			match(SCHEMA);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Add_region_defContext extends ParserRuleContext {
		public TerminalNode ADD() { return getToken(KVQLParser.ADD, 0); }
		public TerminalNode REGIONS() { return getToken(KVQLParser.REGIONS, 0); }
		public Region_namesContext region_names() {
			return getRuleContext(Region_namesContext.class,0);
		}
		public Add_region_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_add_region_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAdd_region_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAdd_region_def(this);
		}
	}

	public final Add_region_defContext add_region_def() throws RecognitionException {
		Add_region_defContext _localctx = new Add_region_defContext(_ctx, getState());
		enterRule(_localctx, 312, RULE_add_region_def);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1798);
			match(ADD);
			setState(1799);
			match(REGIONS);
			setState(1800);
			region_names();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Drop_region_defContext extends ParserRuleContext {
		public TerminalNode DROP() { return getToken(KVQLParser.DROP, 0); }
		public TerminalNode REGIONS() { return getToken(KVQLParser.REGIONS, 0); }
		public Region_namesContext region_names() {
			return getRuleContext(Region_namesContext.class,0);
		}
		public Drop_region_defContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_drop_region_def; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterDrop_region_def(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitDrop_region_def(this);
		}
	}

	public final Drop_region_defContext drop_region_def() throws RecognitionException {
		Drop_region_defContext _localctx = new Drop_region_defContext(_ctx, getState());
		enterRule(_localctx, 314, RULE_drop_region_def);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1802);
			match(DROP);
			setState(1803);
			match(REGIONS);
			setState(1804);
			region_names();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Alter_field_statementsContext extends ParserRuleContext {
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public List<Add_field_statementContext> add_field_statement() {
			return getRuleContexts(Add_field_statementContext.class);
		}
		public Add_field_statementContext add_field_statement(int i) {
			return getRuleContext(Add_field_statementContext.class,i);
		}
		public List<Drop_field_statementContext> drop_field_statement() {
			return getRuleContexts(Drop_field_statementContext.class);
		}
		public Drop_field_statementContext drop_field_statement(int i) {
			return getRuleContext(Drop_field_statementContext.class,i);
		}
		public List<Modify_field_statementContext> modify_field_statement() {
			return getRuleContexts(Modify_field_statementContext.class);
		}
		public Modify_field_statementContext modify_field_statement(int i) {
			return getRuleContext(Modify_field_statementContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Alter_field_statementsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_alter_field_statements; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAlter_field_statements(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAlter_field_statements(this);
		}
	}

	public final Alter_field_statementsContext alter_field_statements() throws RecognitionException {
		Alter_field_statementsContext _localctx = new Alter_field_statementsContext(_ctx, getState());
		enterRule(_localctx, 316, RULE_alter_field_statements);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1806);
			match(LP);
			setState(1810);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ADD:
				{
				setState(1807);
				add_field_statement();
				}
				break;
			case DROP:
				{
				setState(1808);
				drop_field_statement();
				}
				break;
			case MODIFY:
				{
				setState(1809);
				modify_field_statement();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(1820);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1812);
				match(COMMA);
				setState(1816);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case ADD:
					{
					setState(1813);
					add_field_statement();
					}
					break;
				case DROP:
					{
					setState(1814);
					drop_field_statement();
					}
					break;
				case MODIFY:
					{
					setState(1815);
					modify_field_statement();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				}
				setState(1822);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1823);
			match(RP);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Add_field_statementContext extends ParserRuleContext {
		public TerminalNode ADD() { return getToken(KVQLParser.ADD, 0); }
		public Schema_pathContext schema_path() {
			return getRuleContext(Schema_pathContext.class,0);
		}
		public Type_defContext type_def() {
			return getRuleContext(Type_defContext.class,0);
		}
		public Default_defContext default_def() {
			return getRuleContext(Default_defContext.class,0);
		}
		public Identity_defContext identity_def() {
			return getRuleContext(Identity_defContext.class,0);
		}
		public Mr_counter_defContext mr_counter_def() {
			return getRuleContext(Mr_counter_defContext.class,0);
		}
		public Uuid_defContext uuid_def() {
			return getRuleContext(Uuid_defContext.class,0);
		}
		public Json_mrcounter_fieldsContext json_mrcounter_fields() {
			return getRuleContext(Json_mrcounter_fieldsContext.class,0);
		}
		public CommentContext comment() {
			return getRuleContext(CommentContext.class,0);
		}
		public Add_field_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_add_field_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAdd_field_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAdd_field_statement(this);
		}
	}

	public final Add_field_statementContext add_field_statement() throws RecognitionException {
		Add_field_statementContext _localctx = new Add_field_statementContext(_ctx, getState());
		enterRule(_localctx, 318, RULE_add_field_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1825);
			match(ADD);
			setState(1826);
			schema_path();
			setState(1827);
			type_def();
			setState(1833);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,180,_ctx) ) {
			case 1:
				{
				setState(1828);
				default_def();
				}
				break;
			case 2:
				{
				setState(1829);
				identity_def();
				}
				break;
			case 3:
				{
				setState(1830);
				mr_counter_def();
				}
				break;
			case 4:
				{
				setState(1831);
				uuid_def();
				}
				break;
			case 5:
				{
				setState(1832);
				json_mrcounter_fields();
				}
				break;
			}
			setState(1836);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMENT) {
				{
				setState(1835);
				comment();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Drop_field_statementContext extends ParserRuleContext {
		public TerminalNode DROP() { return getToken(KVQLParser.DROP, 0); }
		public Schema_pathContext schema_path() {
			return getRuleContext(Schema_pathContext.class,0);
		}
		public Drop_field_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_drop_field_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterDrop_field_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitDrop_field_statement(this);
		}
	}

	public final Drop_field_statementContext drop_field_statement() throws RecognitionException {
		Drop_field_statementContext _localctx = new Drop_field_statementContext(_ctx, getState());
		enterRule(_localctx, 320, RULE_drop_field_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1838);
			match(DROP);
			setState(1839);
			schema_path();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Modify_field_statementContext extends ParserRuleContext {
		public TerminalNode MODIFY() { return getToken(KVQLParser.MODIFY, 0); }
		public Schema_pathContext schema_path() {
			return getRuleContext(Schema_pathContext.class,0);
		}
		public Identity_defContext identity_def() {
			return getRuleContext(Identity_defContext.class,0);
		}
		public Uuid_defContext uuid_def() {
			return getRuleContext(Uuid_defContext.class,0);
		}
		public TerminalNode DROP() { return getToken(KVQLParser.DROP, 0); }
		public TerminalNode IDENTITY() { return getToken(KVQLParser.IDENTITY, 0); }
		public Type_defContext type_def() {
			return getRuleContext(Type_defContext.class,0);
		}
		public Default_defContext default_def() {
			return getRuleContext(Default_defContext.class,0);
		}
		public CommentContext comment() {
			return getRuleContext(CommentContext.class,0);
		}
		public Modify_field_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_modify_field_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterModify_field_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitModify_field_statement(this);
		}
	}

	public final Modify_field_statementContext modify_field_statement() throws RecognitionException {
		Modify_field_statementContext _localctx = new Modify_field_statementContext(_ctx, getState());
		enterRule(_localctx, 322, RULE_modify_field_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1841);
			match(MODIFY);
			setState(1842);
			schema_path();
			setState(1854);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case JSON:
			case ARRAY_T:
			case BINARY_T:
			case BOOLEAN_T:
			case DOUBLE_T:
			case ENUM_T:
			case FLOAT_T:
			case INTEGER_T:
			case LONG_T:
			case MAP_T:
			case NUMBER_T:
			case RECORD_T:
			case STRING_T:
			case TIMESTAMP_T:
			case ANY_T:
			case ANYATOMIC_T:
			case ANYJSONATOMIC_T:
			case ANYRECORD_T:
				{
				{
				setState(1843);
				type_def();
				setState(1845);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==DEFAULT || _la==NOT) {
					{
					setState(1844);
					default_def();
					}
				}

				setState(1848);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==COMMENT) {
					{
					setState(1847);
					comment();
					}
				}

				}
				}
				break;
			case GENERATED:
				{
				setState(1850);
				identity_def();
				}
				break;
			case AS:
				{
				setState(1851);
				uuid_def();
				}
				break;
			case DROP:
				{
				setState(1852);
				match(DROP);
				setState(1853);
				match(IDENTITY);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Schema_pathContext extends ParserRuleContext {
		public Init_schema_path_stepContext init_schema_path_step() {
			return getRuleContext(Init_schema_path_stepContext.class,0);
		}
		public List<TerminalNode> DOT() { return getTokens(KVQLParser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(KVQLParser.DOT, i);
		}
		public List<Schema_path_stepContext> schema_path_step() {
			return getRuleContexts(Schema_path_stepContext.class);
		}
		public Schema_path_stepContext schema_path_step(int i) {
			return getRuleContext(Schema_path_stepContext.class,i);
		}
		public Schema_pathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_schema_path; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterSchema_path(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitSchema_path(this);
		}
	}

	public final Schema_pathContext schema_path() throws RecognitionException {
		Schema_pathContext _localctx = new Schema_pathContext(_ctx, getState());
		enterRule(_localctx, 324, RULE_schema_path);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1856);
			init_schema_path_step();
			setState(1861);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==DOT) {
				{
				{
				setState(1857);
				match(DOT);
				setState(1858);
				schema_path_step();
				}
				}
				setState(1863);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Init_schema_path_stepContext extends ParserRuleContext {
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public List<TerminalNode> LBRACK() { return getTokens(KVQLParser.LBRACK); }
		public TerminalNode LBRACK(int i) {
			return getToken(KVQLParser.LBRACK, i);
		}
		public List<TerminalNode> RBRACK() { return getTokens(KVQLParser.RBRACK); }
		public TerminalNode RBRACK(int i) {
			return getToken(KVQLParser.RBRACK, i);
		}
		public Init_schema_path_stepContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_init_schema_path_step; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterInit_schema_path_step(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitInit_schema_path_step(this);
		}
	}

	public final Init_schema_path_stepContext init_schema_path_step() throws RecognitionException {
		Init_schema_path_stepContext _localctx = new Init_schema_path_stepContext(_ctx, getState());
		enterRule(_localctx, 326, RULE_init_schema_path_step);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1864);
			id();
			setState(1869);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==LBRACK) {
				{
				{
				setState(1865);
				match(LBRACK);
				setState(1866);
				match(RBRACK);
				}
				}
				setState(1871);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Schema_path_stepContext extends ParserRuleContext {
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public List<TerminalNode> LBRACK() { return getTokens(KVQLParser.LBRACK); }
		public TerminalNode LBRACK(int i) {
			return getToken(KVQLParser.LBRACK, i);
		}
		public List<TerminalNode> RBRACK() { return getTokens(KVQLParser.RBRACK); }
		public TerminalNode RBRACK(int i) {
			return getToken(KVQLParser.RBRACK, i);
		}
		public TerminalNode VALUES() { return getToken(KVQLParser.VALUES, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Schema_path_stepContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_schema_path_step; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterSchema_path_step(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitSchema_path_step(this);
		}
	}

	public final Schema_path_stepContext schema_path_step() throws RecognitionException {
		Schema_path_stepContext _localctx = new Schema_path_stepContext(_ctx, getState());
		enterRule(_localctx, 328, RULE_schema_path_step);
		int _la;
		try {
			setState(1883);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,188,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1872);
				id();
				setState(1877);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==LBRACK) {
					{
					{
					setState(1873);
					match(LBRACK);
					setState(1874);
					match(RBRACK);
					}
					}
					setState(1879);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1880);
				match(VALUES);
				setState(1881);
				match(LP);
				setState(1882);
				match(RP);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Drop_table_statementContext extends ParserRuleContext {
		public TerminalNode DROP() { return getToken(KVQLParser.DROP, 0); }
		public TerminalNode TABLE() { return getToken(KVQLParser.TABLE, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode IF() { return getToken(KVQLParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(KVQLParser.EXISTS, 0); }
		public Drop_table_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_drop_table_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterDrop_table_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitDrop_table_statement(this);
		}
	}

	public final Drop_table_statementContext drop_table_statement() throws RecognitionException {
		Drop_table_statementContext _localctx = new Drop_table_statementContext(_ctx, getState());
		enterRule(_localctx, 330, RULE_drop_table_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1885);
			match(DROP);
			setState(1886);
			match(TABLE);
			setState(1889);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,189,_ctx) ) {
			case 1:
				{
				setState(1887);
				match(IF);
				setState(1888);
				match(EXISTS);
				}
				break;
			}
			setState(1891);
			table_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Create_index_statementContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(KVQLParser.CREATE, 0); }
		public TerminalNode INDEX() { return getToken(KVQLParser.INDEX, 0); }
		public Index_nameContext index_name() {
			return getRuleContext(Index_nameContext.class,0);
		}
		public TerminalNode ON() { return getToken(KVQLParser.ON, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode IF() { return getToken(KVQLParser.IF, 0); }
		public TerminalNode NOT() { return getToken(KVQLParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(KVQLParser.EXISTS, 0); }
		public CommentContext comment() {
			return getRuleContext(CommentContext.class,0);
		}
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public Index_field_listContext index_field_list() {
			return getRuleContext(Index_field_listContext.class,0);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public List<TerminalNode> WITH() { return getTokens(KVQLParser.WITH); }
		public TerminalNode WITH(int i) {
			return getToken(KVQLParser.WITH, i);
		}
		public TerminalNode NULLS() { return getToken(KVQLParser.NULLS, 0); }
		public TerminalNode UNIQUE() { return getToken(KVQLParser.UNIQUE, 0); }
		public TerminalNode KEYS() { return getToken(KVQLParser.KEYS, 0); }
		public TerminalNode PER() { return getToken(KVQLParser.PER, 0); }
		public TerminalNode ROW() { return getToken(KVQLParser.ROW, 0); }
		public TerminalNode NO() { return getToken(KVQLParser.NO, 0); }
		public Create_index_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_index_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterCreate_index_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitCreate_index_statement(this);
		}
	}

	public final Create_index_statementContext create_index_statement() throws RecognitionException {
		Create_index_statementContext _localctx = new Create_index_statementContext(_ctx, getState());
		enterRule(_localctx, 332, RULE_create_index_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1893);
			match(CREATE);
			setState(1894);
			match(INDEX);
			setState(1898);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,190,_ctx) ) {
			case 1:
				{
				setState(1895);
				match(IF);
				setState(1896);
				match(NOT);
				setState(1897);
				match(EXISTS);
				}
				break;
			}
			setState(1900);
			index_name();
			setState(1901);
			match(ON);
			setState(1902);
			table_name();
			setState(1924);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,194,_ctx) ) {
			case 1:
				{
				{
				setState(1903);
				match(LP);
				setState(1904);
				index_field_list();
				setState(1905);
				match(RP);
				setState(1911);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,192,_ctx) ) {
				case 1:
					{
					setState(1906);
					match(WITH);
					setState(1908);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==NO) {
						{
						setState(1907);
						match(NO);
						}
					}

					setState(1910);
					match(NULLS);
					}
					break;
				}
				setState(1918);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==WITH) {
					{
					setState(1913);
					match(WITH);
					setState(1914);
					match(UNIQUE);
					setState(1915);
					match(KEYS);
					setState(1916);
					match(PER);
					setState(1917);
					match(ROW);
					}
				}

				}
				}
				break;
			case 2:
				{
				{
				setState(1920);
				match(LP);
				setState(1921);
				index_field_list();
				 notifyErrorListeners("Missing closing ')'"); 
				}
				}
				break;
			}
			setState(1927);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMENT) {
				{
				setState(1926);
				comment();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Index_nameContext extends ParserRuleContext {
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Index_nameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_index_name; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterIndex_name(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitIndex_name(this);
		}
	}

	public final Index_nameContext index_name() throws RecognitionException {
		Index_nameContext _localctx = new Index_nameContext(_ctx, getState());
		enterRule(_localctx, 334, RULE_index_name);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1929);
			id();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Index_field_listContext extends ParserRuleContext {
		public List<Index_fieldContext> index_field() {
			return getRuleContexts(Index_fieldContext.class);
		}
		public Index_fieldContext index_field(int i) {
			return getRuleContext(Index_fieldContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Index_field_listContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_index_field_list; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterIndex_field_list(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitIndex_field_list(this);
		}
	}

	public final Index_field_listContext index_field_list() throws RecognitionException {
		Index_field_listContext _localctx = new Index_field_listContext(_ctx, getState());
		enterRule(_localctx, 336, RULE_index_field_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1931);
			index_field();
			setState(1936);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1932);
				match(COMMA);
				setState(1933);
				index_field();
				}
				}
				setState(1938);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Index_fieldContext extends ParserRuleContext {
		public Index_pathContext index_path() {
			return getRuleContext(Index_pathContext.class,0);
		}
		public Path_typeContext path_type() {
			return getRuleContext(Path_typeContext.class,0);
		}
		public Index_functionContext index_function() {
			return getRuleContext(Index_functionContext.class,0);
		}
		public Index_fieldContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_index_field; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterIndex_field(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitIndex_field(this);
		}
	}

	public final Index_fieldContext index_field() throws RecognitionException {
		Index_fieldContext _localctx = new Index_fieldContext(_ctx, getState());
		enterRule(_localctx, 338, RULE_index_field);
		int _la;
		try {
			setState(1944);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,198,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(1939);
				index_path();
				setState(1941);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==AS) {
					{
					setState(1940);
					path_type();
					}
				}

				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1943);
				index_function();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Index_functionContext extends ParserRuleContext {
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Index_pathContext index_path() {
			return getRuleContext(Index_pathContext.class,0);
		}
		public Path_typeContext path_type() {
			return getRuleContext(Path_typeContext.class,0);
		}
		public Index_function_argsContext index_function_args() {
			return getRuleContext(Index_function_argsContext.class,0);
		}
		public Index_functionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_index_function; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterIndex_function(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitIndex_function(this);
		}
	}

	public final Index_functionContext index_function() throws RecognitionException {
		Index_functionContext _localctx = new Index_functionContext(_ctx, getState());
		enterRule(_localctx, 340, RULE_index_function);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1946);
			id();
			setState(1947);
			match(LP);
			setState(1949);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,199,_ctx) ) {
			case 1:
				{
				setState(1948);
				index_path();
				}
				break;
			}
			setState(1952);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AS) {
				{
				setState(1951);
				path_type();
				}
			}

			setState(1955);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMA) {
				{
				setState(1954);
				index_function_args();
				}
			}

			setState(1957);
			match(RP);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Index_function_argsContext extends ParserRuleContext {
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public List<Const_exprContext> const_expr() {
			return getRuleContexts(Const_exprContext.class);
		}
		public Const_exprContext const_expr(int i) {
			return getRuleContext(Const_exprContext.class,i);
		}
		public Index_function_argsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_index_function_args; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterIndex_function_args(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitIndex_function_args(this);
		}
	}

	public final Index_function_argsContext index_function_args() throws RecognitionException {
		Index_function_argsContext _localctx = new Index_function_argsContext(_ctx, getState());
		enterRule(_localctx, 342, RULE_index_function_args);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1961); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(1959);
				match(COMMA);
				setState(1960);
				const_expr();
				}
				}
				setState(1963); 
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( _la==COMMA );
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Index_pathContext extends ParserRuleContext {
		public Name_pathContext name_path() {
			return getRuleContext(Name_pathContext.class,0);
		}
		public Multikey_path_prefixContext multikey_path_prefix() {
			return getRuleContext(Multikey_path_prefixContext.class,0);
		}
		public Row_metadataContext row_metadata() {
			return getRuleContext(Row_metadataContext.class,0);
		}
		public Multikey_path_suffixContext multikey_path_suffix() {
			return getRuleContext(Multikey_path_suffixContext.class,0);
		}
		public Old_index_pathContext old_index_path() {
			return getRuleContext(Old_index_pathContext.class,0);
		}
		public Index_pathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_index_path; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterIndex_path(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitIndex_path(this);
		}
	}

	public final Index_pathContext index_path() throws RecognitionException {
		Index_pathContext _localctx = new Index_pathContext(_ctx, getState());
		enterRule(_localctx, 344, RULE_index_path);
		int _la;
		try {
			setState(1976);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,206,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(1966);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__3) {
					{
					setState(1965);
					row_metadata();
					}
				}

				setState(1973);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,205,_ctx) ) {
				case 1:
					{
					setState(1968);
					name_path();
					}
					break;
				case 2:
					{
					setState(1969);
					multikey_path_prefix();
					setState(1971);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==DOT) {
						{
						setState(1970);
						multikey_path_suffix();
						}
					}

					}
					break;
				}
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1975);
				old_index_path();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Old_index_pathContext extends ParserRuleContext {
		public TerminalNode ELEMENTOF() { return getToken(KVQLParser.ELEMENTOF, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public Name_pathContext name_path() {
			return getRuleContext(Name_pathContext.class,0);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Multikey_path_suffixContext multikey_path_suffix() {
			return getRuleContext(Multikey_path_suffixContext.class,0);
		}
		public TerminalNode KEYOF() { return getToken(KVQLParser.KEYOF, 0); }
		public TerminalNode KEYS() { return getToken(KVQLParser.KEYS, 0); }
		public Old_index_pathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_old_index_path; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterOld_index_path(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitOld_index_path(this);
		}
	}

	public final Old_index_pathContext old_index_path() throws RecognitionException {
		Old_index_pathContext _localctx = new Old_index_pathContext(_ctx, getState());
		enterRule(_localctx, 346, RULE_old_index_path);
		int _la;
		try {
			setState(1995);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ELEMENTOF:
				enterOuterAlt(_localctx, 1);
				{
				setState(1978);
				match(ELEMENTOF);
				setState(1979);
				match(LP);
				setState(1980);
				name_path();
				setState(1981);
				match(RP);
				setState(1983);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==DOT) {
					{
					setState(1982);
					multikey_path_suffix();
					}
				}

				}
				break;
			case KEYOF:
				enterOuterAlt(_localctx, 2);
				{
				setState(1985);
				match(KEYOF);
				setState(1986);
				match(LP);
				setState(1987);
				name_path();
				setState(1988);
				match(RP);
				}
				break;
			case KEYS:
				enterOuterAlt(_localctx, 3);
				{
				setState(1990);
				match(KEYS);
				setState(1991);
				match(LP);
				setState(1992);
				name_path();
				setState(1993);
				match(RP);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Row_metadataContext extends ParserRuleContext {
		public Row_metadataContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_row_metadata; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterRow_metadata(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitRow_metadata(this);
		}
	}

	public final Row_metadataContext row_metadata() throws RecognitionException {
		Row_metadataContext _localctx = new Row_metadataContext(_ctx, getState());
		enterRule(_localctx, 348, RULE_row_metadata);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1997);
			match(T__3);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Multikey_path_prefixContext extends ParserRuleContext {
		public List<Field_nameContext> field_name() {
			return getRuleContexts(Field_nameContext.class);
		}
		public Field_nameContext field_name(int i) {
			return getRuleContext(Field_nameContext.class,i);
		}
		public List<TerminalNode> LBRACK() { return getTokens(KVQLParser.LBRACK); }
		public TerminalNode LBRACK(int i) {
			return getToken(KVQLParser.LBRACK, i);
		}
		public List<TerminalNode> RBRACK() { return getTokens(KVQLParser.RBRACK); }
		public TerminalNode RBRACK(int i) {
			return getToken(KVQLParser.RBRACK, i);
		}
		public List<TerminalNode> DOT() { return getTokens(KVQLParser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(KVQLParser.DOT, i);
		}
		public List<TerminalNode> VALUES() { return getTokens(KVQLParser.VALUES); }
		public TerminalNode VALUES(int i) {
			return getToken(KVQLParser.VALUES, i);
		}
		public List<TerminalNode> LP() { return getTokens(KVQLParser.LP); }
		public TerminalNode LP(int i) {
			return getToken(KVQLParser.LP, i);
		}
		public List<TerminalNode> RP() { return getTokens(KVQLParser.RP); }
		public TerminalNode RP(int i) {
			return getToken(KVQLParser.RP, i);
		}
		public TerminalNode KEYS() { return getToken(KVQLParser.KEYS, 0); }
		public Multikey_path_prefixContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multikey_path_prefix; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterMultikey_path_prefix(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitMultikey_path_prefix(this);
		}
	}

	public final Multikey_path_prefixContext multikey_path_prefix() throws RecognitionException {
		Multikey_path_prefixContext _localctx = new Multikey_path_prefixContext(_ctx, getState());
		enterRule(_localctx, 350, RULE_multikey_path_prefix);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1999);
			field_name();
			setState(2010);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,210,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					setState(2008);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,209,_ctx) ) {
					case 1:
						{
						{
						setState(2000);
						match(DOT);
						setState(2001);
						field_name();
						}
						}
						break;
					case 2:
						{
						{
						setState(2002);
						match(DOT);
						setState(2003);
						match(VALUES);
						setState(2004);
						match(LP);
						setState(2005);
						match(RP);
						}
						}
						break;
					case 3:
						{
						{
						setState(2006);
						match(LBRACK);
						setState(2007);
						match(RBRACK);
						}
						}
						break;
					}
					} 
				}
				setState(2012);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,210,_ctx);
			}
			setState(2023);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,211,_ctx) ) {
			case 1:
				{
				{
				setState(2013);
				match(LBRACK);
				setState(2014);
				match(RBRACK);
				}
				}
				break;
			case 2:
				{
				{
				setState(2015);
				match(DOT);
				setState(2016);
				match(VALUES);
				setState(2017);
				match(LP);
				setState(2018);
				match(RP);
				}
				}
				break;
			case 3:
				{
				{
				setState(2019);
				match(DOT);
				setState(2020);
				match(KEYS);
				setState(2021);
				match(LP);
				setState(2022);
				match(RP);
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Multikey_path_suffixContext extends ParserRuleContext {
		public TerminalNode DOT() { return getToken(KVQLParser.DOT, 0); }
		public Name_pathContext name_path() {
			return getRuleContext(Name_pathContext.class,0);
		}
		public Multikey_path_suffixContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multikey_path_suffix; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterMultikey_path_suffix(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitMultikey_path_suffix(this);
		}
	}

	public final Multikey_path_suffixContext multikey_path_suffix() throws RecognitionException {
		Multikey_path_suffixContext _localctx = new Multikey_path_suffixContext(_ctx, getState());
		enterRule(_localctx, 352, RULE_multikey_path_suffix);
		try {
			enterOuterAlt(_localctx, 1);
			{
			{
			setState(2025);
			match(DOT);
			setState(2026);
			name_path();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Path_typeContext extends ParserRuleContext {
		public TerminalNode AS() { return getToken(KVQLParser.AS, 0); }
		public TerminalNode INTEGER_T() { return getToken(KVQLParser.INTEGER_T, 0); }
		public TerminalNode LONG_T() { return getToken(KVQLParser.LONG_T, 0); }
		public TerminalNode DOUBLE_T() { return getToken(KVQLParser.DOUBLE_T, 0); }
		public TerminalNode STRING_T() { return getToken(KVQLParser.STRING_T, 0); }
		public TerminalNode BOOLEAN_T() { return getToken(KVQLParser.BOOLEAN_T, 0); }
		public TerminalNode NUMBER_T() { return getToken(KVQLParser.NUMBER_T, 0); }
		public TerminalNode ANYATOMIC_T() { return getToken(KVQLParser.ANYATOMIC_T, 0); }
		public TerminalNode POINT_T() { return getToken(KVQLParser.POINT_T, 0); }
		public TerminalNode GEOMETRY_T() { return getToken(KVQLParser.GEOMETRY_T, 0); }
		public JsobjectContext jsobject() {
			return getRuleContext(JsobjectContext.class,0);
		}
		public Path_typeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_path_type; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterPath_type(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitPath_type(this);
		}
	}

	public final Path_typeContext path_type() throws RecognitionException {
		Path_typeContext _localctx = new Path_typeContext(_ctx, getState());
		enterRule(_localctx, 354, RULE_path_type);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2028);
			match(AS);
			setState(2041);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case INTEGER_T:
				{
				setState(2029);
				match(INTEGER_T);
				}
				break;
			case LONG_T:
				{
				setState(2030);
				match(LONG_T);
				}
				break;
			case DOUBLE_T:
				{
				setState(2031);
				match(DOUBLE_T);
				}
				break;
			case STRING_T:
				{
				setState(2032);
				match(STRING_T);
				}
				break;
			case BOOLEAN_T:
				{
				setState(2033);
				match(BOOLEAN_T);
				}
				break;
			case NUMBER_T:
				{
				setState(2034);
				match(NUMBER_T);
				}
				break;
			case ANYATOMIC_T:
				{
				setState(2035);
				match(ANYATOMIC_T);
				}
				break;
			case GEOMETRY_T:
				{
				{
				setState(2036);
				match(GEOMETRY_T);
				setState(2038);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==LBRACE) {
					{
					setState(2037);
					jsobject();
					}
				}

				}
				}
				break;
			case POINT_T:
				{
				setState(2040);
				match(POINT_T);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Create_text_index_statementContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(KVQLParser.CREATE, 0); }
		public TerminalNode FULLTEXT() { return getToken(KVQLParser.FULLTEXT, 0); }
		public TerminalNode INDEX() { return getToken(KVQLParser.INDEX, 0); }
		public Index_nameContext index_name() {
			return getRuleContext(Index_nameContext.class,0);
		}
		public TerminalNode ON() { return getToken(KVQLParser.ON, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public Fts_field_listContext fts_field_list() {
			return getRuleContext(Fts_field_listContext.class,0);
		}
		public TerminalNode IF() { return getToken(KVQLParser.IF, 0); }
		public TerminalNode NOT() { return getToken(KVQLParser.NOT, 0); }
		public TerminalNode EXISTS() { return getToken(KVQLParser.EXISTS, 0); }
		public Es_propertiesContext es_properties() {
			return getRuleContext(Es_propertiesContext.class,0);
		}
		public TerminalNode OVERRIDE() { return getToken(KVQLParser.OVERRIDE, 0); }
		public CommentContext comment() {
			return getRuleContext(CommentContext.class,0);
		}
		public Create_text_index_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_text_index_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterCreate_text_index_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitCreate_text_index_statement(this);
		}
	}

	public final Create_text_index_statementContext create_text_index_statement() throws RecognitionException {
		Create_text_index_statementContext _localctx = new Create_text_index_statementContext(_ctx, getState());
		enterRule(_localctx, 356, RULE_create_text_index_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2043);
			match(CREATE);
			setState(2044);
			match(FULLTEXT);
			setState(2045);
			match(INDEX);
			setState(2049);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,214,_ctx) ) {
			case 1:
				{
				setState(2046);
				match(IF);
				setState(2047);
				match(NOT);
				setState(2048);
				match(EXISTS);
				}
				break;
			}
			setState(2051);
			index_name();
			setState(2052);
			match(ON);
			setState(2053);
			table_name();
			setState(2054);
			fts_field_list();
			setState(2056);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ES_SHARDS || _la==ES_REPLICAS) {
				{
				setState(2055);
				es_properties();
				}
			}

			setState(2059);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==OVERRIDE) {
				{
				setState(2058);
				match(OVERRIDE);
				}
			}

			setState(2062);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==COMMENT) {
				{
				setState(2061);
				comment();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Fts_field_listContext extends ParserRuleContext {
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public Fts_path_listContext fts_path_list() {
			return getRuleContext(Fts_path_listContext.class,0);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Fts_field_listContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fts_field_list; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterFts_field_list(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitFts_field_list(this);
		}
	}

	public final Fts_field_listContext fts_field_list() throws RecognitionException {
		Fts_field_listContext _localctx = new Fts_field_listContext(_ctx, getState());
		enterRule(_localctx, 358, RULE_fts_field_list);
		try {
			setState(2072);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,218,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(2064);
				match(LP);
				setState(2065);
				fts_path_list();
				setState(2066);
				match(RP);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(2068);
				match(LP);
				setState(2069);
				fts_path_list();
				notifyErrorListeners("Missing closing ')'");
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Fts_path_listContext extends ParserRuleContext {
		public List<Fts_pathContext> fts_path() {
			return getRuleContexts(Fts_pathContext.class);
		}
		public Fts_pathContext fts_path(int i) {
			return getRuleContext(Fts_pathContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Fts_path_listContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fts_path_list; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterFts_path_list(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitFts_path_list(this);
		}
	}

	public final Fts_path_listContext fts_path_list() throws RecognitionException {
		Fts_path_listContext _localctx = new Fts_path_listContext(_ctx, getState());
		enterRule(_localctx, 360, RULE_fts_path_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2074);
			fts_path();
			setState(2079);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(2075);
				match(COMMA);
				setState(2076);
				fts_path();
				}
				}
				setState(2081);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Fts_pathContext extends ParserRuleContext {
		public Index_pathContext index_path() {
			return getRuleContext(Index_pathContext.class,0);
		}
		public JsobjectContext jsobject() {
			return getRuleContext(JsobjectContext.class,0);
		}
		public Fts_pathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fts_path; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterFts_path(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitFts_path(this);
		}
	}

	public final Fts_pathContext fts_path() throws RecognitionException {
		Fts_pathContext _localctx = new Fts_pathContext(_ctx, getState());
		enterRule(_localctx, 362, RULE_fts_path);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2082);
			index_path();
			setState(2084);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==LBRACE) {
				{
				setState(2083);
				jsobject();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Es_propertiesContext extends ParserRuleContext {
		public List<Es_property_assignmentContext> es_property_assignment() {
			return getRuleContexts(Es_property_assignmentContext.class);
		}
		public Es_property_assignmentContext es_property_assignment(int i) {
			return getRuleContext(Es_property_assignmentContext.class,i);
		}
		public Es_propertiesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_es_properties; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterEs_properties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitEs_properties(this);
		}
	}

	public final Es_propertiesContext es_properties() throws RecognitionException {
		Es_propertiesContext _localctx = new Es_propertiesContext(_ctx, getState());
		enterRule(_localctx, 364, RULE_es_properties);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2086);
			es_property_assignment();
			setState(2090);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==ES_SHARDS || _la==ES_REPLICAS) {
				{
				{
				setState(2087);
				es_property_assignment();
				}
				}
				setState(2092);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Es_property_assignmentContext extends ParserRuleContext {
		public TerminalNode ES_SHARDS() { return getToken(KVQLParser.ES_SHARDS, 0); }
		public TerminalNode EQ() { return getToken(KVQLParser.EQ, 0); }
		public TerminalNode INT() { return getToken(KVQLParser.INT, 0); }
		public TerminalNode ES_REPLICAS() { return getToken(KVQLParser.ES_REPLICAS, 0); }
		public Es_property_assignmentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_es_property_assignment; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterEs_property_assignment(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitEs_property_assignment(this);
		}
	}

	public final Es_property_assignmentContext es_property_assignment() throws RecognitionException {
		Es_property_assignmentContext _localctx = new Es_property_assignmentContext(_ctx, getState());
		enterRule(_localctx, 366, RULE_es_property_assignment);
		try {
			setState(2099);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ES_SHARDS:
				enterOuterAlt(_localctx, 1);
				{
				setState(2093);
				match(ES_SHARDS);
				setState(2094);
				match(EQ);
				setState(2095);
				match(INT);
				}
				break;
			case ES_REPLICAS:
				enterOuterAlt(_localctx, 2);
				{
				setState(2096);
				match(ES_REPLICAS);
				setState(2097);
				match(EQ);
				setState(2098);
				match(INT);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Drop_index_statementContext extends ParserRuleContext {
		public TerminalNode DROP() { return getToken(KVQLParser.DROP, 0); }
		public TerminalNode INDEX() { return getToken(KVQLParser.INDEX, 0); }
		public Index_nameContext index_name() {
			return getRuleContext(Index_nameContext.class,0);
		}
		public TerminalNode ON() { return getToken(KVQLParser.ON, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode IF() { return getToken(KVQLParser.IF, 0); }
		public TerminalNode EXISTS() { return getToken(KVQLParser.EXISTS, 0); }
		public TerminalNode OVERRIDE() { return getToken(KVQLParser.OVERRIDE, 0); }
		public Drop_index_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_drop_index_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterDrop_index_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitDrop_index_statement(this);
		}
	}

	public final Drop_index_statementContext drop_index_statement() throws RecognitionException {
		Drop_index_statementContext _localctx = new Drop_index_statementContext(_ctx, getState());
		enterRule(_localctx, 368, RULE_drop_index_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2101);
			match(DROP);
			setState(2102);
			match(INDEX);
			setState(2105);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,223,_ctx) ) {
			case 1:
				{
				setState(2103);
				match(IF);
				setState(2104);
				match(EXISTS);
				}
				break;
			}
			setState(2107);
			index_name();
			setState(2108);
			match(ON);
			setState(2109);
			table_name();
			setState(2111);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==OVERRIDE) {
				{
				setState(2110);
				match(OVERRIDE);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Describe_statementContext extends ParserRuleContext {
		public TerminalNode DESCRIBE() { return getToken(KVQLParser.DESCRIBE, 0); }
		public TerminalNode DESC() { return getToken(KVQLParser.DESC, 0); }
		public TerminalNode TABLE() { return getToken(KVQLParser.TABLE, 0); }
		public TerminalNode INDEX() { return getToken(KVQLParser.INDEX, 0); }
		public Index_nameContext index_name() {
			return getRuleContext(Index_nameContext.class,0);
		}
		public TerminalNode ON() { return getToken(KVQLParser.ON, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode AS() { return getToken(KVQLParser.AS, 0); }
		public TerminalNode JSON() { return getToken(KVQLParser.JSON, 0); }
		public TerminalNode LP() { return getToken(KVQLParser.LP, 0); }
		public Schema_path_listContext schema_path_list() {
			return getRuleContext(Schema_path_listContext.class,0);
		}
		public TerminalNode RP() { return getToken(KVQLParser.RP, 0); }
		public Describe_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_describe_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterDescribe_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitDescribe_statement(this);
		}
	}

	public final Describe_statementContext describe_statement() throws RecognitionException {
		Describe_statementContext _localctx = new Describe_statementContext(_ctx, getState());
		enterRule(_localctx, 370, RULE_describe_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2113);
			_la = _input.LA(1);
			if ( !(_la==DESC || _la==DESCRIBE) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			setState(2116);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AS) {
				{
				setState(2114);
				match(AS);
				setState(2115);
				match(JSON);
				}
			}

			setState(2135);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case TABLE:
				{
				setState(2118);
				match(TABLE);
				{
				setState(2119);
				table_name();
				}
				setState(2128);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,226,_ctx) ) {
				case 1:
					{
					{
					setState(2120);
					match(LP);
					setState(2121);
					schema_path_list();
					setState(2122);
					match(RP);
					}
					}
					break;
				case 2:
					{
					{
					setState(2124);
					match(LP);
					setState(2125);
					schema_path_list();
					 notifyErrorListeners("Missing closing ')'")
					             ; 
					}
					}
					break;
				}
				}
				break;
			case INDEX:
				{
				setState(2130);
				match(INDEX);
				setState(2131);
				index_name();
				setState(2132);
				match(ON);
				setState(2133);
				table_name();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Schema_path_listContext extends ParserRuleContext {
		public List<Schema_pathContext> schema_path() {
			return getRuleContexts(Schema_pathContext.class);
		}
		public Schema_pathContext schema_path(int i) {
			return getRuleContext(Schema_pathContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Schema_path_listContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_schema_path_list; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterSchema_path_list(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitSchema_path_list(this);
		}
	}

	public final Schema_path_listContext schema_path_list() throws RecognitionException {
		Schema_path_listContext _localctx = new Schema_path_listContext(_ctx, getState());
		enterRule(_localctx, 372, RULE_schema_path_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2137);
			schema_path();
			setState(2142);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(2138);
				match(COMMA);
				setState(2139);
				schema_path();
				}
				}
				setState(2144);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Show_statementContext extends ParserRuleContext {
		public TerminalNode SHOW() { return getToken(KVQLParser.SHOW, 0); }
		public TerminalNode TABLES() { return getToken(KVQLParser.TABLES, 0); }
		public TerminalNode USERS() { return getToken(KVQLParser.USERS, 0); }
		public TerminalNode ROLES() { return getToken(KVQLParser.ROLES, 0); }
		public TerminalNode USER() { return getToken(KVQLParser.USER, 0); }
		public Identifier_or_stringContext identifier_or_string() {
			return getRuleContext(Identifier_or_stringContext.class,0);
		}
		public TerminalNode ROLE() { return getToken(KVQLParser.ROLE, 0); }
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public TerminalNode INDEXES() { return getToken(KVQLParser.INDEXES, 0); }
		public TerminalNode ON() { return getToken(KVQLParser.ON, 0); }
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public TerminalNode TABLE() { return getToken(KVQLParser.TABLE, 0); }
		public TerminalNode NAMESPACES() { return getToken(KVQLParser.NAMESPACES, 0); }
		public TerminalNode REGIONS() { return getToken(KVQLParser.REGIONS, 0); }
		public TerminalNode AS() { return getToken(KVQLParser.AS, 0); }
		public TerminalNode JSON() { return getToken(KVQLParser.JSON, 0); }
		public Show_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_show_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterShow_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitShow_statement(this);
		}
	}

	public final Show_statementContext show_statement() throws RecognitionException {
		Show_statementContext _localctx = new Show_statementContext(_ctx, getState());
		enterRule(_localctx, 374, RULE_show_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2145);
			match(SHOW);
			setState(2148);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==AS) {
				{
				setState(2146);
				match(AS);
				setState(2147);
				match(JSON);
				}
			}

			setState(2164);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case TABLES:
				{
				setState(2150);
				match(TABLES);
				}
				break;
			case USERS:
				{
				setState(2151);
				match(USERS);
				}
				break;
			case ROLES:
				{
				setState(2152);
				match(ROLES);
				}
				break;
			case USER:
				{
				setState(2153);
				match(USER);
				setState(2154);
				identifier_or_string();
				}
				break;
			case ROLE:
				{
				setState(2155);
				match(ROLE);
				setState(2156);
				id();
				}
				break;
			case INDEXES:
				{
				setState(2157);
				match(INDEXES);
				setState(2158);
				match(ON);
				setState(2159);
				table_name();
				}
				break;
			case TABLE:
				{
				setState(2160);
				match(TABLE);
				setState(2161);
				table_name();
				}
				break;
			case NAMESPACES:
				{
				setState(2162);
				match(NAMESPACES);
				}
				break;
			case REGIONS:
				{
				setState(2163);
				match(REGIONS);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Create_user_statementContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(KVQLParser.CREATE, 0); }
		public TerminalNode USER() { return getToken(KVQLParser.USER, 0); }
		public Create_user_identified_clauseContext create_user_identified_clause() {
			return getRuleContext(Create_user_identified_clauseContext.class,0);
		}
		public Account_lockContext account_lock() {
			return getRuleContext(Account_lockContext.class,0);
		}
		public TerminalNode ADMIN() { return getToken(KVQLParser.ADMIN, 0); }
		public Create_user_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_user_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterCreate_user_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitCreate_user_statement(this);
		}
	}

	public final Create_user_statementContext create_user_statement() throws RecognitionException {
		Create_user_statementContext _localctx = new Create_user_statementContext(_ctx, getState());
		enterRule(_localctx, 376, RULE_create_user_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2166);
			match(CREATE);
			setState(2167);
			match(USER);
			setState(2168);
			create_user_identified_clause();
			setState(2170);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ACCOUNT) {
				{
				setState(2169);
				account_lock();
				}
			}

			setState(2173);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ADMIN) {
				{
				setState(2172);
				match(ADMIN);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Create_role_statementContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(KVQLParser.CREATE, 0); }
		public TerminalNode ROLE() { return getToken(KVQLParser.ROLE, 0); }
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Create_role_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_role_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterCreate_role_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitCreate_role_statement(this);
		}
	}

	public final Create_role_statementContext create_role_statement() throws RecognitionException {
		Create_role_statementContext _localctx = new Create_role_statementContext(_ctx, getState());
		enterRule(_localctx, 378, RULE_create_role_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2175);
			match(CREATE);
			setState(2176);
			match(ROLE);
			setState(2177);
			id();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Alter_user_statementContext extends ParserRuleContext {
		public TerminalNode ALTER() { return getToken(KVQLParser.ALTER, 0); }
		public TerminalNode USER() { return getToken(KVQLParser.USER, 0); }
		public Identifier_or_stringContext identifier_or_string() {
			return getRuleContext(Identifier_or_stringContext.class,0);
		}
		public Reset_password_clauseContext reset_password_clause() {
			return getRuleContext(Reset_password_clauseContext.class,0);
		}
		public TerminalNode CLEAR_RETAINED_PASSWORD() { return getToken(KVQLParser.CLEAR_RETAINED_PASSWORD, 0); }
		public TerminalNode PASSWORD_EXPIRE() { return getToken(KVQLParser.PASSWORD_EXPIRE, 0); }
		public Password_lifetimeContext password_lifetime() {
			return getRuleContext(Password_lifetimeContext.class,0);
		}
		public Account_lockContext account_lock() {
			return getRuleContext(Account_lockContext.class,0);
		}
		public Alter_user_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_alter_user_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAlter_user_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAlter_user_statement(this);
		}
	}

	public final Alter_user_statementContext alter_user_statement() throws RecognitionException {
		Alter_user_statementContext _localctx = new Alter_user_statementContext(_ctx, getState());
		enterRule(_localctx, 380, RULE_alter_user_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2179);
			match(ALTER);
			setState(2180);
			match(USER);
			setState(2181);
			identifier_or_string();
			setState(2183);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==IDENTIFIED) {
				{
				setState(2182);
				reset_password_clause();
				}
			}

			setState(2186);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==CLEAR_RETAINED_PASSWORD) {
				{
				setState(2185);
				match(CLEAR_RETAINED_PASSWORD);
				}
			}

			setState(2189);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==PASSWORD_EXPIRE) {
				{
				setState(2188);
				match(PASSWORD_EXPIRE);
				}
			}

			setState(2192);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==PASSWORD) {
				{
				setState(2191);
				password_lifetime();
				}
			}

			setState(2195);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==ACCOUNT) {
				{
				setState(2194);
				account_lock();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Drop_user_statementContext extends ParserRuleContext {
		public TerminalNode DROP() { return getToken(KVQLParser.DROP, 0); }
		public TerminalNode USER() { return getToken(KVQLParser.USER, 0); }
		public Identifier_or_stringContext identifier_or_string() {
			return getRuleContext(Identifier_or_stringContext.class,0);
		}
		public TerminalNode CASCADE() { return getToken(KVQLParser.CASCADE, 0); }
		public Drop_user_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_drop_user_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterDrop_user_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitDrop_user_statement(this);
		}
	}

	public final Drop_user_statementContext drop_user_statement() throws RecognitionException {
		Drop_user_statementContext _localctx = new Drop_user_statementContext(_ctx, getState());
		enterRule(_localctx, 382, RULE_drop_user_statement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2197);
			match(DROP);
			setState(2198);
			match(USER);
			setState(2199);
			identifier_or_string();
			setState(2201);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==CASCADE) {
				{
				setState(2200);
				match(CASCADE);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Drop_role_statementContext extends ParserRuleContext {
		public TerminalNode DROP() { return getToken(KVQLParser.DROP, 0); }
		public TerminalNode ROLE() { return getToken(KVQLParser.ROLE, 0); }
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Drop_role_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_drop_role_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterDrop_role_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitDrop_role_statement(this);
		}
	}

	public final Drop_role_statementContext drop_role_statement() throws RecognitionException {
		Drop_role_statementContext _localctx = new Drop_role_statementContext(_ctx, getState());
		enterRule(_localctx, 384, RULE_drop_role_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2203);
			match(DROP);
			setState(2204);
			match(ROLE);
			setState(2205);
			id();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Grant_statementContext extends ParserRuleContext {
		public TerminalNode GRANT() { return getToken(KVQLParser.GRANT, 0); }
		public Grant_rolesContext grant_roles() {
			return getRuleContext(Grant_rolesContext.class,0);
		}
		public Grant_system_privilegesContext grant_system_privileges() {
			return getRuleContext(Grant_system_privilegesContext.class,0);
		}
		public Grant_object_privilegesContext grant_object_privileges() {
			return getRuleContext(Grant_object_privilegesContext.class,0);
		}
		public Grant_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_grant_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterGrant_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitGrant_statement(this);
		}
	}

	public final Grant_statementContext grant_statement() throws RecognitionException {
		Grant_statementContext _localctx = new Grant_statementContext(_ctx, getState());
		enterRule(_localctx, 386, RULE_grant_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2207);
			match(GRANT);
			setState(2211);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,239,_ctx) ) {
			case 1:
				{
				setState(2208);
				grant_roles();
				}
				break;
			case 2:
				{
				setState(2209);
				grant_system_privileges();
				}
				break;
			case 3:
				{
				setState(2210);
				grant_object_privileges();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Revoke_statementContext extends ParserRuleContext {
		public TerminalNode REVOKE() { return getToken(KVQLParser.REVOKE, 0); }
		public Revoke_rolesContext revoke_roles() {
			return getRuleContext(Revoke_rolesContext.class,0);
		}
		public Revoke_system_privilegesContext revoke_system_privileges() {
			return getRuleContext(Revoke_system_privilegesContext.class,0);
		}
		public Revoke_object_privilegesContext revoke_object_privileges() {
			return getRuleContext(Revoke_object_privilegesContext.class,0);
		}
		public Revoke_statementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_revoke_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterRevoke_statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitRevoke_statement(this);
		}
	}

	public final Revoke_statementContext revoke_statement() throws RecognitionException {
		Revoke_statementContext _localctx = new Revoke_statementContext(_ctx, getState());
		enterRule(_localctx, 388, RULE_revoke_statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2213);
			match(REVOKE);
			setState(2217);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,240,_ctx) ) {
			case 1:
				{
				setState(2214);
				revoke_roles();
				}
				break;
			case 2:
				{
				setState(2215);
				revoke_system_privileges();
				}
				break;
			case 3:
				{
				setState(2216);
				revoke_object_privileges();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Identifier_or_stringContext extends ParserRuleContext {
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public Identifier_or_stringContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifier_or_string; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterIdentifier_or_string(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitIdentifier_or_string(this);
		}
	}

	public final Identifier_or_stringContext identifier_or_string() throws RecognitionException {
		Identifier_or_stringContext _localctx = new Identifier_or_stringContext(_ctx, getState());
		enterRule(_localctx, 390, RULE_identifier_or_string);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2221);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ACCOUNT:
			case ADD:
			case ADMIN:
			case ALL:
			case ALTER:
			case ALWAYS:
			case ANCESTORS:
			case AND:
			case AS:
			case ASC:
			case ARRAY_COLLECT:
			case BEFORE:
			case BETWEEN:
			case BY:
			case CACHE:
			case CASE:
			case CAST:
			case COLLECTION:
			case COMMENT:
			case COUNT:
			case CREATE:
			case CYCLE:
			case DAYS:
			case DECLARE:
			case DEFAULT:
			case DELETE:
			case DESC:
			case DESCENDANTS:
			case DESCRIBE:
			case DISABLE:
			case DISTINCT:
			case DROP:
			case ELEMENTOF:
			case ELEMENTS:
			case ELSE:
			case ENABLE:
			case END:
			case ES_SHARDS:
			case ES_REPLICAS:
			case EXISTS:
			case EXTRACT:
			case FIELDS:
			case FIRST:
			case FREEZE:
			case FROM:
			case FROZEN:
			case FULLTEXT:
			case GENERATED:
			case GRANT:
			case GROUP:
			case HOURS:
			case IDENTIFIED:
			case IDENTITY:
			case IF:
			case IMAGE:
			case IN:
			case INCREMENT:
			case INDEX:
			case INDEXES:
			case INSERT:
			case INTO:
			case IS:
			case JSON:
			case KEY:
			case KEYOF:
			case KEYS:
			case LAST:
			case LIFETIME:
			case LIMIT:
			case LOCAL:
			case LOCK:
			case MERGE:
			case MINUTES:
			case MODIFY:
			case MR_COUNTER:
			case NAMESPACES:
			case NESTED:
			case NO:
			case NOT:
			case NULLS:
			case OFFSET:
			case OF:
			case ON:
			case OR:
			case ORDER:
			case OVERRIDE:
			case PASSWORD:
			case PATCH:
			case PER:
			case PRIMARY:
			case PUT:
			case REGION:
			case REGIONS:
			case REMOVE:
			case RETURNING:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROW:
			case SCHEMA:
			case SECONDS:
			case SELECT:
			case SEQ_TRANSFORM:
			case SET:
			case SHARD:
			case SHOW:
			case START:
			case TABLE:
			case TABLES:
			case THEN:
			case TO:
			case TTL:
			case TYPE:
			case UNFREEZE:
			case UNLOCK:
			case UPDATE:
			case UPSERT:
			case USER:
			case USERS:
			case USING:
			case VALUES:
			case WHEN:
			case WHERE:
			case WITH:
			case UNIQUE:
			case UNNEST:
			case ARRAY_T:
			case BINARY_T:
			case BOOLEAN_T:
			case DOUBLE_T:
			case ENUM_T:
			case FLOAT_T:
			case GEOMETRY_T:
			case INTEGER_T:
			case LONG_T:
			case MAP_T:
			case NUMBER_T:
			case POINT_T:
			case RECORD_T:
			case STRING_T:
			case TIMESTAMP_T:
			case ANY_T:
			case ANYATOMIC_T:
			case ANYJSONATOMIC_T:
			case ANYRECORD_T:
			case SCALAR_T:
			case RDIV:
			case ID:
			case BAD_ID:
				{
				setState(2219);
				id();
				}
				break;
			case DSTRING:
			case STRING:
				{
				setState(2220);
				string();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Identified_clauseContext extends ParserRuleContext {
		public TerminalNode IDENTIFIED() { return getToken(KVQLParser.IDENTIFIED, 0); }
		public By_passwordContext by_password() {
			return getRuleContext(By_passwordContext.class,0);
		}
		public Identified_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identified_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterIdentified_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitIdentified_clause(this);
		}
	}

	public final Identified_clauseContext identified_clause() throws RecognitionException {
		Identified_clauseContext _localctx = new Identified_clauseContext(_ctx, getState());
		enterRule(_localctx, 392, RULE_identified_clause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2223);
			match(IDENTIFIED);
			setState(2224);
			by_password();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Create_user_identified_clauseContext extends ParserRuleContext {
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Identified_clauseContext identified_clause() {
			return getRuleContext(Identified_clauseContext.class,0);
		}
		public TerminalNode PASSWORD_EXPIRE() { return getToken(KVQLParser.PASSWORD_EXPIRE, 0); }
		public Password_lifetimeContext password_lifetime() {
			return getRuleContext(Password_lifetimeContext.class,0);
		}
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public TerminalNode IDENTIFIED_EXTERNALLY() { return getToken(KVQLParser.IDENTIFIED_EXTERNALLY, 0); }
		public Create_user_identified_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_create_user_identified_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterCreate_user_identified_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitCreate_user_identified_clause(this);
		}
	}

	public final Create_user_identified_clauseContext create_user_identified_clause() throws RecognitionException {
		Create_user_identified_clauseContext _localctx = new Create_user_identified_clauseContext(_ctx, getState());
		enterRule(_localctx, 394, RULE_create_user_identified_clause);
		int _la;
		try {
			setState(2237);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ACCOUNT:
			case ADD:
			case ADMIN:
			case ALL:
			case ALTER:
			case ALWAYS:
			case ANCESTORS:
			case AND:
			case AS:
			case ASC:
			case ARRAY_COLLECT:
			case BEFORE:
			case BETWEEN:
			case BY:
			case CACHE:
			case CASE:
			case CAST:
			case COLLECTION:
			case COMMENT:
			case COUNT:
			case CREATE:
			case CYCLE:
			case DAYS:
			case DECLARE:
			case DEFAULT:
			case DELETE:
			case DESC:
			case DESCENDANTS:
			case DESCRIBE:
			case DISABLE:
			case DISTINCT:
			case DROP:
			case ELEMENTOF:
			case ELEMENTS:
			case ELSE:
			case ENABLE:
			case END:
			case ES_SHARDS:
			case ES_REPLICAS:
			case EXISTS:
			case EXTRACT:
			case FIELDS:
			case FIRST:
			case FREEZE:
			case FROM:
			case FROZEN:
			case FULLTEXT:
			case GENERATED:
			case GRANT:
			case GROUP:
			case HOURS:
			case IDENTIFIED:
			case IDENTITY:
			case IF:
			case IMAGE:
			case IN:
			case INCREMENT:
			case INDEX:
			case INDEXES:
			case INSERT:
			case INTO:
			case IS:
			case JSON:
			case KEY:
			case KEYOF:
			case KEYS:
			case LAST:
			case LIFETIME:
			case LIMIT:
			case LOCAL:
			case LOCK:
			case MERGE:
			case MINUTES:
			case MODIFY:
			case MR_COUNTER:
			case NAMESPACES:
			case NESTED:
			case NO:
			case NOT:
			case NULLS:
			case OFFSET:
			case OF:
			case ON:
			case OR:
			case ORDER:
			case OVERRIDE:
			case PASSWORD:
			case PATCH:
			case PER:
			case PRIMARY:
			case PUT:
			case REGION:
			case REGIONS:
			case REMOVE:
			case RETURNING:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROW:
			case SCHEMA:
			case SECONDS:
			case SELECT:
			case SEQ_TRANSFORM:
			case SET:
			case SHARD:
			case SHOW:
			case START:
			case TABLE:
			case TABLES:
			case THEN:
			case TO:
			case TTL:
			case TYPE:
			case UNFREEZE:
			case UNLOCK:
			case UPDATE:
			case UPSERT:
			case USER:
			case USERS:
			case USING:
			case VALUES:
			case WHEN:
			case WHERE:
			case WITH:
			case UNIQUE:
			case UNNEST:
			case ARRAY_T:
			case BINARY_T:
			case BOOLEAN_T:
			case DOUBLE_T:
			case ENUM_T:
			case FLOAT_T:
			case GEOMETRY_T:
			case INTEGER_T:
			case LONG_T:
			case MAP_T:
			case NUMBER_T:
			case POINT_T:
			case RECORD_T:
			case STRING_T:
			case TIMESTAMP_T:
			case ANY_T:
			case ANYATOMIC_T:
			case ANYJSONATOMIC_T:
			case ANYRECORD_T:
			case SCALAR_T:
			case RDIV:
			case ID:
			case BAD_ID:
				enterOuterAlt(_localctx, 1);
				{
				setState(2226);
				id();
				setState(2227);
				identified_clause();
				setState(2229);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PASSWORD_EXPIRE) {
					{
					setState(2228);
					match(PASSWORD_EXPIRE);
					}
				}

				setState(2232);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==PASSWORD) {
					{
					setState(2231);
					password_lifetime();
					}
				}

				}
				break;
			case DSTRING:
			case STRING:
				enterOuterAlt(_localctx, 2);
				{
				setState(2234);
				string();
				setState(2235);
				match(IDENTIFIED_EXTERNALLY);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class By_passwordContext extends ParserRuleContext {
		public TerminalNode BY() { return getToken(KVQLParser.BY, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public By_passwordContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_by_password; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterBy_password(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitBy_password(this);
		}
	}

	public final By_passwordContext by_password() throws RecognitionException {
		By_passwordContext _localctx = new By_passwordContext(_ctx, getState());
		enterRule(_localctx, 396, RULE_by_password);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2239);
			match(BY);
			setState(2240);
			string();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Password_lifetimeContext extends ParserRuleContext {
		public TerminalNode PASSWORD() { return getToken(KVQLParser.PASSWORD, 0); }
		public TerminalNode LIFETIME() { return getToken(KVQLParser.LIFETIME, 0); }
		public DurationContext duration() {
			return getRuleContext(DurationContext.class,0);
		}
		public Password_lifetimeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_password_lifetime; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterPassword_lifetime(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitPassword_lifetime(this);
		}
	}

	public final Password_lifetimeContext password_lifetime() throws RecognitionException {
		Password_lifetimeContext _localctx = new Password_lifetimeContext(_ctx, getState());
		enterRule(_localctx, 398, RULE_password_lifetime);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2242);
			match(PASSWORD);
			setState(2243);
			match(LIFETIME);
			setState(2244);
			duration();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Reset_password_clauseContext extends ParserRuleContext {
		public Identified_clauseContext identified_clause() {
			return getRuleContext(Identified_clauseContext.class,0);
		}
		public TerminalNode RETAIN_CURRENT_PASSWORD() { return getToken(KVQLParser.RETAIN_CURRENT_PASSWORD, 0); }
		public Reset_password_clauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_reset_password_clause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterReset_password_clause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitReset_password_clause(this);
		}
	}

	public final Reset_password_clauseContext reset_password_clause() throws RecognitionException {
		Reset_password_clauseContext _localctx = new Reset_password_clauseContext(_ctx, getState());
		enterRule(_localctx, 400, RULE_reset_password_clause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2246);
			identified_clause();
			setState(2248);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==RETAIN_CURRENT_PASSWORD) {
				{
				setState(2247);
				match(RETAIN_CURRENT_PASSWORD);
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Account_lockContext extends ParserRuleContext {
		public TerminalNode ACCOUNT() { return getToken(KVQLParser.ACCOUNT, 0); }
		public TerminalNode LOCK() { return getToken(KVQLParser.LOCK, 0); }
		public TerminalNode UNLOCK() { return getToken(KVQLParser.UNLOCK, 0); }
		public Account_lockContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_account_lock; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterAccount_lock(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitAccount_lock(this);
		}
	}

	public final Account_lockContext account_lock() throws RecognitionException {
		Account_lockContext _localctx = new Account_lockContext(_ctx, getState());
		enterRule(_localctx, 402, RULE_account_lock);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2250);
			match(ACCOUNT);
			setState(2251);
			_la = _input.LA(1);
			if ( !(_la==LOCK || _la==UNLOCK) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Grant_rolesContext extends ParserRuleContext {
		public Id_listContext id_list() {
			return getRuleContext(Id_listContext.class,0);
		}
		public TerminalNode TO() { return getToken(KVQLParser.TO, 0); }
		public PrincipalContext principal() {
			return getRuleContext(PrincipalContext.class,0);
		}
		public Grant_rolesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_grant_roles; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterGrant_roles(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitGrant_roles(this);
		}
	}

	public final Grant_rolesContext grant_roles() throws RecognitionException {
		Grant_rolesContext _localctx = new Grant_rolesContext(_ctx, getState());
		enterRule(_localctx, 404, RULE_grant_roles);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2253);
			id_list();
			setState(2254);
			match(TO);
			setState(2255);
			principal();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Grant_system_privilegesContext extends ParserRuleContext {
		public Sys_priv_listContext sys_priv_list() {
			return getRuleContext(Sys_priv_listContext.class,0);
		}
		public TerminalNode TO() { return getToken(KVQLParser.TO, 0); }
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Grant_system_privilegesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_grant_system_privileges; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterGrant_system_privileges(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitGrant_system_privileges(this);
		}
	}

	public final Grant_system_privilegesContext grant_system_privileges() throws RecognitionException {
		Grant_system_privilegesContext _localctx = new Grant_system_privilegesContext(_ctx, getState());
		enterRule(_localctx, 406, RULE_grant_system_privileges);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2257);
			sys_priv_list();
			setState(2258);
			match(TO);
			setState(2259);
			id();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Grant_object_privilegesContext extends ParserRuleContext {
		public Obj_priv_listContext obj_priv_list() {
			return getRuleContext(Obj_priv_listContext.class,0);
		}
		public TerminalNode ON() { return getToken(KVQLParser.ON, 0); }
		public TerminalNode TO() { return getToken(KVQLParser.TO, 0); }
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public ObjectContext object() {
			return getRuleContext(ObjectContext.class,0);
		}
		public TerminalNode NAMESPACE() { return getToken(KVQLParser.NAMESPACE, 0); }
		public NamespaceContext namespace() {
			return getRuleContext(NamespaceContext.class,0);
		}
		public Grant_object_privilegesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_grant_object_privileges; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterGrant_object_privileges(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitGrant_object_privileges(this);
		}
	}

	public final Grant_object_privilegesContext grant_object_privileges() throws RecognitionException {
		Grant_object_privilegesContext _localctx = new Grant_object_privilegesContext(_ctx, getState());
		enterRule(_localctx, 408, RULE_grant_object_privileges);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2261);
			obj_priv_list();
			setState(2262);
			match(ON);
			setState(2266);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ACCOUNT:
			case ADD:
			case ADMIN:
			case ALL:
			case ALTER:
			case ALWAYS:
			case ANCESTORS:
			case AND:
			case AS:
			case ASC:
			case ARRAY_COLLECT:
			case BEFORE:
			case BETWEEN:
			case BY:
			case CACHE:
			case CASE:
			case CAST:
			case COLLECTION:
			case COMMENT:
			case COUNT:
			case CREATE:
			case CYCLE:
			case DAYS:
			case DECLARE:
			case DEFAULT:
			case DELETE:
			case DESC:
			case DESCENDANTS:
			case DESCRIBE:
			case DISABLE:
			case DISTINCT:
			case DROP:
			case ELEMENTOF:
			case ELEMENTS:
			case ELSE:
			case ENABLE:
			case END:
			case ES_SHARDS:
			case ES_REPLICAS:
			case EXISTS:
			case EXTRACT:
			case FIELDS:
			case FIRST:
			case FREEZE:
			case FROM:
			case FROZEN:
			case FULLTEXT:
			case GENERATED:
			case GRANT:
			case GROUP:
			case HOURS:
			case IDENTIFIED:
			case IDENTITY:
			case IF:
			case IMAGE:
			case IN:
			case INCREMENT:
			case INDEX:
			case INDEXES:
			case INSERT:
			case INTO:
			case IS:
			case JSON:
			case KEY:
			case KEYOF:
			case KEYS:
			case LAST:
			case LIFETIME:
			case LIMIT:
			case LOCAL:
			case LOCK:
			case MERGE:
			case MINUTES:
			case MODIFY:
			case MR_COUNTER:
			case NAMESPACES:
			case NESTED:
			case NO:
			case NOT:
			case NULLS:
			case OFFSET:
			case OF:
			case ON:
			case OR:
			case ORDER:
			case OVERRIDE:
			case PASSWORD:
			case PATCH:
			case PER:
			case PRIMARY:
			case PUT:
			case REGION:
			case REGIONS:
			case REMOVE:
			case RETURNING:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROW:
			case SCHEMA:
			case SECONDS:
			case SELECT:
			case SEQ_TRANSFORM:
			case SET:
			case SHARD:
			case SHOW:
			case START:
			case TABLE:
			case TABLES:
			case THEN:
			case TO:
			case TTL:
			case TYPE:
			case UNFREEZE:
			case UNLOCK:
			case UPDATE:
			case UPSERT:
			case USER:
			case USERS:
			case USING:
			case VALUES:
			case WHEN:
			case WHERE:
			case WITH:
			case UNIQUE:
			case UNNEST:
			case ARRAY_T:
			case BINARY_T:
			case BOOLEAN_T:
			case DOUBLE_T:
			case ENUM_T:
			case FLOAT_T:
			case GEOMETRY_T:
			case INTEGER_T:
			case LONG_T:
			case MAP_T:
			case NUMBER_T:
			case POINT_T:
			case RECORD_T:
			case STRING_T:
			case TIMESTAMP_T:
			case ANY_T:
			case ANYATOMIC_T:
			case ANYJSONATOMIC_T:
			case ANYRECORD_T:
			case SCALAR_T:
			case RDIV:
			case SYSDOLAR:
			case ID:
			case BAD_ID:
				{
				setState(2263);
				object();
				}
				break;
			case NAMESPACE:
				{
				setState(2264);
				match(NAMESPACE);
				setState(2265);
				namespace();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(2268);
			match(TO);
			setState(2269);
			id();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Revoke_rolesContext extends ParserRuleContext {
		public Id_listContext id_list() {
			return getRuleContext(Id_listContext.class,0);
		}
		public TerminalNode FROM() { return getToken(KVQLParser.FROM, 0); }
		public PrincipalContext principal() {
			return getRuleContext(PrincipalContext.class,0);
		}
		public Revoke_rolesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_revoke_roles; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterRevoke_roles(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitRevoke_roles(this);
		}
	}

	public final Revoke_rolesContext revoke_roles() throws RecognitionException {
		Revoke_rolesContext _localctx = new Revoke_rolesContext(_ctx, getState());
		enterRule(_localctx, 410, RULE_revoke_roles);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2271);
			id_list();
			setState(2272);
			match(FROM);
			setState(2273);
			principal();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Revoke_system_privilegesContext extends ParserRuleContext {
		public Sys_priv_listContext sys_priv_list() {
			return getRuleContext(Sys_priv_listContext.class,0);
		}
		public TerminalNode FROM() { return getToken(KVQLParser.FROM, 0); }
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public Revoke_system_privilegesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_revoke_system_privileges; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterRevoke_system_privileges(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitRevoke_system_privileges(this);
		}
	}

	public final Revoke_system_privilegesContext revoke_system_privileges() throws RecognitionException {
		Revoke_system_privilegesContext _localctx = new Revoke_system_privilegesContext(_ctx, getState());
		enterRule(_localctx, 412, RULE_revoke_system_privileges);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2275);
			sys_priv_list();
			setState(2276);
			match(FROM);
			setState(2277);
			id();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Revoke_object_privilegesContext extends ParserRuleContext {
		public Obj_priv_listContext obj_priv_list() {
			return getRuleContext(Obj_priv_listContext.class,0);
		}
		public TerminalNode ON() { return getToken(KVQLParser.ON, 0); }
		public TerminalNode FROM() { return getToken(KVQLParser.FROM, 0); }
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public ObjectContext object() {
			return getRuleContext(ObjectContext.class,0);
		}
		public TerminalNode NAMESPACE() { return getToken(KVQLParser.NAMESPACE, 0); }
		public NamespaceContext namespace() {
			return getRuleContext(NamespaceContext.class,0);
		}
		public Revoke_object_privilegesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_revoke_object_privileges; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterRevoke_object_privileges(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitRevoke_object_privileges(this);
		}
	}

	public final Revoke_object_privilegesContext revoke_object_privileges() throws RecognitionException {
		Revoke_object_privilegesContext _localctx = new Revoke_object_privilegesContext(_ctx, getState());
		enterRule(_localctx, 414, RULE_revoke_object_privileges);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2279);
			obj_priv_list();
			setState(2280);
			match(ON);
			setState(2284);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ACCOUNT:
			case ADD:
			case ADMIN:
			case ALL:
			case ALTER:
			case ALWAYS:
			case ANCESTORS:
			case AND:
			case AS:
			case ASC:
			case ARRAY_COLLECT:
			case BEFORE:
			case BETWEEN:
			case BY:
			case CACHE:
			case CASE:
			case CAST:
			case COLLECTION:
			case COMMENT:
			case COUNT:
			case CREATE:
			case CYCLE:
			case DAYS:
			case DECLARE:
			case DEFAULT:
			case DELETE:
			case DESC:
			case DESCENDANTS:
			case DESCRIBE:
			case DISABLE:
			case DISTINCT:
			case DROP:
			case ELEMENTOF:
			case ELEMENTS:
			case ELSE:
			case ENABLE:
			case END:
			case ES_SHARDS:
			case ES_REPLICAS:
			case EXISTS:
			case EXTRACT:
			case FIELDS:
			case FIRST:
			case FREEZE:
			case FROM:
			case FROZEN:
			case FULLTEXT:
			case GENERATED:
			case GRANT:
			case GROUP:
			case HOURS:
			case IDENTIFIED:
			case IDENTITY:
			case IF:
			case IMAGE:
			case IN:
			case INCREMENT:
			case INDEX:
			case INDEXES:
			case INSERT:
			case INTO:
			case IS:
			case JSON:
			case KEY:
			case KEYOF:
			case KEYS:
			case LAST:
			case LIFETIME:
			case LIMIT:
			case LOCAL:
			case LOCK:
			case MERGE:
			case MINUTES:
			case MODIFY:
			case MR_COUNTER:
			case NAMESPACES:
			case NESTED:
			case NO:
			case NOT:
			case NULLS:
			case OFFSET:
			case OF:
			case ON:
			case OR:
			case ORDER:
			case OVERRIDE:
			case PASSWORD:
			case PATCH:
			case PER:
			case PRIMARY:
			case PUT:
			case REGION:
			case REGIONS:
			case REMOVE:
			case RETURNING:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROW:
			case SCHEMA:
			case SECONDS:
			case SELECT:
			case SEQ_TRANSFORM:
			case SET:
			case SHARD:
			case SHOW:
			case START:
			case TABLE:
			case TABLES:
			case THEN:
			case TO:
			case TTL:
			case TYPE:
			case UNFREEZE:
			case UNLOCK:
			case UPDATE:
			case UPSERT:
			case USER:
			case USERS:
			case USING:
			case VALUES:
			case WHEN:
			case WHERE:
			case WITH:
			case UNIQUE:
			case UNNEST:
			case ARRAY_T:
			case BINARY_T:
			case BOOLEAN_T:
			case DOUBLE_T:
			case ENUM_T:
			case FLOAT_T:
			case GEOMETRY_T:
			case INTEGER_T:
			case LONG_T:
			case MAP_T:
			case NUMBER_T:
			case POINT_T:
			case RECORD_T:
			case STRING_T:
			case TIMESTAMP_T:
			case ANY_T:
			case ANYATOMIC_T:
			case ANYJSONATOMIC_T:
			case ANYRECORD_T:
			case SCALAR_T:
			case RDIV:
			case SYSDOLAR:
			case ID:
			case BAD_ID:
				{
				setState(2281);
				object();
				}
				break;
			case NAMESPACE:
				{
				setState(2282);
				match(NAMESPACE);
				setState(2283);
				namespace();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			setState(2286);
			match(FROM);
			setState(2287);
			id();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class PrincipalContext extends ParserRuleContext {
		public TerminalNode USER() { return getToken(KVQLParser.USER, 0); }
		public Identifier_or_stringContext identifier_or_string() {
			return getRuleContext(Identifier_or_stringContext.class,0);
		}
		public TerminalNode ROLE() { return getToken(KVQLParser.ROLE, 0); }
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public PrincipalContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_principal; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterPrincipal(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitPrincipal(this);
		}
	}

	public final PrincipalContext principal() throws RecognitionException {
		PrincipalContext _localctx = new PrincipalContext(_ctx, getState());
		enterRule(_localctx, 416, RULE_principal);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2293);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case USER:
				{
				setState(2289);
				match(USER);
				setState(2290);
				identifier_or_string();
				}
				break;
			case ROLE:
				{
				setState(2291);
				match(ROLE);
				setState(2292);
				id();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Sys_priv_listContext extends ParserRuleContext {
		public List<Priv_itemContext> priv_item() {
			return getRuleContexts(Priv_itemContext.class);
		}
		public Priv_itemContext priv_item(int i) {
			return getRuleContext(Priv_itemContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Sys_priv_listContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sys_priv_list; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterSys_priv_list(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitSys_priv_list(this);
		}
	}

	public final Sys_priv_listContext sys_priv_list() throws RecognitionException {
		Sys_priv_listContext _localctx = new Sys_priv_listContext(_ctx, getState());
		enterRule(_localctx, 418, RULE_sys_priv_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2295);
			priv_item();
			setState(2300);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(2296);
				match(COMMA);
				setState(2297);
				priv_item();
				}
				}
				setState(2302);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Priv_itemContext extends ParserRuleContext {
		public IdContext id() {
			return getRuleContext(IdContext.class,0);
		}
		public TerminalNode ALL_PRIVILEGES() { return getToken(KVQLParser.ALL_PRIVILEGES, 0); }
		public Priv_itemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_priv_item; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterPriv_item(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitPriv_item(this);
		}
	}

	public final Priv_itemContext priv_item() throws RecognitionException {
		Priv_itemContext _localctx = new Priv_itemContext(_ctx, getState());
		enterRule(_localctx, 420, RULE_priv_item);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2305);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ACCOUNT:
			case ADD:
			case ADMIN:
			case ALL:
			case ALTER:
			case ALWAYS:
			case ANCESTORS:
			case AND:
			case AS:
			case ASC:
			case ARRAY_COLLECT:
			case BEFORE:
			case BETWEEN:
			case BY:
			case CACHE:
			case CASE:
			case CAST:
			case COLLECTION:
			case COMMENT:
			case COUNT:
			case CREATE:
			case CYCLE:
			case DAYS:
			case DECLARE:
			case DEFAULT:
			case DELETE:
			case DESC:
			case DESCENDANTS:
			case DESCRIBE:
			case DISABLE:
			case DISTINCT:
			case DROP:
			case ELEMENTOF:
			case ELEMENTS:
			case ELSE:
			case ENABLE:
			case END:
			case ES_SHARDS:
			case ES_REPLICAS:
			case EXISTS:
			case EXTRACT:
			case FIELDS:
			case FIRST:
			case FREEZE:
			case FROM:
			case FROZEN:
			case FULLTEXT:
			case GENERATED:
			case GRANT:
			case GROUP:
			case HOURS:
			case IDENTIFIED:
			case IDENTITY:
			case IF:
			case IMAGE:
			case IN:
			case INCREMENT:
			case INDEX:
			case INDEXES:
			case INSERT:
			case INTO:
			case IS:
			case JSON:
			case KEY:
			case KEYOF:
			case KEYS:
			case LAST:
			case LIFETIME:
			case LIMIT:
			case LOCAL:
			case LOCK:
			case MERGE:
			case MINUTES:
			case MODIFY:
			case MR_COUNTER:
			case NAMESPACES:
			case NESTED:
			case NO:
			case NOT:
			case NULLS:
			case OFFSET:
			case OF:
			case ON:
			case OR:
			case ORDER:
			case OVERRIDE:
			case PASSWORD:
			case PATCH:
			case PER:
			case PRIMARY:
			case PUT:
			case REGION:
			case REGIONS:
			case REMOVE:
			case RETURNING:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROW:
			case SCHEMA:
			case SECONDS:
			case SELECT:
			case SEQ_TRANSFORM:
			case SET:
			case SHARD:
			case SHOW:
			case START:
			case TABLE:
			case TABLES:
			case THEN:
			case TO:
			case TTL:
			case TYPE:
			case UNFREEZE:
			case UNLOCK:
			case UPDATE:
			case UPSERT:
			case USER:
			case USERS:
			case USING:
			case VALUES:
			case WHEN:
			case WHERE:
			case WITH:
			case UNIQUE:
			case UNNEST:
			case ARRAY_T:
			case BINARY_T:
			case BOOLEAN_T:
			case DOUBLE_T:
			case ENUM_T:
			case FLOAT_T:
			case GEOMETRY_T:
			case INTEGER_T:
			case LONG_T:
			case MAP_T:
			case NUMBER_T:
			case POINT_T:
			case RECORD_T:
			case STRING_T:
			case TIMESTAMP_T:
			case ANY_T:
			case ANYATOMIC_T:
			case ANYJSONATOMIC_T:
			case ANYRECORD_T:
			case SCALAR_T:
			case RDIV:
			case ID:
			case BAD_ID:
				{
				setState(2303);
				id();
				}
				break;
			case ALL_PRIVILEGES:
				{
				setState(2304);
				match(ALL_PRIVILEGES);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Obj_priv_listContext extends ParserRuleContext {
		public List<Priv_itemContext> priv_item() {
			return getRuleContexts(Priv_itemContext.class);
		}
		public Priv_itemContext priv_item(int i) {
			return getRuleContext(Priv_itemContext.class,i);
		}
		public List<TerminalNode> ALL() { return getTokens(KVQLParser.ALL); }
		public TerminalNode ALL(int i) {
			return getToken(KVQLParser.ALL, i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Obj_priv_listContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_obj_priv_list; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterObj_priv_list(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitObj_priv_list(this);
		}
	}

	public final Obj_priv_listContext obj_priv_list() throws RecognitionException {
		Obj_priv_listContext _localctx = new Obj_priv_listContext(_ctx, getState());
		enterRule(_localctx, 422, RULE_obj_priv_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2309);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,251,_ctx) ) {
			case 1:
				{
				setState(2307);
				priv_item();
				}
				break;
			case 2:
				{
				setState(2308);
				match(ALL);
				}
				break;
			}
			setState(2318);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(2311);
				match(COMMA);
				setState(2314);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,252,_ctx) ) {
				case 1:
					{
					setState(2312);
					priv_item();
					}
					break;
				case 2:
					{
					setState(2313);
					match(ALL);
					}
					break;
				}
				}
				}
				setState(2320);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ObjectContext extends ParserRuleContext {
		public Table_nameContext table_name() {
			return getRuleContext(Table_nameContext.class,0);
		}
		public ObjectContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_object; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterObject(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitObject(this);
		}
	}

	public final ObjectContext object() throws RecognitionException {
		ObjectContext _localctx = new ObjectContext(_ctx, getState());
		enterRule(_localctx, 424, RULE_object);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2321);
			table_name();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Json_textContext extends ParserRuleContext {
		public JsobjectContext jsobject() {
			return getRuleContext(JsobjectContext.class,0);
		}
		public JsarrayContext jsarray() {
			return getRuleContext(JsarrayContext.class,0);
		}
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public TerminalNode TRUE() { return getToken(KVQLParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(KVQLParser.FALSE, 0); }
		public TerminalNode NULL() { return getToken(KVQLParser.NULL, 0); }
		public Json_textContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_json_text; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterJson_text(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitJson_text(this);
		}
	}

	public final Json_textContext json_text() throws RecognitionException {
		Json_textContext _localctx = new Json_textContext(_ctx, getState());
		enterRule(_localctx, 426, RULE_json_text);
		try {
			setState(2330);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case LBRACE:
				enterOuterAlt(_localctx, 1);
				{
				setState(2323);
				jsobject();
				}
				break;
			case LBRACK:
				enterOuterAlt(_localctx, 2);
				{
				setState(2324);
				jsarray();
				}
				break;
			case DSTRING:
			case STRING:
				enterOuterAlt(_localctx, 3);
				{
				setState(2325);
				string();
				}
				break;
			case MINUS:
			case INT:
			case FLOAT:
			case NUMBER:
				enterOuterAlt(_localctx, 4);
				{
				setState(2326);
				number();
				}
				break;
			case TRUE:
				enterOuterAlt(_localctx, 5);
				{
				setState(2327);
				match(TRUE);
				}
				break;
			case FALSE:
				enterOuterAlt(_localctx, 6);
				{
				setState(2328);
				match(FALSE);
				}
				break;
			case NULL:
				enterOuterAlt(_localctx, 7);
				{
				setState(2329);
				match(NULL);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class JsobjectContext extends ParserRuleContext {
		public JsobjectContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_jsobject; }
	 
		public JsobjectContext() { }
		public void copyFrom(JsobjectContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class JsonObjectContext extends JsobjectContext {
		public TerminalNode LBRACE() { return getToken(KVQLParser.LBRACE, 0); }
		public List<JspairContext> jspair() {
			return getRuleContexts(JspairContext.class);
		}
		public JspairContext jspair(int i) {
			return getRuleContext(JspairContext.class,i);
		}
		public TerminalNode RBRACE() { return getToken(KVQLParser.RBRACE, 0); }
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public JsonObjectContext(JsobjectContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterJsonObject(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitJsonObject(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class EmptyJsonObjectContext extends JsobjectContext {
		public TerminalNode LBRACE() { return getToken(KVQLParser.LBRACE, 0); }
		public TerminalNode RBRACE() { return getToken(KVQLParser.RBRACE, 0); }
		public EmptyJsonObjectContext(JsobjectContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterEmptyJsonObject(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitEmptyJsonObject(this);
		}
	}

	public final JsobjectContext jsobject() throws RecognitionException {
		JsobjectContext _localctx = new JsobjectContext(_ctx, getState());
		enterRule(_localctx, 428, RULE_jsobject);
		int _la;
		try {
			setState(2345);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,256,_ctx) ) {
			case 1:
				_localctx = new JsonObjectContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2332);
				match(LBRACE);
				setState(2333);
				jspair();
				setState(2338);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(2334);
					match(COMMA);
					setState(2335);
					jspair();
					}
					}
					setState(2340);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(2341);
				match(RBRACE);
				}
				break;
			case 2:
				_localctx = new EmptyJsonObjectContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2343);
				match(LBRACE);
				setState(2344);
				match(RBRACE);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class JsarrayContext extends ParserRuleContext {
		public JsarrayContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_jsarray; }
	 
		public JsarrayContext() { }
		public void copyFrom(JsarrayContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class EmptyJsonArrayContext extends JsarrayContext {
		public TerminalNode LBRACK() { return getToken(KVQLParser.LBRACK, 0); }
		public TerminalNode RBRACK() { return getToken(KVQLParser.RBRACK, 0); }
		public EmptyJsonArrayContext(JsarrayContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterEmptyJsonArray(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitEmptyJsonArray(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ArrayOfJsonValuesContext extends JsarrayContext {
		public TerminalNode LBRACK() { return getToken(KVQLParser.LBRACK, 0); }
		public List<JsvalueContext> jsvalue() {
			return getRuleContexts(JsvalueContext.class);
		}
		public JsvalueContext jsvalue(int i) {
			return getRuleContext(JsvalueContext.class,i);
		}
		public TerminalNode RBRACK() { return getToken(KVQLParser.RBRACK, 0); }
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public ArrayOfJsonValuesContext(JsarrayContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterArrayOfJsonValues(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitArrayOfJsonValues(this);
		}
	}

	public final JsarrayContext jsarray() throws RecognitionException {
		JsarrayContext _localctx = new JsarrayContext(_ctx, getState());
		enterRule(_localctx, 430, RULE_jsarray);
		int _la;
		try {
			setState(2360);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,258,_ctx) ) {
			case 1:
				_localctx = new ArrayOfJsonValuesContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2347);
				match(LBRACK);
				setState(2348);
				jsvalue();
				setState(2353);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(2349);
					match(COMMA);
					setState(2350);
					jsvalue();
					}
					}
					setState(2355);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(2356);
				match(RBRACK);
				}
				break;
			case 2:
				_localctx = new EmptyJsonArrayContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2358);
				match(LBRACK);
				setState(2359);
				match(RBRACK);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class JspairContext extends ParserRuleContext {
		public JspairContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_jspair; }
	 
		public JspairContext() { }
		public void copyFrom(JspairContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class JsonPairContext extends JspairContext {
		public TerminalNode DSTRING() { return getToken(KVQLParser.DSTRING, 0); }
		public TerminalNode COLON() { return getToken(KVQLParser.COLON, 0); }
		public JsvalueContext jsvalue() {
			return getRuleContext(JsvalueContext.class,0);
		}
		public JsonPairContext(JspairContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterJsonPair(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitJsonPair(this);
		}
	}

	public final JspairContext jspair() throws RecognitionException {
		JspairContext _localctx = new JspairContext(_ctx, getState());
		enterRule(_localctx, 432, RULE_jspair);
		try {
			_localctx = new JsonPairContext(_localctx);
			enterOuterAlt(_localctx, 1);
			{
			setState(2362);
			match(DSTRING);
			setState(2363);
			match(COLON);
			setState(2364);
			jsvalue();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class JsvalueContext extends ParserRuleContext {
		public JsvalueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_jsvalue; }
	 
		public JsvalueContext() { }
		public void copyFrom(JsvalueContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class JsonAtomContext extends JsvalueContext {
		public TerminalNode DSTRING() { return getToken(KVQLParser.DSTRING, 0); }
		public NumberContext number() {
			return getRuleContext(NumberContext.class,0);
		}
		public TerminalNode TRUE() { return getToken(KVQLParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(KVQLParser.FALSE, 0); }
		public TerminalNode NULL() { return getToken(KVQLParser.NULL, 0); }
		public JsonAtomContext(JsvalueContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterJsonAtom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitJsonAtom(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class JsonArrayValueContext extends JsvalueContext {
		public JsarrayContext jsarray() {
			return getRuleContext(JsarrayContext.class,0);
		}
		public JsonArrayValueContext(JsvalueContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterJsonArrayValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitJsonArrayValue(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class JsonObjectValueContext extends JsvalueContext {
		public JsobjectContext jsobject() {
			return getRuleContext(JsobjectContext.class,0);
		}
		public JsonObjectValueContext(JsvalueContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterJsonObjectValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitJsonObjectValue(this);
		}
	}

	public final JsvalueContext jsvalue() throws RecognitionException {
		JsvalueContext _localctx = new JsvalueContext(_ctx, getState());
		enterRule(_localctx, 434, RULE_jsvalue);
		try {
			setState(2373);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case LBRACE:
				_localctx = new JsonObjectValueContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(2366);
				jsobject();
				}
				break;
			case LBRACK:
				_localctx = new JsonArrayValueContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(2367);
				jsarray();
				}
				break;
			case DSTRING:
				_localctx = new JsonAtomContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(2368);
				match(DSTRING);
				}
				break;
			case MINUS:
			case INT:
			case FLOAT:
			case NUMBER:
				_localctx = new JsonAtomContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(2369);
				number();
				}
				break;
			case TRUE:
				_localctx = new JsonAtomContext(_localctx);
				enterOuterAlt(_localctx, 5);
				{
				setState(2370);
				match(TRUE);
				}
				break;
			case FALSE:
				_localctx = new JsonAtomContext(_localctx);
				enterOuterAlt(_localctx, 6);
				{
				setState(2371);
				match(FALSE);
				}
				break;
			case NULL:
				_localctx = new JsonAtomContext(_localctx);
				enterOuterAlt(_localctx, 7);
				{
				setState(2372);
				match(NULL);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class CommentContext extends ParserRuleContext {
		public TerminalNode COMMENT() { return getToken(KVQLParser.COMMENT, 0); }
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public CommentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comment; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterComment(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitComment(this);
		}
	}

	public final CommentContext comment() throws RecognitionException {
		CommentContext _localctx = new CommentContext(_ctx, getState());
		enterRule(_localctx, 436, RULE_comment);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2375);
			match(COMMENT);
			setState(2376);
			string();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class DurationContext extends ParserRuleContext {
		public TerminalNode INT() { return getToken(KVQLParser.INT, 0); }
		public Time_unitContext time_unit() {
			return getRuleContext(Time_unitContext.class,0);
		}
		public DurationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_duration; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterDuration(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitDuration(this);
		}
	}

	public final DurationContext duration() throws RecognitionException {
		DurationContext _localctx = new DurationContext(_ctx, getState());
		enterRule(_localctx, 438, RULE_duration);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2378);
			match(INT);
			setState(2379);
			time_unit();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Time_unitContext extends ParserRuleContext {
		public TerminalNode SECONDS() { return getToken(KVQLParser.SECONDS, 0); }
		public TerminalNode MINUTES() { return getToken(KVQLParser.MINUTES, 0); }
		public TerminalNode HOURS() { return getToken(KVQLParser.HOURS, 0); }
		public TerminalNode DAYS() { return getToken(KVQLParser.DAYS, 0); }
		public Time_unitContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_time_unit; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterTime_unit(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitTime_unit(this);
		}
	}

	public final Time_unitContext time_unit() throws RecognitionException {
		Time_unitContext _localctx = new Time_unitContext(_ctx, getState());
		enterRule(_localctx, 440, RULE_time_unit);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2381);
			_la = _input.LA(1);
			if ( !(_la==DAYS || _la==HOURS || _la==MINUTES || _la==SECONDS) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class NumberContext extends ParserRuleContext {
		public TerminalNode INT() { return getToken(KVQLParser.INT, 0); }
		public TerminalNode FLOAT() { return getToken(KVQLParser.FLOAT, 0); }
		public TerminalNode NUMBER() { return getToken(KVQLParser.NUMBER, 0); }
		public TerminalNode MINUS() { return getToken(KVQLParser.MINUS, 0); }
		public NumberContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_number; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterNumber(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitNumber(this);
		}
	}

	public final NumberContext number() throws RecognitionException {
		NumberContext _localctx = new NumberContext(_ctx, getState());
		enterRule(_localctx, 442, RULE_number);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2384);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==MINUS) {
				{
				setState(2383);
				match(MINUS);
				}
			}

			setState(2386);
			_la = _input.LA(1);
			if ( !(((((_la - 205)) & ~0x3f) == 0 && ((1L << (_la - 205)) & 7L) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Signed_intContext extends ParserRuleContext {
		public TerminalNode INT() { return getToken(KVQLParser.INT, 0); }
		public TerminalNode MINUS() { return getToken(KVQLParser.MINUS, 0); }
		public TerminalNode PLUS() { return getToken(KVQLParser.PLUS, 0); }
		public Signed_intContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_signed_int; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterSigned_int(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitSigned_int(this);
		}
	}

	public final Signed_intContext signed_int() throws RecognitionException {
		Signed_intContext _localctx = new Signed_intContext(_ctx, getState());
		enterRule(_localctx, 444, RULE_signed_int);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2389);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==PLUS || _la==MINUS) {
				{
				setState(2388);
				_la = _input.LA(1);
				if ( !(_la==PLUS || _la==MINUS) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
			}

			setState(2391);
			match(INT);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class StringContext extends ParserRuleContext {
		public TerminalNode STRING() { return getToken(KVQLParser.STRING, 0); }
		public TerminalNode DSTRING() { return getToken(KVQLParser.DSTRING, 0); }
		public StringContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_string; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterString(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitString(this);
		}
	}

	public final StringContext string() throws RecognitionException {
		StringContext _localctx = new StringContext(_ctx, getState());
		enterRule(_localctx, 446, RULE_string);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(2393);
			_la = _input.LA(1);
			if ( !(_la==DSTRING || _la==STRING) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Id_listContext extends ParserRuleContext {
		public List<IdContext> id() {
			return getRuleContexts(IdContext.class);
		}
		public IdContext id(int i) {
			return getRuleContext(IdContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(KVQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(KVQLParser.COMMA, i);
		}
		public Id_listContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_id_list; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterId_list(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitId_list(this);
		}
	}

	public final Id_listContext id_list() throws RecognitionException {
		Id_listContext _localctx = new Id_listContext(_ctx, getState());
		enterRule(_localctx, 448, RULE_id_list);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(2395);
			id();
			setState(2400);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,262,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(2396);
					match(COMMA);
					setState(2397);
					id();
					}
					} 
				}
				setState(2402);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,262,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class IdContext extends ParserRuleContext {
		public TerminalNode ACCOUNT() { return getToken(KVQLParser.ACCOUNT, 0); }
		public TerminalNode ADD() { return getToken(KVQLParser.ADD, 0); }
		public TerminalNode ADMIN() { return getToken(KVQLParser.ADMIN, 0); }
		public TerminalNode ALL() { return getToken(KVQLParser.ALL, 0); }
		public TerminalNode ALTER() { return getToken(KVQLParser.ALTER, 0); }
		public TerminalNode ALWAYS() { return getToken(KVQLParser.ALWAYS, 0); }
		public TerminalNode ANCESTORS() { return getToken(KVQLParser.ANCESTORS, 0); }
		public TerminalNode AND() { return getToken(KVQLParser.AND, 0); }
		public TerminalNode ANY_T() { return getToken(KVQLParser.ANY_T, 0); }
		public TerminalNode ANYATOMIC_T() { return getToken(KVQLParser.ANYATOMIC_T, 0); }
		public TerminalNode ANYJSONATOMIC_T() { return getToken(KVQLParser.ANYJSONATOMIC_T, 0); }
		public TerminalNode ANYRECORD_T() { return getToken(KVQLParser.ANYRECORD_T, 0); }
		public TerminalNode ARRAY_COLLECT() { return getToken(KVQLParser.ARRAY_COLLECT, 0); }
		public TerminalNode AS() { return getToken(KVQLParser.AS, 0); }
		public TerminalNode ASC() { return getToken(KVQLParser.ASC, 0); }
		public TerminalNode BEFORE() { return getToken(KVQLParser.BEFORE, 0); }
		public TerminalNode BETWEEN() { return getToken(KVQLParser.BETWEEN, 0); }
		public TerminalNode BY() { return getToken(KVQLParser.BY, 0); }
		public TerminalNode CACHE() { return getToken(KVQLParser.CACHE, 0); }
		public TerminalNode CASE() { return getToken(KVQLParser.CASE, 0); }
		public TerminalNode CAST() { return getToken(KVQLParser.CAST, 0); }
		public TerminalNode COLLECTION() { return getToken(KVQLParser.COLLECTION, 0); }
		public TerminalNode COMMENT() { return getToken(KVQLParser.COMMENT, 0); }
		public TerminalNode COUNT() { return getToken(KVQLParser.COUNT, 0); }
		public TerminalNode CREATE() { return getToken(KVQLParser.CREATE, 0); }
		public TerminalNode CYCLE() { return getToken(KVQLParser.CYCLE, 0); }
		public TerminalNode DAYS() { return getToken(KVQLParser.DAYS, 0); }
		public TerminalNode DECLARE() { return getToken(KVQLParser.DECLARE, 0); }
		public TerminalNode DEFAULT() { return getToken(KVQLParser.DEFAULT, 0); }
		public TerminalNode DELETE() { return getToken(KVQLParser.DELETE, 0); }
		public TerminalNode DESC() { return getToken(KVQLParser.DESC, 0); }
		public TerminalNode DESCENDANTS() { return getToken(KVQLParser.DESCENDANTS, 0); }
		public TerminalNode DESCRIBE() { return getToken(KVQLParser.DESCRIBE, 0); }
		public TerminalNode DISABLE() { return getToken(KVQLParser.DISABLE, 0); }
		public TerminalNode DISTINCT() { return getToken(KVQLParser.DISTINCT, 0); }
		public TerminalNode DROP() { return getToken(KVQLParser.DROP, 0); }
		public TerminalNode ELEMENTOF() { return getToken(KVQLParser.ELEMENTOF, 0); }
		public TerminalNode ELEMENTS() { return getToken(KVQLParser.ELEMENTS, 0); }
		public TerminalNode ELSE() { return getToken(KVQLParser.ELSE, 0); }
		public TerminalNode ENABLE() { return getToken(KVQLParser.ENABLE, 0); }
		public TerminalNode END() { return getToken(KVQLParser.END, 0); }
		public TerminalNode ES_SHARDS() { return getToken(KVQLParser.ES_SHARDS, 0); }
		public TerminalNode ES_REPLICAS() { return getToken(KVQLParser.ES_REPLICAS, 0); }
		public TerminalNode EXISTS() { return getToken(KVQLParser.EXISTS, 0); }
		public TerminalNode EXTRACT() { return getToken(KVQLParser.EXTRACT, 0); }
		public TerminalNode FIELDS() { return getToken(KVQLParser.FIELDS, 0); }
		public TerminalNode FIRST() { return getToken(KVQLParser.FIRST, 0); }
		public TerminalNode FREEZE() { return getToken(KVQLParser.FREEZE, 0); }
		public TerminalNode FROM() { return getToken(KVQLParser.FROM, 0); }
		public TerminalNode FROZEN() { return getToken(KVQLParser.FROZEN, 0); }
		public TerminalNode FULLTEXT() { return getToken(KVQLParser.FULLTEXT, 0); }
		public TerminalNode GENERATED() { return getToken(KVQLParser.GENERATED, 0); }
		public TerminalNode GRANT() { return getToken(KVQLParser.GRANT, 0); }
		public TerminalNode GROUP() { return getToken(KVQLParser.GROUP, 0); }
		public TerminalNode HOURS() { return getToken(KVQLParser.HOURS, 0); }
		public TerminalNode IDENTIFIED() { return getToken(KVQLParser.IDENTIFIED, 0); }
		public TerminalNode IDENTITY() { return getToken(KVQLParser.IDENTITY, 0); }
		public TerminalNode IF() { return getToken(KVQLParser.IF, 0); }
		public TerminalNode INCREMENT() { return getToken(KVQLParser.INCREMENT, 0); }
		public TerminalNode IMAGE() { return getToken(KVQLParser.IMAGE, 0); }
		public TerminalNode INDEX() { return getToken(KVQLParser.INDEX, 0); }
		public TerminalNode INDEXES() { return getToken(KVQLParser.INDEXES, 0); }
		public TerminalNode INSERT() { return getToken(KVQLParser.INSERT, 0); }
		public TerminalNode INTO() { return getToken(KVQLParser.INTO, 0); }
		public TerminalNode IN() { return getToken(KVQLParser.IN, 0); }
		public TerminalNode IS() { return getToken(KVQLParser.IS, 0); }
		public TerminalNode JSON() { return getToken(KVQLParser.JSON, 0); }
		public TerminalNode KEY() { return getToken(KVQLParser.KEY, 0); }
		public TerminalNode KEYOF() { return getToken(KVQLParser.KEYOF, 0); }
		public TerminalNode KEYS() { return getToken(KVQLParser.KEYS, 0); }
		public TerminalNode LIFETIME() { return getToken(KVQLParser.LIFETIME, 0); }
		public TerminalNode LAST() { return getToken(KVQLParser.LAST, 0); }
		public TerminalNode LIMIT() { return getToken(KVQLParser.LIMIT, 0); }
		public TerminalNode LOCAL() { return getToken(KVQLParser.LOCAL, 0); }
		public TerminalNode LOCK() { return getToken(KVQLParser.LOCK, 0); }
		public TerminalNode MERGE() { return getToken(KVQLParser.MERGE, 0); }
		public TerminalNode MINUTES() { return getToken(KVQLParser.MINUTES, 0); }
		public TerminalNode MODIFY() { return getToken(KVQLParser.MODIFY, 0); }
		public TerminalNode MR_COUNTER() { return getToken(KVQLParser.MR_COUNTER, 0); }
		public TerminalNode NAMESPACE() { return getToken(KVQLParser.NAMESPACE, 0); }
		public TerminalNode NAMESPACES() { return getToken(KVQLParser.NAMESPACES, 0); }
		public TerminalNode NESTED() { return getToken(KVQLParser.NESTED, 0); }
		public TerminalNode NO() { return getToken(KVQLParser.NO, 0); }
		public TerminalNode NOT() { return getToken(KVQLParser.NOT, 0); }
		public TerminalNode NULLS() { return getToken(KVQLParser.NULLS, 0); }
		public TerminalNode OF() { return getToken(KVQLParser.OF, 0); }
		public TerminalNode OFFSET() { return getToken(KVQLParser.OFFSET, 0); }
		public TerminalNode ON() { return getToken(KVQLParser.ON, 0); }
		public TerminalNode OR() { return getToken(KVQLParser.OR, 0); }
		public TerminalNode ORDER() { return getToken(KVQLParser.ORDER, 0); }
		public TerminalNode OVERRIDE() { return getToken(KVQLParser.OVERRIDE, 0); }
		public TerminalNode PER() { return getToken(KVQLParser.PER, 0); }
		public TerminalNode PASSWORD() { return getToken(KVQLParser.PASSWORD, 0); }
		public TerminalNode PATCH() { return getToken(KVQLParser.PATCH, 0); }
		public TerminalNode PRIMARY() { return getToken(KVQLParser.PRIMARY, 0); }
		public TerminalNode PUT() { return getToken(KVQLParser.PUT, 0); }
		public TerminalNode RDIV() { return getToken(KVQLParser.RDIV, 0); }
		public TerminalNode REGION() { return getToken(KVQLParser.REGION, 0); }
		public TerminalNode REGIONS() { return getToken(KVQLParser.REGIONS, 0); }
		public TerminalNode REMOVE() { return getToken(KVQLParser.REMOVE, 0); }
		public TerminalNode RETURNING() { return getToken(KVQLParser.RETURNING, 0); }
		public TerminalNode ROW() { return getToken(KVQLParser.ROW, 0); }
		public TerminalNode ROLE() { return getToken(KVQLParser.ROLE, 0); }
		public TerminalNode ROLES() { return getToken(KVQLParser.ROLES, 0); }
		public TerminalNode REVOKE() { return getToken(KVQLParser.REVOKE, 0); }
		public TerminalNode SCHEMA() { return getToken(KVQLParser.SCHEMA, 0); }
		public TerminalNode SECONDS() { return getToken(KVQLParser.SECONDS, 0); }
		public TerminalNode SELECT() { return getToken(KVQLParser.SELECT, 0); }
		public TerminalNode SEQ_TRANSFORM() { return getToken(KVQLParser.SEQ_TRANSFORM, 0); }
		public TerminalNode SET() { return getToken(KVQLParser.SET, 0); }
		public TerminalNode SHARD() { return getToken(KVQLParser.SHARD, 0); }
		public TerminalNode SHOW() { return getToken(KVQLParser.SHOW, 0); }
		public TerminalNode START() { return getToken(KVQLParser.START, 0); }
		public TerminalNode TABLE() { return getToken(KVQLParser.TABLE, 0); }
		public TerminalNode TABLES() { return getToken(KVQLParser.TABLES, 0); }
		public TerminalNode THEN() { return getToken(KVQLParser.THEN, 0); }
		public TerminalNode TO() { return getToken(KVQLParser.TO, 0); }
		public TerminalNode TTL() { return getToken(KVQLParser.TTL, 0); }
		public TerminalNode TYPE() { return getToken(KVQLParser.TYPE, 0); }
		public TerminalNode UNFREEZE() { return getToken(KVQLParser.UNFREEZE, 0); }
		public TerminalNode UNLOCK() { return getToken(KVQLParser.UNLOCK, 0); }
		public TerminalNode UNIQUE() { return getToken(KVQLParser.UNIQUE, 0); }
		public TerminalNode UNNEST() { return getToken(KVQLParser.UNNEST, 0); }
		public TerminalNode UPDATE() { return getToken(KVQLParser.UPDATE, 0); }
		public TerminalNode UPSERT() { return getToken(KVQLParser.UPSERT, 0); }
		public TerminalNode USER() { return getToken(KVQLParser.USER, 0); }
		public TerminalNode USERS() { return getToken(KVQLParser.USERS, 0); }
		public TerminalNode USING() { return getToken(KVQLParser.USING, 0); }
		public TerminalNode VALUES() { return getToken(KVQLParser.VALUES, 0); }
		public TerminalNode WHEN() { return getToken(KVQLParser.WHEN, 0); }
		public TerminalNode WHERE() { return getToken(KVQLParser.WHERE, 0); }
		public TerminalNode WITH() { return getToken(KVQLParser.WITH, 0); }
		public TerminalNode ARRAY_T() { return getToken(KVQLParser.ARRAY_T, 0); }
		public TerminalNode BINARY_T() { return getToken(KVQLParser.BINARY_T, 0); }
		public TerminalNode BOOLEAN_T() { return getToken(KVQLParser.BOOLEAN_T, 0); }
		public TerminalNode DOUBLE_T() { return getToken(KVQLParser.DOUBLE_T, 0); }
		public TerminalNode ENUM_T() { return getToken(KVQLParser.ENUM_T, 0); }
		public TerminalNode FLOAT_T() { return getToken(KVQLParser.FLOAT_T, 0); }
		public TerminalNode GEOMETRY_T() { return getToken(KVQLParser.GEOMETRY_T, 0); }
		public TerminalNode LONG_T() { return getToken(KVQLParser.LONG_T, 0); }
		public TerminalNode INTEGER_T() { return getToken(KVQLParser.INTEGER_T, 0); }
		public TerminalNode MAP_T() { return getToken(KVQLParser.MAP_T, 0); }
		public TerminalNode NUMBER_T() { return getToken(KVQLParser.NUMBER_T, 0); }
		public TerminalNode POINT_T() { return getToken(KVQLParser.POINT_T, 0); }
		public TerminalNode RECORD_T() { return getToken(KVQLParser.RECORD_T, 0); }
		public TerminalNode STRING_T() { return getToken(KVQLParser.STRING_T, 0); }
		public TerminalNode TIMESTAMP_T() { return getToken(KVQLParser.TIMESTAMP_T, 0); }
		public TerminalNode SCALAR_T() { return getToken(KVQLParser.SCALAR_T, 0); }
		public TerminalNode ID() { return getToken(KVQLParser.ID, 0); }
		public TerminalNode BAD_ID() { return getToken(KVQLParser.BAD_ID, 0); }
		public IdContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_id; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).enterId(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof KVQLListener ) ((KVQLListener)listener).exitId(this);
		}
	}

	public final IdContext id() throws RecognitionException {
		IdContext _localctx = new IdContext(_ctx, getState());
		enterRule(_localctx, 450, RULE_id);
		try {
			setState(2556);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ACCOUNT:
			case ADD:
			case ADMIN:
			case ALL:
			case ALTER:
			case ALWAYS:
			case ANCESTORS:
			case AND:
			case AS:
			case ASC:
			case ARRAY_COLLECT:
			case BEFORE:
			case BETWEEN:
			case BY:
			case CACHE:
			case CASE:
			case CAST:
			case COLLECTION:
			case COMMENT:
			case COUNT:
			case CREATE:
			case CYCLE:
			case DAYS:
			case DECLARE:
			case DEFAULT:
			case DELETE:
			case DESC:
			case DESCENDANTS:
			case DESCRIBE:
			case DISABLE:
			case DISTINCT:
			case DROP:
			case ELEMENTOF:
			case ELEMENTS:
			case ELSE:
			case ENABLE:
			case END:
			case ES_SHARDS:
			case ES_REPLICAS:
			case EXISTS:
			case EXTRACT:
			case FIELDS:
			case FIRST:
			case FREEZE:
			case FROM:
			case FROZEN:
			case FULLTEXT:
			case GENERATED:
			case GRANT:
			case GROUP:
			case HOURS:
			case IDENTIFIED:
			case IDENTITY:
			case IF:
			case IMAGE:
			case IN:
			case INCREMENT:
			case INDEX:
			case INDEXES:
			case INSERT:
			case INTO:
			case IS:
			case JSON:
			case KEY:
			case KEYOF:
			case KEYS:
			case LAST:
			case LIFETIME:
			case LIMIT:
			case LOCAL:
			case LOCK:
			case MERGE:
			case MINUTES:
			case MODIFY:
			case MR_COUNTER:
			case NAMESPACES:
			case NESTED:
			case NO:
			case NOT:
			case NULLS:
			case OFFSET:
			case OF:
			case ON:
			case OR:
			case ORDER:
			case OVERRIDE:
			case PASSWORD:
			case PATCH:
			case PER:
			case PRIMARY:
			case PUT:
			case REGION:
			case REGIONS:
			case REMOVE:
			case RETURNING:
			case REVOKE:
			case ROLE:
			case ROLES:
			case ROW:
			case SCHEMA:
			case SECONDS:
			case SELECT:
			case SEQ_TRANSFORM:
			case SET:
			case SHARD:
			case SHOW:
			case START:
			case TABLE:
			case TABLES:
			case THEN:
			case TO:
			case TTL:
			case TYPE:
			case UNFREEZE:
			case UNLOCK:
			case UPDATE:
			case UPSERT:
			case USER:
			case USERS:
			case USING:
			case VALUES:
			case WHEN:
			case WHERE:
			case WITH:
			case UNIQUE:
			case UNNEST:
			case ARRAY_T:
			case BINARY_T:
			case BOOLEAN_T:
			case DOUBLE_T:
			case ENUM_T:
			case FLOAT_T:
			case GEOMETRY_T:
			case INTEGER_T:
			case LONG_T:
			case MAP_T:
			case NUMBER_T:
			case POINT_T:
			case RECORD_T:
			case STRING_T:
			case TIMESTAMP_T:
			case ANY_T:
			case ANYATOMIC_T:
			case ANYJSONATOMIC_T:
			case ANYRECORD_T:
			case SCALAR_T:
			case RDIV:
			case ID:
				enterOuterAlt(_localctx, 1);
				{
				setState(2552);
				_errHandler.sync(this);
				switch (_input.LA(1)) {
				case ACCOUNT:
					{
					setState(2403);
					match(ACCOUNT);
					}
					break;
				case ADD:
					{
					setState(2404);
					match(ADD);
					}
					break;
				case ADMIN:
					{
					setState(2405);
					match(ADMIN);
					}
					break;
				case ALL:
					{
					setState(2406);
					match(ALL);
					}
					break;
				case ALTER:
					{
					setState(2407);
					match(ALTER);
					}
					break;
				case ALWAYS:
					{
					setState(2408);
					match(ALWAYS);
					}
					break;
				case ANCESTORS:
					{
					setState(2409);
					match(ANCESTORS);
					}
					break;
				case AND:
					{
					setState(2410);
					match(AND);
					}
					break;
				case ANY_T:
					{
					setState(2411);
					match(ANY_T);
					}
					break;
				case ANYATOMIC_T:
					{
					setState(2412);
					match(ANYATOMIC_T);
					}
					break;
				case ANYJSONATOMIC_T:
					{
					setState(2413);
					match(ANYJSONATOMIC_T);
					}
					break;
				case ANYRECORD_T:
					{
					setState(2414);
					match(ANYRECORD_T);
					}
					break;
				case ARRAY_COLLECT:
					{
					setState(2415);
					match(ARRAY_COLLECT);
					}
					break;
				case AS:
					{
					setState(2416);
					match(AS);
					}
					break;
				case ASC:
					{
					setState(2417);
					match(ASC);
					}
					break;
				case BEFORE:
					{
					setState(2418);
					match(BEFORE);
					}
					break;
				case BETWEEN:
					{
					setState(2419);
					match(BETWEEN);
					}
					break;
				case BY:
					{
					setState(2420);
					match(BY);
					}
					break;
				case CACHE:
					{
					setState(2421);
					match(CACHE);
					}
					break;
				case CASE:
					{
					setState(2422);
					match(CASE);
					}
					break;
				case CAST:
					{
					setState(2423);
					match(CAST);
					}
					break;
				case COLLECTION:
					{
					setState(2424);
					match(COLLECTION);
					}
					break;
				case COMMENT:
					{
					setState(2425);
					match(COMMENT);
					}
					break;
				case COUNT:
					{
					setState(2426);
					match(COUNT);
					}
					break;
				case CREATE:
					{
					setState(2427);
					match(CREATE);
					}
					break;
				case CYCLE:
					{
					setState(2428);
					match(CYCLE);
					}
					break;
				case DAYS:
					{
					setState(2429);
					match(DAYS);
					}
					break;
				case DECLARE:
					{
					setState(2430);
					match(DECLARE);
					}
					break;
				case DEFAULT:
					{
					setState(2431);
					match(DEFAULT);
					}
					break;
				case DELETE:
					{
					setState(2432);
					match(DELETE);
					}
					break;
				case DESC:
					{
					setState(2433);
					match(DESC);
					}
					break;
				case DESCENDANTS:
					{
					setState(2434);
					match(DESCENDANTS);
					}
					break;
				case DESCRIBE:
					{
					setState(2435);
					match(DESCRIBE);
					}
					break;
				case DISABLE:
					{
					setState(2436);
					match(DISABLE);
					}
					break;
				case DISTINCT:
					{
					setState(2437);
					match(DISTINCT);
					}
					break;
				case DROP:
					{
					setState(2438);
					match(DROP);
					}
					break;
				case ELEMENTOF:
					{
					setState(2439);
					match(ELEMENTOF);
					}
					break;
				case ELEMENTS:
					{
					setState(2440);
					match(ELEMENTS);
					}
					break;
				case ELSE:
					{
					setState(2441);
					match(ELSE);
					}
					break;
				case ENABLE:
					{
					setState(2442);
					match(ENABLE);
					}
					break;
				case END:
					{
					setState(2443);
					match(END);
					}
					break;
				case ES_SHARDS:
					{
					setState(2444);
					match(ES_SHARDS);
					}
					break;
				case ES_REPLICAS:
					{
					setState(2445);
					match(ES_REPLICAS);
					}
					break;
				case EXISTS:
					{
					setState(2446);
					match(EXISTS);
					}
					break;
				case EXTRACT:
					{
					setState(2447);
					match(EXTRACT);
					}
					break;
				case FIELDS:
					{
					setState(2448);
					match(FIELDS);
					}
					break;
				case FIRST:
					{
					setState(2449);
					match(FIRST);
					}
					break;
				case FREEZE:
					{
					setState(2450);
					match(FREEZE);
					}
					break;
				case FROM:
					{
					setState(2451);
					match(FROM);
					}
					break;
				case FROZEN:
					{
					setState(2452);
					match(FROZEN);
					}
					break;
				case FULLTEXT:
					{
					setState(2453);
					match(FULLTEXT);
					}
					break;
				case GENERATED:
					{
					setState(2454);
					match(GENERATED);
					}
					break;
				case GRANT:
					{
					setState(2455);
					match(GRANT);
					}
					break;
				case GROUP:
					{
					setState(2456);
					match(GROUP);
					}
					break;
				case HOURS:
					{
					setState(2457);
					match(HOURS);
					}
					break;
				case IDENTIFIED:
					{
					setState(2458);
					match(IDENTIFIED);
					}
					break;
				case IDENTITY:
					{
					setState(2459);
					match(IDENTITY);
					}
					break;
				case IF:
					{
					setState(2460);
					match(IF);
					}
					break;
				case INCREMENT:
					{
					setState(2461);
					match(INCREMENT);
					}
					break;
				case IMAGE:
					{
					setState(2462);
					match(IMAGE);
					}
					break;
				case INDEX:
					{
					setState(2463);
					match(INDEX);
					}
					break;
				case INDEXES:
					{
					setState(2464);
					match(INDEXES);
					}
					break;
				case INSERT:
					{
					setState(2465);
					match(INSERT);
					}
					break;
				case INTO:
					{
					setState(2466);
					match(INTO);
					}
					break;
				case IN:
					{
					setState(2467);
					match(IN);
					}
					break;
				case IS:
					{
					setState(2468);
					match(IS);
					}
					break;
				case JSON:
					{
					setState(2469);
					match(JSON);
					}
					break;
				case KEY:
					{
					setState(2470);
					match(KEY);
					}
					break;
				case KEYOF:
					{
					setState(2471);
					match(KEYOF);
					}
					break;
				case KEYS:
					{
					setState(2472);
					match(KEYS);
					}
					break;
				case LIFETIME:
					{
					setState(2473);
					match(LIFETIME);
					}
					break;
				case LAST:
					{
					setState(2474);
					match(LAST);
					}
					break;
				case LIMIT:
					{
					setState(2475);
					match(LIMIT);
					}
					break;
				case LOCAL:
					{
					setState(2476);
					match(LOCAL);
					}
					break;
				case LOCK:
					{
					setState(2477);
					match(LOCK);
					}
					break;
				case MERGE:
					{
					setState(2478);
					match(MERGE);
					}
					break;
				case MINUTES:
					{
					setState(2479);
					match(MINUTES);
					}
					break;
				case MODIFY:
					{
					setState(2480);
					match(MODIFY);
					}
					break;
				case MR_COUNTER:
					{
					setState(2481);
					match(MR_COUNTER);
					setState(2482);
					match(NAMESPACE);
					}
					break;
				case NAMESPACES:
					{
					setState(2483);
					match(NAMESPACES);
					}
					break;
				case NESTED:
					{
					setState(2484);
					match(NESTED);
					}
					break;
				case NO:
					{
					setState(2485);
					match(NO);
					}
					break;
				case NOT:
					{
					setState(2486);
					match(NOT);
					}
					break;
				case NULLS:
					{
					setState(2487);
					match(NULLS);
					}
					break;
				case OF:
					{
					setState(2488);
					match(OF);
					}
					break;
				case OFFSET:
					{
					setState(2489);
					match(OFFSET);
					}
					break;
				case ON:
					{
					setState(2490);
					match(ON);
					}
					break;
				case OR:
					{
					setState(2491);
					match(OR);
					}
					break;
				case ORDER:
					{
					setState(2492);
					match(ORDER);
					}
					break;
				case OVERRIDE:
					{
					setState(2493);
					match(OVERRIDE);
					}
					break;
				case PER:
					{
					setState(2494);
					match(PER);
					}
					break;
				case PASSWORD:
					{
					setState(2495);
					match(PASSWORD);
					}
					break;
				case PATCH:
					{
					setState(2496);
					match(PATCH);
					}
					break;
				case PRIMARY:
					{
					setState(2497);
					match(PRIMARY);
					}
					break;
				case PUT:
					{
					setState(2498);
					match(PUT);
					}
					break;
				case RDIV:
					{
					setState(2499);
					match(RDIV);
					}
					break;
				case REGION:
					{
					setState(2500);
					match(REGION);
					}
					break;
				case REGIONS:
					{
					setState(2501);
					match(REGIONS);
					}
					break;
				case REMOVE:
					{
					setState(2502);
					match(REMOVE);
					}
					break;
				case RETURNING:
					{
					setState(2503);
					match(RETURNING);
					}
					break;
				case ROW:
					{
					setState(2504);
					match(ROW);
					}
					break;
				case ROLE:
					{
					setState(2505);
					match(ROLE);
					}
					break;
				case ROLES:
					{
					setState(2506);
					match(ROLES);
					}
					break;
				case REVOKE:
					{
					setState(2507);
					match(REVOKE);
					}
					break;
				case SCHEMA:
					{
					setState(2508);
					match(SCHEMA);
					}
					break;
				case SECONDS:
					{
					setState(2509);
					match(SECONDS);
					}
					break;
				case SELECT:
					{
					setState(2510);
					match(SELECT);
					}
					break;
				case SEQ_TRANSFORM:
					{
					setState(2511);
					match(SEQ_TRANSFORM);
					}
					break;
				case SET:
					{
					setState(2512);
					match(SET);
					}
					break;
				case SHARD:
					{
					setState(2513);
					match(SHARD);
					}
					break;
				case SHOW:
					{
					setState(2514);
					match(SHOW);
					}
					break;
				case START:
					{
					setState(2515);
					match(START);
					}
					break;
				case TABLE:
					{
					setState(2516);
					match(TABLE);
					}
					break;
				case TABLES:
					{
					setState(2517);
					match(TABLES);
					}
					break;
				case THEN:
					{
					setState(2518);
					match(THEN);
					}
					break;
				case TO:
					{
					setState(2519);
					match(TO);
					}
					break;
				case TTL:
					{
					setState(2520);
					match(TTL);
					}
					break;
				case TYPE:
					{
					setState(2521);
					match(TYPE);
					}
					break;
				case UNFREEZE:
					{
					setState(2522);
					match(UNFREEZE);
					}
					break;
				case UNLOCK:
					{
					setState(2523);
					match(UNLOCK);
					}
					break;
				case UNIQUE:
					{
					setState(2524);
					match(UNIQUE);
					}
					break;
				case UNNEST:
					{
					setState(2525);
					match(UNNEST);
					}
					break;
				case UPDATE:
					{
					setState(2526);
					match(UPDATE);
					}
					break;
				case UPSERT:
					{
					setState(2527);
					match(UPSERT);
					}
					break;
				case USER:
					{
					setState(2528);
					match(USER);
					}
					break;
				case USERS:
					{
					setState(2529);
					match(USERS);
					}
					break;
				case USING:
					{
					setState(2530);
					match(USING);
					}
					break;
				case VALUES:
					{
					setState(2531);
					match(VALUES);
					}
					break;
				case WHEN:
					{
					setState(2532);
					match(WHEN);
					}
					break;
				case WHERE:
					{
					setState(2533);
					match(WHERE);
					}
					break;
				case WITH:
					{
					setState(2534);
					match(WITH);
					}
					break;
				case ARRAY_T:
					{
					setState(2535);
					match(ARRAY_T);
					}
					break;
				case BINARY_T:
					{
					setState(2536);
					match(BINARY_T);
					}
					break;
				case BOOLEAN_T:
					{
					setState(2537);
					match(BOOLEAN_T);
					}
					break;
				case DOUBLE_T:
					{
					setState(2538);
					match(DOUBLE_T);
					}
					break;
				case ENUM_T:
					{
					setState(2539);
					match(ENUM_T);
					}
					break;
				case FLOAT_T:
					{
					setState(2540);
					match(FLOAT_T);
					}
					break;
				case GEOMETRY_T:
					{
					setState(2541);
					match(GEOMETRY_T);
					}
					break;
				case LONG_T:
					{
					setState(2542);
					match(LONG_T);
					}
					break;
				case INTEGER_T:
					{
					setState(2543);
					match(INTEGER_T);
					}
					break;
				case MAP_T:
					{
					setState(2544);
					match(MAP_T);
					}
					break;
				case NUMBER_T:
					{
					setState(2545);
					match(NUMBER_T);
					}
					break;
				case POINT_T:
					{
					setState(2546);
					match(POINT_T);
					}
					break;
				case RECORD_T:
					{
					setState(2547);
					match(RECORD_T);
					}
					break;
				case STRING_T:
					{
					setState(2548);
					match(STRING_T);
					}
					break;
				case TIMESTAMP_T:
					{
					setState(2549);
					match(TIMESTAMP_T);
					}
					break;
				case SCALAR_T:
					{
					setState(2550);
					match(SCALAR_T);
					}
					break;
				case ID:
					{
					setState(2551);
					match(ID);
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			case BAD_ID:
				enterOuterAlt(_localctx, 2);
				{
				setState(2554);
				match(BAD_ID);

				        notifyErrorListeners("Identifiers must start with a letter: " + _input.getText(_localctx.start, _input.LT(-1)));
				     
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 30:
			return or_expr_sempred((Or_exprContext)_localctx, predIndex);
		case 31:
			return and_expr_sempred((And_exprContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean or_expr_sempred(Or_exprContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 1);
		}
		return true;
	}
	private boolean and_expr_sempred(And_exprContext _localctx, int predIndex) {
		switch (predIndex) {
		case 1:
			return precpred(_ctx, 1);
		}
		return true;
	}

	private static final String _serializedATNSegment0 =
		"\u0004\u0001\u00d9\u09ff\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001"+
		"\u0002\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004"+
		"\u0002\u0005\u0007\u0005\u0002\u0006\u0007\u0006\u0002\u0007\u0007\u0007"+
		"\u0002\b\u0007\b\u0002\t\u0007\t\u0002\n\u0007\n\u0002\u000b\u0007\u000b"+
		"\u0002\f\u0007\f\u0002\r\u0007\r\u0002\u000e\u0007\u000e\u0002\u000f\u0007"+
		"\u000f\u0002\u0010\u0007\u0010\u0002\u0011\u0007\u0011\u0002\u0012\u0007"+
		"\u0012\u0002\u0013\u0007\u0013\u0002\u0014\u0007\u0014\u0002\u0015\u0007"+
		"\u0015\u0002\u0016\u0007\u0016\u0002\u0017\u0007\u0017\u0002\u0018\u0007"+
		"\u0018\u0002\u0019\u0007\u0019\u0002\u001a\u0007\u001a\u0002\u001b\u0007"+
		"\u001b\u0002\u001c\u0007\u001c\u0002\u001d\u0007\u001d\u0002\u001e\u0007"+
		"\u001e\u0002\u001f\u0007\u001f\u0002 \u0007 \u0002!\u0007!\u0002\"\u0007"+
		"\"\u0002#\u0007#\u0002$\u0007$\u0002%\u0007%\u0002&\u0007&\u0002\'\u0007"+
		"\'\u0002(\u0007(\u0002)\u0007)\u0002*\u0007*\u0002+\u0007+\u0002,\u0007"+
		",\u0002-\u0007-\u0002.\u0007.\u0002/\u0007/\u00020\u00070\u00021\u0007"+
		"1\u00022\u00072\u00023\u00073\u00024\u00074\u00025\u00075\u00026\u0007"+
		"6\u00027\u00077\u00028\u00078\u00029\u00079\u0002:\u0007:\u0002;\u0007"+
		";\u0002<\u0007<\u0002=\u0007=\u0002>\u0007>\u0002?\u0007?\u0002@\u0007"+
		"@\u0002A\u0007A\u0002B\u0007B\u0002C\u0007C\u0002D\u0007D\u0002E\u0007"+
		"E\u0002F\u0007F\u0002G\u0007G\u0002H\u0007H\u0002I\u0007I\u0002J\u0007"+
		"J\u0002K\u0007K\u0002L\u0007L\u0002M\u0007M\u0002N\u0007N\u0002O\u0007"+
		"O\u0002P\u0007P\u0002Q\u0007Q\u0002R\u0007R\u0002S\u0007S\u0002T\u0007"+
		"T\u0002U\u0007U\u0002V\u0007V\u0002W\u0007W\u0002X\u0007X\u0002Y\u0007"+
		"Y\u0002Z\u0007Z\u0002[\u0007[\u0002\\\u0007\\\u0002]\u0007]\u0002^\u0007"+
		"^\u0002_\u0007_\u0002`\u0007`\u0002a\u0007a\u0002b\u0007b\u0002c\u0007"+
		"c\u0002d\u0007d\u0002e\u0007e\u0002f\u0007f\u0002g\u0007g\u0002h\u0007"+
		"h\u0002i\u0007i\u0002j\u0007j\u0002k\u0007k\u0002l\u0007l\u0002m\u0007"+
		"m\u0002n\u0007n\u0002o\u0007o\u0002p\u0007p\u0002q\u0007q\u0002r\u0007"+
		"r\u0002s\u0007s\u0002t\u0007t\u0002u\u0007u\u0002v\u0007v\u0002w\u0007"+
		"w\u0002x\u0007x\u0002y\u0007y\u0002z\u0007z\u0002{\u0007{\u0002|\u0007"+
		"|\u0002}\u0007}\u0002~\u0007~\u0002\u007f\u0007\u007f\u0002\u0080\u0007"+
		"\u0080\u0002\u0081\u0007\u0081\u0002\u0082\u0007\u0082\u0002\u0083\u0007"+
		"\u0083\u0002\u0084\u0007\u0084\u0002\u0085\u0007\u0085\u0002\u0086\u0007"+
		"\u0086\u0002\u0087\u0007\u0087\u0002\u0088\u0007\u0088\u0002\u0089\u0007"+
		"\u0089\u0002\u008a\u0007\u008a\u0002\u008b\u0007\u008b\u0002\u008c\u0007"+
		"\u008c\u0002\u008d\u0007\u008d\u0002\u008e\u0007\u008e\u0002\u008f\u0007"+
		"\u008f\u0002\u0090\u0007\u0090\u0002\u0091\u0007\u0091\u0002\u0092\u0007"+
		"\u0092\u0002\u0093\u0007\u0093\u0002\u0094\u0007\u0094\u0002\u0095\u0007"+
		"\u0095\u0002\u0096\u0007\u0096\u0002\u0097\u0007\u0097\u0002\u0098\u0007"+
		"\u0098\u0002\u0099\u0007\u0099\u0002\u009a\u0007\u009a\u0002\u009b\u0007"+
		"\u009b\u0002\u009c\u0007\u009c\u0002\u009d\u0007\u009d\u0002\u009e\u0007"+
		"\u009e\u0002\u009f\u0007\u009f\u0002\u00a0\u0007\u00a0\u0002\u00a1\u0007"+
		"\u00a1\u0002\u00a2\u0007\u00a2\u0002\u00a3\u0007\u00a3\u0002\u00a4\u0007"+
		"\u00a4\u0002\u00a5\u0007\u00a5\u0002\u00a6\u0007\u00a6\u0002\u00a7\u0007"+
		"\u00a7\u0002\u00a8\u0007\u00a8\u0002\u00a9\u0007\u00a9\u0002\u00aa\u0007"+
		"\u00aa\u0002\u00ab\u0007\u00ab\u0002\u00ac\u0007\u00ac\u0002\u00ad\u0007"+
		"\u00ad\u0002\u00ae\u0007\u00ae\u0002\u00af\u0007\u00af\u0002\u00b0\u0007"+
		"\u00b0\u0002\u00b1\u0007\u00b1\u0002\u00b2\u0007\u00b2\u0002\u00b3\u0007"+
		"\u00b3\u0002\u00b4\u0007\u00b4\u0002\u00b5\u0007\u00b5\u0002\u00b6\u0007"+
		"\u00b6\u0002\u00b7\u0007\u00b7\u0002\u00b8\u0007\u00b8\u0002\u00b9\u0007"+
		"\u00b9\u0002\u00ba\u0007\u00ba\u0002\u00bb\u0007\u00bb\u0002\u00bc\u0007"+
		"\u00bc\u0002\u00bd\u0007\u00bd\u0002\u00be\u0007\u00be\u0002\u00bf\u0007"+
		"\u00bf\u0002\u00c0\u0007\u00c0\u0002\u00c1\u0007\u00c1\u0002\u00c2\u0007"+
		"\u00c2\u0002\u00c3\u0007\u00c3\u0002\u00c4\u0007\u00c4\u0002\u00c5\u0007"+
		"\u00c5\u0002\u00c6\u0007\u00c6\u0002\u00c7\u0007\u00c7\u0002\u00c8\u0007"+
		"\u00c8\u0002\u00c9\u0007\u00c9\u0002\u00ca\u0007\u00ca\u0002\u00cb\u0007"+
		"\u00cb\u0002\u00cc\u0007\u00cc\u0002\u00cd\u0007\u00cd\u0002\u00ce\u0007"+
		"\u00ce\u0002\u00cf\u0007\u00cf\u0002\u00d0\u0007\u00d0\u0002\u00d1\u0007"+
		"\u00d1\u0002\u00d2\u0007\u00d2\u0002\u00d3\u0007\u00d3\u0002\u00d4\u0007"+
		"\u00d4\u0002\u00d5\u0007\u00d5\u0002\u00d6\u0007\u00d6\u0002\u00d7\u0007"+
		"\u00d7\u0002\u00d8\u0007\u00d8\u0002\u00d9\u0007\u00d9\u0002\u00da\u0007"+
		"\u00da\u0002\u00db\u0007\u00db\u0002\u00dc\u0007\u00dc\u0002\u00dd\u0007"+
		"\u00dd\u0002\u00de\u0007\u00de\u0002\u00df\u0007\u00df\u0002\u00e0\u0007"+
		"\u00e0\u0002\u00e1\u0007\u00e1\u0001\u0000\u0001\u0000\u0001\u0000\u0001"+
		"\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001"+
		"\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001"+
		"\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001"+
		"\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001\u0001"+
		"\u0001\u0003\u0001\u01e1\b\u0001\u0001\u0002\u0003\u0002\u01e4\b\u0002"+
		"\u0001\u0002\u0001\u0002\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003"+
		"\u0001\u0003\u0001\u0003\u0005\u0003\u01ee\b\u0003\n\u0003\f\u0003\u01f1"+
		"\t\u0003\u0001\u0004\u0001\u0004\u0001\u0004\u0001\u0005\u0001\u0005\u0001"+
		"\u0005\u0001\u0006\u0001\u0006\u0001\u0007\u0001\u0007\u0001\u0007\u0003"+
		"\u0007\u01fe\b\u0007\u0001\u0007\u0003\u0007\u0201\b\u0007\u0001\u0007"+
		"\u0003\u0007\u0204\b\u0007\u0001\u0007\u0003\u0007\u0207\b\u0007\u0001"+
		"\u0007\u0003\u0007\u020a\b\u0007\u0001\b\u0001\b\u0001\b\u0001\b\u0005"+
		"\b\u0210\b\b\n\b\f\b\u0213\t\b\u0001\b\u0001\b\u0001\b\u0003\b\u0218\b"+
		"\b\u0001\b\u0001\b\u0001\b\u0003\b\u021d\b\b\u0005\b\u021f\b\b\n\b\f\b"+
		"\u0222\t\b\u0001\t\u0001\t\u0001\t\u0003\t\u0227\b\t\u0001\n\u0001\n\u0001"+
		"\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0001\n\u0003\n\u0232\b\n\u0001"+
		"\n\u0001\n\u0001\n\u0001\n\u0001\n\u0003\n\u0239\b\n\u0001\n\u0001\n\u0001"+
		"\u000b\u0001\u000b\u0001\u000b\u0005\u000b\u0240\b\u000b\n\u000b\f\u000b"+
		"\u0243\t\u000b\u0001\f\u0001\f\u0001\f\u0005\f\u0248\b\f\n\f\f\f\u024b"+
		"\t\f\u0001\r\u0001\r\u0001\r\u0005\r\u0250\b\r\n\r\f\r\u0253\t\r\u0001"+
		"\u000e\u0001\u000e\u0001\u000e\u0001\u000f\u0001\u000f\u0001\u000f\u0003"+
		"\u000f\u025b\b\u000f\u0001\u0010\u0001\u0010\u0003\u0010\u025f\b\u0010"+
		"\u0001\u0010\u0003\u0010\u0262\b\u0010\u0001\u0011\u0001\u0011\u0003\u0011"+
		"\u0266\b\u0011\u0001\u0012\u0001\u0012\u0001\u0012\u0001\u0012\u0003\u0012"+
		"\u026c\b\u0012\u0001\u0012\u0001\u0012\u0001\u0012\u0001\u0012\u0001\u0012"+
		"\u0003\u0012\u0273\b\u0012\u0001\u0012\u0001\u0012\u0005\u0012\u0277\b"+
		"\u0012\n\u0012\f\u0012\u027a\t\u0012\u0001\u0012\u0001\u0012\u0001\u0013"+
		"\u0001\u0013\u0001\u0013\u0001\u0014\u0001\u0014\u0001\u0014\u0001\u0015"+
		"\u0003\u0015\u0285\b\u0015\u0001\u0015\u0003\u0015\u0288\b\u0015\u0001"+
		"\u0015\u0001\u0015\u0001\u0015\u0001\u0015\u0001\u0015\u0001\u0015\u0001"+
		"\u0015\u0005\u0015\u0291\b\u0015\n\u0015\f\u0015\u0294\t\u0015\u0003\u0015"+
		"\u0296\b\u0015\u0001\u0016\u0001\u0016\u0005\u0016\u029a\b\u0016\n\u0016"+
		"\f\u0016\u029d\t\u0016\u0001\u0016\u0001\u0016\u0001\u0017\u0001\u0017"+
		"\u0001\u0017\u0001\u0017\u0005\u0017\u02a5\b\u0017\n\u0017\f\u0017\u02a8"+
		"\t\u0017\u0001\u0017\u0001\u0017\u0001\u0017\u0001\u0017\u0001\u0017\u0001"+
		"\u0017\u0001\u0017\u0001\u0017\u0001\u0017\u0001\u0017\u0001\u0017\u0001"+
		"\u0017\u0001\u0017\u0001\u0017\u0001\u0017\u0001\u0017\u0001\u0017\u0001"+
		"\u0017\u0003\u0017\u02bc\b\u0017\u0001\u0017\u0003\u0017\u02bf\b\u0017"+
		"\u0001\u0018\u0001\u0018\u0003\u0018\u02c3\b\u0018\u0001\u0019\u0001\u0019"+
		"\u0001\u0019\u0001\u0019\u0001\u0019\u0001\u0019\u0001\u0019\u0001\u0019"+
		"\u0005\u0019\u02cd\b\u0019\n\u0019\f\u0019\u02d0\t\u0019\u0001\u001a\u0003"+
		"\u001a\u02d3\b\u001a\u0001\u001a\u0001\u001a\u0003\u001a\u02d7\b\u001a"+
		"\u0001\u001b\u0001\u001b\u0001\u001b\u0001\u001b\u0001\u001b\u0005\u001b"+
		"\u02de\b\u001b\n\u001b\f\u001b\u02e1\t\u001b\u0001\u001c\u0001\u001c\u0001"+
		"\u001c\u0001\u001d\u0001\u001d\u0001\u001d\u0001\u001e\u0001\u001e\u0001"+
		"\u001e\u0001\u001e\u0001\u001e\u0001\u001e\u0005\u001e\u02ef\b\u001e\n"+
		"\u001e\f\u001e\u02f2\t\u001e\u0001\u001f\u0001\u001f\u0001\u001f\u0001"+
		"\u001f\u0001\u001f\u0001\u001f\u0005\u001f\u02fa\b\u001f\n\u001f\f\u001f"+
		"\u02fd\t\u001f\u0001 \u0003 \u0300\b \u0001 \u0001 \u0001!\u0001!\u0001"+
		"!\u0003!\u0307\b!\u0001!\u0003!\u030a\b!\u0001\"\u0001\"\u0001\"\u0001"+
		"\"\u0001\"\u0003\"\u0311\b\"\u0001#\u0001#\u0001#\u0001#\u0001#\u0001"+
		"#\u0001$\u0001$\u0001$\u0003$\u031c\b$\u0001$\u0001$\u0003$\u0320\b$\u0001"+
		"%\u0001%\u0001&\u0001&\u0001&\u0001&\u0001&\u0001&\u0003&\u032a\b&\u0001"+
		"\'\u0001\'\u0001\'\u0003\'\u032f\b\'\u0001(\u0001(\u0001(\u0001(\u0001"+
		"(\u0001(\u0004(\u0337\b(\u000b(\f(\u0338\u0001(\u0001(\u0001)\u0001)\u0001"+
		")\u0001)\u0005)\u0341\b)\n)\f)\u0344\t)\u0001)\u0001)\u0001*\u0001*\u0001"+
		"*\u0001*\u0005*\u034c\b*\n*\f*\u034f\t*\u0001*\u0001*\u0001+\u0001+\u0001"+
		"+\u0001+\u0001+\u0001+\u0004+\u0359\b+\u000b+\f+\u035a\u0001+\u0001+\u0001"+
		",\u0001,\u0001,\u0001,\u0001,\u0005,\u0364\b,\n,\f,\u0367\t,\u0001,\u0001"+
		",\u0003,\u036b\b,\u0001,\u0001,\u0001,\u0001-\u0001-\u0001-\u0001.\u0001"+
		".\u0001.\u0003.\u0376\b.\u0001.\u0001.\u0003.\u037a\b.\u0001.\u0001.\u0003"+
		".\u037e\b.\u0001.\u0001.\u0001.\u0003.\u0383\b.\u0001.\u0005.\u0386\b"+
		".\n.\f.\u0389\t.\u0001.\u0001.\u0001/\u0001/\u0001/\u0005/\u0390\b/\n"+
		"/\f/\u0393\t/\u00010\u00010\u00010\u00050\u0398\b0\n0\f0\u039b\t0\u0001"+
		"1\u00011\u00011\u00051\u03a0\b1\n1\f1\u03a3\t1\u00012\u00012\u00012\u0003"+
		"2\u03a8\b2\u00013\u00013\u00013\u00053\u03ad\b3\n3\f3\u03b0\t3\u00014"+
		"\u00014\u00014\u00034\u03b5\b4\u00015\u00015\u00015\u00015\u00015\u0003"+
		"5\u03bc\b5\u00016\u00016\u00016\u00036\u03c1\b6\u00016\u00016\u00017\u0001"+
		"7\u00037\u03c7\b7\u00018\u00018\u00038\u03cb\b8\u00018\u00018\u00038\u03cf"+
		"\b8\u00018\u00018\u00019\u00019\u00039\u03d5\b9\u00019\u00019\u0001:\u0001"+
		":\u0001:\u0001:\u0001:\u0001:\u0001:\u0001:\u0001:\u0001:\u0001:\u0001"+
		":\u0001:\u0001:\u0003:\u03e7\b:\u0001;\u0001;\u0001;\u0001;\u0003;\u03ed"+
		"\b;\u0003;\u03ef\b;\u0001<\u0001<\u0001<\u0001<\u0001<\u0003<\u03f6\b"+
		"<\u0001=\u0001=\u0001>\u0001>\u0003>\u03fc\b>\u0001>\u0001>\u0005>\u0400"+
		"\b>\n>\f>\u0403\t>\u0001>\u0001>\u0001?\u0001?\u0001?\u0001?\u0001?\u0001"+
		"?\u0001?\u0001?\u0001?\u0005?\u0410\b?\n?\f?\u0413\t?\u0001?\u0001?\u0001"+
		"?\u0001?\u0003?\u0419\b?\u0001@\u0001@\u0001@\u0001@\u0001@\u0001@\u0001"+
		"@\u0001A\u0001A\u0001B\u0001B\u0001B\u0003B\u0427\bB\u0001B\u0001B\u0001"+
		"B\u0001C\u0001C\u0001C\u0001C\u0001C\u0005C\u0431\bC\nC\fC\u0434\tC\u0003"+
		"C\u0436\bC\u0001C\u0001C\u0001D\u0001D\u0001D\u0001D\u0001D\u0001E\u0001"+
		"E\u0001E\u0001E\u0001E\u0001E\u0001F\u0001F\u0001F\u0001F\u0001F\u0001"+
		"F\u0001F\u0001F\u0001F\u0001F\u0005F\u044f\bF\nF\fF\u0452\tF\u0001F\u0001"+
		"F\u0003F\u0456\bF\u0001F\u0001F\u0001G\u0001G\u0001G\u0001G\u0001G\u0001"+
		"G\u0001G\u0001H\u0001H\u0001H\u0001H\u0001I\u0001I\u0001I\u0001I\u0001"+
		"I\u0001I\u0001I\u0001J\u0003J\u046d\bJ\u0001J\u0001J\u0001J\u0001J\u0003"+
		"J\u0473\bJ\u0001J\u0003J\u0476\bJ\u0001J\u0001J\u0001J\u0001J\u0005J\u047c"+
		"\bJ\nJ\fJ\u047f\tJ\u0001J\u0001J\u0003J\u0483\bJ\u0001J\u0001J\u0001J"+
		"\u0001J\u0001J\u0005J\u048a\bJ\nJ\fJ\u048d\tJ\u0001J\u0001J\u0001J\u0001"+
		"J\u0003J\u0493\bJ\u0001J\u0003J\u0496\bJ\u0001K\u0001K\u0003K\u049a\b"+
		"K\u0001L\u0001L\u0001L\u0001M\u0001M\u0003M\u04a1\bM\u0001N\u0001N\u0001"+
		"N\u0001N\u0001N\u0001N\u0003N\u04a9\bN\u0001O\u0003O\u04ac\bO\u0001O\u0001"+
		"O\u0001O\u0003O\u04b1\bO\u0001O\u0003O\u04b4\bO\u0001O\u0001O\u0001O\u0005"+
		"O\u04b9\bO\nO\fO\u04bc\tO\u0001O\u0001O\u0001O\u0003O\u04c1\bO\u0001P"+
		"\u0001P\u0001P\u0001Q\u0001Q\u0001Q\u0001Q\u0001Q\u0003Q\u04cb\bQ\u0005"+
		"Q\u04cd\bQ\nQ\fQ\u04d0\tQ\u0001Q\u0001Q\u0001Q\u0001Q\u0001Q\u0003Q\u04d7"+
		"\bQ\u0005Q\u04d9\bQ\nQ\fQ\u04dc\tQ\u0001Q\u0001Q\u0001Q\u0001Q\u0001Q"+
		"\u0003Q\u04e3\bQ\u0005Q\u04e5\bQ\nQ\fQ\u04e8\tQ\u0001Q\u0001Q\u0001Q\u0001"+
		"Q\u0001Q\u0003Q\u04ef\bQ\u0005Q\u04f1\bQ\nQ\fQ\u04f4\tQ\u0001Q\u0001Q"+
		"\u0001Q\u0001Q\u0001Q\u0001Q\u0003Q\u04fc\bQ\u0005Q\u04fe\bQ\nQ\fQ\u0501"+
		"\tQ\u0001Q\u0001Q\u0001Q\u0001Q\u0001Q\u0005Q\u0508\bQ\nQ\fQ\u050b\tQ"+
		"\u0003Q\u050d\bQ\u0001R\u0001R\u0001R\u0001R\u0001S\u0003S\u0514\bS\u0001"+
		"S\u0001S\u0003S\u0518\bS\u0001S\u0003S\u051b\bS\u0001S\u0003S\u051e\b"+
		"S\u0001S\u0001S\u0001T\u0003T\u0523\bT\u0001T\u0001T\u0003T\u0527\bT\u0001"+
		"T\u0001T\u0001U\u0001U\u0001V\u0001V\u0001V\u0001V\u0001V\u0001W\u0001"+
		"W\u0001W\u0001W\u0003W\u0536\bW\u0001X\u0001X\u0001X\u0001X\u0001X\u0001"+
		"X\u0003X\u053e\bX\u0001Y\u0001Y\u0001Z\u0001Z\u0001[\u0003[\u0545\b[\u0001"+
		"[\u0001[\u0001[\u0001[\u0003[\u054b\b[\u0001[\u0003[\u054e\b[\u0001[\u0001"+
		"[\u0003[\u0552\b[\u0001[\u0003[\u0555\b[\u0001\\\u0001\\\u0001\\\u0001"+
		"]\u0001]\u0003]\u055c\b]\u0001^\u0001^\u0001^\u0001^\u0001^\u0001^\u0001"+
		"^\u0001^\u0001^\u0001^\u0001^\u0001^\u0001^\u0001^\u0001^\u0003^\u056d"+
		"\b^\u0001_\u0001_\u0001_\u0001_\u0001_\u0005_\u0574\b_\n_\f_\u0577\t_"+
		"\u0001_\u0001_\u0001`\u0001`\u0001`\u0003`\u057e\b`\u0001`\u0003`\u0581"+
		"\b`\u0001a\u0001a\u0003a\u0585\ba\u0001a\u0001a\u0003a\u0589\ba\u0003"+
		"a\u058b\ba\u0001b\u0001b\u0001b\u0001b\u0001b\u0001b\u0003b\u0593\bb\u0001"+
		"c\u0001c\u0001c\u0001d\u0001d\u0001d\u0001d\u0001d\u0001e\u0001e\u0001"+
		"e\u0001e\u0001e\u0001f\u0001f\u0001g\u0001g\u0001h\u0001h\u0001i\u0001"+
		"i\u0001j\u0001j\u0001j\u0001j\u0001j\u0001j\u0001j\u0001j\u0001j\u0001"+
		"j\u0003j\u05b4\bj\u0001k\u0001k\u0001l\u0001l\u0001l\u0001l\u0003l\u05bc"+
		"\bl\u0001m\u0001m\u0001m\u0001m\u0003m\u05c2\bm\u0001n\u0001n\u0001o\u0001"+
		"o\u0001p\u0001p\u0001q\u0001q\u0001r\u0001r\u0001r\u0005r\u05cf\br\nr"+
		"\fr\u05d2\tr\u0001s\u0001s\u0001s\u0005s\u05d7\bs\ns\fs\u05da\ts\u0001"+
		"t\u0003t\u05dd\bt\u0001t\u0001t\u0001u\u0001u\u0001u\u0005u\u05e4\bu\n"+
		"u\fu\u05e7\tu\u0001v\u0001v\u0003v\u05eb\bv\u0001w\u0001w\u0001w\u0001"+
		"w\u0001w\u0003w\u05f2\bw\u0001w\u0001w\u0001x\u0001x\u0001x\u0001x\u0003"+
		"x\u05fa\bx\u0001x\u0001x\u0003x\u05fe\bx\u0001y\u0001y\u0001z\u0001z\u0001"+
		"z\u0001z\u0001{\u0001{\u0001{\u0001{\u0001|\u0001|\u0001|\u0001|\u0001"+
		"|\u0001}\u0001}\u0001}\u0001}\u0001}\u0003}\u0614\b}\u0001}\u0001}\u0003"+
		"}\u0618\b}\u0001}\u0001}\u0001}\u0001}\u0003}\u061e\b}\u0001~\u0001~\u0001"+
		"~\u0003~\u0623\b~\u0001~\u0001~\u0001\u007f\u0001\u007f\u0001\u0080\u0001"+
		"\u0080\u0001\u0080\u0003\u0080\u062c\b\u0080\u0001\u0080\u0001\u0080\u0001"+
		"\u0080\u0001\u0080\u0003\u0080\u0632\b\u0080\u0005\u0080\u0634\b\u0080"+
		"\n\u0080\f\u0080\u0637\t\u0080\u0001\u0081\u0001\u0081\u0001\u0081\u0001"+
		"\u0081\u0001\u0081\u0001\u0081\u0001\u0081\u0003\u0081\u0640\b\u0081\u0001"+
		"\u0081\u0003\u0081\u0643\b\u0081\u0001\u0082\u0001\u0082\u0001\u0082\u0001"+
		"\u0082\u0005\u0082\u0649\b\u0082\n\u0082\f\u0082\u064c\t\u0082\u0001\u0082"+
		"\u0001\u0082\u0001\u0083\u0001\u0083\u0001\u0083\u0001\u0083\u0001\u0083"+
		"\u0001\u0084\u0001\u0084\u0001\u0085\u0001\u0085\u0003\u0085\u0659\b\u0085"+
		"\u0001\u0085\u0001\u0085\u0001\u0085\u0003\u0085\u065e\b\u0085\u0005\u0085"+
		"\u0660\b\u0085\n\u0085\f\u0085\u0663\t\u0085\u0001\u0086\u0001\u0086\u0001"+
		"\u0086\u0001\u0086\u0001\u0086\u0003\u0086\u066a\b\u0086\u0003\u0086\u066c"+
		"\b\u0086\u0001\u0086\u0003\u0086\u066f\b\u0086\u0001\u0086\u0001\u0086"+
		"\u0001\u0087\u0001\u0087\u0001\u0087\u0001\u0087\u0001\u0087\u0001\u0087"+
		"\u0001\u0087\u0001\u0087\u0001\u0087\u0003\u0087\u067c\b\u0087\u0001\u0088"+
		"\u0001\u0088\u0001\u0088\u0005\u0088\u0681\b\u0088\n\u0088\f\u0088\u0684"+
		"\t\u0088\u0001\u0089\u0001\u0089\u0003\u0089\u0688\b\u0089\u0001\u008a"+
		"\u0001\u008a\u0001\u008a\u0001\u008a\u0001\u008b\u0001\u008b\u0001\u008b"+
		"\u0001\u008b\u0001\u008b\u0004\u008b\u0693\b\u008b\u000b\u008b\f\u008b"+
		"\u0694\u0001\u008c\u0001\u008c\u0001\u008c\u0001\u008c\u0001\u008d\u0001"+
		"\u008d\u0001\u008e\u0001\u008e\u0001\u008e\u0001\u008e\u0001\u008f\u0001"+
		"\u008f\u0001\u008f\u0001\u008f\u0003\u008f\u06a5\b\u008f\u0001\u0090\u0001"+
		"\u0090\u0001\u0090\u0001\u0090\u0001\u0091\u0001\u0091\u0001\u0091\u0001"+
		"\u0091\u0003\u0091\u06af\b\u0091\u0001\u0092\u0001\u0092\u0001\u0092\u0001"+
		"\u0092\u0001\u0093\u0001\u0093\u0001\u0093\u0001\u0093\u0001\u0094\u0001"+
		"\u0094\u0001\u0094\u0001\u0094\u0001\u0094\u0001\u0094\u0003\u0094\u06bf"+
		"\b\u0094\u0003\u0094\u06c1\b\u0094\u0001\u0094\u0001\u0094\u0001\u0094"+
		"\u0001\u0094\u0004\u0094\u06c7\b\u0094\u000b\u0094\f\u0094\u06c8\u0001"+
		"\u0094\u0001\u0094\u0003\u0094\u06cd\b\u0094\u0001\u0095\u0001\u0095\u0001"+
		"\u0095\u0001\u0095\u0001\u0095\u0001\u0095\u0001\u0095\u0001\u0095\u0001"+
		"\u0095\u0001\u0095\u0001\u0095\u0001\u0095\u0001\u0095\u0001\u0095\u0001"+
		"\u0095\u0001\u0095\u0001\u0095\u0001\u0095\u0001\u0095\u0001\u0095\u0001"+
		"\u0095\u0003\u0095\u06e4\b\u0095\u0001\u0096\u0001\u0096\u0001\u0096\u0001"+
		"\u0097\u0001\u0097\u0001\u0097\u0001\u0097\u0001\u0097\u0003\u0097\u06ee"+
		"\b\u0097\u0001\u0098\u0001\u0098\u0001\u0098\u0001\u0098\u0001\u0098\u0001"+
		"\u0099\u0001\u0099\u0001\u0099\u0001\u0099\u0001\u0099\u0001\u0099\u0001"+
		"\u0099\u0001\u0099\u0003\u0099\u06fd\b\u0099\u0001\u009a\u0001\u009a\u0001"+
		"\u009a\u0003\u009a\u0702\b\u009a\u0001\u009b\u0001\u009b\u0001\u009b\u0001"+
		"\u009c\u0001\u009c\u0001\u009c\u0001\u009c\u0001\u009d\u0001\u009d\u0001"+
		"\u009d\u0001\u009d\u0001\u009e\u0001\u009e\u0001\u009e\u0001\u009e\u0003"+
		"\u009e\u0713\b\u009e\u0001\u009e\u0001\u009e\u0001\u009e\u0001\u009e\u0003"+
		"\u009e\u0719\b\u009e\u0005\u009e\u071b\b\u009e\n\u009e\f\u009e\u071e\t"+
		"\u009e\u0001\u009e\u0001\u009e\u0001\u009f\u0001\u009f\u0001\u009f\u0001"+
		"\u009f\u0001\u009f\u0001\u009f\u0001\u009f\u0001\u009f\u0003\u009f\u072a"+
		"\b\u009f\u0001\u009f\u0003\u009f\u072d\b\u009f\u0001\u00a0\u0001\u00a0"+
		"\u0001\u00a0\u0001\u00a1\u0001\u00a1\u0001\u00a1\u0001\u00a1\u0003\u00a1"+
		"\u0736\b\u00a1\u0001\u00a1\u0003\u00a1\u0739\b\u00a1\u0001\u00a1\u0001"+
		"\u00a1\u0001\u00a1\u0001\u00a1\u0003\u00a1\u073f\b\u00a1\u0001\u00a2\u0001"+
		"\u00a2\u0001\u00a2\u0005\u00a2\u0744\b\u00a2\n\u00a2\f\u00a2\u0747\t\u00a2"+
		"\u0001\u00a3\u0001\u00a3\u0001\u00a3\u0005\u00a3\u074c\b\u00a3\n\u00a3"+
		"\f\u00a3\u074f\t\u00a3\u0001\u00a4\u0001\u00a4\u0001\u00a4\u0005\u00a4"+
		"\u0754\b\u00a4\n\u00a4\f\u00a4\u0757\t\u00a4\u0001\u00a4\u0001\u00a4\u0001"+
		"\u00a4\u0003\u00a4\u075c\b\u00a4\u0001\u00a5\u0001\u00a5\u0001\u00a5\u0001"+
		"\u00a5\u0003\u00a5\u0762\b\u00a5\u0001\u00a5\u0001\u00a5\u0001\u00a6\u0001"+
		"\u00a6\u0001\u00a6\u0001\u00a6\u0001\u00a6\u0003\u00a6\u076b\b\u00a6\u0001"+
		"\u00a6\u0001\u00a6\u0001\u00a6\u0001\u00a6\u0001\u00a6\u0001\u00a6\u0001"+
		"\u00a6\u0001\u00a6\u0003\u00a6\u0775\b\u00a6\u0001\u00a6\u0003\u00a6\u0778"+
		"\b\u00a6\u0001\u00a6\u0001\u00a6\u0001\u00a6\u0001\u00a6\u0001\u00a6\u0003"+
		"\u00a6\u077f\b\u00a6\u0001\u00a6\u0001\u00a6\u0001\u00a6\u0001\u00a6\u0003"+
		"\u00a6\u0785\b\u00a6\u0001\u00a6\u0003\u00a6\u0788\b\u00a6\u0001\u00a7"+
		"\u0001\u00a7\u0001\u00a8\u0001\u00a8\u0001\u00a8\u0005\u00a8\u078f\b\u00a8"+
		"\n\u00a8\f\u00a8\u0792\t\u00a8\u0001\u00a9\u0001\u00a9\u0003\u00a9\u0796"+
		"\b\u00a9\u0001\u00a9\u0003\u00a9\u0799\b\u00a9\u0001\u00aa\u0001\u00aa"+
		"\u0001\u00aa\u0003\u00aa\u079e\b\u00aa\u0001\u00aa\u0003\u00aa\u07a1\b"+
		"\u00aa\u0001\u00aa\u0003\u00aa\u07a4\b\u00aa\u0001\u00aa\u0001\u00aa\u0001"+
		"\u00ab\u0001\u00ab\u0004\u00ab\u07aa\b\u00ab\u000b\u00ab\f\u00ab\u07ab"+
		"\u0001\u00ac\u0003\u00ac\u07af\b\u00ac\u0001\u00ac\u0001\u00ac\u0001\u00ac"+
		"\u0003\u00ac\u07b4\b\u00ac\u0003\u00ac\u07b6\b\u00ac\u0001\u00ac\u0003"+
		"\u00ac\u07b9\b\u00ac\u0001\u00ad\u0001\u00ad\u0001\u00ad\u0001\u00ad\u0001"+
		"\u00ad\u0003\u00ad\u07c0\b\u00ad\u0001\u00ad\u0001\u00ad\u0001\u00ad\u0001"+
		"\u00ad\u0001\u00ad\u0001\u00ad\u0001\u00ad\u0001\u00ad\u0001\u00ad\u0001"+
		"\u00ad\u0003\u00ad\u07cc\b\u00ad\u0001\u00ae\u0001\u00ae\u0001\u00af\u0001"+
		"\u00af\u0001\u00af\u0001\u00af\u0001\u00af\u0001\u00af\u0001\u00af\u0001"+
		"\u00af\u0001\u00af\u0005\u00af\u07d9\b\u00af\n\u00af\f\u00af\u07dc\t\u00af"+
		"\u0001\u00af\u0001\u00af\u0001\u00af\u0001\u00af\u0001\u00af\u0001\u00af"+
		"\u0001\u00af\u0001\u00af\u0001\u00af\u0001\u00af\u0003\u00af\u07e8\b\u00af"+
		"\u0001\u00b0\u0001\u00b0\u0001\u00b0\u0001\u00b1\u0001\u00b1\u0001\u00b1"+
		"\u0001\u00b1\u0001\u00b1\u0001\u00b1\u0001\u00b1\u0001\u00b1\u0001\u00b1"+
		"\u0001\u00b1\u0003\u00b1\u07f7\b\u00b1\u0001\u00b1\u0003\u00b1\u07fa\b"+
		"\u00b1\u0001\u00b2\u0001\u00b2\u0001\u00b2\u0001\u00b2\u0001\u00b2\u0001"+
		"\u00b2\u0003\u00b2\u0802\b\u00b2\u0001\u00b2\u0001\u00b2\u0001\u00b2\u0001"+
		"\u00b2\u0001\u00b2\u0003\u00b2\u0809\b\u00b2\u0001\u00b2\u0003\u00b2\u080c"+
		"\b\u00b2\u0001\u00b2\u0003\u00b2\u080f\b\u00b2\u0001\u00b3\u0001\u00b3"+
		"\u0001\u00b3\u0001\u00b3\u0001\u00b3\u0001\u00b3\u0001\u00b3\u0001\u00b3"+
		"\u0003\u00b3\u0819\b\u00b3\u0001\u00b4\u0001\u00b4\u0001\u00b4\u0005\u00b4"+
		"\u081e\b\u00b4\n\u00b4\f\u00b4\u0821\t\u00b4\u0001\u00b5\u0001\u00b5\u0003"+
		"\u00b5\u0825\b\u00b5\u0001\u00b6\u0001\u00b6\u0005\u00b6\u0829\b\u00b6"+
		"\n\u00b6\f\u00b6\u082c\t\u00b6\u0001\u00b7\u0001\u00b7\u0001\u00b7\u0001"+
		"\u00b7\u0001\u00b7\u0001\u00b7\u0003\u00b7\u0834\b\u00b7\u0001\u00b8\u0001"+
		"\u00b8\u0001\u00b8\u0001\u00b8\u0003\u00b8\u083a\b\u00b8\u0001\u00b8\u0001"+
		"\u00b8\u0001\u00b8\u0001\u00b8\u0003\u00b8\u0840\b\u00b8\u0001\u00b9\u0001"+
		"\u00b9\u0001\u00b9\u0003\u00b9\u0845\b\u00b9\u0001\u00b9\u0001\u00b9\u0001"+
		"\u00b9\u0001\u00b9\u0001\u00b9\u0001\u00b9\u0001\u00b9\u0001\u00b9\u0001"+
		"\u00b9\u0001\u00b9\u0003\u00b9\u0851\b\u00b9\u0001\u00b9\u0001\u00b9\u0001"+
		"\u00b9\u0001\u00b9\u0001\u00b9\u0003\u00b9\u0858\b\u00b9\u0001\u00ba\u0001"+
		"\u00ba\u0001\u00ba\u0005\u00ba\u085d\b\u00ba\n\u00ba\f\u00ba\u0860\t\u00ba"+
		"\u0001\u00bb\u0001\u00bb\u0001\u00bb\u0003\u00bb\u0865\b\u00bb\u0001\u00bb"+
		"\u0001\u00bb\u0001\u00bb\u0001\u00bb\u0001\u00bb\u0001\u00bb\u0001\u00bb"+
		"\u0001\u00bb\u0001\u00bb\u0001\u00bb\u0001\u00bb\u0001\u00bb\u0001\u00bb"+
		"\u0001\u00bb\u0003\u00bb\u0875\b\u00bb\u0001\u00bc\u0001\u00bc\u0001\u00bc"+
		"\u0001\u00bc\u0003\u00bc\u087b\b\u00bc\u0001\u00bc\u0003\u00bc\u087e\b"+
		"\u00bc\u0001\u00bd\u0001\u00bd\u0001\u00bd\u0001\u00bd\u0001\u00be\u0001"+
		"\u00be\u0001\u00be\u0001\u00be\u0003\u00be\u0888\b\u00be\u0001\u00be\u0003"+
		"\u00be\u088b\b\u00be\u0001\u00be\u0003\u00be\u088e\b\u00be\u0001\u00be"+
		"\u0003\u00be\u0891\b\u00be\u0001\u00be\u0003\u00be\u0894\b\u00be\u0001"+
		"\u00bf\u0001\u00bf\u0001\u00bf\u0001\u00bf\u0003\u00bf\u089a\b\u00bf\u0001"+
		"\u00c0\u0001\u00c0\u0001\u00c0\u0001\u00c0\u0001\u00c1\u0001\u00c1\u0001"+
		"\u00c1\u0001\u00c1\u0003\u00c1\u08a4\b\u00c1\u0001\u00c2\u0001\u00c2\u0001"+
		"\u00c2\u0001\u00c2\u0003\u00c2\u08aa\b\u00c2\u0001\u00c3\u0001\u00c3\u0003"+
		"\u00c3\u08ae\b\u00c3\u0001\u00c4\u0001\u00c4\u0001\u00c4\u0001\u00c5\u0001"+
		"\u00c5\u0001\u00c5\u0003\u00c5\u08b6\b\u00c5\u0001\u00c5\u0003\u00c5\u08b9"+
		"\b\u00c5\u0001\u00c5\u0001\u00c5\u0001\u00c5\u0003\u00c5\u08be\b\u00c5"+
		"\u0001\u00c6\u0001\u00c6\u0001\u00c6\u0001\u00c7\u0001\u00c7\u0001\u00c7"+
		"\u0001\u00c7\u0001\u00c8\u0001\u00c8\u0003\u00c8\u08c9\b\u00c8\u0001\u00c9"+
		"\u0001\u00c9\u0001\u00c9\u0001\u00ca\u0001\u00ca\u0001\u00ca\u0001\u00ca"+
		"\u0001\u00cb\u0001\u00cb\u0001\u00cb\u0001\u00cb\u0001\u00cc\u0001\u00cc"+
		"\u0001\u00cc\u0001\u00cc\u0001\u00cc\u0003\u00cc\u08db\b\u00cc\u0001\u00cc"+
		"\u0001\u00cc\u0001\u00cc\u0001\u00cd\u0001\u00cd\u0001\u00cd\u0001\u00cd"+
		"\u0001\u00ce\u0001\u00ce\u0001\u00ce\u0001\u00ce\u0001\u00cf\u0001\u00cf"+
		"\u0001\u00cf\u0001\u00cf\u0001\u00cf\u0003\u00cf\u08ed\b\u00cf\u0001\u00cf"+
		"\u0001\u00cf\u0001\u00cf\u0001\u00d0\u0001\u00d0\u0001\u00d0\u0001\u00d0"+
		"\u0003\u00d0\u08f6\b\u00d0\u0001\u00d1\u0001\u00d1\u0001\u00d1\u0005\u00d1"+
		"\u08fb\b\u00d1\n\u00d1\f\u00d1\u08fe\t\u00d1\u0001\u00d2\u0001\u00d2\u0003"+
		"\u00d2\u0902\b\u00d2\u0001\u00d3\u0001\u00d3\u0003\u00d3\u0906\b\u00d3"+
		"\u0001\u00d3\u0001\u00d3\u0001\u00d3\u0003\u00d3\u090b\b\u00d3\u0005\u00d3"+
		"\u090d\b\u00d3\n\u00d3\f\u00d3\u0910\t\u00d3\u0001\u00d4\u0001\u00d4\u0001"+
		"\u00d5\u0001\u00d5\u0001\u00d5\u0001\u00d5\u0001\u00d5\u0001\u00d5\u0001"+
		"\u00d5\u0003\u00d5\u091b\b\u00d5\u0001\u00d6\u0001\u00d6\u0001\u00d6\u0001"+
		"\u00d6\u0005\u00d6\u0921\b\u00d6\n\u00d6\f\u00d6\u0924\t\u00d6\u0001\u00d6"+
		"\u0001\u00d6\u0001\u00d6\u0001\u00d6\u0003\u00d6\u092a\b\u00d6\u0001\u00d7"+
		"\u0001\u00d7\u0001\u00d7\u0001\u00d7\u0005\u00d7\u0930\b\u00d7\n\u00d7"+
		"\f\u00d7\u0933\t\u00d7\u0001\u00d7\u0001\u00d7\u0001\u00d7\u0001\u00d7"+
		"\u0003\u00d7\u0939\b\u00d7\u0001\u00d8\u0001\u00d8\u0001\u00d8\u0001\u00d8"+
		"\u0001\u00d9\u0001\u00d9\u0001\u00d9\u0001\u00d9\u0001\u00d9\u0001\u00d9"+
		"\u0001\u00d9\u0003\u00d9\u0946\b\u00d9\u0001\u00da\u0001\u00da\u0001\u00da"+
		"\u0001\u00db\u0001\u00db\u0001\u00db\u0001\u00dc\u0001\u00dc\u0001\u00dd"+
		"\u0003\u00dd\u0951\b\u00dd\u0001\u00dd\u0001\u00dd\u0001\u00de\u0003\u00de"+
		"\u0956\b\u00de\u0001\u00de\u0001\u00de\u0001\u00df\u0001\u00df\u0001\u00e0"+
		"\u0001\u00e0\u0001\u00e0\u0005\u00e0\u095f\b\u00e0\n\u00e0\f\u00e0\u0962"+
		"\t\u00e0\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001"+
		"\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001"+
		"\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001"+
		"\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001"+
		"\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001"+
		"\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001"+
		"\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001"+
		"\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001"+
		"\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001"+
		"\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001"+
		"\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001"+
		"\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001"+
		"\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001"+
		"\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001"+
		"\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001"+
		"\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001"+
		"\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001"+
		"\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001"+
		"\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001"+
		"\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001"+
		"\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001"+
		"\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001"+
		"\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001"+
		"\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001"+
		"\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0001\u00e1\u0003"+
		"\u00e1\u09f9\b\u00e1\u0001\u00e1\u0001\u00e1\u0003\u00e1\u09fd\b\u00e1"+
		"\u0001\u00e1\u0000\u0002<>\u00e2\u0000\u0002\u0004\u0006\b\n\f\u000e\u0010"+
		"\u0012\u0014\u0016\u0018\u001a\u001c\u001e \"$&(*,.02468:<>@BDFHJLNPR"+
		"TVXZ\\^`bdfhjlnprtvxz|~\u0080\u0082\u0084\u0086\u0088\u008a\u008c\u008e"+
		"\u0090\u0092\u0094\u0096\u0098\u009a\u009c\u009e\u00a0\u00a2\u00a4\u00a6"+
		"\u00a8\u00aa\u00ac\u00ae\u00b0\u00b2\u00b4\u00b6\u00b8\u00ba\u00bc\u00be"+
		"\u00c0\u00c2\u00c4\u00c6\u00c8\u00ca\u00cc\u00ce\u00d0\u00d2\u00d4\u00d6"+
		"\u00d8\u00da\u00dc\u00de\u00e0\u00e2\u00e4\u00e6\u00e8\u00ea\u00ec\u00ee"+
		"\u00f0\u00f2\u00f4\u00f6\u00f8\u00fa\u00fc\u00fe\u0100\u0102\u0104\u0106"+
		"\u0108\u010a\u010c\u010e\u0110\u0112\u0114\u0116\u0118\u011a\u011c\u011e"+
		"\u0120\u0122\u0124\u0126\u0128\u012a\u012c\u012e\u0130\u0132\u0134\u0136"+
		"\u0138\u013a\u013c\u013e\u0140\u0142\u0144\u0146\u0148\u014a\u014c\u014e"+
		"\u0150\u0152\u0154\u0156\u0158\u015a\u015c\u015e\u0160\u0162\u0164\u0166"+
		"\u0168\u016a\u016c\u016e\u0170\u0172\u0174\u0176\u0178\u017a\u017c\u017e"+
		"\u0180\u0182\u0184\u0186\u0188\u018a\u018c\u018e\u0190\u0192\u0194\u0196"+
		"\u0198\u019a\u019c\u019e\u01a0\u01a2\u01a4\u01a6\u01a8\u01aa\u01ac\u01ae"+
		"\u01b0\u01b2\u01b4\u01b6\u01b8\u01ba\u01bc\u01be\u01c0\u01c2\u0000\u0012"+
		"\u0002\u0000\u000f\u000f!!\u0002\u000011MM\u0001\u0000\u00b9\u00be\u0001"+
		"\u0000\u00c5\u00c6\u0002\u0000\u00b5\u00b5\u00c7\u00c8\u0002\u0000LL\u008b"+
		"\u008b\u0002\u0000\u0005\u0005\u00b7\u00b8\u0002\u0000EE\u0087\u0087\u0002"+
		"\u0000\u001d\u001d<<\u0003\u0000\u00b5\u00b5\u00b8\u00b8\u00c5\u00c5\u0001"+
		"\u0000\u009f\u00a0\u0003\u0000\u009b\u009b\u009d\u009d\u00a2\u00a2\u0002"+
		"\u0000\u009f\u00a0\u00a2\u00a2\u0002\u0000!!##\u0002\u0000RR\u0085\u0085"+
		"\u0004\u0000\u001d\u001d<<UUww\u0001\u0000\u00cd\u00cf\u0001\u0000\u00d0"+
		"\u00d1\u0b3e\u0000\u01c4\u0001\u0000\u0000\u0000\u0002\u01e0\u0001\u0000"+
		"\u0000\u0000\u0004\u01e3\u0001\u0000\u0000\u0000\u0006\u01e7\u0001\u0000"+
		"\u0000\u0000\b\u01f2\u0001\u0000\u0000\u0000\n\u01f5\u0001\u0000\u0000"+
		"\u0000\f\u01f8\u0001\u0000\u0000\u0000\u000e\u01fa\u0001\u0000\u0000\u0000"+
		"\u0010\u020b\u0001\u0000\u0000\u0000\u0012\u0226\u0001\u0000\u0000\u0000"+
		"\u0014\u0228\u0001\u0000\u0000\u0000\u0016\u023c\u0001\u0000\u0000\u0000"+
		"\u0018\u0244\u0001\u0000\u0000\u0000\u001a\u024c\u0001\u0000\u0000\u0000"+
		"\u001c\u0254\u0001\u0000\u0000\u0000\u001e\u0257\u0001\u0000\u0000\u0000"+
		" \u025c\u0001\u0000\u0000\u0000\"\u0265\u0001\u0000\u0000\u0000$\u0267"+
		"\u0001\u0000\u0000\u0000&\u027d\u0001\u0000\u0000\u0000(\u0280\u0001\u0000"+
		"\u0000\u0000*\u0284\u0001\u0000\u0000\u0000,\u0297\u0001\u0000\u0000\u0000"+
		".\u02bb\u0001\u0000\u0000\u00000\u02c2\u0001\u0000\u0000\u00002\u02c4"+
		"\u0001\u0000\u0000\u00004\u02d2\u0001\u0000\u0000\u00006\u02d8\u0001\u0000"+
		"\u0000\u00008\u02e2\u0001\u0000\u0000\u0000:\u02e5\u0001\u0000\u0000\u0000"+
		"<\u02e8\u0001\u0000\u0000\u0000>\u02f3\u0001\u0000\u0000\u0000@\u02ff"+
		"\u0001\u0000\u0000\u0000B\u0303\u0001\u0000\u0000\u0000D\u0310\u0001\u0000"+
		"\u0000\u0000F\u0312\u0001\u0000\u0000\u0000H\u0318\u0001\u0000\u0000\u0000"+
		"J\u0321\u0001\u0000\u0000\u0000L\u0329\u0001\u0000\u0000\u0000N\u032e"+
		"\u0001\u0000\u0000\u0000P\u0330\u0001\u0000\u0000\u0000R\u033c\u0001\u0000"+
		"\u0000\u0000T\u0347\u0001\u0000\u0000\u0000V\u0352\u0001\u0000\u0000\u0000"+
		"X\u036a\u0001\u0000\u0000\u0000Z\u036f\u0001\u0000\u0000\u0000\\\u0372"+
		"\u0001\u0000\u0000\u0000^\u038c\u0001\u0000\u0000\u0000`\u0394\u0001\u0000"+
		"\u0000\u0000b\u039c\u0001\u0000\u0000\u0000d\u03a7\u0001\u0000\u0000\u0000"+
		"f\u03a9\u0001\u0000\u0000\u0000h\u03b1\u0001\u0000\u0000\u0000j\u03bb"+
		"\u0001\u0000\u0000\u0000l\u03bd\u0001\u0000\u0000\u0000n\u03c6\u0001\u0000"+
		"\u0000\u0000p\u03c8\u0001\u0000\u0000\u0000r\u03d2\u0001\u0000\u0000\u0000"+
		"t\u03e6\u0001\u0000\u0000\u0000v\u03e8\u0001\u0000\u0000\u0000x\u03f5"+
		"\u0001\u0000\u0000\u0000z\u03f7\u0001\u0000\u0000\u0000|\u03f9\u0001\u0000"+
		"\u0000\u0000~\u0418\u0001\u0000\u0000\u0000\u0080\u041a\u0001\u0000\u0000"+
		"\u0000\u0082\u0421\u0001\u0000\u0000\u0000\u0084\u0423\u0001\u0000\u0000"+
		"\u0000\u0086\u042b\u0001\u0000\u0000\u0000\u0088\u0439\u0001\u0000\u0000"+
		"\u0000\u008a\u043e\u0001\u0000\u0000\u0000\u008c\u0444\u0001\u0000\u0000"+
		"\u0000\u008e\u0459\u0001\u0000\u0000\u0000\u0090\u0460\u0001\u0000\u0000"+
		"\u0000\u0092\u0464\u0001\u0000\u0000\u0000\u0094\u046c\u0001\u0000\u0000"+
		"\u0000\u0096\u0499\u0001\u0000\u0000\u0000\u0098\u049b\u0001\u0000\u0000"+
		"\u0000\u009a\u04a0\u0001\u0000\u0000\u0000\u009c\u04a8\u0001\u0000\u0000"+
		"\u0000\u009e\u04ab\u0001\u0000\u0000\u0000\u00a0\u04c2\u0001\u0000\u0000"+
		"\u0000\u00a2\u050c\u0001\u0000\u0000\u0000\u00a4\u050e\u0001\u0000\u0000"+
		"\u0000\u00a6\u0513\u0001\u0000\u0000\u0000\u00a8\u0522\u0001\u0000\u0000"+
		"\u0000\u00aa\u052a\u0001\u0000\u0000\u0000\u00ac\u052c\u0001\u0000\u0000"+
		"\u0000\u00ae\u0535\u0001\u0000\u0000\u0000\u00b0\u053d\u0001\u0000\u0000"+
		"\u0000\u00b2\u053f\u0001\u0000\u0000\u0000\u00b4\u0541\u0001\u0000\u0000"+
		"\u0000\u00b6\u0544\u0001\u0000\u0000\u0000\u00b8\u0556\u0001\u0000\u0000"+
		"\u0000\u00ba\u0559\u0001\u0000\u0000\u0000\u00bc\u056c\u0001\u0000\u0000"+
		"\u0000\u00be\u056e\u0001\u0000\u0000\u0000\u00c0\u057a\u0001\u0000\u0000"+
		"\u0000\u00c2\u058a\u0001\u0000\u0000\u0000\u00c4\u058c\u0001\u0000\u0000"+
		"\u0000\u00c6\u0594\u0001\u0000\u0000\u0000\u00c8\u0597\u0001\u0000\u0000"+
		"\u0000\u00ca\u059c\u0001\u0000\u0000\u0000\u00cc\u05a1\u0001\u0000\u0000"+
		"\u0000\u00ce\u05a3\u0001\u0000\u0000\u0000\u00d0\u05a5\u0001\u0000\u0000"+
		"\u0000\u00d2\u05a7\u0001\u0000\u0000\u0000\u00d4\u05b3\u0001\u0000\u0000"+
		"\u0000\u00d6\u05b5\u0001\u0000\u0000\u0000\u00d8\u05b7\u0001\u0000\u0000"+
		"\u0000\u00da\u05bd\u0001\u0000\u0000\u0000\u00dc\u05c3\u0001\u0000\u0000"+
		"\u0000\u00de\u05c5\u0001\u0000\u0000\u0000\u00e0\u05c7\u0001\u0000\u0000"+
		"\u0000\u00e2\u05c9\u0001\u0000\u0000\u0000\u00e4\u05cb\u0001\u0000\u0000"+
		"\u0000\u00e6\u05d3\u0001\u0000\u0000\u0000\u00e8\u05dc\u0001\u0000\u0000"+
		"\u0000\u00ea\u05e0\u0001\u0000\u0000\u0000\u00ec\u05ea\u0001\u0000\u0000"+
		"\u0000\u00ee\u05ec\u0001\u0000\u0000\u0000\u00f0\u05f5\u0001\u0000\u0000"+
		"\u0000\u00f2\u05ff\u0001\u0000\u0000\u0000\u00f4\u0601\u0001\u0000\u0000"+
		"\u0000\u00f6\u0605\u0001\u0000\u0000\u0000\u00f8\u0609\u0001\u0000\u0000"+
		"\u0000\u00fa\u060e\u0001\u0000\u0000\u0000\u00fc\u0622\u0001\u0000\u0000"+
		"\u0000\u00fe\u0626\u0001\u0000\u0000\u0000\u0100\u062b\u0001\u0000\u0000"+
		"\u0000\u0102\u0638\u0001\u0000\u0000\u0000\u0104\u0644\u0001\u0000\u0000"+
		"\u0000\u0106\u064f\u0001\u0000\u0000\u0000\u0108\u0654\u0001\u0000\u0000"+
		"\u0000\u010a\u0658\u0001\u0000\u0000\u0000\u010c\u0664\u0001\u0000\u0000"+
		"\u0000\u010e\u067b\u0001\u0000\u0000\u0000\u0110\u067d\u0001\u0000\u0000"+
		"\u0000\u0112\u0685\u0001\u0000\u0000\u0000\u0114\u0689\u0001\u0000\u0000"+
		"\u0000\u0116\u0692\u0001\u0000\u0000\u0000\u0118\u0696\u0001\u0000\u0000"+
		"\u0000\u011a\u069a\u0001\u0000\u0000\u0000\u011c\u069c\u0001\u0000\u0000"+
		"\u0000\u011e\u06a0\u0001\u0000\u0000\u0000\u0120\u06a6\u0001\u0000\u0000"+
		"\u0000\u0122\u06aa\u0001\u0000\u0000\u0000\u0124\u06b0\u0001\u0000\u0000"+
		"\u0000\u0126\u06b4\u0001\u0000\u0000\u0000\u0128\u06b8\u0001\u0000\u0000"+
		"\u0000\u012a\u06e3\u0001\u0000\u0000\u0000\u012c\u06e5\u0001\u0000\u0000"+
		"\u0000\u012e\u06e8\u0001\u0000\u0000\u0000\u0130\u06ef\u0001\u0000\u0000"+
		"\u0000\u0132\u06fc\u0001\u0000\u0000\u0000\u0134\u06fe\u0001\u0000\u0000"+
		"\u0000\u0136\u0703\u0001\u0000\u0000\u0000\u0138\u0706\u0001\u0000\u0000"+
		"\u0000\u013a\u070a\u0001\u0000\u0000\u0000\u013c\u070e\u0001\u0000\u0000"+
		"\u0000\u013e\u0721\u0001\u0000\u0000\u0000\u0140\u072e\u0001\u0000\u0000"+
		"\u0000\u0142\u0731\u0001\u0000\u0000\u0000\u0144\u0740\u0001\u0000\u0000"+
		"\u0000\u0146\u0748\u0001\u0000\u0000\u0000\u0148\u075b\u0001\u0000\u0000"+
		"\u0000\u014a\u075d\u0001\u0000\u0000\u0000\u014c\u0765\u0001\u0000\u0000"+
		"\u0000\u014e\u0789\u0001\u0000\u0000\u0000\u0150\u078b\u0001\u0000\u0000"+
		"\u0000\u0152\u0798\u0001\u0000\u0000\u0000\u0154\u079a\u0001\u0000\u0000"+
		"\u0000\u0156\u07a9\u0001\u0000\u0000\u0000\u0158\u07b8\u0001\u0000\u0000"+
		"\u0000\u015a\u07cb\u0001\u0000\u0000\u0000\u015c\u07cd\u0001\u0000\u0000"+
		"\u0000\u015e\u07cf\u0001\u0000\u0000\u0000\u0160\u07e9\u0001\u0000\u0000"+
		"\u0000\u0162\u07ec\u0001\u0000\u0000\u0000\u0164\u07fb\u0001\u0000\u0000"+
		"\u0000\u0166\u0818\u0001\u0000\u0000\u0000\u0168\u081a\u0001\u0000\u0000"+
		"\u0000\u016a\u0822\u0001\u0000\u0000\u0000\u016c\u0826\u0001\u0000\u0000"+
		"\u0000\u016e\u0833\u0001\u0000\u0000\u0000\u0170\u0835\u0001\u0000\u0000"+
		"\u0000\u0172\u0841\u0001\u0000\u0000\u0000\u0174\u0859\u0001\u0000\u0000"+
		"\u0000\u0176\u0861\u0001\u0000\u0000\u0000\u0178\u0876\u0001\u0000\u0000"+
		"\u0000\u017a\u087f\u0001\u0000\u0000\u0000\u017c\u0883\u0001\u0000\u0000"+
		"\u0000\u017e\u0895\u0001\u0000\u0000\u0000\u0180\u089b\u0001\u0000\u0000"+
		"\u0000\u0182\u089f\u0001\u0000\u0000\u0000\u0184\u08a5\u0001\u0000\u0000"+
		"\u0000\u0186\u08ad\u0001\u0000\u0000\u0000\u0188\u08af\u0001\u0000\u0000"+
		"\u0000\u018a\u08bd\u0001\u0000\u0000\u0000\u018c\u08bf\u0001\u0000\u0000"+
		"\u0000\u018e\u08c2\u0001\u0000\u0000\u0000\u0190\u08c6\u0001\u0000\u0000"+
		"\u0000\u0192\u08ca\u0001\u0000\u0000\u0000\u0194\u08cd\u0001\u0000\u0000"+
		"\u0000\u0196\u08d1\u0001\u0000\u0000\u0000\u0198\u08d5\u0001\u0000\u0000"+
		"\u0000\u019a\u08df\u0001\u0000\u0000\u0000\u019c\u08e3\u0001\u0000\u0000"+
		"\u0000\u019e\u08e7\u0001\u0000\u0000\u0000\u01a0\u08f5\u0001\u0000\u0000"+
		"\u0000\u01a2\u08f7\u0001\u0000\u0000\u0000\u01a4\u0901\u0001\u0000\u0000"+
		"\u0000\u01a6\u0905\u0001\u0000\u0000\u0000\u01a8\u0911\u0001\u0000\u0000"+
		"\u0000\u01aa\u091a\u0001\u0000\u0000\u0000\u01ac\u0929\u0001\u0000\u0000"+
		"\u0000\u01ae\u0938\u0001\u0000\u0000\u0000\u01b0\u093a\u0001\u0000\u0000"+
		"\u0000\u01b2\u0945\u0001\u0000\u0000\u0000\u01b4\u0947\u0001\u0000\u0000"+
		"\u0000\u01b6\u094a\u0001\u0000\u0000\u0000\u01b8\u094d\u0001\u0000\u0000"+
		"\u0000\u01ba\u0950\u0001\u0000\u0000\u0000\u01bc\u0955\u0001\u0000\u0000"+
		"\u0000\u01be\u0959\u0001\u0000\u0000\u0000\u01c0\u095b\u0001\u0000\u0000"+
		"\u0000\u01c2\u09fc\u0001\u0000\u0000\u0000\u01c4\u01c5\u0003\u0002\u0001"+
		"\u0000\u01c5\u01c6\u0005\u0000\u0000\u0001\u01c6\u0001\u0001\u0000\u0000"+
		"\u0000\u01c7\u01e1\u0003\u0004\u0002\u0000\u01c8\u01e1\u0003\u0094J\u0000"+
		"\u01c9\u01e1\u0003\u009eO\u0000\u01ca\u01e1\u0003\u00b6[\u0000\u01cb\u01e1"+
		"\u0003\u00fa}\u0000\u01cc\u01e1\u0003\u014c\u00a6\u0000\u01cd\u01e1\u0003"+
		"\u0178\u00bc\u0000\u01ce\u01e1\u0003\u017a\u00bd\u0000\u01cf\u01e1\u0003"+
		"\u00eew\u0000\u01d0\u01e1\u0003\u00f4z\u0000\u01d1\u01e1\u0003\u0170\u00b8"+
		"\u0000\u01d2\u01e1\u0003\u00f0x\u0000\u01d3\u01e1\u0003\u00f6{\u0000\u01d4"+
		"\u01e1\u0003\u0164\u00b2\u0000\u01d5\u01e1\u0003\u0180\u00c0\u0000\u01d6"+
		"\u01e1\u0003\u017e\u00bf\u0000\u01d7\u01e1\u0003\u0130\u0098\u0000\u01d8"+
		"\u01e1\u0003\u017c\u00be\u0000\u01d9\u01e1\u0003\u014a\u00a5\u0000\u01da"+
		"\u01e1\u0003\u0182\u00c1\u0000\u01db\u01e1\u0003\u0184\u00c2\u0000\u01dc"+
		"\u01e1\u0003\u0172\u00b9\u0000\u01dd\u01e1\u0003\u00f8|\u0000\u01de\u01e1"+
		"\u0003\u0176\u00bb\u0000\u01df\u01e1\u0003\n\u0005\u0000\u01e0\u01c7\u0001"+
		"\u0000\u0000\u0000\u01e0\u01c8\u0001\u0000\u0000\u0000\u01e0\u01c9\u0001"+
		"\u0000\u0000\u0000\u01e0\u01ca\u0001\u0000\u0000\u0000\u01e0\u01cb\u0001"+
		"\u0000\u0000\u0000\u01e0\u01cc\u0001\u0000\u0000\u0000\u01e0\u01cd\u0001"+
		"\u0000\u0000\u0000\u01e0\u01ce\u0001\u0000\u0000\u0000\u01e0\u01cf\u0001"+
		"\u0000\u0000\u0000\u01e0\u01d0\u0001\u0000\u0000\u0000\u01e0\u01d1\u0001"+
		"\u0000\u0000\u0000\u01e0\u01d2\u0001\u0000\u0000\u0000\u01e0\u01d3\u0001"+
		"\u0000\u0000\u0000\u01e0\u01d4\u0001\u0000\u0000\u0000\u01e0\u01d5\u0001"+
		"\u0000\u0000\u0000\u01e0\u01d6\u0001\u0000\u0000\u0000\u01e0\u01d7\u0001"+
		"\u0000\u0000\u0000\u01e0\u01d8\u0001\u0000\u0000\u0000\u01e0\u01d9\u0001"+
		"\u0000\u0000\u0000\u01e0\u01da\u0001\u0000\u0000\u0000\u01e0\u01db\u0001"+
		"\u0000\u0000\u0000\u01e0\u01dc\u0001\u0000\u0000\u0000\u01e0\u01dd\u0001"+
		"\u0000\u0000\u0000\u01e0\u01de\u0001\u0000\u0000\u0000\u01e0\u01df\u0001"+
		"\u0000\u0000\u0000\u01e1\u0003\u0001\u0000\u0000\u0000\u01e2\u01e4\u0003"+
		"\u0006\u0003\u0000\u01e3\u01e2\u0001\u0000\u0000\u0000\u01e3\u01e4\u0001"+
		"\u0000\u0000\u0000\u01e4\u01e5\u0001\u0000\u0000\u0000\u01e5\u01e6\u0003"+
		"\u000e\u0007\u0000\u01e6\u0005\u0001\u0000\u0000\u0000\u01e7\u01e8\u0005"+
		"\u001e\u0000\u0000\u01e8\u01e9\u0003\b\u0004\u0000\u01e9\u01ef\u0005\u00ac"+
		"\u0000\u0000\u01ea\u01eb\u0003\b\u0004\u0000\u01eb\u01ec\u0005\u00ac\u0000"+
		"\u0000\u01ec\u01ee\u0001\u0000\u0000\u0000\u01ed\u01ea\u0001\u0000\u0000"+
		"\u0000\u01ee\u01f1\u0001\u0000\u0000\u0000\u01ef\u01ed\u0001\u0000\u0000"+
		"\u0000\u01ef\u01f0\u0001\u0000\u0000\u0000\u01f0\u0007\u0001\u0000\u0000"+
		"\u0000\u01f1\u01ef\u0001\u0000\u0000\u0000\u01f2\u01f3\u0005\u0005\u0000"+
		"\u0000\u01f3\u01f4\u0003\u00bc^\u0000\u01f4\t\u0001\u0000\u0000\u0000"+
		"\u01f5\u01f6\u0003\u0006\u0003\u0000\u01f6\u01f7\u0003\u0086C\u0000\u01f7"+
		"\u000b\u0001\u0000\u0000\u0000\u01f8\u01f9\u0003<\u001e\u0000\u01f9\r"+
		"\u0001\u0000\u0000\u0000\u01fa\u01fb\u0003(\u0014\u0000\u01fb\u01fd\u0003"+
		"\u0010\b\u0000\u01fc\u01fe\u0003&\u0013\u0000\u01fd\u01fc\u0001\u0000"+
		"\u0000\u0000\u01fd\u01fe\u0001\u0000\u0000\u0000\u01fe\u0200\u0001\u0000"+
		"\u0000\u0000\u01ff\u0201\u00036\u001b\u0000\u0200\u01ff\u0001\u0000\u0000"+
		"\u0000\u0200\u0201\u0001\u0000\u0000\u0000\u0201\u0203\u0001\u0000\u0000"+
		"\u0000\u0202\u0204\u00032\u0019\u0000\u0203\u0202\u0001\u0000\u0000\u0000"+
		"\u0203\u0204\u0001\u0000\u0000\u0000\u0204\u0206\u0001\u0000\u0000\u0000"+
		"\u0205\u0207\u00038\u001c\u0000\u0206\u0205\u0001\u0000\u0000\u0000\u0206"+
		"\u0207\u0001\u0000\u0000\u0000\u0207\u0209\u0001\u0000\u0000\u0000\u0208"+
		"\u020a\u0003:\u001d\u0000\u0209\u0208\u0001\u0000\u0000\u0000\u0209\u020a"+
		"\u0001\u0000\u0000\u0000\u020a\u000f\u0001\u0000\u0000\u0000\u020b\u020c"+
		"\u00056\u0000\u0000\u020c\u0211\u0003\u0012\t\u0000\u020d\u020e\u0005"+
		"\u00ad\u0000\u0000\u020e\u0210\u0003\u0012\t\u0000\u020f\u020d\u0001\u0000"+
		"\u0000\u0000\u0210\u0213\u0001\u0000\u0000\u0000\u0211\u020f\u0001\u0000"+
		"\u0000\u0000\u0211\u0212\u0001\u0000\u0000\u0000\u0212\u0220\u0001\u0000"+
		"\u0000\u0000\u0213\u0211\u0001\u0000\u0000\u0000\u0214\u021c\u0005\u00ad"+
		"\u0000\u0000\u0215\u0217\u0003\f\u0006\u0000\u0216\u0218\u0005\u000e\u0000"+
		"\u0000\u0217\u0216\u0001\u0000\u0000\u0000\u0217\u0218\u0001\u0000\u0000"+
		"\u0000\u0218\u0219\u0001\u0000\u0000\u0000\u0219\u021a\u0005\u0005\u0000"+
		"\u0000\u021a\u021d\u0001\u0000\u0000\u0000\u021b\u021d\u0003$\u0012\u0000"+
		"\u021c\u0215\u0001\u0000\u0000\u0000\u021c\u021b\u0001\u0000\u0000\u0000"+
		"\u021d\u021f\u0001\u0000\u0000\u0000\u021e\u0214\u0001\u0000\u0000\u0000"+
		"\u021f\u0222\u0001\u0000\u0000\u0000\u0220\u021e\u0001\u0000\u0000\u0000"+
		"\u0220\u0221\u0001\u0000\u0000\u0000\u0221\u0011\u0001\u0000\u0000\u0000"+
		"\u0222\u0220\u0001\u0000\u0000\u0000\u0223\u0227\u0003\u001e\u000f\u0000"+
		"\u0224\u0227\u0003\u0014\n\u0000\u0225\u0227\u0003\u001a\r\u0000\u0226"+
		"\u0223\u0001\u0000\u0000\u0000\u0226\u0224\u0001\u0000\u0000\u0000\u0226"+
		"\u0225\u0001\u0000\u0000\u0000\u0227\u0013\u0001\u0000\u0000\u0000\u0228"+
		"\u0229\u0005[\u0000\u0000\u0229\u022a\u0005\u007f\u0000\u0000\u022a\u022b"+
		"\u0005\u00af\u0000\u0000\u022b\u0231\u0003\u001e\u000f\u0000\u022c\u022d"+
		"\u0005\f\u0000\u0000\u022d\u022e\u0005\u00af\u0000\u0000\u022e\u022f\u0003"+
		"\u0016\u000b\u0000\u022f\u0230\u0005\u00b0\u0000\u0000\u0230\u0232\u0001"+
		"\u0000\u0000\u0000\u0231\u022c\u0001\u0000\u0000\u0000\u0231\u0232\u0001"+
		"\u0000\u0000\u0000\u0232\u0238\u0001\u0000\u0000\u0000\u0233\u0234\u0005"+
		"\"\u0000\u0000\u0234\u0235\u0005\u00af\u0000\u0000\u0235\u0236\u0003\u0018"+
		"\f\u0000\u0236\u0237\u0005\u00b0\u0000\u0000\u0237\u0239\u0001\u0000\u0000"+
		"\u0000\u0238\u0233\u0001\u0000\u0000\u0000\u0238\u0239\u0001\u0000\u0000"+
		"\u0000\u0239\u023a\u0001\u0000\u0000\u0000\u023a\u023b\u0005\u00b0\u0000"+
		"\u0000\u023b\u0015\u0001\u0000\u0000\u0000\u023c\u0241\u0003\u001e\u000f"+
		"\u0000\u023d\u023e\u0005\u00ad\u0000\u0000\u023e\u0240\u0003\u001e\u000f"+
		"\u0000\u023f\u023d\u0001\u0000\u0000\u0000\u0240\u0243\u0001\u0000\u0000"+
		"\u0000\u0241\u023f\u0001\u0000\u0000\u0000\u0241\u0242\u0001\u0000\u0000"+
		"\u0000\u0242\u0017\u0001\u0000\u0000\u0000\u0243\u0241\u0001\u0000\u0000"+
		"\u0000\u0244\u0249\u0003\u001e\u000f\u0000\u0245\u0246\u0005\u00ad\u0000"+
		"\u0000\u0246\u0248\u0003\u001e\u000f\u0000\u0247\u0245\u0001\u0000\u0000"+
		"\u0000\u0248\u024b\u0001\u0000\u0000\u0000\u0249\u0247\u0001\u0000\u0000"+
		"\u0000\u0249\u024a\u0001\u0000\u0000\u0000\u024a\u0019\u0001\u0000\u0000"+
		"\u0000\u024b\u0249\u0001\u0000\u0000\u0000\u024c\u024d\u0003\u001e\u000f"+
		"\u0000\u024d\u0251\u0003\u001c\u000e\u0000\u024e\u0250\u0003\u001c\u000e"+
		"\u0000\u024f\u024e\u0001\u0000\u0000\u0000\u0250\u0253\u0001\u0000\u0000"+
		"\u0000\u0251\u024f\u0001\u0000\u0000\u0000\u0251\u0252\u0001\u0000\u0000"+
		"\u0000\u0252\u001b\u0001\u0000\u0000\u0000\u0253\u0251\u0001\u0000\u0000"+
		"\u0000\u0254\u0255\u0005\u0097\u0000\u0000\u0255\u0256\u0003\u001e\u000f"+
		"\u0000\u0256\u001d\u0001\u0000\u0000\u0000\u0257\u025a\u0003 \u0010\u0000"+
		"\u0258\u0259\u0005a\u0000\u0000\u0259\u025b\u0003<\u001e\u0000\u025a\u0258"+
		"\u0001\u0000\u0000\u0000\u025a\u025b\u0001\u0000\u0000\u0000\u025b\u001f"+
		"\u0001\u0000\u0000\u0000\u025c\u0261\u0003\u00fc~\u0000\u025d\u025f\u0005"+
		"\u000e\u0000\u0000\u025e\u025d\u0001\u0000\u0000\u0000\u025e\u025f\u0001"+
		"\u0000\u0000\u0000\u025f\u0260\u0001\u0000\u0000\u0000\u0260\u0262\u0003"+
		"\"\u0011\u0000\u0261\u025e\u0001\u0000\u0000\u0000\u0261\u0262\u0001\u0000"+
		"\u0000\u0000\u0262!\u0001\u0000\u0000\u0000\u0263\u0266\u0005\u0005\u0000"+
		"\u0000\u0264\u0266\u0003\u01c2\u00e1\u0000\u0265\u0263\u0001\u0000\u0000"+
		"\u0000\u0265\u0264\u0001\u0000\u0000\u0000\u0266#\u0001\u0000\u0000\u0000"+
		"\u0267\u0268\u0005\u0090\u0000\u0000\u0268\u0269\u0005\u00af\u0000\u0000"+
		"\u0269\u026b\u0003f3\u0000\u026a\u026c\u0005\u000e\u0000\u0000\u026b\u026a"+
		"\u0001\u0000\u0000\u0000\u026b\u026c\u0001\u0000\u0000\u0000\u026c\u026d"+
		"\u0001\u0000\u0000\u0000\u026d\u026e\u0005\u0005\u0000\u0000\u026e\u0278"+
		"\u0001\u0000\u0000\u0000\u026f\u0270\u0005\u00ad\u0000\u0000\u0270\u0272"+
		"\u0003f3\u0000\u0271\u0273\u0005\u000e\u0000\u0000\u0272\u0271\u0001\u0000"+
		"\u0000\u0000\u0272\u0273\u0001\u0000\u0000\u0000\u0273\u0274\u0001\u0000"+
		"\u0000\u0000\u0274\u0275\u0005\u0005\u0000\u0000\u0275\u0277\u0001\u0000"+
		"\u0000\u0000\u0276\u026f\u0001\u0000\u0000\u0000\u0277\u027a\u0001\u0000"+
		"\u0000\u0000\u0278\u0276\u0001\u0000\u0000\u0000\u0278\u0279\u0001\u0000"+
		"\u0000\u0000\u0279\u027b\u0001\u0000\u0000\u0000\u027a\u0278\u0001\u0000"+
		"\u0000\u0000\u027b\u027c\u0005\u00b0\u0000\u0000\u027c%\u0001\u0000\u0000"+
		"\u0000\u027d\u027e\u0005\u008d\u0000\u0000\u027e\u027f\u0003\f\u0006\u0000"+
		"\u027f\'\u0001\u0000\u0000\u0000\u0280\u0281\u0005x\u0000\u0000\u0281"+
		"\u0282\u0003*\u0015\u0000\u0282)\u0001\u0000\u0000\u0000\u0283\u0285\u0003"+
		",\u0016\u0000\u0284\u0283\u0001\u0000\u0000\u0000\u0284\u0285\u0001\u0000"+
		"\u0000\u0000\u0285\u0287\u0001\u0000\u0000\u0000\u0286\u0288\u0005%\u0000"+
		"\u0000\u0287\u0286\u0001\u0000\u0000\u0000\u0287\u0288\u0001\u0000\u0000"+
		"\u0000\u0288\u0295\u0001\u0000\u0000\u0000\u0289\u0296\u0005\u00b5\u0000"+
		"\u0000\u028a\u028b\u0003\f\u0006\u0000\u028b\u0292\u00030\u0018\u0000"+
		"\u028c\u028d\u0005\u00ad\u0000\u0000\u028d\u028e\u0003\f\u0006\u0000\u028e"+
		"\u028f\u00030\u0018\u0000\u028f\u0291\u0001\u0000\u0000\u0000\u0290\u028c"+
		"\u0001\u0000\u0000\u0000\u0291\u0294\u0001\u0000\u0000\u0000\u0292\u0290"+
		"\u0001\u0000\u0000\u0000\u0292\u0293\u0001\u0000\u0000\u0000\u0293\u0296"+
		"\u0001\u0000\u0000\u0000\u0294\u0292\u0001\u0000\u0000\u0000\u0295\u0289"+
		"\u0001\u0000\u0000\u0000\u0295\u028a\u0001\u0000\u0000\u0000\u0296+\u0001"+
		"\u0000\u0000\u0000\u0297\u029b\u0005\u0001\u0000\u0000\u0298\u029a\u0003"+
		".\u0017\u0000\u0299\u0298\u0001\u0000\u0000\u0000\u029a\u029d\u0001\u0000"+
		"\u0000\u0000\u029b\u0299\u0001\u0000\u0000\u0000\u029b\u029c\u0001\u0000"+
		"\u0000\u0000\u029c\u029e\u0001\u0000\u0000\u0000\u029d\u029b\u0001\u0000"+
		"\u0000\u0000\u029e\u029f\u0005\u0002\u0000\u0000\u029f-\u0001\u0000\u0000"+
		"\u0000\u02a0\u02a1\u0005j\u0000\u0000\u02a1\u02a2\u0005\u00af\u0000\u0000"+
		"\u02a2\u02a6\u0003\u00fc~\u0000\u02a3\u02a5\u0003\u014e\u00a7\u0000\u02a4"+
		"\u02a3\u0001\u0000\u0000\u0000\u02a5\u02a8\u0001\u0000\u0000\u0000\u02a6"+
		"\u02a4\u0001\u0000\u0000\u0000\u02a6\u02a7\u0001\u0000\u0000\u0000\u02a7"+
		"\u02a9\u0001\u0000\u0000\u0000\u02a8\u02a6\u0001\u0000\u0000\u0000\u02a9"+
		"\u02aa\u0005\u00b0\u0000\u0000\u02aa\u02bc\u0001\u0000\u0000\u0000\u02ab"+
		"\u02ac\u00053\u0000\u0000\u02ac\u02ad\u0005\u00af\u0000\u0000\u02ad\u02ae"+
		"\u0003\u00fc~\u0000\u02ae\u02af\u0003\u014e\u00a7\u0000\u02af\u02b0\u0005"+
		"\u00b0\u0000\u0000\u02b0\u02bc\u0001\u0000\u0000\u0000\u02b1\u02b2\u0005"+
		"k\u0000\u0000\u02b2\u02b3\u0005\u00af\u0000\u0000\u02b3\u02b4\u0003\u00fc"+
		"~\u0000\u02b4\u02b5\u0005\u00b0\u0000\u0000\u02b5\u02bc\u0001\u0000\u0000"+
		"\u0000\u02b6\u02b7\u00054\u0000\u0000\u02b7\u02b8\u0005\u00af\u0000\u0000"+
		"\u02b8\u02b9\u0003\u00fc~\u0000\u02b9\u02ba\u0005\u00b0\u0000\u0000\u02ba"+
		"\u02bc\u0001\u0000\u0000\u0000\u02bb\u02a0\u0001\u0000\u0000\u0000\u02bb"+
		"\u02ab\u0001\u0000\u0000\u0000\u02bb\u02b1\u0001\u0000\u0000\u0000\u02bb"+
		"\u02b6\u0001\u0000\u0000\u0000\u02bc\u02be\u0001\u0000\u0000\u0000\u02bd"+
		"\u02bf\u0005\u00d1\u0000\u0000\u02be\u02bd\u0001\u0000\u0000\u0000\u02be"+
		"\u02bf\u0001\u0000\u0000\u0000\u02bf/\u0001\u0000\u0000\u0000\u02c0\u02c1"+
		"\u0005\u000e\u0000\u0000\u02c1\u02c3\u0003\u01c2\u00e1\u0000\u02c2\u02c0"+
		"\u0001\u0000\u0000\u0000\u02c2\u02c3\u0001\u0000\u0000\u0000\u02c31\u0001"+
		"\u0000\u0000\u0000\u02c4\u02c5\u0005d\u0000\u0000\u02c5\u02c6\u0005\u0013"+
		"\u0000\u0000\u02c6\u02c7\u0003\f\u0006\u0000\u02c7\u02ce\u00034\u001a"+
		"\u0000\u02c8\u02c9\u0005\u00ad\u0000\u0000\u02c9\u02ca\u0003\f\u0006\u0000"+
		"\u02ca\u02cb\u00034\u001a\u0000\u02cb\u02cd\u0001\u0000\u0000\u0000\u02cc"+
		"\u02c8\u0001\u0000\u0000\u0000\u02cd\u02d0\u0001\u0000\u0000\u0000\u02ce"+
		"\u02cc\u0001\u0000\u0000\u0000\u02ce\u02cf\u0001\u0000\u0000\u0000\u02cf"+
		"3\u0001\u0000\u0000\u0000\u02d0\u02ce\u0001\u0000\u0000\u0000\u02d1\u02d3"+
		"\u0007\u0000\u0000\u0000\u02d2\u02d1\u0001\u0000\u0000\u0000\u02d2\u02d3"+
		"\u0001\u0000\u0000\u0000\u02d3\u02d6\u0001\u0000\u0000\u0000\u02d4\u02d5"+
		"\u0005^\u0000\u0000\u02d5\u02d7\u0007\u0001\u0000\u0000\u02d6\u02d4\u0001"+
		"\u0000\u0000\u0000\u02d6\u02d7\u0001\u0000\u0000\u0000\u02d75\u0001\u0000"+
		"\u0000\u0000\u02d8\u02d9\u0005;\u0000\u0000\u02d9\u02da\u0005\u0013\u0000"+
		"\u0000\u02da\u02df\u0003\f\u0006\u0000\u02db\u02dc\u0005\u00ad\u0000\u0000"+
		"\u02dc\u02de\u0003\f\u0006\u0000\u02dd\u02db\u0001\u0000\u0000\u0000\u02de"+
		"\u02e1\u0001\u0000\u0000\u0000\u02df\u02dd\u0001\u0000\u0000\u0000\u02df"+
		"\u02e0\u0001\u0000\u0000\u0000\u02e07\u0001\u0000\u0000\u0000\u02e1\u02df"+
		"\u0001\u0000\u0000\u0000\u02e2\u02e3\u0005P\u0000\u0000\u02e3\u02e4\u0003"+
		"`0\u0000\u02e49\u0001\u0000\u0000\u0000\u02e5\u02e6\u0005_\u0000\u0000"+
		"\u02e6\u02e7\u0003`0\u0000\u02e7;\u0001\u0000\u0000\u0000\u02e8\u02e9"+
		"\u0006\u001e\uffff\uffff\u0000\u02e9\u02ea\u0003>\u001f\u0000\u02ea\u02f0"+
		"\u0001\u0000\u0000\u0000\u02eb\u02ec\n\u0001\u0000\u0000\u02ec\u02ed\u0005"+
		"c\u0000\u0000\u02ed\u02ef\u0003>\u001f\u0000\u02ee\u02eb\u0001\u0000\u0000"+
		"\u0000\u02ef\u02f2\u0001\u0000\u0000\u0000\u02f0\u02ee\u0001\u0000\u0000"+
		"\u0000\u02f0\u02f1\u0001\u0000\u0000\u0000\u02f1=\u0001\u0000\u0000\u0000"+
		"\u02f2\u02f0\u0001\u0000\u0000\u0000\u02f3\u02f4\u0006\u001f\uffff\uffff"+
		"\u0000\u02f4\u02f5\u0003@ \u0000\u02f5\u02fb\u0001\u0000\u0000\u0000\u02f6"+
		"\u02f7\n\u0001\u0000\u0000\u02f7\u02f8\u0005\r\u0000\u0000\u02f8\u02fa"+
		"\u0003@ \u0000\u02f9\u02f6\u0001\u0000\u0000\u0000\u02fa\u02fd\u0001\u0000"+
		"\u0000\u0000\u02fb\u02f9\u0001\u0000\u0000\u0000\u02fb\u02fc\u0001\u0000"+
		"\u0000\u0000\u02fc?\u0001\u0000\u0000\u0000\u02fd\u02fb\u0001\u0000\u0000"+
		"\u0000\u02fe\u0300\u0005]\u0000\u0000\u02ff\u02fe\u0001\u0000\u0000\u0000"+
		"\u02ff\u0300\u0001\u0000\u0000\u0000\u0300\u0301\u0001\u0000\u0000\u0000"+
		"\u0301\u0302\u0003B!\u0000\u0302A\u0001\u0000\u0000\u0000\u0303\u0309"+
		"\u0003D\"\u0000\u0304\u0306\u0005G\u0000\u0000\u0305\u0307\u0005]\u0000"+
		"\u0000\u0306\u0305\u0001\u0000\u0000\u0000\u0306\u0307\u0001\u0000\u0000"+
		"\u0000\u0307\u0308\u0001\u0000\u0000\u0000\u0308\u030a\u0005\u00ca\u0000"+
		"\u0000\u0309\u0304\u0001\u0000\u0000\u0000\u0309\u030a\u0001\u0000\u0000"+
		"\u0000\u030aC\u0001\u0000\u0000\u0000\u030b\u0311\u0003F#\u0000\u030c"+
		"\u0311\u0003H$\u0000\u030d\u0311\u0003N\'\u0000\u030e\u0311\u0003Z-\u0000"+
		"\u030f\u0311\u0003\\.\u0000\u0310\u030b\u0001\u0000\u0000\u0000\u0310"+
		"\u030c\u0001\u0000\u0000\u0000\u0310\u030d\u0001\u0000\u0000\u0000\u0310"+
		"\u030e\u0001\u0000\u0000\u0000\u0310\u030f\u0001\u0000\u0000\u0000\u0311"+
		"E\u0001\u0000\u0000\u0000\u0312\u0313\u0003^/\u0000\u0313\u0314\u0005"+
		"\u0012\u0000\u0000\u0314\u0315\u0003^/\u0000\u0315\u0316\u0005\r\u0000"+
		"\u0000\u0316\u0317\u0003^/\u0000\u0317G\u0001\u0000\u0000\u0000\u0318"+
		"\u031f\u0003^/\u0000\u0319\u031c\u0003J%\u0000\u031a\u031c\u0003L&\u0000"+
		"\u031b\u0319\u0001\u0000\u0000\u0000\u031b\u031a\u0001\u0000\u0000\u0000"+
		"\u031c\u031d\u0001\u0000\u0000\u0000\u031d\u031e\u0003^/\u0000\u031e\u0320"+
		"\u0001\u0000\u0000\u0000\u031f\u031b\u0001\u0000\u0000\u0000\u031f\u0320"+
		"\u0001\u0000\u0000\u0000\u0320I\u0001\u0000\u0000\u0000\u0321\u0322\u0007"+
		"\u0002\u0000\u0000\u0322K\u0001\u0000\u0000\u0000\u0323\u032a\u0005\u00c3"+
		"\u0000\u0000\u0324\u032a\u0005\u00c4\u0000\u0000\u0325\u032a\u0005\u00c1"+
		"\u0000\u0000\u0326\u032a\u0005\u00c2\u0000\u0000\u0327\u032a\u0005\u00bf"+
		"\u0000\u0000\u0328\u032a\u0005\u00c0\u0000\u0000\u0329\u0323\u0001\u0000"+
		"\u0000\u0000\u0329\u0324\u0001\u0000\u0000\u0000\u0329\u0325\u0001\u0000"+
		"\u0000\u0000\u0329\u0326\u0001\u0000\u0000\u0000\u0329\u0327\u0001\u0000"+
		"\u0000\u0000\u0329\u0328\u0001\u0000\u0000\u0000\u032aM\u0001\u0000\u0000"+
		"\u0000\u032b\u032f\u0003P(\u0000\u032c\u032f\u0003V+\u0000\u032d\u032f"+
		"\u0003X,\u0000\u032e\u032b\u0001\u0000\u0000\u0000\u032e\u032c\u0001\u0000"+
		"\u0000\u0000\u032e\u032d\u0001\u0000\u0000\u0000\u032fO\u0001\u0000\u0000"+
		"\u0000\u0330\u0331\u0003R)\u0000\u0331\u0332\u0005A\u0000\u0000\u0332"+
		"\u0333\u0005\u00af\u0000\u0000\u0333\u0336\u0003T*\u0000\u0334\u0335\u0005"+
		"\u00ad\u0000\u0000\u0335\u0337\u0003T*\u0000\u0336\u0334\u0001\u0000\u0000"+
		"\u0000\u0337\u0338\u0001\u0000\u0000\u0000\u0338\u0336\u0001\u0000\u0000"+
		"\u0000\u0338\u0339\u0001\u0000\u0000\u0000\u0339\u033a\u0001\u0000\u0000"+
		"\u0000\u033a\u033b\u0005\u00b0\u0000\u0000\u033bQ\u0001\u0000\u0000\u0000"+
		"\u033c\u033d\u0005\u00af\u0000\u0000\u033d\u0342\u0003^/\u0000\u033e\u033f"+
		"\u0005\u00ad\u0000\u0000\u033f\u0341\u0003^/\u0000\u0340\u033e\u0001\u0000"+
		"\u0000\u0000\u0341\u0344\u0001\u0000\u0000\u0000\u0342\u0340\u0001\u0000"+
		"\u0000\u0000\u0342\u0343\u0001\u0000\u0000\u0000\u0343\u0345\u0001\u0000"+
		"\u0000\u0000\u0344\u0342\u0001\u0000\u0000\u0000\u0345\u0346\u0005\u00b0"+
		"\u0000\u0000\u0346S\u0001\u0000\u0000\u0000\u0347\u0348\u0005\u00af\u0000"+
		"\u0000\u0348\u034d\u0003\f\u0006\u0000\u0349\u034a\u0005\u00ad\u0000\u0000"+
		"\u034a\u034c\u0003\f\u0006\u0000\u034b\u0349\u0001\u0000\u0000\u0000\u034c"+
		"\u034f\u0001\u0000\u0000\u0000\u034d\u034b\u0001\u0000\u0000\u0000\u034d"+
		"\u034e\u0001\u0000\u0000\u0000\u034e\u0350\u0001\u0000\u0000\u0000\u034f"+
		"\u034d\u0001\u0000\u0000\u0000\u0350\u0351\u0005\u00b0\u0000\u0000\u0351"+
		"U\u0001\u0000\u0000\u0000\u0352\u0353\u0003^/\u0000\u0353\u0354\u0005"+
		"A\u0000\u0000\u0354\u0355\u0005\u00af\u0000\u0000\u0355\u0358\u0003\f"+
		"\u0006\u0000\u0356\u0357\u0005\u00ad\u0000\u0000\u0357\u0359\u0003\f\u0006"+
		"\u0000\u0358\u0356\u0001\u0000\u0000\u0000\u0359\u035a\u0001\u0000\u0000"+
		"\u0000\u035a\u0358\u0001\u0000\u0000\u0000\u035a\u035b\u0001\u0000\u0000"+
		"\u0000\u035b\u035c\u0001\u0000\u0000\u0000\u035c\u035d\u0005\u00b0\u0000"+
		"\u0000\u035dW\u0001\u0000\u0000\u0000\u035e\u036b\u0003^/\u0000\u035f"+
		"\u0360\u0005\u00af\u0000\u0000\u0360\u0365\u0003^/\u0000\u0361\u0362\u0005"+
		"\u00ad\u0000\u0000\u0362\u0364\u0003^/\u0000\u0363\u0361\u0001\u0000\u0000"+
		"\u0000\u0364\u0367\u0001\u0000\u0000\u0000\u0365\u0363\u0001\u0000\u0000"+
		"\u0000\u0365\u0366\u0001\u0000\u0000\u0000\u0366\u0368\u0001\u0000\u0000"+
		"\u0000\u0367\u0365\u0001\u0000\u0000\u0000\u0368\u0369\u0005\u00b0\u0000"+
		"\u0000\u0369\u036b\u0001\u0000\u0000\u0000\u036a\u035e\u0001\u0000\u0000"+
		"\u0000\u036a\u035f\u0001\u0000\u0000\u0000\u036b\u036c\u0001\u0000\u0000"+
		"\u0000\u036c\u036d\u0005A\u0000\u0000\u036d\u036e\u0003f3\u0000\u036e"+
		"Y\u0001\u0000\u0000\u0000\u036f\u0370\u0005.\u0000\u0000\u0370\u0371\u0003"+
		"^/\u0000\u0371[\u0001\u0000\u0000\u0000\u0372\u0373\u0003^/\u0000\u0373"+
		"\u0375\u0005G\u0000\u0000\u0374\u0376\u0005]\u0000\u0000\u0375\u0374\u0001"+
		"\u0000\u0000\u0000\u0375\u0376\u0001\u0000\u0000\u0000\u0376\u0377\u0001"+
		"\u0000\u0000\u0000\u0377\u0379\u0005`\u0000\u0000\u0378\u037a\u0005\u0083"+
		"\u0000\u0000\u0379\u0378\u0001\u0000\u0000\u0000\u0379\u037a\u0001\u0000"+
		"\u0000\u0000\u037a\u037b\u0001\u0000\u0000\u0000\u037b\u037d\u0005\u00af"+
		"\u0000\u0000\u037c\u037e\u0005b\u0000\u0000\u037d\u037c\u0001\u0000\u0000"+
		"\u0000\u037d\u037e\u0001\u0000\u0000\u0000\u037e\u037f\u0001\u0000\u0000"+
		"\u0000\u037f\u0387\u0003\u00ba]\u0000\u0380\u0382\u0005\u00ad\u0000\u0000"+
		"\u0381\u0383\u0005b\u0000\u0000\u0382\u0381\u0001\u0000\u0000\u0000\u0382"+
		"\u0383\u0001\u0000\u0000\u0000\u0383\u0384\u0001\u0000\u0000\u0000\u0384"+
		"\u0386\u0003\u00ba]\u0000\u0385\u0380\u0001\u0000\u0000\u0000\u0386\u0389"+
		"\u0001\u0000\u0000\u0000\u0387\u0385\u0001\u0000\u0000\u0000\u0387\u0388"+
		"\u0001\u0000\u0000\u0000\u0388\u038a\u0001\u0000\u0000\u0000\u0389\u0387"+
		"\u0001\u0000\u0000\u0000\u038a\u038b\u0005\u00b0\u0000\u0000\u038b]\u0001"+
		"\u0000\u0000\u0000\u038c\u0391\u0003`0\u0000\u038d\u038e\u0005\u00c9\u0000"+
		"\u0000\u038e\u0390\u0003`0\u0000\u038f\u038d\u0001\u0000\u0000\u0000\u0390"+
		"\u0393\u0001\u0000\u0000\u0000\u0391\u038f\u0001\u0000\u0000\u0000\u0391"+
		"\u0392\u0001\u0000\u0000\u0000\u0392_\u0001\u0000\u0000\u0000\u0393\u0391"+
		"\u0001\u0000\u0000\u0000\u0394\u0399\u0003b1\u0000\u0395\u0396\u0007\u0003"+
		"\u0000\u0000\u0396\u0398\u0003b1\u0000\u0397\u0395\u0001\u0000\u0000\u0000"+
		"\u0398\u039b\u0001\u0000\u0000\u0000\u0399\u0397\u0001\u0000\u0000\u0000"+
		"\u0399\u039a\u0001\u0000\u0000\u0000\u039aa\u0001\u0000\u0000\u0000\u039b"+
		"\u0399\u0001\u0000\u0000\u0000\u039c\u03a1\u0003d2\u0000\u039d\u039e\u0007"+
		"\u0004\u0000\u0000\u039e\u03a0\u0003d2\u0000\u039f\u039d\u0001\u0000\u0000"+
		"\u0000\u03a0\u03a3\u0001\u0000\u0000\u0000\u03a1\u039f\u0001\u0000\u0000"+
		"\u0000\u03a1\u03a2\u0001\u0000\u0000\u0000\u03a2c\u0001\u0000\u0000\u0000"+
		"\u03a3\u03a1\u0001\u0000\u0000\u0000\u03a4\u03a8\u0003f3\u0000\u03a5\u03a6"+
		"\u0007\u0003\u0000\u0000\u03a6\u03a8\u0003d2\u0000\u03a7\u03a4\u0001\u0000"+
		"\u0000\u0000\u03a7\u03a5\u0001\u0000\u0000\u0000\u03a8e\u0001\u0000\u0000"+
		"\u0000\u03a9\u03ae\u0003t:\u0000\u03aa\u03ad\u0003h4\u0000\u03ab\u03ad"+
		"\u0003n7\u0000\u03ac\u03aa\u0001\u0000\u0000\u0000\u03ac\u03ab\u0001\u0000"+
		"\u0000\u0000\u03ad\u03b0\u0001\u0000\u0000\u0000\u03ae\u03ac\u0001\u0000"+
		"\u0000\u0000\u03ae\u03af\u0001\u0000\u0000\u0000\u03afg\u0001\u0000\u0000"+
		"\u0000\u03b0\u03ae\u0001\u0000\u0000\u0000\u03b1\u03b4\u0005\u00b6\u0000"+
		"\u0000\u03b2\u03b5\u0003l6\u0000\u03b3\u03b5\u0003j5\u0000\u03b4\u03b2"+
		"\u0001\u0000\u0000\u0000\u03b4\u03b3\u0001\u0000\u0000\u0000\u03b5i\u0001"+
		"\u0000\u0000\u0000\u03b6\u03bc\u0003\u01c2\u00e1\u0000\u03b7\u03bc\u0003"+
		"\u01be\u00df\u0000\u03b8\u03bc\u0003z=\u0000\u03b9\u03bc\u0003\u0090H"+
		"\u0000\u03ba\u03bc\u0003\u0086C\u0000\u03bb\u03b6\u0001\u0000\u0000\u0000"+
		"\u03bb\u03b7\u0001\u0000\u0000\u0000\u03bb\u03b8\u0001\u0000\u0000\u0000"+
		"\u03bb\u03b9\u0001\u0000\u0000\u0000\u03bb\u03ba\u0001\u0000\u0000\u0000"+
		"\u03bck\u0001\u0000\u0000\u0000\u03bd\u03be\u0007\u0005\u0000\u0000\u03be"+
		"\u03c0\u0005\u00af\u0000\u0000\u03bf\u03c1\u0003\f\u0006\u0000\u03c0\u03bf"+
		"\u0001\u0000\u0000\u0000\u03c0\u03c1\u0001\u0000\u0000\u0000\u03c1\u03c2"+
		"\u0001\u0000\u0000\u0000\u03c2\u03c3\u0005\u00b0\u0000\u0000\u03c3m\u0001"+
		"\u0000\u0000\u0000\u03c4\u03c7\u0003r9\u0000\u03c5\u03c7\u0003p8\u0000"+
		"\u03c6\u03c4\u0001\u0000\u0000\u0000\u03c6\u03c5\u0001\u0000\u0000\u0000"+
		"\u03c7o\u0001\u0000\u0000\u0000\u03c8\u03ca\u0005\u00b1\u0000\u0000\u03c9"+
		"\u03cb\u0003\f\u0006\u0000\u03ca\u03c9\u0001\u0000\u0000\u0000\u03ca\u03cb"+
		"\u0001\u0000\u0000\u0000\u03cb\u03cc\u0001\u0000\u0000\u0000\u03cc\u03ce"+
		"\u0005\u00ae\u0000\u0000\u03cd\u03cf\u0003\f\u0006\u0000\u03ce\u03cd\u0001"+
		"\u0000\u0000\u0000\u03ce\u03cf\u0001\u0000\u0000\u0000\u03cf\u03d0\u0001"+
		"\u0000\u0000\u0000\u03d0\u03d1\u0005\u00b2\u0000\u0000\u03d1q\u0001\u0000"+
		"\u0000\u0000\u03d2\u03d4\u0005\u00b1\u0000\u0000\u03d3\u03d5\u0003\f\u0006"+
		"\u0000\u03d4\u03d3\u0001\u0000\u0000\u0000\u03d4\u03d5\u0001\u0000\u0000"+
		"\u0000\u03d5\u03d6\u0001\u0000\u0000\u0000\u03d6\u03d7\u0005\u00b2\u0000"+
		"\u0000\u03d7s\u0001\u0000\u0000\u0000\u03d8\u03e7\u0003x<\u0000\u03d9"+
		"\u03e7\u0003v;\u0000\u03da\u03e7\u0003z=\u0000\u03db\u03e7\u0003|>\u0000"+
		"\u03dc\u03e7\u0003~?\u0000\u03dd\u03e7\u0003\u0080@\u0000\u03de\u03e7"+
		"\u0003\u0084B\u0000\u03df\u03e7\u0003\u0086C\u0000\u03e0\u03e7\u0003\u0088"+
		"D\u0000\u03e1\u03e7\u0003\u008aE\u0000\u03e2\u03e7\u0003\u008cF\u0000"+
		"\u03e3\u03e7\u0003\u008eG\u0000\u03e4\u03e7\u0003\u0090H\u0000\u03e5\u03e7"+
		"\u0003\u0092I\u0000\u03e6\u03d8\u0001\u0000\u0000\u0000\u03e6\u03d9\u0001"+
		"\u0000\u0000\u0000\u03e6\u03da\u0001\u0000\u0000\u0000\u03e6\u03db\u0001"+
		"\u0000\u0000\u0000\u03e6\u03dc\u0001\u0000\u0000\u0000\u03e6\u03dd\u0001"+
		"\u0000\u0000\u0000\u03e6\u03de\u0001\u0000\u0000\u0000\u03e6\u03df\u0001"+
		"\u0000\u0000\u0000\u03e6\u03e0\u0001\u0000\u0000\u0000\u03e6\u03e1\u0001"+
		"\u0000\u0000\u0000\u03e6\u03e2\u0001\u0000\u0000\u0000\u03e6\u03e3\u0001"+
		"\u0000\u0000\u0000\u03e6\u03e4\u0001\u0000\u0000\u0000\u03e6\u03e5\u0001"+
		"\u0000\u0000\u0000\u03e7u\u0001\u0000\u0000\u0000\u03e8\u03ee\u0003\u01c2"+
		"\u00e1\u0000\u03e9\u03ec\u0005\u00b6\u0000\u0000\u03ea\u03ed\u0003\u01c2"+
		"\u00e1\u0000\u03eb\u03ed\u0003\u01be\u00df\u0000\u03ec\u03ea\u0001\u0000"+
		"\u0000\u0000\u03ec\u03eb\u0001\u0000\u0000\u0000\u03ed\u03ef\u0001\u0000"+
		"\u0000\u0000\u03ee\u03e9\u0001\u0000\u0000\u0000\u03ee\u03ef\u0001\u0000"+
		"\u0000\u0000\u03efw\u0001\u0000\u0000\u0000\u03f0\u03f6\u0003\u01ba\u00dd"+
		"\u0000\u03f1\u03f6\u0003\u01be\u00df\u0000\u03f2\u03f6\u0005\u00cc\u0000"+
		"\u0000\u03f3\u03f6\u0005\u00cb\u0000\u0000\u03f4\u03f6\u0005\u00ca\u0000"+
		"\u0000\u03f5\u03f0\u0001\u0000\u0000\u0000\u03f5\u03f1\u0001\u0000\u0000"+
		"\u0000\u03f5\u03f2\u0001\u0000\u0000\u0000\u03f5\u03f3\u0001\u0000\u0000"+
		"\u0000\u03f5\u03f4\u0001\u0000\u0000\u0000\u03f6y\u0001\u0000\u0000\u0000"+
		"\u03f7\u03f8\u0007\u0006\u0000\u0000\u03f8{\u0001\u0000\u0000\u0000\u03f9"+
		"\u03fb\u0005\u00b1\u0000\u0000\u03fa\u03fc\u0003\f\u0006\u0000\u03fb\u03fa"+
		"\u0001\u0000\u0000\u0000\u03fb\u03fc\u0001\u0000\u0000\u0000\u03fc\u0401"+
		"\u0001\u0000\u0000\u0000\u03fd\u03fe\u0005\u00ad\u0000\u0000\u03fe\u0400"+
		"\u0003\f\u0006\u0000\u03ff\u03fd\u0001\u0000\u0000\u0000\u0400\u0403\u0001"+
		"\u0000\u0000\u0000\u0401\u03ff\u0001\u0000\u0000\u0000\u0401\u0402\u0001"+
		"\u0000\u0000\u0000\u0402\u0404\u0001\u0000\u0000\u0000\u0403\u0401\u0001"+
		"\u0000\u0000\u0000\u0404\u0405\u0005\u00b2\u0000\u0000\u0405}\u0001\u0000"+
		"\u0000\u0000\u0406\u0407\u0005\u00b3\u0000\u0000\u0407\u0408\u0003\f\u0006"+
		"\u0000\u0408\u0409\u0005\u00ae\u0000\u0000\u0409\u0411\u0003\f\u0006\u0000"+
		"\u040a\u040b\u0005\u00ad\u0000\u0000\u040b\u040c\u0003\f\u0006\u0000\u040c"+
		"\u040d\u0005\u00ae\u0000\u0000\u040d\u040e\u0003\f\u0006\u0000\u040e\u0410"+
		"\u0001\u0000\u0000\u0000\u040f\u040a\u0001\u0000\u0000\u0000\u0410\u0413"+
		"\u0001\u0000\u0000\u0000\u0411\u040f\u0001\u0000\u0000\u0000\u0411\u0412"+
		"\u0001\u0000\u0000\u0000\u0412\u0414\u0001\u0000\u0000\u0000\u0413\u0411"+
		"\u0001\u0000\u0000\u0000\u0414\u0415\u0005\u00b4\u0000\u0000\u0415\u0419"+
		"\u0001\u0000\u0000\u0000\u0416\u0417\u0005\u00b3\u0000\u0000\u0417\u0419"+
		"\u0005\u00b4\u0000\u0000\u0418\u0406\u0001\u0000\u0000\u0000\u0418\u0416"+
		"\u0001\u0000\u0000\u0000\u0419\u007f\u0001\u0000\u0000\u0000\u041a\u041b"+
		"\u0005y\u0000\u0000\u041b\u041c\u0005\u00af\u0000\u0000\u041c\u041d\u0003"+
		"\u0082A\u0000\u041d\u041e\u0005\u00ad\u0000\u0000\u041e\u041f\u0003\f"+
		"\u0006\u0000\u041f\u0420\u0005\u00b0\u0000\u0000\u0420\u0081\u0001\u0000"+
		"\u0000\u0000\u0421\u0422\u0003\f\u0006\u0000\u0422\u0083\u0001\u0000\u0000"+
		"\u0000\u0423\u0424\u0005\u0010\u0000\u0000\u0424\u0426\u0005\u00af\u0000"+
		"\u0000\u0425\u0427\u0005%\u0000\u0000\u0426\u0425\u0001\u0000\u0000\u0000"+
		"\u0426\u0427\u0001\u0000\u0000\u0000\u0427\u0428\u0001\u0000\u0000\u0000"+
		"\u0428\u0429\u0003\f\u0006\u0000\u0429\u042a\u0005\u00b0\u0000\u0000\u042a"+
		"\u0085\u0001\u0000\u0000\u0000\u042b\u042c\u0003\u01c2\u00e1\u0000\u042c"+
		"\u0435\u0005\u00af\u0000\u0000\u042d\u0432\u0003\f\u0006\u0000\u042e\u042f"+
		"\u0005\u00ad\u0000\u0000\u042f\u0431\u0003\f\u0006\u0000\u0430\u042e\u0001"+
		"\u0000\u0000\u0000\u0431\u0434\u0001\u0000\u0000\u0000\u0432\u0430\u0001"+
		"\u0000\u0000\u0000\u0432\u0433\u0001\u0000\u0000\u0000\u0433\u0436\u0001"+
		"\u0000\u0000\u0000\u0434\u0432\u0001\u0000\u0000\u0000\u0435\u042d\u0001"+
		"\u0000\u0000\u0000\u0435\u0436\u0001\u0000\u0000\u0000\u0436\u0437\u0001"+
		"\u0000\u0000\u0000\u0437\u0438\u0005\u00b0\u0000\u0000\u0438\u0087\u0001"+
		"\u0000\u0000\u0000\u0439\u043a\u0005\u001a\u0000\u0000\u043a\u043b\u0005"+
		"\u00af\u0000\u0000\u043b\u043c\u0005\u00b5\u0000\u0000\u043c\u043d\u0005"+
		"\u00b0\u0000\u0000\u043d\u0089\u0001\u0000\u0000\u0000\u043e\u043f\u0005"+
		"\u001a\u0000\u0000\u043f\u0440\u0005\u00af\u0000\u0000\u0440\u0441\u0005"+
		"%\u0000\u0000\u0441\u0442\u0003\f\u0006\u0000\u0442\u0443\u0005\u00b0"+
		"\u0000\u0000\u0443\u008b\u0001\u0000\u0000\u0000\u0444\u0445\u0005\u0015"+
		"\u0000\u0000\u0445\u0446\u0005\u008c\u0000\u0000\u0446\u0447\u0003\f\u0006"+
		"\u0000\u0447\u0448\u0005\u0080\u0000\u0000\u0448\u0450\u0003\f\u0006\u0000"+
		"\u0449\u044a\u0005\u008c\u0000\u0000\u044a\u044b\u0003\f\u0006\u0000\u044b"+
		"\u044c\u0005\u0080\u0000\u0000\u044c\u044d\u0003\f\u0006\u0000\u044d\u044f"+
		"\u0001\u0000\u0000\u0000\u044e\u0449\u0001\u0000\u0000\u0000\u044f\u0452"+
		"\u0001\u0000\u0000\u0000\u0450\u044e\u0001\u0000\u0000\u0000\u0450\u0451"+
		"\u0001\u0000\u0000\u0000\u0451\u0455\u0001\u0000\u0000\u0000\u0452\u0450"+
		"\u0001\u0000\u0000\u0000\u0453\u0454\u0005)\u0000\u0000\u0454\u0456\u0003"+
		"\f\u0006\u0000\u0455\u0453\u0001\u0000\u0000\u0000\u0455\u0456\u0001\u0000"+
		"\u0000\u0000\u0456\u0457\u0001\u0000\u0000\u0000\u0457\u0458\u0005+\u0000"+
		"\u0000\u0458\u008d\u0001\u0000\u0000\u0000\u0459\u045a\u0005\u0017\u0000"+
		"\u0000\u045a\u045b\u0005\u00af\u0000\u0000\u045b\u045c\u0003\f\u0006\u0000"+
		"\u045c\u045d\u0005\u000e\u0000\u0000\u045d\u045e\u0003\u00ba]\u0000\u045e"+
		"\u045f\u0005\u00b0\u0000\u0000\u045f\u008f\u0001\u0000\u0000\u0000\u0460"+
		"\u0461\u0005\u00af\u0000\u0000\u0461\u0462\u0003\f\u0006\u0000\u0462\u0463"+
		"\u0005\u00b0\u0000\u0000\u0463\u0091\u0001\u0000\u0000\u0000\u0464\u0465"+
		"\u0005/\u0000\u0000\u0465\u0466\u0005\u00af\u0000\u0000\u0466\u0467\u0003"+
		"\u01c2\u00e1\u0000\u0467\u0468\u00056\u0000\u0000\u0468\u0469\u0003\f"+
		"\u0006\u0000\u0469\u046a\u0005\u00b0\u0000\u0000\u046a\u0093\u0001\u0000"+
		"\u0000\u0000\u046b\u046d\u0003\u0006\u0003\u0000\u046c\u046b\u0001\u0000"+
		"\u0000\u0000\u046c\u046d\u0001\u0000\u0000\u0000\u046d\u046e\u0001\u0000"+
		"\u0000\u0000\u046e\u046f\u0007\u0007\u0000\u0000\u046f\u0470\u0005F\u0000"+
		"\u0000\u0470\u0475\u0003\u00fc~\u0000\u0471\u0473\u0005\u000e\u0000\u0000"+
		"\u0472\u0471\u0001\u0000\u0000\u0000\u0472\u0473\u0001\u0000\u0000\u0000"+
		"\u0473\u0474\u0001\u0000\u0000\u0000\u0474\u0476\u0003\"\u0011\u0000\u0475"+
		"\u0472\u0001\u0000\u0000\u0000\u0475\u0476\u0001\u0000\u0000\u0000\u0476"+
		"\u0482\u0001\u0000\u0000\u0000\u0477\u0478\u0005\u00af\u0000\u0000\u0478"+
		"\u047d\u0003\u0096K\u0000\u0479\u047a\u0005\u00ad\u0000\u0000\u047a\u047c"+
		"\u0003\u0096K\u0000\u047b\u0479\u0001\u0000\u0000\u0000\u047c\u047f\u0001"+
		"\u0000\u0000\u0000\u047d\u047b\u0001\u0000\u0000\u0000\u047d\u047e\u0001"+
		"\u0000\u0000\u0000\u047e\u0480\u0001\u0000\u0000\u0000\u047f\u047d\u0001"+
		"\u0000\u0000\u0000\u0480\u0481\u0005\u00b0\u0000\u0000\u0481\u0483\u0001"+
		"\u0000\u0000\u0000\u0482\u0477\u0001\u0000\u0000\u0000\u0482\u0483\u0001"+
		"\u0000\u0000\u0000\u0483\u0484\u0001\u0000\u0000\u0000\u0484\u0485\u0005"+
		"\u008b\u0000\u0000\u0485\u0486\u0005\u00af\u0000\u0000\u0486\u048b\u0003"+
		"\u009aM\u0000\u0487\u0488\u0005\u00ad\u0000\u0000\u0488\u048a\u0003\u009a"+
		"M\u0000\u0489\u0487\u0001\u0000\u0000\u0000\u048a\u048d\u0001\u0000\u0000"+
		"\u0000\u048b\u0489\u0001\u0000\u0000\u0000\u048b\u048c\u0001\u0000\u0000"+
		"\u0000\u048c\u048e\u0001\u0000\u0000\u0000\u048d\u048b\u0001\u0000\u0000"+
		"\u0000\u048e\u0492\u0005\u00b0\u0000\u0000\u048f\u0490\u0005z\u0000\u0000"+
		"\u0490\u0491\u0005\u0082\u0000\u0000\u0491\u0493\u0003\u009cN\u0000\u0492"+
		"\u048f\u0001\u0000\u0000\u0000\u0492\u0493\u0001\u0000\u0000\u0000\u0493"+
		"\u0495\u0001\u0000\u0000\u0000\u0494\u0496\u0003\u0098L\u0000\u0495\u0494"+
		"\u0001\u0000\u0000\u0000\u0495\u0496\u0001\u0000\u0000\u0000\u0496\u0095"+
		"\u0001\u0000\u0000\u0000\u0497\u049a\u0003\u01c2\u00e1\u0000\u0498\u049a"+
		"\u0003\u01be\u00df\u0000\u0499\u0497\u0001\u0000\u0000\u0000\u0499\u0498"+
		"\u0001\u0000\u0000\u0000\u049a\u0097\u0001\u0000\u0000\u0000\u049b\u049c"+
		"\u0005q\u0000\u0000\u049c\u049d\u0003*\u0015\u0000\u049d\u0099\u0001\u0000"+
		"\u0000\u0000\u049e\u04a1\u0005\u001f\u0000\u0000\u049f\u04a1\u0003\f\u0006"+
		"\u0000\u04a0\u049e\u0001\u0000\u0000\u0000\u04a0\u049f\u0001\u0000\u0000"+
		"\u0000\u04a1\u009b\u0001\u0000\u0000\u0000\u04a2\u04a3\u0003`0\u0000\u04a3"+
		"\u04a4\u0007\b\u0000\u0000\u04a4\u04a9\u0001\u0000\u0000\u0000\u04a5\u04a6"+
		"\u0005\u008a\u0000\u0000\u04a6\u04a7\u0005~\u0000\u0000\u04a7\u04a9\u0005"+
		"\u001f\u0000\u0000\u04a8\u04a2\u0001\u0000\u0000\u0000\u04a8\u04a5\u0001"+
		"\u0000\u0000\u0000\u04a9\u009d\u0001\u0000\u0000\u0000\u04aa\u04ac\u0003"+
		"\u0006\u0003\u0000\u04ab\u04aa\u0001\u0000\u0000\u0000\u04ab\u04ac\u0001"+
		"\u0000\u0000\u0000\u04ac\u04ad\u0001\u0000\u0000\u0000\u04ad\u04ae\u0005"+
		"\u0086\u0000\u0000\u04ae\u04b3\u0003\u00fc~\u0000\u04af\u04b1\u0005\u000e"+
		"\u0000\u0000\u04b0\u04af\u0001\u0000\u0000\u0000\u04b0\u04b1\u0001\u0000"+
		"\u0000\u0000\u04b1\u04b2\u0001\u0000\u0000\u0000\u04b2\u04b4\u0003\"\u0011"+
		"\u0000\u04b3\u04b0\u0001\u0000\u0000\u0000\u04b3\u04b4\u0001\u0000\u0000"+
		"\u0000\u04b4\u04b5\u0001\u0000\u0000\u0000\u04b5\u04ba\u0003\u00a2Q\u0000"+
		"\u04b6\u04b7\u0005\u00ad\u0000\u0000\u04b7\u04b9\u0003\u00a2Q\u0000\u04b8"+
		"\u04b6\u0001\u0000\u0000\u0000\u04b9\u04bc\u0001\u0000\u0000\u0000\u04ba"+
		"\u04b8\u0001\u0000\u0000\u0000\u04ba\u04bb\u0001\u0000\u0000\u0000\u04bb"+
		"\u04bd\u0001\u0000\u0000\u0000\u04bc\u04ba\u0001\u0000\u0000\u0000\u04bd"+
		"\u04be\u0005\u008d\u0000\u0000\u04be\u04c0\u0003\f\u0006\u0000\u04bf\u04c1"+
		"\u0003\u00a0P\u0000\u04c0\u04bf\u0001\u0000\u0000\u0000\u04c0\u04c1\u0001"+
		"\u0000\u0000\u0000\u04c1\u009f\u0001\u0000\u0000\u0000\u04c2\u04c3\u0005"+
		"q\u0000\u0000\u04c3\u04c4\u0003*\u0015\u0000\u04c4\u00a1\u0001\u0000\u0000"+
		"\u0000\u04c5\u04c6\u0005z\u0000\u0000\u04c6\u04ce\u0003\u00a4R\u0000\u04c7"+
		"\u04ca\u0005\u00ad\u0000\u0000\u04c8\u04cb\u0003\u00a2Q\u0000\u04c9\u04cb"+
		"\u0003\u00a4R\u0000\u04ca\u04c8\u0001\u0000\u0000\u0000\u04ca\u04c9\u0001"+
		"\u0000\u0000\u0000\u04cb\u04cd\u0001\u0000\u0000\u0000\u04cc\u04c7\u0001"+
		"\u0000\u0000\u0000\u04cd\u04d0\u0001\u0000\u0000\u0000\u04ce\u04cc\u0001"+
		"\u0000\u0000\u0000\u04ce\u04cf\u0001\u0000\u0000\u0000\u04cf\u050d\u0001"+
		"\u0000\u0000\u0000\u04d0\u04ce\u0001\u0000\u0000\u0000\u04d1\u04d2\u0005"+
		"\u0007\u0000\u0000\u04d2\u04da\u0003\u00a6S\u0000\u04d3\u04d6\u0005\u00ad"+
		"\u0000\u0000\u04d4\u04d7\u0003\u00a2Q\u0000\u04d5\u04d7\u0003\u00a6S\u0000"+
		"\u04d6\u04d4\u0001\u0000\u0000\u0000\u04d6\u04d5\u0001\u0000\u0000\u0000"+
		"\u04d7\u04d9\u0001\u0000\u0000\u0000\u04d8\u04d3\u0001\u0000\u0000\u0000"+
		"\u04d9\u04dc\u0001\u0000\u0000\u0000\u04da\u04d8\u0001\u0000\u0000\u0000"+
		"\u04da\u04db\u0001\u0000\u0000\u0000\u04db\u050d\u0001\u0000\u0000\u0000"+
		"\u04dc\u04da\u0001\u0000\u0000\u0000\u04dd\u04de\u0005m\u0000\u0000\u04de"+
		"\u04e6\u0003\u00a8T\u0000\u04df\u04e2\u0005\u00ad\u0000\u0000\u04e0\u04e3"+
		"\u0003\u00a2Q\u0000\u04e1\u04e3\u0003\u00a8T\u0000\u04e2\u04e0\u0001\u0000"+
		"\u0000\u0000\u04e2\u04e1\u0001\u0000\u0000\u0000\u04e3\u04e5\u0001\u0000"+
		"\u0000\u0000\u04e4\u04df\u0001\u0000\u0000\u0000\u04e5\u04e8\u0001\u0000"+
		"\u0000\u0000\u04e6\u04e4\u0001\u0000\u0000\u0000\u04e6\u04e7\u0001\u0000"+
		"\u0000\u0000\u04e7\u050d\u0001\u0000\u0000\u0000\u04e8\u04e6\u0001\u0000"+
		"\u0000\u0000\u04e9\u04ea\u0005p\u0000\u0000\u04ea\u04f2\u0003\u00aaU\u0000"+
		"\u04eb\u04ee\u0005\u00ad\u0000\u0000\u04ec\u04ef\u0003\u00a2Q\u0000\u04ed"+
		"\u04ef\u0003\u00aaU\u0000\u04ee\u04ec\u0001\u0000\u0000\u0000\u04ee\u04ed"+
		"\u0001\u0000\u0000\u0000\u04ef\u04f1\u0001\u0000\u0000\u0000\u04f0\u04eb"+
		"\u0001\u0000\u0000\u0000\u04f1\u04f4\u0001\u0000\u0000\u0000\u04f2\u04f0"+
		"\u0001\u0000\u0000\u0000\u04f2\u04f3\u0001\u0000\u0000\u0000\u04f3\u050d"+
		"\u0001\u0000\u0000\u0000\u04f4\u04f2\u0001\u0000\u0000\u0000\u04f5\u04f6"+
		"\u0005H\u0000\u0000\u04f6\u04f7\u0005T\u0000\u0000\u04f7\u04ff\u0003\u00ac"+
		"V\u0000\u04f8\u04fb\u0005\u00ad\u0000\u0000\u04f9\u04fc\u0003\u00a2Q\u0000"+
		"\u04fa\u04fc\u0003\u00acV\u0000\u04fb\u04f9\u0001\u0000\u0000\u0000\u04fb"+
		"\u04fa\u0001\u0000\u0000\u0000\u04fc\u04fe\u0001\u0000\u0000\u0000\u04fd"+
		"\u04f8\u0001\u0000\u0000\u0000\u04fe\u0501\u0001\u0000\u0000\u0000\u04ff"+
		"\u04fd\u0001\u0000\u0000\u0000\u04ff\u0500\u0001\u0000\u0000\u0000\u0500"+
		"\u050d\u0001\u0000\u0000\u0000\u0501\u04ff\u0001\u0000\u0000\u0000\u0502"+
		"\u0503\u0005z\u0000\u0000\u0503\u0504\u0005\u0082\u0000\u0000\u0504\u0509"+
		"\u0003\u00b0X\u0000\u0505\u0506\u0005\u00ad\u0000\u0000\u0506\u0508\u0003"+
		"\u00a2Q\u0000\u0507\u0505\u0001\u0000\u0000\u0000\u0508\u050b\u0001\u0000"+
		"\u0000\u0000\u0509\u0507\u0001\u0000\u0000\u0000\u0509\u050a\u0001\u0000"+
		"\u0000\u0000\u050a\u050d\u0001\u0000\u0000\u0000\u050b\u0509\u0001\u0000"+
		"\u0000\u0000\u050c\u04c5\u0001\u0000\u0000\u0000\u050c\u04d1\u0001\u0000"+
		"\u0000\u0000\u050c\u04dd\u0001\u0000\u0000\u0000\u050c\u04e9\u0001\u0000"+
		"\u0000\u0000\u050c\u04f5\u0001\u0000\u0000\u0000\u050c\u0502\u0001\u0000"+
		"\u0000\u0000\u050d\u00a3\u0001\u0000\u0000\u0000\u050e\u050f\u0003\u00b2"+
		"Y\u0000\u050f\u0510\u0005\u00bd\u0000\u0000\u0510\u0511\u0003\f\u0006"+
		"\u0000\u0511\u00a5\u0001\u0000\u0000\u0000\u0512\u0514\u0005F\u0000\u0000"+
		"\u0513\u0512\u0001\u0000\u0000\u0000\u0513\u0514\u0001\u0000\u0000\u0000"+
		"\u0514\u0515\u0001\u0000\u0000\u0000\u0515\u051a\u0003\u00b2Y\u0000\u0516"+
		"\u0518\u0005\u0003\u0000\u0000\u0517\u0516\u0001\u0000\u0000\u0000\u0517"+
		"\u0518\u0001\u0000\u0000\u0000\u0518\u0519\u0001\u0000\u0000\u0000\u0519"+
		"\u051b\u0003\u00b4Z\u0000\u051a\u0517\u0001\u0000\u0000\u0000\u051a\u051b"+
		"\u0001\u0000\u0000\u0000\u051b\u051d\u0001\u0000\u0000\u0000\u051c\u051e"+
		"\u0005(\u0000\u0000\u051d\u051c\u0001\u0000\u0000\u0000\u051d\u051e\u0001"+
		"\u0000\u0000\u0000\u051e\u051f\u0001\u0000\u0000\u0000\u051f\u0520\u0003"+
		"\f\u0006\u0000\u0520\u00a7\u0001\u0000\u0000\u0000\u0521\u0523\u0005F"+
		"\u0000\u0000\u0522\u0521\u0001\u0000\u0000\u0000\u0522\u0523\u0001\u0000"+
		"\u0000\u0000\u0523\u0524\u0001\u0000\u0000\u0000\u0524\u0526\u0003\u00b2"+
		"Y\u0000\u0525\u0527\u00050\u0000\u0000\u0526\u0525\u0001\u0000\u0000\u0000"+
		"\u0526\u0527\u0001\u0000\u0000\u0000\u0527\u0528\u0001\u0000\u0000\u0000"+
		"\u0528\u0529\u0003\f\u0006\u0000\u0529\u00a9\u0001\u0000\u0000\u0000\u052a"+
		"\u052b\u0003\u00b2Y\u0000\u052b\u00ab\u0001\u0000\u0000\u0000\u052c\u052d"+
		"\u0003\u00b2Y\u0000\u052d\u052e\u0005\u008e\u0000\u0000\u052e\u052f\u0005"+
		"h\u0000\u0000\u052f\u0530\u0003\u00aeW\u0000\u0530\u00ad\u0001\u0000\u0000"+
		"\u0000\u0531\u0536\u0003~?\u0000\u0532\u0536\u0003|>\u0000\u0533\u0536"+
		"\u0003x<\u0000\u0534\u0536\u0003z=\u0000\u0535\u0531\u0001\u0000\u0000"+
		"\u0000\u0535\u0532\u0001\u0000\u0000\u0000\u0535\u0533\u0001\u0000\u0000"+
		"\u0000\u0535\u0534\u0001\u0000\u0000\u0000\u0536\u00af\u0001\u0000\u0000"+
		"\u0000\u0537\u0538\u0003`0\u0000\u0538\u0539\u0007\b\u0000\u0000\u0539"+
		"\u053e\u0001\u0000\u0000\u0000\u053a\u053b\u0005\u008a\u0000\u0000\u053b"+
		"\u053c\u0005~\u0000\u0000\u053c\u053e\u0005\u001f\u0000\u0000\u053d\u0537"+
		"\u0001\u0000\u0000\u0000\u053d\u053a\u0001\u0000\u0000\u0000\u053e\u00b1"+
		"\u0001\u0000\u0000\u0000\u053f\u0540\u0003f3\u0000\u0540\u00b3\u0001\u0000"+
		"\u0000\u0000\u0541\u0542\u0003`0\u0000\u0542\u00b5\u0001\u0000\u0000\u0000"+
		"\u0543\u0545\u0003\u0006\u0003\u0000\u0544\u0543\u0001\u0000\u0000\u0000"+
		"\u0544\u0545\u0001\u0000\u0000\u0000\u0545\u0546\u0001\u0000\u0000\u0000"+
		"\u0546\u0547\u0005 \u0000\u0000\u0547\u0548\u00056\u0000\u0000\u0548\u054d"+
		"\u0003\u00fc~\u0000\u0549\u054b\u0005\u000e\u0000\u0000\u054a\u0549\u0001"+
		"\u0000\u0000\u0000\u054a\u054b\u0001\u0000\u0000\u0000\u054b\u054c\u0001"+
		"\u0000\u0000\u0000\u054c\u054e\u0003\"\u0011\u0000\u054d\u054a\u0001\u0000"+
		"\u0000\u0000\u054d\u054e\u0001\u0000\u0000\u0000\u054e\u0551\u0001\u0000"+
		"\u0000\u0000\u054f\u0550\u0005\u008d\u0000\u0000\u0550\u0552\u0003\f\u0006"+
		"\u0000\u0551\u054f\u0001\u0000\u0000\u0000\u0551\u0552\u0001\u0000\u0000"+
		"\u0000\u0552\u0554\u0001\u0000\u0000\u0000\u0553\u0555\u0003\u00b8\\\u0000"+
		"\u0554\u0553\u0001\u0000\u0000\u0000\u0554\u0555\u0001\u0000\u0000\u0000"+
		"\u0555\u00b7\u0001\u0000\u0000\u0000\u0556\u0557\u0005q\u0000\u0000\u0557"+
		"\u0558\u0003*\u0015\u0000\u0558\u00b9\u0001\u0000\u0000\u0000\u0559\u055b"+
		"\u0003\u00bc^\u0000\u055a\u055c\u0007\t\u0000\u0000\u055b\u055a\u0001"+
		"\u0000\u0000\u0000\u055b\u055c\u0001\u0000\u0000\u0000\u055c\u00bb\u0001"+
		"\u0000\u0000\u0000\u055d\u056d\u0003\u00d8l\u0000\u055e\u056d\u0003\u00ca"+
		"e\u0000\u055f\u056d\u0003\u00d6k\u0000\u0560\u056d\u0003\u00d4j\u0000"+
		"\u0561\u056d\u0003\u00d0h\u0000\u0562\u056d\u0003\u00ccf\u0000\u0563\u056d"+
		"\u0003\u00ceg\u0000\u0564\u056d\u0003\u00c8d\u0000\u0565\u056d\u0003\u00be"+
		"_\u0000\u0566\u056d\u0003\u00d2i\u0000\u0567\u056d\u0003\u00dam\u0000"+
		"\u0568\u056d\u0003\u00dcn\u0000\u0569\u056d\u0003\u00deo\u0000\u056a\u056d"+
		"\u0003\u00e0p\u0000\u056b\u056d\u0003\u00e2q\u0000\u056c\u055d\u0001\u0000"+
		"\u0000\u0000\u056c\u055e\u0001\u0000\u0000\u0000\u056c\u055f\u0001\u0000"+
		"\u0000\u0000\u056c\u0560\u0001\u0000\u0000\u0000\u056c\u0561\u0001\u0000"+
		"\u0000\u0000\u056c\u0562\u0001\u0000\u0000\u0000\u056c\u0563\u0001\u0000"+
		"\u0000\u0000\u056c\u0564\u0001\u0000\u0000\u0000\u056c\u0565\u0001\u0000"+
		"\u0000\u0000\u056c\u0566\u0001\u0000\u0000\u0000\u056c\u0567\u0001\u0000"+
		"\u0000\u0000\u056c\u0568\u0001\u0000\u0000\u0000\u056c\u0569\u0001\u0000"+
		"\u0000\u0000\u056c\u056a\u0001\u0000\u0000\u0000\u056c\u056b\u0001\u0000"+
		"\u0000\u0000\u056d\u00bd\u0001\u0000\u0000\u0000\u056e\u056f\u0005\u00a4"+
		"\u0000\u0000\u056f\u0570\u0005\u00af\u0000\u0000\u0570\u0575\u0003\u00c0"+
		"`\u0000\u0571\u0572\u0005\u00ad\u0000\u0000\u0572\u0574\u0003\u00c0`\u0000"+
		"\u0573\u0571\u0001\u0000\u0000\u0000\u0574\u0577\u0001\u0000\u0000\u0000"+
		"\u0575\u0573\u0001\u0000\u0000\u0000\u0575\u0576\u0001\u0000\u0000\u0000"+
		"\u0576\u0578\u0001\u0000\u0000\u0000\u0577\u0575\u0001\u0000\u0000\u0000"+
		"\u0578\u0579\u0005\u00b0\u0000\u0000\u0579\u00bf\u0001\u0000\u0000\u0000"+
		"\u057a\u057b\u0003\u01c2\u00e1\u0000\u057b\u057d\u0003\u00bc^\u0000\u057c"+
		"\u057e\u0003\u00c2a\u0000\u057d\u057c\u0001\u0000\u0000\u0000\u057d\u057e"+
		"\u0001\u0000\u0000\u0000\u057e\u0580\u0001\u0000\u0000\u0000\u057f\u0581"+
		"\u0003\u01b4\u00da\u0000\u0580\u057f\u0001\u0000\u0000\u0000\u0580\u0581"+
		"\u0001\u0000\u0000\u0000\u0581\u00c1\u0001\u0000\u0000\u0000\u0582\u0584"+
		"\u0003\u00c4b\u0000\u0583\u0585\u0003\u00c6c\u0000\u0584\u0583\u0001\u0000"+
		"\u0000\u0000\u0584\u0585\u0001\u0000\u0000\u0000\u0585\u058b\u0001\u0000"+
		"\u0000\u0000\u0586\u0588\u0003\u00c6c\u0000\u0587\u0589\u0003\u00c4b\u0000"+
		"\u0588\u0587\u0001\u0000\u0000\u0000\u0588\u0589\u0001\u0000\u0000\u0000"+
		"\u0589\u058b\u0001\u0000\u0000\u0000\u058a\u0582\u0001\u0000\u0000\u0000"+
		"\u058a\u0586\u0001\u0000\u0000\u0000\u058b\u00c3\u0001\u0000\u0000\u0000"+
		"\u058c\u0592\u0005\u001f\u0000\u0000\u058d\u0593\u0003\u01ba\u00dd\u0000"+
		"\u058e\u0593\u0003\u01be\u00df\u0000\u058f\u0593\u0005\u00cc\u0000\u0000"+
		"\u0590\u0593\u0005\u00cb\u0000\u0000\u0591\u0593\u0003\u01c2\u00e1\u0000"+
		"\u0592\u058d\u0001\u0000\u0000\u0000\u0592\u058e\u0001\u0000\u0000\u0000"+
		"\u0592\u058f\u0001\u0000\u0000\u0000\u0592\u0590\u0001\u0000\u0000\u0000"+
		"\u0592\u0591\u0001\u0000\u0000\u0000\u0593\u00c5\u0001\u0000\u0000\u0000"+
		"\u0594\u0595\u0005]\u0000\u0000\u0595\u0596\u0005\u00ca\u0000\u0000\u0596"+
		"\u00c7\u0001\u0000\u0000\u0000\u0597\u0598\u0005\u00a1\u0000\u0000\u0598"+
		"\u0599\u0005\u00af\u0000\u0000\u0599\u059a\u0003\u00bc^\u0000\u059a\u059b"+
		"\u0005\u00b0\u0000\u0000\u059b\u00c9\u0001\u0000\u0000\u0000\u059c\u059d"+
		"\u0005\u0098\u0000\u0000\u059d\u059e\u0005\u00af\u0000\u0000\u059e\u059f"+
		"\u0003\u00bc^\u0000\u059f\u05a0\u0005\u00b0\u0000\u0000\u05a0\u00cb\u0001"+
		"\u0000\u0000\u0000\u05a1\u05a2\u0007\n\u0000\u0000\u05a2\u00cd\u0001\u0000"+
		"\u0000\u0000\u05a3\u05a4\u0005H\u0000\u0000\u05a4\u00cf\u0001\u0000\u0000"+
		"\u0000\u05a5\u05a6\u0007\u000b\u0000\u0000\u05a6\u00d1\u0001\u0000\u0000"+
		"\u0000\u05a7\u05a8\u0005\u00a5\u0000\u0000\u05a8\u00d3\u0001\u0000\u0000"+
		"\u0000\u05a9\u05aa\u0005\u009c\u0000\u0000\u05aa\u05ab\u0005\u00af\u0000"+
		"\u0000\u05ab\u05ac\u0003\u01c0\u00e0\u0000\u05ac\u05ad\u0005\u00b0\u0000"+
		"\u0000\u05ad\u05b4\u0001\u0000\u0000\u0000\u05ae\u05af\u0005\u009c\u0000"+
		"\u0000\u05af\u05b0\u0005\u00af\u0000\u0000\u05b0\u05b1\u0003\u01c0\u00e0"+
		"\u0000\u05b1\u05b2\u0006j\uffff\uffff\u0000\u05b2\u05b4\u0001\u0000\u0000"+
		"\u0000\u05b3\u05a9\u0001\u0000\u0000\u0000\u05b3\u05ae\u0001\u0000\u0000"+
		"\u0000\u05b4\u00d5\u0001\u0000\u0000\u0000\u05b5\u05b6\u0005\u009a\u0000"+
		"\u0000\u05b6\u00d7\u0001\u0000\u0000\u0000\u05b7\u05bb\u0005\u0099\u0000"+
		"\u0000\u05b8\u05b9\u0005\u00af\u0000\u0000\u05b9\u05ba\u0005\u00cd\u0000"+
		"\u0000\u05ba\u05bc\u0005\u00b0\u0000\u0000\u05bb\u05b8\u0001\u0000\u0000"+
		"\u0000\u05bb\u05bc\u0001\u0000\u0000\u0000\u05bc\u00d9\u0001\u0000\u0000"+
		"\u0000\u05bd\u05c1\u0005\u00a6\u0000\u0000\u05be\u05bf\u0005\u00af\u0000"+
		"\u0000\u05bf\u05c0\u0005\u00cd\u0000\u0000\u05c0\u05c2\u0005\u00b0\u0000"+
		"\u0000\u05c1\u05be\u0001\u0000\u0000\u0000\u05c1\u05c2\u0001\u0000\u0000"+
		"\u0000\u05c2\u00db\u0001\u0000\u0000\u0000\u05c3\u05c4\u0005\u00a7\u0000"+
		"\u0000\u05c4\u00dd\u0001\u0000\u0000\u0000\u05c5\u05c6\u0005\u00a8\u0000"+
		"\u0000\u05c6\u00df\u0001\u0000\u0000\u0000\u05c7\u05c8\u0005\u00a9\u0000"+
		"\u0000\u05c8\u00e1\u0001\u0000\u0000\u0000\u05c9\u05ca\u0005\u00aa\u0000"+
		"\u0000\u05ca\u00e3\u0001\u0000\u0000\u0000\u05cb\u05d0\u0003\u01c2\u00e1"+
		"\u0000\u05cc\u05cd\u0005\u00b6\u0000\u0000\u05cd\u05cf\u0003\u01c2\u00e1"+
		"\u0000\u05ce\u05cc\u0001\u0000\u0000\u0000\u05cf\u05d2\u0001\u0000\u0000"+
		"\u0000\u05d0\u05ce\u0001\u0000\u0000\u0000\u05d0\u05d1\u0001\u0000\u0000"+
		"\u0000\u05d1\u00e5\u0001\u0000\u0000\u0000\u05d2\u05d0\u0001\u0000\u0000"+
		"\u0000\u05d3\u05d8\u0003\u00e8t\u0000\u05d4\u05d5\u0005\u00b6\u0000\u0000"+
		"\u05d5\u05d7\u0003\u00e8t\u0000\u05d6\u05d4\u0001\u0000\u0000\u0000\u05d7"+
		"\u05da\u0001\u0000\u0000\u0000\u05d8\u05d6\u0001\u0000\u0000\u0000\u05d8"+
		"\u05d9\u0001\u0000\u0000\u0000\u05d9\u00e7\u0001\u0000\u0000\u0000\u05da"+
		"\u05d8\u0001\u0000\u0000\u0000\u05db\u05dd\u0005\u00d2\u0000\u0000\u05dc"+
		"\u05db\u0001\u0000\u0000\u0000\u05dc\u05dd\u0001\u0000\u0000\u0000\u05dd"+
		"\u05de\u0001\u0000\u0000\u0000\u05de\u05df\u0003\u01c2\u00e1\u0000\u05df"+
		"\u00e9\u0001\u0000\u0000\u0000\u05e0\u05e5\u0003\u00ecv\u0000\u05e1\u05e2"+
		"\u0005\u00b6\u0000\u0000\u05e2\u05e4\u0003\u00ecv\u0000\u05e3\u05e1\u0001"+
		"\u0000\u0000\u0000\u05e4\u05e7\u0001\u0000\u0000\u0000\u05e5\u05e3\u0001"+
		"\u0000\u0000\u0000\u05e5\u05e6\u0001\u0000\u0000\u0000\u05e6\u00eb\u0001"+
		"\u0000\u0000\u0000\u05e7\u05e5\u0001\u0000\u0000\u0000\u05e8\u05eb\u0003"+
		"\u01c2\u00e1\u0000\u05e9\u05eb\u0005\u00d0\u0000\u0000\u05ea\u05e8\u0001"+
		"\u0000\u0000\u0000\u05ea\u05e9\u0001\u0000\u0000\u0000\u05eb\u00ed\u0001"+
		"\u0000\u0000\u0000\u05ec\u05ed\u0005\u001b\u0000\u0000\u05ed\u05f1\u0005"+
		"Y\u0000\u0000\u05ee\u05ef\u0005?\u0000\u0000\u05ef\u05f0\u0005]\u0000"+
		"\u0000\u05f0\u05f2\u0005.\u0000\u0000\u05f1\u05ee\u0001\u0000\u0000\u0000"+
		"\u05f1\u05f2\u0001\u0000\u0000\u0000\u05f2\u05f3\u0001\u0000\u0000\u0000"+
		"\u05f3\u05f4\u0003\u00fe\u007f\u0000\u05f4\u00ef\u0001\u0000\u0000\u0000"+
		"\u05f5\u05f6\u0005&\u0000\u0000\u05f6\u05f9\u0005Y\u0000\u0000\u05f7\u05f8"+
		"\u0005?\u0000\u0000\u05f8\u05fa\u0005.\u0000\u0000\u05f9\u05f7\u0001\u0000"+
		"\u0000\u0000\u05f9\u05fa\u0001\u0000\u0000\u0000\u05fa\u05fb\u0001\u0000"+
		"\u0000\u0000\u05fb\u05fd\u0003\u00fe\u007f\u0000\u05fc\u05fe\u0005\u0016"+
		"\u0000\u0000\u05fd\u05fc\u0001\u0000\u0000\u0000\u05fd\u05fe\u0001\u0000"+
		"\u0000\u0000\u05fe\u00f1\u0001\u0000\u0000\u0000\u05ff\u0600\u0003\u01c2"+
		"\u00e1\u0000\u0600\u00f3\u0001\u0000\u0000\u0000\u0601\u0602\u0005\u001b"+
		"\u0000\u0000\u0602\u0603\u0005n\u0000\u0000\u0603\u0604\u0003\u00f2y\u0000"+
		"\u0604\u00f5\u0001\u0000\u0000\u0000\u0605\u0606\u0005&\u0000\u0000\u0606"+
		"\u0607\u0005n\u0000\u0000\u0607\u0608\u0003\u00f2y\u0000\u0608\u00f7\u0001"+
		"\u0000\u0000\u0000\u0609\u060a\u0005z\u0000\u0000\u060a\u060b\u0005Q\u0000"+
		"\u0000\u060b\u060c\u0005n\u0000\u0000\u060c\u060d\u0003\u00f2y\u0000\u060d"+
		"\u00f9\u0001\u0000\u0000\u0000\u060e\u060f\u0005\u001b\u0000\u0000\u060f"+
		"\u0613\u0005~\u0000\u0000\u0610\u0611\u0005?\u0000\u0000\u0611\u0612\u0005"+
		"]\u0000\u0000\u0612\u0614\u0005.\u0000\u0000\u0613\u0610\u0001\u0000\u0000"+
		"\u0000\u0613\u0614\u0001\u0000\u0000\u0000\u0614\u0615\u0001\u0000\u0000"+
		"\u0000\u0615\u0617\u0003\u00fc~\u0000\u0616\u0618\u0003\u01b4\u00da\u0000"+
		"\u0617\u0616\u0001\u0000\u0000\u0000\u0617\u0618\u0001\u0000\u0000\u0000"+
		"\u0618\u0619\u0001\u0000\u0000\u0000\u0619\u061a\u0005\u00af\u0000\u0000"+
		"\u061a\u061b\u0003\u0100\u0080\u0000\u061b\u061d\u0005\u00b0\u0000\u0000"+
		"\u061c\u061e\u0003\u0116\u008b\u0000\u061d\u061c\u0001\u0000\u0000\u0000"+
		"\u061d\u061e\u0001\u0000\u0000\u0000\u061e\u00fb\u0001\u0000\u0000\u0000"+
		"\u061f\u0620\u0003\u00fe\u007f\u0000\u0620\u0621\u0005\u00ae\u0000\u0000"+
		"\u0621\u0623\u0001\u0000\u0000\u0000\u0622\u061f\u0001\u0000\u0000\u0000"+
		"\u0622\u0623\u0001\u0000\u0000\u0000\u0623\u0624\u0001\u0000\u0000\u0000"+
		"\u0624\u0625\u0003\u00e6s\u0000\u0625\u00fd\u0001\u0000\u0000\u0000\u0626"+
		"\u0627\u0003\u00e4r\u0000\u0627\u00ff\u0001\u0000\u0000\u0000\u0628\u062c"+
		"\u0003\u0102\u0081\u0000\u0629\u062c\u0003\u010c\u0086\u0000\u062a\u062c"+
		"\u0003\u0108\u0084\u0000\u062b\u0628\u0001\u0000\u0000\u0000\u062b\u0629"+
		"\u0001\u0000\u0000\u0000\u062b\u062a\u0001\u0000\u0000\u0000\u062c\u0635"+
		"\u0001\u0000\u0000\u0000\u062d\u0631\u0005\u00ad\u0000\u0000\u062e\u0632"+
		"\u0003\u0102\u0081\u0000\u062f\u0632\u0003\u010c\u0086\u0000\u0630\u0632"+
		"\u0003\u0108\u0084\u0000\u0631\u062e\u0001\u0000\u0000\u0000\u0631\u062f"+
		"\u0001\u0000\u0000\u0000\u0631\u0630\u0001\u0000\u0000\u0000\u0632\u0634"+
		"\u0001\u0000\u0000\u0000\u0633\u062d\u0001\u0000\u0000\u0000\u0634\u0637"+
		"\u0001\u0000\u0000\u0000\u0635\u0633\u0001\u0000\u0000\u0000\u0635\u0636"+
		"\u0001\u0000\u0000\u0000\u0636\u0101\u0001\u0000\u0000\u0000\u0637\u0635"+
		"\u0001\u0000\u0000\u0000\u0638\u0639\u0003\u01c2\u00e1\u0000\u0639\u063f"+
		"\u0003\u00bc^\u0000\u063a\u0640\u0003\u00c2a\u0000\u063b\u0640\u0003\u0128"+
		"\u0094\u0000\u063c\u0640\u0003\u012e\u0097\u0000\u063d\u0640\u0003\u012c"+
		"\u0096\u0000\u063e\u0640\u0003\u0104\u0082\u0000\u063f\u063a\u0001\u0000"+
		"\u0000\u0000\u063f\u063b\u0001\u0000\u0000\u0000\u063f\u063c\u0001\u0000"+
		"\u0000\u0000\u063f\u063d\u0001\u0000\u0000\u0000\u063f\u063e\u0001\u0000"+
		"\u0000\u0000\u063f\u0640\u0001\u0000\u0000\u0000\u0640\u0642\u0001\u0000"+
		"\u0000\u0000\u0641\u0643\u0003\u01b4\u00da\u0000\u0642\u0641\u0001\u0000"+
		"\u0000\u0000\u0642\u0643\u0001\u0000\u0000\u0000\u0643\u0103\u0001\u0000"+
		"\u0000\u0000\u0644\u0645\u0005\u00af\u0000\u0000\u0645\u064a\u0003\u0106"+
		"\u0083\u0000\u0646\u0647\u0005\u00ad\u0000\u0000\u0647\u0649\u0003\u0106"+
		"\u0083\u0000\u0648\u0646\u0001\u0000\u0000\u0000\u0649\u064c\u0001\u0000"+
		"\u0000\u0000\u064a\u0648\u0001\u0000\u0000\u0000\u064a\u064b\u0001\u0000"+
		"\u0000\u0000\u064b\u064d\u0001\u0000\u0000\u0000\u064c\u064a\u0001\u0000"+
		"\u0000\u0000\u064d\u064e\u0005\u00b0\u0000\u0000\u064e\u0105\u0001\u0000"+
		"\u0000\u0000\u064f\u0650\u0003\u010a\u0085\u0000\u0650\u0651\u0005\u000e"+
		"\u0000\u0000\u0651\u0652\u0007\f\u0000\u0000\u0652\u0653\u0005X\u0000"+
		"\u0000\u0653\u0107\u0001\u0000\u0000\u0000\u0654\u0655\u0003\u0106\u0083"+
		"\u0000\u0655\u0109\u0001\u0000\u0000\u0000\u0656\u0659\u0003\u01c2\u00e1"+
		"\u0000\u0657\u0659\u0003\u01be\u00df\u0000\u0658\u0656\u0001\u0000\u0000"+
		"\u0000\u0658\u0657\u0001\u0000\u0000\u0000\u0659\u0661\u0001\u0000\u0000"+
		"\u0000\u065a\u065d\u0005\u00b6\u0000\u0000\u065b\u065e\u0003\u01c2\u00e1"+
		"\u0000\u065c\u065e\u0003\u01be\u00df\u0000\u065d\u065b\u0001\u0000\u0000"+
		"\u0000\u065d\u065c\u0001\u0000\u0000\u0000\u065e\u0660\u0001\u0000\u0000"+
		"\u0000\u065f\u065a\u0001\u0000\u0000\u0000\u0660\u0663\u0001\u0000\u0000"+
		"\u0000\u0661\u065f\u0001\u0000\u0000\u0000\u0661\u0662\u0001\u0000\u0000"+
		"\u0000\u0662\u010b\u0001\u0000\u0000\u0000\u0663\u0661\u0001\u0000\u0000"+
		"\u0000\u0664\u0665\u0005l\u0000\u0000\u0665\u0666\u0005J\u0000\u0000\u0666"+
		"\u066b\u0005\u00af\u0000\u0000\u0667\u0669\u0003\u010e\u0087\u0000\u0668"+
		"\u066a\u0005\u00ad\u0000\u0000\u0669\u0668\u0001\u0000\u0000\u0000\u0669"+
		"\u066a\u0001\u0000\u0000\u0000\u066a\u066c\u0001\u0000\u0000\u0000\u066b"+
		"\u0667\u0001\u0000\u0000\u0000\u066b\u066c\u0001\u0000\u0000\u0000\u066c"+
		"\u066e\u0001\u0000\u0000\u0000\u066d\u066f\u0003\u0110\u0088\u0000\u066e"+
		"\u066d\u0001\u0000\u0000\u0000\u066e\u066f\u0001\u0000\u0000\u0000\u066f"+
		"\u0670\u0001\u0000\u0000\u0000\u0670\u0671\u0005\u00b0\u0000\u0000\u0671"+
		"\u010d\u0001\u0000\u0000\u0000\u0672\u0673\u0005{\u0000\u0000\u0673\u0674"+
		"\u0005\u00af\u0000\u0000\u0674\u0675\u0003\u0110\u0088\u0000\u0675\u0676"+
		"\u0005\u00b0\u0000\u0000\u0676\u067c\u0001\u0000\u0000\u0000\u0677\u0678"+
		"\u0005\u00af\u0000\u0000\u0678\u0679\u0003\u0110\u0088\u0000\u0679\u067a"+
		"\u0006\u0087\uffff\uffff\u0000\u067a\u067c\u0001\u0000\u0000\u0000\u067b"+
		"\u0672\u0001\u0000\u0000\u0000\u067b\u0677\u0001\u0000\u0000\u0000\u067c"+
		"\u010f\u0001\u0000\u0000\u0000\u067d\u0682\u0003\u0112\u0089\u0000\u067e"+
		"\u067f\u0005\u00ad\u0000\u0000\u067f\u0681\u0003\u0112\u0089\u0000\u0680"+
		"\u067e\u0001\u0000\u0000\u0000\u0681\u0684\u0001\u0000\u0000\u0000\u0682"+
		"\u0680\u0001\u0000\u0000\u0000\u0682\u0683\u0001\u0000\u0000\u0000\u0683"+
		"\u0111\u0001\u0000\u0000\u0000\u0684\u0682\u0001\u0000\u0000\u0000\u0685"+
		"\u0687\u0003\u01c2\u00e1\u0000\u0686\u0688\u0003\u0114\u008a\u0000\u0687"+
		"\u0686\u0001\u0000\u0000\u0000\u0687\u0688\u0001\u0000\u0000\u0000\u0688"+
		"\u0113\u0001\u0000\u0000\u0000\u0689\u068a\u0005\u00af\u0000\u0000\u068a"+
		"\u068b\u0005\u00cd\u0000\u0000\u068b\u068c\u0005\u00b0\u0000\u0000\u068c"+
		"\u0115\u0001\u0000\u0000\u0000\u068d\u0693\u0003\u0118\u008c\u0000\u068e"+
		"\u0693\u0003\u011c\u008e\u0000\u068f\u0693\u0003\u011e\u008f\u0000\u0690"+
		"\u0693\u0003\u0120\u0090\u0000\u0691\u0693\u0003\u0122\u0091\u0000\u0692"+
		"\u068d\u0001\u0000\u0000\u0000\u0692\u068e\u0001\u0000\u0000\u0000\u0692"+
		"\u068f\u0001\u0000\u0000\u0000\u0692\u0690\u0001\u0000\u0000\u0000\u0692"+
		"\u0691\u0001\u0000\u0000\u0000\u0693\u0694\u0001\u0000\u0000\u0000\u0694"+
		"\u0692\u0001\u0000\u0000\u0000\u0694\u0695\u0001\u0000\u0000\u0000\u0695"+
		"\u0117\u0001\u0000\u0000\u0000\u0696\u0697\u0005\u008a\u0000\u0000\u0697"+
		"\u0698\u0005\u0082\u0000\u0000\u0698\u0699\u0003\u01b6\u00db\u0000\u0699"+
		"\u0119\u0001\u0000\u0000\u0000\u069a\u069b\u0003\u01c0\u00e0\u0000\u069b"+
		"\u011b\u0001\u0000\u0000\u0000\u069c\u069d\u0005A\u0000\u0000\u069d\u069e"+
		"\u0005o\u0000\u0000\u069e\u069f\u0003\u011a\u008d\u0000\u069f\u011d\u0001"+
		"\u0000\u0000\u0000\u06a0\u06a1\u0005\u008e\u0000\u0000\u06a1\u06a2\u0005"+
		"v\u0000\u0000\u06a2\u06a4\u00057\u0000\u0000\u06a3\u06a5\u00052\u0000"+
		"\u0000\u06a4\u06a3\u0001\u0000\u0000\u0000\u06a4\u06a5\u0001\u0000\u0000"+
		"\u0000\u06a5\u011f\u0001\u0000\u0000\u0000\u06a6\u06a7\u0005\u000e\u0000"+
		"\u0000\u06a7\u06a8\u0005H\u0000\u0000\u06a8\u06a9\u0005\u0018\u0000\u0000"+
		"\u06a9\u0121\u0001\u0000\u0000\u0000\u06aa\u06ab\u0005*\u0000\u0000\u06ab"+
		"\u06ac\u0005\u0011\u0000\u0000\u06ac\u06ae\u0005@\u0000\u0000\u06ad\u06af"+
		"\u0003\u0124\u0092\u0000\u06ae\u06ad\u0001\u0000\u0000\u0000\u06ae\u06af"+
		"\u0001\u0000\u0000\u0000\u06af\u0123\u0001\u0000\u0000\u0000\u06b0\u06b1"+
		"\u0005\u008a\u0000\u0000\u06b1\u06b2\u0005\u0082\u0000\u0000\u06b2\u06b3"+
		"\u0003\u01b6\u00db\u0000\u06b3\u0125\u0001\u0000\u0000\u0000\u06b4\u06b5"+
		"\u0005$\u0000\u0000\u06b5\u06b6\u0005\u0011\u0000\u0000\u06b6\u06b7\u0005"+
		"@\u0000\u0000\u06b7\u0127\u0001\u0000\u0000\u0000\u06b8\u06c0\u00059\u0000"+
		"\u0000\u06b9\u06c1\u0005\u000b\u0000\u0000\u06ba\u06bb\u0005\u0013\u0000"+
		"\u0000\u06bb\u06be\u0005\u001f\u0000\u0000\u06bc\u06bd\u0005a\u0000\u0000"+
		"\u06bd\u06bf\u0005\u00ca\u0000\u0000\u06be\u06bc\u0001\u0000\u0000\u0000"+
		"\u06be\u06bf\u0001\u0000\u0000\u0000\u06bf\u06c1\u0001\u0000\u0000\u0000"+
		"\u06c0\u06b9\u0001\u0000\u0000\u0000\u06c0\u06ba\u0001\u0000\u0000\u0000"+
		"\u06c1\u06c2\u0001\u0000\u0000\u0000\u06c2\u06c3\u0005\u000e\u0000\u0000"+
		"\u06c3\u06cc\u0005>\u0000\u0000\u06c4\u06c6\u0005\u00af\u0000\u0000\u06c5"+
		"\u06c7\u0003\u012a\u0095\u0000\u06c6\u06c5\u0001\u0000\u0000\u0000\u06c7"+
		"\u06c8\u0001\u0000\u0000\u0000\u06c8\u06c6\u0001\u0000\u0000\u0000\u06c8"+
		"\u06c9\u0001\u0000\u0000\u0000\u06c9\u06ca\u0001\u0000\u0000\u0000\u06ca"+
		"\u06cb\u0005\u00b0\u0000\u0000\u06cb\u06cd\u0001\u0000\u0000\u0000\u06cc"+
		"\u06c4\u0001\u0000\u0000\u0000\u06cc\u06cd\u0001\u0000\u0000\u0000\u06cd"+
		"\u0129\u0001\u0000\u0000\u0000\u06ce\u06cf\u0005}\u0000\u0000\u06cf\u06d0"+
		"\u0005\u008e\u0000\u0000\u06d0\u06e4\u0003\u01bc\u00de\u0000\u06d1\u06d2"+
		"\u0005B\u0000\u0000\u06d2\u06d3\u0005\u0013\u0000\u0000\u06d3\u06e4\u0003"+
		"\u01bc\u00de\u0000\u06d4\u06d5\u0005S\u0000\u0000\u06d5\u06e4\u0003\u01bc"+
		"\u00de\u0000\u06d6\u06d7\u0005\\\u0000\u0000\u06d7\u06e4\u0005S\u0000"+
		"\u0000\u06d8\u06d9\u0005V\u0000\u0000\u06d9\u06e4\u0003\u01bc\u00de\u0000"+
		"\u06da\u06db\u0005\\\u0000\u0000\u06db\u06e4\u0005V\u0000\u0000\u06dc"+
		"\u06dd\u0005\u0014\u0000\u0000\u06dd\u06e4\u0005\u00cd\u0000\u0000\u06de"+
		"\u06df\u0005\\\u0000\u0000\u06df\u06e4\u0005\u0014\u0000\u0000\u06e0\u06e4"+
		"\u0005\u001c\u0000\u0000\u06e1\u06e2\u0005\\\u0000\u0000\u06e2\u06e4\u0005"+
		"\u001c\u0000\u0000\u06e3\u06ce\u0001\u0000\u0000\u0000\u06e3\u06d1\u0001"+
		"\u0000\u0000\u0000\u06e3\u06d4\u0001\u0000\u0000\u0000\u06e3\u06d6\u0001"+
		"\u0000\u0000\u0000\u06e3\u06d8\u0001\u0000\u0000\u0000\u06e3\u06da\u0001"+
		"\u0000\u0000\u0000\u06e3\u06dc\u0001\u0000\u0000\u0000\u06e3\u06de\u0001"+
		"\u0000\u0000\u0000\u06e3\u06e0\u0001\u0000\u0000\u0000\u06e3\u06e1\u0001"+
		"\u0000\u0000\u0000\u06e4\u012b\u0001\u0000\u0000\u0000\u06e5\u06e6\u0005"+
		"\u000e\u0000\u0000\u06e6\u06e7\u0005X\u0000\u0000\u06e7\u012d\u0001\u0000"+
		"\u0000\u0000\u06e8\u06e9\u0005\u000e\u0000\u0000\u06e9\u06ed\u0005\u0091"+
		"\u0000\u0000\u06ea\u06eb\u00059\u0000\u0000\u06eb\u06ec\u0005\u0013\u0000"+
		"\u0000\u06ec\u06ee\u0005\u001f\u0000\u0000\u06ed\u06ea\u0001\u0000\u0000"+
		"\u0000\u06ed\u06ee\u0001\u0000\u0000\u0000\u06ee\u012f\u0001\u0000\u0000"+
		"\u0000\u06ef\u06f0\u0005\n\u0000\u0000\u06f0\u06f1\u0005~\u0000\u0000"+
		"\u06f1\u06f2\u0003\u00fc~\u0000\u06f2\u06f3\u0003\u0132\u0099\u0000\u06f3"+
		"\u0131\u0001\u0000\u0000\u0000\u06f4\u06fd\u0003\u013c\u009e\u0000\u06f5"+
		"\u06fd\u0003\u0118\u008c\u0000\u06f6\u06fd\u0003\u0138\u009c\u0000\u06f7"+
		"\u06fd\u0003\u013a\u009d\u0000\u06f8\u06fd\u0003\u0134\u009a\u0000\u06f9"+
		"\u06fd\u0003\u0136\u009b\u0000\u06fa\u06fd\u0003\u0122\u0091\u0000\u06fb"+
		"\u06fd\u0003\u0126\u0093\u0000\u06fc\u06f4\u0001\u0000\u0000\u0000\u06fc"+
		"\u06f5\u0001\u0000\u0000\u0000\u06fc\u06f6\u0001\u0000\u0000\u0000\u06fc"+
		"\u06f7\u0001\u0000\u0000\u0000\u06fc\u06f8\u0001\u0000\u0000\u0000\u06fc"+
		"\u06f9\u0001\u0000\u0000\u0000\u06fc\u06fa\u0001\u0000\u0000\u0000\u06fc"+
		"\u06fb\u0001\u0000\u0000\u0000\u06fd\u0133\u0001\u0000\u0000\u0000\u06fe"+
		"\u06ff\u00055\u0000\u0000\u06ff\u0701\u0005v\u0000\u0000\u0700\u0702\u0005"+
		"2\u0000\u0000\u0701\u0700\u0001\u0000\u0000\u0000\u0701\u0702\u0001\u0000"+
		"\u0000\u0000\u0702\u0135\u0001\u0000\u0000\u0000\u0703\u0704\u0005\u0084"+
		"\u0000\u0000\u0704\u0705\u0005v\u0000\u0000\u0705\u0137\u0001\u0000\u0000"+
		"\u0000\u0706\u0707\u0005\u0007\u0000\u0000\u0707\u0708\u0005o\u0000\u0000"+
		"\u0708\u0709\u0003\u011a\u008d\u0000\u0709\u0139\u0001\u0000\u0000\u0000"+
		"\u070a\u070b\u0005&\u0000\u0000\u070b\u070c\u0005o\u0000\u0000\u070c\u070d"+
		"\u0003\u011a\u008d\u0000\u070d\u013b\u0001\u0000\u0000\u0000\u070e\u0712"+
		"\u0005\u00af\u0000\u0000\u070f\u0713\u0003\u013e\u009f\u0000\u0710\u0713"+
		"\u0003\u0140\u00a0\u0000\u0711\u0713\u0003\u0142\u00a1\u0000\u0712\u070f"+
		"\u0001\u0000\u0000\u0000\u0712\u0710\u0001\u0000\u0000\u0000\u0712\u0711"+
		"\u0001\u0000\u0000\u0000\u0713\u071c\u0001\u0000\u0000\u0000\u0714\u0718"+
		"\u0005\u00ad\u0000\u0000\u0715\u0719\u0003\u013e\u009f\u0000\u0716\u0719"+
		"\u0003\u0140\u00a0\u0000\u0717\u0719\u0003\u0142\u00a1\u0000\u0718\u0715"+
		"\u0001\u0000\u0000\u0000\u0718\u0716\u0001\u0000\u0000\u0000\u0718\u0717"+
		"\u0001\u0000\u0000\u0000\u0719\u071b\u0001\u0000\u0000\u0000\u071a\u0714"+
		"\u0001\u0000\u0000\u0000\u071b\u071e\u0001\u0000\u0000\u0000\u071c\u071a"+
		"\u0001\u0000\u0000\u0000\u071c\u071d\u0001\u0000\u0000\u0000\u071d\u071f"+
		"\u0001\u0000\u0000\u0000\u071e\u071c\u0001\u0000\u0000\u0000\u071f\u0720"+
		"\u0005\u00b0\u0000\u0000\u0720\u013d\u0001\u0000\u0000\u0000\u0721\u0722"+
		"\u0005\u0007\u0000\u0000\u0722\u0723\u0003\u0144\u00a2\u0000\u0723\u0729"+
		"\u0003\u00bc^\u0000\u0724\u072a\u0003\u00c2a\u0000\u0725\u072a\u0003\u0128"+
		"\u0094\u0000\u0726\u072a\u0003\u012c\u0096\u0000\u0727\u072a\u0003\u012e"+
		"\u0097\u0000\u0728\u072a\u0003\u0104\u0082\u0000\u0729\u0724\u0001\u0000"+
		"\u0000\u0000\u0729\u0725\u0001\u0000\u0000\u0000\u0729\u0726\u0001\u0000"+
		"\u0000\u0000\u0729\u0727\u0001\u0000\u0000\u0000\u0729\u0728\u0001\u0000"+
		"\u0000\u0000\u0729\u072a\u0001\u0000\u0000\u0000\u072a\u072c\u0001\u0000"+
		"\u0000\u0000\u072b\u072d\u0003\u01b4\u00da\u0000\u072c\u072b\u0001\u0000"+
		"\u0000\u0000\u072c\u072d\u0001\u0000\u0000\u0000\u072d\u013f\u0001\u0000"+
		"\u0000\u0000\u072e\u072f\u0005&\u0000\u0000\u072f\u0730\u0003\u0144\u00a2"+
		"\u0000\u0730\u0141\u0001\u0000\u0000\u0000\u0731\u0732\u0005W\u0000\u0000"+
		"\u0732\u073e\u0003\u0144\u00a2\u0000\u0733\u0735\u0003\u00bc^\u0000\u0734"+
		"\u0736\u0003\u00c2a\u0000\u0735\u0734\u0001\u0000\u0000\u0000\u0735\u0736"+
		"\u0001\u0000\u0000\u0000\u0736\u0738\u0001\u0000\u0000\u0000\u0737\u0739"+
		"\u0003\u01b4\u00da\u0000\u0738\u0737\u0001\u0000\u0000\u0000\u0738\u0739"+
		"\u0001\u0000\u0000\u0000\u0739\u073f\u0001\u0000\u0000\u0000\u073a\u073f"+
		"\u0003\u0128\u0094\u0000\u073b\u073f\u0003\u012e\u0097\u0000\u073c\u073d"+
		"\u0005&\u0000\u0000\u073d\u073f\u0005>\u0000\u0000\u073e\u0733\u0001\u0000"+
		"\u0000\u0000\u073e\u073a\u0001\u0000\u0000\u0000\u073e\u073b\u0001\u0000"+
		"\u0000\u0000\u073e\u073c\u0001\u0000\u0000\u0000\u073f\u0143\u0001\u0000"+
		"\u0000\u0000\u0740\u0745\u0003\u0146\u00a3\u0000\u0741\u0742\u0005\u00b6"+
		"\u0000\u0000\u0742\u0744\u0003\u0148\u00a4\u0000\u0743\u0741\u0001\u0000"+
		"\u0000\u0000\u0744\u0747\u0001\u0000\u0000\u0000\u0745\u0743\u0001\u0000"+
		"\u0000\u0000\u0745\u0746\u0001\u0000\u0000\u0000\u0746\u0145\u0001\u0000"+
		"\u0000\u0000\u0747\u0745\u0001\u0000\u0000\u0000\u0748\u074d\u0003\u01c2"+
		"\u00e1\u0000\u0749\u074a\u0005\u00b1\u0000\u0000\u074a\u074c\u0005\u00b2"+
		"\u0000\u0000\u074b\u0749\u0001\u0000\u0000\u0000\u074c\u074f\u0001\u0000"+
		"\u0000\u0000\u074d\u074b\u0001\u0000\u0000\u0000\u074d\u074e\u0001\u0000"+
		"\u0000\u0000\u074e\u0147\u0001\u0000\u0000\u0000\u074f\u074d\u0001\u0000"+
		"\u0000\u0000\u0750\u0755\u0003\u01c2\u00e1\u0000\u0751\u0752\u0005\u00b1"+
		"\u0000\u0000\u0752\u0754\u0005\u00b2\u0000\u0000\u0753\u0751\u0001\u0000"+
		"\u0000\u0000\u0754\u0757\u0001\u0000\u0000\u0000\u0755\u0753\u0001\u0000"+
		"\u0000\u0000\u0755\u0756\u0001\u0000\u0000\u0000\u0756\u075c\u0001\u0000"+
		"\u0000\u0000\u0757\u0755\u0001\u0000\u0000\u0000\u0758\u0759\u0005\u008b"+
		"\u0000\u0000\u0759\u075a\u0005\u00af\u0000\u0000\u075a\u075c\u0005\u00b0"+
		"\u0000\u0000\u075b\u0750\u0001\u0000\u0000\u0000\u075b\u0758\u0001\u0000"+
		"\u0000\u0000\u075c\u0149\u0001\u0000\u0000\u0000\u075d\u075e\u0005&\u0000"+
		"\u0000\u075e\u0761\u0005~\u0000\u0000\u075f\u0760\u0005?\u0000\u0000\u0760"+
		"\u0762\u0005.\u0000\u0000\u0761\u075f\u0001\u0000\u0000\u0000\u0761\u0762"+
		"\u0001\u0000\u0000\u0000\u0762\u0763\u0001\u0000\u0000\u0000\u0763\u0764"+
		"\u0003\u00fc~\u0000\u0764\u014b\u0001\u0000\u0000\u0000\u0765\u0766\u0005"+
		"\u001b\u0000\u0000\u0766\u076a\u0005C\u0000\u0000\u0767\u0768\u0005?\u0000"+
		"\u0000\u0768\u0769\u0005]\u0000\u0000\u0769\u076b\u0005.\u0000\u0000\u076a"+
		"\u0767\u0001\u0000\u0000\u0000\u076a\u076b\u0001\u0000\u0000\u0000\u076b"+
		"\u076c\u0001\u0000\u0000\u0000\u076c\u076d\u0003\u014e\u00a7\u0000\u076d"+
		"\u076e\u0005a\u0000\u0000\u076e\u0784\u0003\u00fc~\u0000\u076f\u0770\u0005"+
		"\u00af\u0000\u0000\u0770\u0771\u0003\u0150\u00a8\u0000\u0771\u0777\u0005"+
		"\u00b0\u0000\u0000\u0772\u0774\u0005\u008e\u0000\u0000\u0773\u0775\u0005"+
		"\\\u0000\u0000\u0774\u0773\u0001\u0000\u0000\u0000\u0774\u0775\u0001\u0000"+
		"\u0000\u0000\u0775\u0776\u0001\u0000\u0000\u0000\u0776\u0778\u0005^\u0000"+
		"\u0000\u0777\u0772\u0001\u0000\u0000\u0000\u0777\u0778\u0001\u0000\u0000"+
		"\u0000\u0778\u077e\u0001\u0000\u0000\u0000\u0779\u077a\u0005\u008e\u0000"+
		"\u0000\u077a\u077b\u0005\u008f\u0000\u0000\u077b\u077c\u0005L\u0000\u0000"+
		"\u077c\u077d\u0005i\u0000\u0000\u077d\u077f\u0005u\u0000\u0000\u077e\u0779"+
		"\u0001\u0000\u0000\u0000\u077e\u077f\u0001\u0000\u0000\u0000\u077f\u0785"+
		"\u0001\u0000\u0000\u0000\u0780\u0781\u0005\u00af\u0000\u0000\u0781\u0782"+
		"\u0003\u0150\u00a8\u0000\u0782\u0783\u0006\u00a6\uffff\uffff\u0000\u0783"+
		"\u0785\u0001\u0000\u0000\u0000\u0784\u076f\u0001\u0000\u0000\u0000\u0784"+
		"\u0780\u0001\u0000\u0000\u0000\u0785\u0787\u0001\u0000\u0000\u0000\u0786"+
		"\u0788\u0003\u01b4\u00da\u0000\u0787\u0786\u0001\u0000\u0000\u0000\u0787"+
		"\u0788\u0001\u0000\u0000\u0000\u0788\u014d\u0001\u0000\u0000\u0000\u0789"+
		"\u078a\u0003\u01c2\u00e1\u0000\u078a\u014f\u0001\u0000\u0000\u0000\u078b"+
		"\u0790\u0003\u0152\u00a9\u0000\u078c\u078d\u0005\u00ad\u0000\u0000\u078d"+
		"\u078f\u0003\u0152\u00a9\u0000\u078e\u078c\u0001\u0000\u0000\u0000\u078f"+
		"\u0792\u0001\u0000\u0000\u0000\u0790\u078e\u0001\u0000\u0000\u0000\u0790"+
		"\u0791\u0001\u0000\u0000\u0000\u0791\u0151\u0001\u0000\u0000\u0000\u0792"+
		"\u0790\u0001\u0000\u0000\u0000\u0793\u0795\u0003\u0158\u00ac\u0000\u0794"+
		"\u0796\u0003\u0162\u00b1\u0000\u0795\u0794\u0001\u0000\u0000\u0000\u0795"+
		"\u0796\u0001\u0000\u0000\u0000\u0796\u0799\u0001\u0000\u0000\u0000\u0797"+
		"\u0799\u0003\u0154\u00aa\u0000\u0798\u0793\u0001\u0000\u0000\u0000\u0798"+
		"\u0797\u0001\u0000\u0000\u0000\u0799\u0153\u0001\u0000\u0000\u0000\u079a"+
		"\u079b\u0003\u01c2\u00e1\u0000\u079b\u079d\u0005\u00af\u0000\u0000\u079c"+
		"\u079e\u0003\u0158\u00ac\u0000\u079d\u079c\u0001\u0000\u0000\u0000\u079d"+
		"\u079e\u0001\u0000\u0000\u0000\u079e\u07a0\u0001\u0000\u0000\u0000\u079f"+
		"\u07a1\u0003\u0162\u00b1\u0000\u07a0\u079f\u0001\u0000\u0000\u0000\u07a0"+
		"\u07a1\u0001\u0000\u0000\u0000\u07a1\u07a3\u0001\u0000\u0000\u0000\u07a2"+
		"\u07a4\u0003\u0156\u00ab\u0000\u07a3\u07a2\u0001\u0000\u0000\u0000\u07a3"+
		"\u07a4\u0001\u0000\u0000\u0000\u07a4\u07a5\u0001\u0000\u0000\u0000\u07a5"+
		"\u07a6\u0005\u00b0\u0000\u0000\u07a6\u0155\u0001\u0000\u0000\u0000\u07a7"+
		"\u07a8\u0005\u00ad\u0000\u0000\u07a8\u07aa\u0003x<\u0000\u07a9\u07a7\u0001"+
		"\u0000\u0000\u0000\u07aa\u07ab\u0001\u0000\u0000\u0000\u07ab\u07a9\u0001"+
		"\u0000\u0000\u0000\u07ab\u07ac\u0001\u0000\u0000\u0000\u07ac\u0157\u0001"+
		"\u0000\u0000\u0000\u07ad\u07af\u0003\u015c\u00ae\u0000\u07ae\u07ad\u0001"+
		"\u0000\u0000\u0000\u07ae\u07af\u0001\u0000\u0000\u0000\u07af\u07b5\u0001"+
		"\u0000\u0000\u0000\u07b0\u07b6\u0003\u00eau\u0000\u07b1\u07b3\u0003\u015e"+
		"\u00af\u0000\u07b2\u07b4\u0003\u0160\u00b0\u0000\u07b3\u07b2\u0001\u0000"+
		"\u0000\u0000\u07b3\u07b4\u0001\u0000\u0000\u0000\u07b4\u07b6\u0001\u0000"+
		"\u0000\u0000\u07b5\u07b0\u0001\u0000\u0000\u0000\u07b5\u07b1\u0001\u0000"+
		"\u0000\u0000\u07b6\u07b9\u0001\u0000\u0000\u0000\u07b7\u07b9\u0003\u015a"+
		"\u00ad\u0000\u07b8\u07ae\u0001\u0000\u0000\u0000\u07b8\u07b7\u0001\u0000"+
		"\u0000\u0000\u07b9\u0159\u0001\u0000\u0000\u0000\u07ba\u07bb\u0005\'\u0000"+
		"\u0000\u07bb\u07bc\u0005\u00af\u0000\u0000\u07bc\u07bd\u0003\u00eau\u0000"+
		"\u07bd\u07bf\u0005\u00b0\u0000\u0000\u07be\u07c0\u0003\u0160\u00b0\u0000"+
		"\u07bf\u07be\u0001\u0000\u0000\u0000\u07bf\u07c0\u0001\u0000\u0000\u0000"+
		"\u07c0\u07cc\u0001\u0000\u0000\u0000\u07c1\u07c2\u0005K\u0000\u0000\u07c2"+
		"\u07c3\u0005\u00af\u0000\u0000\u07c3\u07c4\u0003\u00eau\u0000\u07c4\u07c5"+
		"\u0005\u00b0\u0000\u0000\u07c5\u07cc\u0001\u0000\u0000\u0000\u07c6\u07c7"+
		"\u0005L\u0000\u0000\u07c7\u07c8\u0005\u00af\u0000\u0000\u07c8\u07c9\u0003"+
		"\u00eau\u0000\u07c9\u07ca\u0005\u00b0\u0000\u0000\u07ca\u07cc\u0001\u0000"+
		"\u0000\u0000\u07cb\u07ba\u0001\u0000\u0000\u0000\u07cb\u07c1\u0001\u0000"+
		"\u0000\u0000\u07cb\u07c6\u0001\u0000\u0000\u0000\u07cc\u015b\u0001\u0000"+
		"\u0000\u0000\u07cd\u07ce\u0005\u0004\u0000\u0000\u07ce\u015d\u0001\u0000"+
		"\u0000\u0000\u07cf\u07da\u0003\u00ecv\u0000\u07d0\u07d1\u0005\u00b6\u0000"+
		"\u0000\u07d1\u07d9\u0003\u00ecv\u0000\u07d2\u07d3\u0005\u00b6\u0000\u0000"+
		"\u07d3\u07d4\u0005\u008b\u0000\u0000\u07d4\u07d5\u0005\u00af\u0000\u0000"+
		"\u07d5\u07d9\u0005\u00b0\u0000\u0000\u07d6\u07d7\u0005\u00b1\u0000\u0000"+
		"\u07d7\u07d9\u0005\u00b2\u0000\u0000\u07d8\u07d0\u0001\u0000\u0000\u0000"+
		"\u07d8\u07d2\u0001\u0000\u0000\u0000\u07d8\u07d6\u0001\u0000\u0000\u0000"+
		"\u07d9\u07dc\u0001\u0000\u0000\u0000\u07da\u07d8\u0001\u0000\u0000\u0000"+
		"\u07da\u07db\u0001\u0000\u0000\u0000\u07db\u07e7\u0001\u0000\u0000\u0000"+
		"\u07dc\u07da\u0001\u0000\u0000\u0000\u07dd\u07de\u0005\u00b1\u0000\u0000"+
		"\u07de\u07e8\u0005\u00b2\u0000\u0000\u07df\u07e0\u0005\u00b6\u0000\u0000"+
		"\u07e0\u07e1\u0005\u008b\u0000\u0000\u07e1\u07e2\u0005\u00af\u0000\u0000"+
		"\u07e2\u07e8\u0005\u00b0\u0000\u0000\u07e3\u07e4\u0005\u00b6\u0000\u0000"+
		"\u07e4\u07e5\u0005L\u0000\u0000\u07e5\u07e6\u0005\u00af\u0000\u0000\u07e6"+
		"\u07e8\u0005\u00b0\u0000\u0000\u07e7\u07dd\u0001\u0000\u0000\u0000\u07e7"+
		"\u07df\u0001\u0000\u0000\u0000\u07e7\u07e3\u0001\u0000\u0000\u0000\u07e8"+
		"\u015f\u0001\u0000\u0000\u0000\u07e9\u07ea\u0005\u00b6\u0000\u0000\u07ea"+
		"\u07eb\u0003\u00eau\u0000\u07eb\u0161\u0001\u0000\u0000\u0000\u07ec\u07f9"+
		"\u0005\u000e\u0000\u0000\u07ed\u07fa\u0005\u009f\u0000\u0000\u07ee\u07fa"+
		"\u0005\u00a0\u0000\u0000\u07ef\u07fa\u0005\u009b\u0000\u0000\u07f0\u07fa"+
		"\u0005\u00a5\u0000\u0000\u07f1\u07fa\u0005\u009a\u0000\u0000\u07f2\u07fa"+
		"\u0005\u00a2\u0000\u0000\u07f3\u07fa\u0005\u00a8\u0000\u0000\u07f4\u07f6"+
		"\u0005\u009e\u0000\u0000\u07f5\u07f7\u0003\u01ac\u00d6\u0000\u07f6\u07f5"+
		"\u0001\u0000\u0000\u0000\u07f6\u07f7\u0001\u0000\u0000\u0000\u07f7\u07fa"+
		"\u0001\u0000\u0000\u0000\u07f8\u07fa\u0005\u00a3\u0000\u0000\u07f9\u07ed"+
		"\u0001\u0000\u0000\u0000\u07f9\u07ee\u0001\u0000\u0000\u0000\u07f9\u07ef"+
		"\u0001\u0000\u0000\u0000\u07f9\u07f0\u0001\u0000\u0000\u0000\u07f9\u07f1"+
		"\u0001\u0000\u0000\u0000\u07f9\u07f2\u0001\u0000\u0000\u0000\u07f9\u07f3"+
		"\u0001\u0000\u0000\u0000\u07f9\u07f4\u0001\u0000\u0000\u0000\u07f9\u07f8"+
		"\u0001\u0000\u0000\u0000\u07fa\u0163\u0001\u0000\u0000\u0000\u07fb\u07fc"+
		"\u0005\u001b\u0000\u0000\u07fc\u07fd\u00058\u0000\u0000\u07fd\u0801\u0005"+
		"C\u0000\u0000\u07fe\u07ff\u0005?\u0000\u0000\u07ff\u0800\u0005]\u0000"+
		"\u0000\u0800\u0802\u0005.\u0000\u0000\u0801\u07fe\u0001\u0000\u0000\u0000"+
		"\u0801\u0802\u0001\u0000\u0000\u0000\u0802\u0803\u0001\u0000\u0000\u0000"+
		"\u0803\u0804\u0003\u014e\u00a7\u0000\u0804\u0805\u0005a\u0000\u0000\u0805"+
		"\u0806\u0003\u00fc~\u0000\u0806\u0808\u0003\u0166\u00b3\u0000\u0807\u0809"+
		"\u0003\u016c\u00b6\u0000\u0808\u0807\u0001\u0000\u0000\u0000\u0808\u0809"+
		"\u0001\u0000\u0000\u0000\u0809\u080b\u0001\u0000\u0000\u0000\u080a\u080c"+
		"\u0005f\u0000\u0000\u080b\u080a\u0001\u0000\u0000\u0000\u080b\u080c\u0001"+
		"\u0000\u0000\u0000\u080c\u080e\u0001\u0000\u0000\u0000\u080d\u080f\u0003"+
		"\u01b4\u00da\u0000\u080e\u080d\u0001\u0000\u0000\u0000\u080e\u080f\u0001"+
		"\u0000\u0000\u0000\u080f\u0165\u0001\u0000\u0000\u0000\u0810\u0811\u0005"+
		"\u00af\u0000\u0000\u0811\u0812\u0003\u0168\u00b4\u0000\u0812\u0813\u0005"+
		"\u00b0\u0000\u0000\u0813\u0819\u0001\u0000\u0000\u0000\u0814\u0815\u0005"+
		"\u00af\u0000\u0000\u0815\u0816\u0003\u0168\u00b4\u0000\u0816\u0817\u0006"+
		"\u00b3\uffff\uffff\u0000\u0817\u0819\u0001\u0000\u0000\u0000\u0818\u0810"+
		"\u0001\u0000\u0000\u0000\u0818\u0814\u0001\u0000\u0000\u0000\u0819\u0167"+
		"\u0001\u0000\u0000\u0000\u081a\u081f\u0003\u016a\u00b5\u0000\u081b\u081c"+
		"\u0005\u00ad\u0000\u0000\u081c\u081e\u0003\u016a\u00b5\u0000\u081d\u081b"+
		"\u0001\u0000\u0000\u0000\u081e\u0821\u0001\u0000\u0000\u0000\u081f\u081d"+
		"\u0001\u0000\u0000\u0000\u081f\u0820\u0001\u0000\u0000\u0000\u0820\u0169"+
		"\u0001\u0000\u0000\u0000\u0821\u081f\u0001\u0000\u0000\u0000\u0822\u0824"+
		"\u0003\u0158\u00ac\u0000\u0823\u0825\u0003\u01ac\u00d6\u0000\u0824\u0823"+
		"\u0001\u0000\u0000\u0000\u0824\u0825\u0001\u0000\u0000\u0000\u0825\u016b"+
		"\u0001\u0000\u0000\u0000\u0826\u082a\u0003\u016e\u00b7\u0000\u0827\u0829"+
		"\u0003\u016e\u00b7\u0000\u0828\u0827\u0001\u0000\u0000\u0000\u0829\u082c"+
		"\u0001\u0000\u0000\u0000\u082a\u0828\u0001\u0000\u0000\u0000\u082a\u082b"+
		"\u0001\u0000\u0000\u0000\u082b\u016d\u0001\u0000\u0000\u0000\u082c\u082a"+
		"\u0001\u0000\u0000\u0000\u082d\u082e\u0005,\u0000\u0000\u082e\u082f\u0005"+
		"\u00bd\u0000\u0000\u082f\u0834\u0005\u00cd\u0000\u0000\u0830\u0831\u0005"+
		"-\u0000\u0000\u0831\u0832\u0005\u00bd\u0000\u0000\u0832\u0834\u0005\u00cd"+
		"\u0000\u0000\u0833\u082d\u0001\u0000\u0000\u0000\u0833\u0830\u0001\u0000"+
		"\u0000\u0000\u0834\u016f\u0001\u0000\u0000\u0000\u0835\u0836\u0005&\u0000"+
		"\u0000\u0836\u0839\u0005C\u0000\u0000\u0837\u0838\u0005?\u0000\u0000\u0838"+
		"\u083a\u0005.\u0000\u0000\u0839\u0837\u0001\u0000\u0000\u0000\u0839\u083a"+
		"\u0001\u0000\u0000\u0000\u083a\u083b\u0001\u0000\u0000\u0000\u083b\u083c"+
		"\u0003\u014e\u00a7\u0000\u083c\u083d\u0005a\u0000\u0000\u083d\u083f\u0003"+
		"\u00fc~\u0000\u083e\u0840\u0005f\u0000\u0000\u083f\u083e\u0001\u0000\u0000"+
		"\u0000\u083f\u0840\u0001\u0000\u0000\u0000\u0840\u0171\u0001\u0000\u0000"+
		"\u0000\u0841\u0844\u0007\r\u0000\u0000\u0842\u0843\u0005\u000e\u0000\u0000"+
		"\u0843\u0845\u0005H\u0000\u0000\u0844\u0842\u0001\u0000\u0000\u0000\u0844"+
		"\u0845\u0001\u0000\u0000\u0000\u0845\u0857\u0001\u0000\u0000\u0000\u0846"+
		"\u0847\u0005~\u0000\u0000\u0847\u0850\u0003\u00fc~\u0000\u0848\u0849\u0005"+
		"\u00af\u0000\u0000\u0849\u084a\u0003\u0174\u00ba\u0000\u084a\u084b\u0005"+
		"\u00b0\u0000\u0000\u084b\u0851\u0001\u0000\u0000\u0000\u084c\u084d\u0005"+
		"\u00af\u0000\u0000\u084d\u084e\u0003\u0174\u00ba\u0000\u084e\u084f\u0006"+
		"\u00b9\uffff\uffff\u0000\u084f\u0851\u0001\u0000\u0000\u0000\u0850\u0848"+
		"\u0001\u0000\u0000\u0000\u0850\u084c\u0001\u0000\u0000\u0000\u0850\u0851"+
		"\u0001\u0000\u0000\u0000\u0851\u0858\u0001\u0000\u0000\u0000\u0852\u0853"+
		"\u0005C\u0000\u0000\u0853\u0854\u0003\u014e\u00a7\u0000\u0854\u0855\u0005"+
		"a\u0000\u0000\u0855\u0856\u0003\u00fc~\u0000\u0856\u0858\u0001\u0000\u0000"+
		"\u0000\u0857\u0846\u0001\u0000\u0000\u0000\u0857\u0852\u0001\u0000\u0000"+
		"\u0000\u0858\u0173\u0001\u0000\u0000\u0000\u0859\u085e\u0003\u0144\u00a2"+
		"\u0000\u085a\u085b\u0005\u00ad\u0000\u0000\u085b\u085d\u0003\u0144\u00a2"+
		"\u0000\u085c\u085a\u0001\u0000\u0000\u0000\u085d\u0860\u0001\u0000\u0000"+
		"\u0000\u085e\u085c\u0001\u0000\u0000\u0000\u085e\u085f\u0001\u0000\u0000"+
		"\u0000\u085f\u0175\u0001\u0000\u0000\u0000\u0860\u085e\u0001\u0000\u0000"+
		"\u0000\u0861\u0864\u0005|\u0000\u0000\u0862\u0863\u0005\u000e\u0000\u0000"+
		"\u0863\u0865\u0005H\u0000\u0000\u0864\u0862\u0001\u0000\u0000\u0000\u0864"+
		"\u0865\u0001\u0000\u0000\u0000\u0865\u0874\u0001\u0000\u0000\u0000\u0866"+
		"\u0875\u0005\u007f\u0000\u0000\u0867\u0875\u0005\u0089\u0000\u0000\u0868"+
		"\u0875\u0005t\u0000\u0000\u0869\u086a\u0005\u0088\u0000\u0000\u086a\u0875"+
		"\u0003\u0186\u00c3\u0000\u086b\u086c\u0005s\u0000\u0000\u086c\u0875\u0003"+
		"\u01c2\u00e1\u0000\u086d\u086e\u0005D\u0000\u0000\u086e\u086f\u0005a\u0000"+
		"\u0000\u086f\u0875\u0003\u00fc~\u0000\u0870\u0871\u0005~\u0000\u0000\u0871"+
		"\u0875\u0003\u00fc~\u0000\u0872\u0875\u0005Z\u0000\u0000\u0873\u0875\u0005"+
		"o\u0000\u0000\u0874\u0866\u0001\u0000\u0000\u0000\u0874\u0867\u0001\u0000"+
		"\u0000\u0000\u0874\u0868\u0001\u0000\u0000\u0000\u0874\u0869\u0001\u0000"+
		"\u0000\u0000\u0874\u086b\u0001\u0000\u0000\u0000\u0874\u086d\u0001\u0000"+
		"\u0000\u0000\u0874\u0870\u0001\u0000\u0000\u0000\u0874\u0872\u0001\u0000"+
		"\u0000\u0000\u0874\u0873\u0001\u0000\u0000\u0000\u0875\u0177\u0001\u0000"+
		"\u0000\u0000\u0876\u0877\u0005\u001b\u0000\u0000\u0877\u0878\u0005\u0088"+
		"\u0000\u0000\u0878\u087a\u0003\u018a\u00c5\u0000\u0879\u087b\u0003\u0192"+
		"\u00c9\u0000\u087a\u0879\u0001\u0000\u0000\u0000\u087a\u087b\u0001\u0000"+
		"\u0000\u0000\u087b\u087d\u0001\u0000\u0000\u0000\u087c\u087e\u0005\b\u0000"+
		"\u0000\u087d\u087c\u0001\u0000\u0000\u0000\u087d\u087e\u0001\u0000\u0000"+
		"\u0000\u087e\u0179\u0001\u0000\u0000\u0000\u087f\u0880\u0005\u001b\u0000"+
		"\u0000\u0880\u0881\u0005s\u0000\u0000\u0881\u0882\u0003\u01c2\u00e1\u0000"+
		"\u0882\u017b\u0001\u0000\u0000\u0000\u0883\u0884\u0005\n\u0000\u0000\u0884"+
		"\u0885\u0005\u0088\u0000\u0000\u0885\u0887\u0003\u0186\u00c3\u0000\u0886"+
		"\u0888\u0003\u0190\u00c8\u0000\u0887\u0886\u0001\u0000\u0000\u0000\u0887"+
		"\u0888\u0001\u0000\u0000\u0000\u0888\u088a\u0001\u0000\u0000\u0000\u0889"+
		"\u088b\u0005\u0096\u0000\u0000\u088a\u0889\u0001\u0000\u0000\u0000\u088a"+
		"\u088b\u0001\u0000\u0000\u0000\u088b\u088d\u0001\u0000\u0000\u0000\u088c"+
		"\u088e\u0005\u0094\u0000\u0000\u088d\u088c\u0001\u0000\u0000\u0000\u088d"+
		"\u088e\u0001\u0000\u0000\u0000\u088e\u0890\u0001\u0000\u0000\u0000\u088f"+
		"\u0891\u0003\u018e\u00c7\u0000\u0890\u088f\u0001\u0000\u0000\u0000\u0890"+
		"\u0891\u0001\u0000\u0000\u0000\u0891\u0893\u0001\u0000\u0000\u0000\u0892"+
		"\u0894\u0003\u0192\u00c9\u0000\u0893\u0892\u0001\u0000\u0000\u0000\u0893"+
		"\u0894\u0001\u0000\u0000\u0000\u0894\u017d\u0001\u0000\u0000\u0000\u0895"+
		"\u0896\u0005&\u0000\u0000\u0896\u0897\u0005\u0088\u0000\u0000\u0897\u0899"+
		"\u0003\u0186\u00c3\u0000\u0898\u089a\u0005\u0016\u0000\u0000\u0899\u0898"+
		"\u0001\u0000\u0000\u0000\u0899\u089a\u0001\u0000\u0000\u0000\u089a\u017f"+
		"\u0001\u0000\u0000\u0000\u089b\u089c\u0005&\u0000\u0000\u089c\u089d\u0005"+
		"s\u0000\u0000\u089d\u089e\u0003\u01c2\u00e1\u0000\u089e\u0181\u0001\u0000"+
		"\u0000\u0000\u089f\u08a3\u0005:\u0000\u0000\u08a0\u08a4\u0003\u0194\u00ca"+
		"\u0000\u08a1\u08a4\u0003\u0196\u00cb\u0000\u08a2\u08a4\u0003\u0198\u00cc"+
		"\u0000\u08a3\u08a0\u0001\u0000\u0000\u0000\u08a3\u08a1\u0001\u0000\u0000"+
		"\u0000\u08a3\u08a2\u0001\u0000\u0000\u0000\u08a4\u0183\u0001\u0000\u0000"+
		"\u0000\u08a5\u08a9\u0005r\u0000\u0000\u08a6\u08aa\u0003\u019a\u00cd\u0000"+
		"\u08a7\u08aa\u0003\u019c\u00ce\u0000\u08a8\u08aa\u0003\u019e\u00cf\u0000"+
		"\u08a9\u08a6\u0001\u0000\u0000\u0000\u08a9\u08a7\u0001\u0000\u0000\u0000"+
		"\u08a9\u08a8\u0001\u0000\u0000\u0000\u08aa\u0185\u0001\u0000\u0000\u0000"+
		"\u08ab\u08ae\u0003\u01c2\u00e1\u0000\u08ac\u08ae\u0003\u01be\u00df\u0000"+
		"\u08ad\u08ab\u0001\u0000\u0000\u0000\u08ad\u08ac\u0001\u0000\u0000\u0000"+
		"\u08ae\u0187\u0001\u0000\u0000\u0000\u08af\u08b0\u0005=\u0000\u0000\u08b0"+
		"\u08b1\u0003\u018c\u00c6\u0000\u08b1\u0189\u0001\u0000\u0000\u0000\u08b2"+
		"\u08b3\u0003\u01c2\u00e1\u0000\u08b3\u08b5\u0003\u0188\u00c4\u0000\u08b4"+
		"\u08b6\u0005\u0094\u0000\u0000\u08b5\u08b4\u0001\u0000\u0000\u0000\u08b5"+
		"\u08b6\u0001\u0000\u0000\u0000\u08b6\u08b8\u0001\u0000\u0000\u0000\u08b7"+
		"\u08b9\u0003\u018e\u00c7\u0000\u08b8\u08b7\u0001\u0000\u0000\u0000\u08b8"+
		"\u08b9\u0001\u0000\u0000\u0000\u08b9\u08be\u0001\u0000\u0000\u0000\u08ba"+
		"\u08bb\u0003\u01be\u00df\u0000\u08bb\u08bc\u0005\u0093\u0000\u0000\u08bc"+
		"\u08be\u0001\u0000\u0000\u0000\u08bd\u08b2\u0001\u0000\u0000\u0000\u08bd"+
		"\u08ba\u0001\u0000\u0000\u0000\u08be\u018b\u0001\u0000\u0000\u0000\u08bf"+
		"\u08c0\u0005\u0013\u0000\u0000\u08c0\u08c1\u0003\u01be\u00df\u0000\u08c1"+
		"\u018d\u0001\u0000\u0000\u0000\u08c2\u08c3\u0005g\u0000\u0000\u08c3\u08c4"+
		"\u0005O\u0000\u0000\u08c4\u08c5\u0003\u01b6\u00db\u0000\u08c5\u018f\u0001"+
		"\u0000\u0000\u0000\u08c6\u08c8\u0003\u0188\u00c4\u0000\u08c7\u08c9\u0005"+
		"\u0095\u0000\u0000\u08c8\u08c7\u0001\u0000\u0000\u0000\u08c8\u08c9\u0001"+
		"\u0000\u0000\u0000\u08c9\u0191\u0001\u0000\u0000\u0000\u08ca\u08cb\u0005"+
		"\u0006\u0000\u0000\u08cb\u08cc\u0007\u000e\u0000\u0000\u08cc\u0193\u0001"+
		"\u0000\u0000\u0000\u08cd\u08ce\u0003\u01c0\u00e0\u0000\u08ce\u08cf\u0005"+
		"\u0081\u0000\u0000\u08cf\u08d0\u0003\u01a0\u00d0\u0000\u08d0\u0195\u0001"+
		"\u0000\u0000\u0000\u08d1\u08d2\u0003\u01a2\u00d1\u0000\u08d2\u08d3\u0005"+
		"\u0081\u0000\u0000\u08d3\u08d4\u0003\u01c2\u00e1\u0000\u08d4\u0197\u0001"+
		"\u0000\u0000\u0000\u08d5\u08d6\u0003\u01a6\u00d3\u0000\u08d6\u08da\u0005"+
		"a\u0000\u0000\u08d7\u08db\u0003\u01a8\u00d4\u0000\u08d8\u08d9\u0005Y\u0000"+
		"\u0000\u08d9\u08db\u0003\u00fe\u007f\u0000\u08da\u08d7\u0001\u0000\u0000"+
		"\u0000\u08da\u08d8\u0001\u0000\u0000\u0000\u08db\u08dc\u0001\u0000\u0000"+
		"\u0000\u08dc\u08dd\u0005\u0081\u0000\u0000\u08dd\u08de\u0003\u01c2\u00e1"+
		"\u0000\u08de\u0199\u0001\u0000\u0000\u0000\u08df\u08e0\u0003\u01c0\u00e0"+
		"\u0000\u08e0\u08e1\u00056\u0000\u0000\u08e1\u08e2\u0003\u01a0\u00d0\u0000"+
		"\u08e2\u019b\u0001\u0000\u0000\u0000\u08e3\u08e4\u0003\u01a2\u00d1\u0000"+
		"\u08e4\u08e5\u00056\u0000\u0000\u08e5\u08e6\u0003\u01c2\u00e1\u0000\u08e6"+
		"\u019d\u0001\u0000\u0000\u0000\u08e7\u08e8\u0003\u01a6\u00d3\u0000\u08e8"+
		"\u08ec\u0005a\u0000\u0000\u08e9\u08ed\u0003\u01a8\u00d4\u0000\u08ea\u08eb"+
		"\u0005Y\u0000\u0000\u08eb\u08ed\u0003\u00fe\u007f\u0000\u08ec\u08e9\u0001"+
		"\u0000\u0000\u0000\u08ec\u08ea\u0001\u0000\u0000\u0000\u08ed\u08ee\u0001"+
		"\u0000\u0000\u0000\u08ee\u08ef\u00056\u0000\u0000\u08ef\u08f0\u0003\u01c2"+
		"\u00e1\u0000\u08f0\u019f\u0001\u0000\u0000\u0000\u08f1\u08f2\u0005\u0088"+
		"\u0000\u0000\u08f2\u08f6\u0003\u0186\u00c3\u0000\u08f3\u08f4\u0005s\u0000"+
		"\u0000\u08f4\u08f6\u0003\u01c2\u00e1\u0000\u08f5\u08f1\u0001\u0000\u0000"+
		"\u0000\u08f5\u08f3\u0001\u0000\u0000\u0000\u08f6\u01a1\u0001\u0000\u0000"+
		"\u0000\u08f7\u08fc\u0003\u01a4\u00d2\u0000\u08f8\u08f9\u0005\u00ad\u0000"+
		"\u0000\u08f9\u08fb\u0003\u01a4\u00d2\u0000\u08fa\u08f8\u0001\u0000\u0000"+
		"\u0000\u08fb\u08fe\u0001\u0000\u0000\u0000\u08fc\u08fa\u0001\u0000\u0000"+
		"\u0000\u08fc\u08fd\u0001\u0000\u0000\u0000\u08fd\u01a3\u0001\u0000\u0000"+
		"\u0000\u08fe\u08fc\u0001\u0000\u0000\u0000\u08ff\u0902\u0003\u01c2\u00e1"+
		"\u0000\u0900\u0902\u0005\u0092\u0000\u0000\u0901\u08ff\u0001\u0000\u0000"+
		"\u0000\u0901\u0900\u0001\u0000\u0000\u0000\u0902\u01a5\u0001\u0000\u0000"+
		"\u0000\u0903\u0906\u0003\u01a4\u00d2\u0000\u0904\u0906\u0005\t\u0000\u0000"+
		"\u0905\u0903\u0001\u0000\u0000\u0000\u0905\u0904\u0001\u0000\u0000\u0000"+
		"\u0906\u090e\u0001\u0000\u0000\u0000\u0907\u090a\u0005\u00ad\u0000\u0000"+
		"\u0908\u090b\u0003\u01a4\u00d2\u0000\u0909\u090b\u0005\t\u0000\u0000\u090a"+
		"\u0908\u0001\u0000\u0000\u0000\u090a\u0909\u0001\u0000\u0000\u0000\u090b"+
		"\u090d\u0001\u0000\u0000\u0000\u090c\u0907\u0001\u0000\u0000\u0000\u090d"+
		"\u0910\u0001\u0000\u0000\u0000\u090e\u090c\u0001\u0000\u0000\u0000\u090e"+
		"\u090f\u0001\u0000\u0000\u0000\u090f\u01a7\u0001\u0000\u0000\u0000\u0910"+
		"\u090e\u0001\u0000\u0000\u0000\u0911\u0912\u0003\u00fc~\u0000\u0912\u01a9"+
		"\u0001\u0000\u0000\u0000\u0913\u091b\u0003\u01ac\u00d6\u0000\u0914\u091b"+
		"\u0003\u01ae\u00d7\u0000\u0915\u091b\u0003\u01be\u00df\u0000\u0916\u091b"+
		"\u0003\u01ba\u00dd\u0000\u0917\u091b\u0005\u00cc\u0000\u0000\u0918\u091b"+
		"\u0005\u00cb\u0000\u0000\u0919\u091b\u0005\u00ca\u0000\u0000\u091a\u0913"+
		"\u0001\u0000\u0000\u0000\u091a\u0914\u0001\u0000\u0000\u0000\u091a\u0915"+
		"\u0001\u0000\u0000\u0000\u091a\u0916\u0001\u0000\u0000\u0000\u091a\u0917"+
		"\u0001\u0000\u0000\u0000\u091a\u0918\u0001\u0000\u0000\u0000\u091a\u0919"+
		"\u0001\u0000\u0000\u0000\u091b\u01ab\u0001\u0000\u0000\u0000\u091c\u091d"+
		"\u0005\u00b3\u0000\u0000\u091d\u0922\u0003\u01b0\u00d8\u0000\u091e\u091f"+
		"\u0005\u00ad\u0000\u0000\u091f\u0921\u0003\u01b0\u00d8\u0000\u0920\u091e"+
		"\u0001\u0000\u0000\u0000\u0921\u0924\u0001\u0000\u0000\u0000\u0922\u0920"+
		"\u0001\u0000\u0000\u0000\u0922\u0923\u0001\u0000\u0000\u0000\u0923\u0925"+
		"\u0001\u0000\u0000\u0000\u0924\u0922\u0001\u0000\u0000\u0000\u0925\u0926"+
		"\u0005\u00b4\u0000\u0000\u0926\u092a\u0001\u0000\u0000\u0000\u0927\u0928"+
		"\u0005\u00b3\u0000\u0000\u0928\u092a\u0005\u00b4\u0000\u0000\u0929\u091c"+
		"\u0001\u0000\u0000\u0000\u0929\u0927\u0001\u0000\u0000\u0000\u092a\u01ad"+
		"\u0001\u0000\u0000\u0000\u092b\u092c\u0005\u00b1\u0000\u0000\u092c\u0931"+
		"\u0003\u01b2\u00d9\u0000\u092d\u092e\u0005\u00ad\u0000\u0000\u092e\u0930"+
		"\u0003\u01b2\u00d9\u0000\u092f\u092d\u0001\u0000\u0000\u0000\u0930\u0933"+
		"\u0001\u0000\u0000\u0000\u0931\u092f\u0001\u0000\u0000\u0000\u0931\u0932"+
		"\u0001\u0000\u0000\u0000\u0932\u0934\u0001\u0000\u0000\u0000\u0933\u0931"+
		"\u0001\u0000\u0000\u0000\u0934\u0935\u0005\u00b2\u0000\u0000\u0935\u0939"+
		"\u0001\u0000\u0000\u0000\u0936\u0937\u0005\u00b1\u0000\u0000\u0937\u0939"+
		"\u0005\u00b2\u0000\u0000\u0938\u092b\u0001\u0000\u0000\u0000\u0938\u0936"+
		"\u0001\u0000\u0000\u0000\u0939\u01af\u0001\u0000\u0000\u0000\u093a\u093b"+
		"\u0005\u00d0\u0000\u0000\u093b\u093c\u0005\u00ae\u0000\u0000\u093c\u093d"+
		"\u0003\u01b2\u00d9\u0000\u093d\u01b1\u0001\u0000\u0000\u0000\u093e\u0946"+
		"\u0003\u01ac\u00d6\u0000\u093f\u0946\u0003\u01ae\u00d7\u0000\u0940\u0946"+
		"\u0005\u00d0\u0000\u0000\u0941\u0946\u0003\u01ba\u00dd\u0000\u0942\u0946"+
		"\u0005\u00cc\u0000\u0000\u0943\u0946\u0005\u00cb\u0000\u0000\u0944\u0946"+
		"\u0005\u00ca\u0000\u0000\u0945\u093e\u0001\u0000\u0000\u0000\u0945\u093f"+
		"\u0001\u0000\u0000\u0000\u0945\u0940\u0001\u0000\u0000\u0000\u0945\u0941"+
		"\u0001\u0000\u0000\u0000\u0945\u0942\u0001\u0000\u0000\u0000\u0945\u0943"+
		"\u0001\u0000\u0000\u0000\u0945\u0944\u0001\u0000\u0000\u0000\u0946\u01b3"+
		"\u0001\u0000\u0000\u0000\u0947\u0948\u0005\u0019\u0000\u0000\u0948\u0949"+
		"\u0003\u01be\u00df\u0000\u0949\u01b5\u0001\u0000\u0000\u0000\u094a\u094b"+
		"\u0005\u00cd\u0000\u0000\u094b\u094c\u0003\u01b8\u00dc\u0000\u094c\u01b7"+
		"\u0001\u0000\u0000\u0000\u094d\u094e\u0007\u000f\u0000\u0000\u094e\u01b9"+
		"\u0001\u0000\u0000\u0000\u094f\u0951\u0005\u00c6\u0000\u0000\u0950\u094f"+
		"\u0001\u0000\u0000\u0000\u0950\u0951\u0001\u0000\u0000\u0000\u0951\u0952"+
		"\u0001\u0000\u0000\u0000\u0952\u0953\u0007\u0010\u0000\u0000\u0953\u01bb"+
		"\u0001\u0000\u0000\u0000\u0954\u0956\u0007\u0003\u0000\u0000\u0955\u0954"+
		"\u0001\u0000\u0000\u0000\u0955\u0956\u0001\u0000\u0000\u0000\u0956\u0957"+
		"\u0001\u0000\u0000\u0000\u0957\u0958\u0005\u00cd\u0000\u0000\u0958\u01bd"+
		"\u0001\u0000\u0000\u0000\u0959\u095a\u0007\u0011\u0000\u0000\u095a\u01bf"+
		"\u0001\u0000\u0000\u0000\u095b\u0960\u0003\u01c2\u00e1\u0000\u095c\u095d"+
		"\u0005\u00ad\u0000\u0000\u095d\u095f\u0003\u01c2\u00e1\u0000\u095e\u095c"+
		"\u0001\u0000\u0000\u0000\u095f\u0962\u0001\u0000\u0000\u0000\u0960\u095e"+
		"\u0001\u0000\u0000\u0000\u0960\u0961\u0001\u0000\u0000\u0000\u0961\u01c1"+
		"\u0001\u0000\u0000\u0000\u0962\u0960\u0001\u0000\u0000\u0000\u0963\u09f9"+
		"\u0005\u0006\u0000\u0000\u0964\u09f9\u0005\u0007\u0000\u0000\u0965\u09f9"+
		"\u0005\b\u0000\u0000\u0966\u09f9\u0005\t\u0000\u0000\u0967\u09f9\u0005"+
		"\n\u0000\u0000\u0968\u09f9\u0005\u000b\u0000\u0000\u0969\u09f9\u0005\f"+
		"\u0000\u0000\u096a\u09f9\u0005\r\u0000\u0000\u096b\u09f9\u0005\u00a7\u0000"+
		"\u0000\u096c\u09f9\u0005\u00a8\u0000\u0000\u096d\u09f9\u0005\u00a9\u0000"+
		"\u0000\u096e\u09f9\u0005\u00aa\u0000\u0000\u096f\u09f9\u0005\u0010\u0000"+
		"\u0000\u0970\u09f9\u0005\u000e\u0000\u0000\u0971\u09f9\u0005\u000f\u0000"+
		"\u0000\u0972\u09f9\u0005\u0011\u0000\u0000\u0973\u09f9\u0005\u0012\u0000"+
		"\u0000\u0974\u09f9\u0005\u0013\u0000\u0000\u0975\u09f9\u0005\u0014\u0000"+
		"\u0000\u0976\u09f9\u0005\u0015\u0000\u0000\u0977\u09f9\u0005\u0017\u0000"+
		"\u0000\u0978\u09f9\u0005\u0018\u0000\u0000\u0979\u09f9\u0005\u0019\u0000"+
		"\u0000\u097a\u09f9\u0005\u001a\u0000\u0000\u097b\u09f9\u0005\u001b\u0000"+
		"\u0000\u097c\u09f9\u0005\u001c\u0000\u0000\u097d\u09f9\u0005\u001d\u0000"+
		"\u0000\u097e\u09f9\u0005\u001e\u0000\u0000\u097f\u09f9\u0005\u001f\u0000"+
		"\u0000\u0980\u09f9\u0005 \u0000\u0000\u0981\u09f9\u0005!\u0000\u0000\u0982"+
		"\u09f9\u0005\"\u0000\u0000\u0983\u09f9\u0005#\u0000\u0000\u0984\u09f9"+
		"\u0005$\u0000\u0000\u0985\u09f9\u0005%\u0000\u0000\u0986\u09f9\u0005&"+
		"\u0000\u0000\u0987\u09f9\u0005\'\u0000\u0000\u0988\u09f9\u0005(\u0000"+
		"\u0000\u0989\u09f9\u0005)\u0000\u0000\u098a\u09f9\u0005*\u0000\u0000\u098b"+
		"\u09f9\u0005+\u0000\u0000\u098c\u09f9\u0005,\u0000\u0000\u098d\u09f9\u0005"+
		"-\u0000\u0000\u098e\u09f9\u0005.\u0000\u0000\u098f\u09f9\u0005/\u0000"+
		"\u0000\u0990\u09f9\u00050\u0000\u0000\u0991\u09f9\u00051\u0000\u0000\u0992"+
		"\u09f9\u00055\u0000\u0000\u0993\u09f9\u00056\u0000\u0000\u0994\u09f9\u0005"+
		"7\u0000\u0000\u0995\u09f9\u00058\u0000\u0000\u0996\u09f9\u00059\u0000"+
		"\u0000\u0997\u09f9\u0005:\u0000\u0000\u0998\u09f9\u0005;\u0000\u0000\u0999"+
		"\u09f9\u0005<\u0000\u0000\u099a\u09f9\u0005=\u0000\u0000\u099b\u09f9\u0005"+
		">\u0000\u0000\u099c\u09f9\u0005?\u0000\u0000\u099d\u09f9\u0005B\u0000"+
		"\u0000\u099e\u09f9\u0005@\u0000\u0000\u099f\u09f9\u0005C\u0000\u0000\u09a0"+
		"\u09f9\u0005D\u0000\u0000\u09a1\u09f9\u0005E\u0000\u0000\u09a2\u09f9\u0005"+
		"F\u0000\u0000\u09a3\u09f9\u0005A\u0000\u0000\u09a4\u09f9\u0005G\u0000"+
		"\u0000\u09a5\u09f9\u0005H\u0000\u0000\u09a6\u09f9\u0005J\u0000\u0000\u09a7"+
		"\u09f9\u0005K\u0000\u0000\u09a8\u09f9\u0005L\u0000\u0000\u09a9\u09f9\u0005"+
		"O\u0000\u0000\u09aa\u09f9\u0005M\u0000\u0000\u09ab\u09f9\u0005P\u0000"+
		"\u0000\u09ac\u09f9\u0005Q\u0000\u0000\u09ad\u09f9\u0005R\u0000\u0000\u09ae"+
		"\u09f9\u0005T\u0000\u0000\u09af\u09f9\u0005U\u0000\u0000\u09b0\u09f9\u0005"+
		"W\u0000\u0000\u09b1\u09b2\u0005X\u0000\u0000\u09b2\u09f9\u0005Y\u0000"+
		"\u0000\u09b3\u09f9\u0005Z\u0000\u0000\u09b4\u09f9\u0005[\u0000\u0000\u09b5"+
		"\u09f9\u0005\\\u0000\u0000\u09b6\u09f9\u0005]\u0000\u0000\u09b7\u09f9"+
		"\u0005^\u0000\u0000\u09b8\u09f9\u0005`\u0000\u0000\u09b9\u09f9\u0005_"+
		"\u0000\u0000\u09ba\u09f9\u0005a\u0000\u0000\u09bb\u09f9\u0005c\u0000\u0000"+
		"\u09bc\u09f9\u0005d\u0000\u0000\u09bd\u09f9\u0005f\u0000\u0000\u09be\u09f9"+
		"\u0005i\u0000\u0000\u09bf\u09f9\u0005g\u0000\u0000\u09c0\u09f9\u0005h"+
		"\u0000\u0000\u09c1\u09f9\u0005l\u0000\u0000\u09c2\u09f9\u0005m\u0000\u0000"+
		"\u09c3\u09f9\u0005\u00c8\u0000\u0000\u09c4\u09f9\u0005n\u0000\u0000\u09c5"+
		"\u09f9\u0005o\u0000\u0000\u09c6\u09f9\u0005p\u0000\u0000\u09c7\u09f9";
	private static final String _serializedATNSegment1 =
		"\u0005q\u0000\u0000\u09c8\u09f9\u0005u\u0000\u0000\u09c9\u09f9\u0005s"+
		"\u0000\u0000\u09ca\u09f9\u0005t\u0000\u0000\u09cb\u09f9\u0005r\u0000\u0000"+
		"\u09cc\u09f9\u0005v\u0000\u0000\u09cd\u09f9\u0005w\u0000\u0000\u09ce\u09f9"+
		"\u0005x\u0000\u0000\u09cf\u09f9\u0005y\u0000\u0000\u09d0\u09f9\u0005z"+
		"\u0000\u0000\u09d1\u09f9\u0005{\u0000\u0000\u09d2\u09f9\u0005|\u0000\u0000"+
		"\u09d3\u09f9\u0005}\u0000\u0000\u09d4\u09f9\u0005~\u0000\u0000\u09d5\u09f9"+
		"\u0005\u007f\u0000\u0000\u09d6\u09f9\u0005\u0080\u0000\u0000\u09d7\u09f9"+
		"\u0005\u0081\u0000\u0000\u09d8\u09f9\u0005\u0082\u0000\u0000\u09d9\u09f9"+
		"\u0005\u0083\u0000\u0000\u09da\u09f9\u0005\u0084\u0000\u0000\u09db\u09f9"+
		"\u0005\u0085\u0000\u0000\u09dc\u09f9\u0005\u008f\u0000\u0000\u09dd\u09f9"+
		"\u0005\u0090\u0000\u0000\u09de\u09f9\u0005\u0086\u0000\u0000\u09df\u09f9"+
		"\u0005\u0087\u0000\u0000\u09e0\u09f9\u0005\u0088\u0000\u0000\u09e1\u09f9"+
		"\u0005\u0089\u0000\u0000\u09e2\u09f9\u0005\u008a\u0000\u0000\u09e3\u09f9"+
		"\u0005\u008b\u0000\u0000\u09e4\u09f9\u0005\u008c\u0000\u0000\u09e5\u09f9"+
		"\u0005\u008d\u0000\u0000\u09e6\u09f9\u0005\u008e\u0000\u0000\u09e7\u09f9"+
		"\u0005\u0098\u0000\u0000\u09e8\u09f9\u0005\u0099\u0000\u0000\u09e9\u09f9"+
		"\u0005\u009a\u0000\u0000\u09ea\u09f9\u0005\u009b\u0000\u0000\u09eb\u09f9"+
		"\u0005\u009c\u0000\u0000\u09ec\u09f9\u0005\u009d\u0000\u0000\u09ed\u09f9"+
		"\u0005\u009e\u0000\u0000\u09ee\u09f9\u0005\u00a0\u0000\u0000\u09ef\u09f9"+
		"\u0005\u009f\u0000\u0000\u09f0\u09f9\u0005\u00a1\u0000\u0000\u09f1\u09f9"+
		"\u0005\u00a2\u0000\u0000\u09f2\u09f9\u0005\u00a3\u0000\u0000\u09f3\u09f9"+
		"\u0005\u00a4\u0000\u0000\u09f4\u09f9\u0005\u00a5\u0000\u0000\u09f5\u09f9"+
		"\u0005\u00a6\u0000\u0000\u09f6\u09f9\u0005\u00ab\u0000\u0000\u09f7\u09f9"+
		"\u0005\u00d3\u0000\u0000\u09f8\u0963\u0001\u0000\u0000\u0000\u09f8\u0964"+
		"\u0001\u0000\u0000\u0000\u09f8\u0965\u0001\u0000\u0000\u0000\u09f8\u0966"+
		"\u0001\u0000\u0000\u0000\u09f8\u0967\u0001\u0000\u0000\u0000\u09f8\u0968"+
		"\u0001\u0000\u0000\u0000\u09f8\u0969\u0001\u0000\u0000\u0000\u09f8\u096a"+
		"\u0001\u0000\u0000\u0000\u09f8\u096b\u0001\u0000\u0000\u0000\u09f8\u096c"+
		"\u0001\u0000\u0000\u0000\u09f8\u096d\u0001\u0000\u0000\u0000\u09f8\u096e"+
		"\u0001\u0000\u0000\u0000\u09f8\u096f\u0001\u0000\u0000\u0000\u09f8\u0970"+
		"\u0001\u0000\u0000\u0000\u09f8\u0971\u0001\u0000\u0000\u0000\u09f8\u0972"+
		"\u0001\u0000\u0000\u0000\u09f8\u0973\u0001\u0000\u0000\u0000\u09f8\u0974"+
		"\u0001\u0000\u0000\u0000\u09f8\u0975\u0001\u0000\u0000\u0000\u09f8\u0976"+
		"\u0001\u0000\u0000\u0000\u09f8\u0977\u0001\u0000\u0000\u0000\u09f8\u0978"+
		"\u0001\u0000\u0000\u0000\u09f8\u0979\u0001\u0000\u0000\u0000\u09f8\u097a"+
		"\u0001\u0000\u0000\u0000\u09f8\u097b\u0001\u0000\u0000\u0000\u09f8\u097c"+
		"\u0001\u0000\u0000\u0000\u09f8\u097d\u0001\u0000\u0000\u0000\u09f8\u097e"+
		"\u0001\u0000\u0000\u0000\u09f8\u097f\u0001\u0000\u0000\u0000\u09f8\u0980"+
		"\u0001\u0000\u0000\u0000\u09f8\u0981\u0001\u0000\u0000\u0000\u09f8\u0982"+
		"\u0001\u0000\u0000\u0000\u09f8\u0983\u0001\u0000\u0000\u0000\u09f8\u0984"+
		"\u0001\u0000\u0000\u0000\u09f8\u0985\u0001\u0000\u0000\u0000\u09f8\u0986"+
		"\u0001\u0000\u0000\u0000\u09f8\u0987\u0001\u0000\u0000\u0000\u09f8\u0988"+
		"\u0001\u0000\u0000\u0000\u09f8\u0989\u0001\u0000\u0000\u0000\u09f8\u098a"+
		"\u0001\u0000\u0000\u0000\u09f8\u098b\u0001\u0000\u0000\u0000\u09f8\u098c"+
		"\u0001\u0000\u0000\u0000\u09f8\u098d\u0001\u0000\u0000\u0000\u09f8\u098e"+
		"\u0001\u0000\u0000\u0000\u09f8\u098f\u0001\u0000\u0000\u0000\u09f8\u0990"+
		"\u0001\u0000\u0000\u0000\u09f8\u0991\u0001\u0000\u0000\u0000\u09f8\u0992"+
		"\u0001\u0000\u0000\u0000\u09f8\u0993\u0001\u0000\u0000\u0000\u09f8\u0994"+
		"\u0001\u0000\u0000\u0000\u09f8\u0995\u0001\u0000\u0000\u0000\u09f8\u0996"+
		"\u0001\u0000\u0000\u0000\u09f8\u0997\u0001\u0000\u0000\u0000\u09f8\u0998"+
		"\u0001\u0000\u0000\u0000\u09f8\u0999\u0001\u0000\u0000\u0000\u09f8\u099a"+
		"\u0001\u0000\u0000\u0000\u09f8\u099b\u0001\u0000\u0000\u0000\u09f8\u099c"+
		"\u0001\u0000\u0000\u0000\u09f8\u099d\u0001\u0000\u0000\u0000\u09f8\u099e"+
		"\u0001\u0000\u0000\u0000\u09f8\u099f\u0001\u0000\u0000\u0000\u09f8\u09a0"+
		"\u0001\u0000\u0000\u0000\u09f8\u09a1\u0001\u0000\u0000\u0000\u09f8\u09a2"+
		"\u0001\u0000\u0000\u0000\u09f8\u09a3\u0001\u0000\u0000\u0000\u09f8\u09a4"+
		"\u0001\u0000\u0000\u0000\u09f8\u09a5\u0001\u0000\u0000\u0000\u09f8\u09a6"+
		"\u0001\u0000\u0000\u0000\u09f8\u09a7\u0001\u0000\u0000\u0000\u09f8\u09a8"+
		"\u0001\u0000\u0000\u0000\u09f8\u09a9\u0001\u0000\u0000\u0000\u09f8\u09aa"+
		"\u0001\u0000\u0000\u0000\u09f8\u09ab\u0001\u0000\u0000\u0000\u09f8\u09ac"+
		"\u0001\u0000\u0000\u0000\u09f8\u09ad\u0001\u0000\u0000\u0000\u09f8\u09ae"+
		"\u0001\u0000\u0000\u0000\u09f8\u09af\u0001\u0000\u0000\u0000\u09f8\u09b0"+
		"\u0001\u0000\u0000\u0000\u09f8\u09b1\u0001\u0000\u0000\u0000\u09f8\u09b3"+
		"\u0001\u0000\u0000\u0000\u09f8\u09b4\u0001\u0000\u0000\u0000\u09f8\u09b5"+
		"\u0001\u0000\u0000\u0000\u09f8\u09b6\u0001\u0000\u0000\u0000\u09f8\u09b7"+
		"\u0001\u0000\u0000\u0000\u09f8\u09b8\u0001\u0000\u0000\u0000\u09f8\u09b9"+
		"\u0001\u0000\u0000\u0000\u09f8\u09ba\u0001\u0000\u0000\u0000\u09f8\u09bb"+
		"\u0001\u0000\u0000\u0000\u09f8\u09bc\u0001\u0000\u0000\u0000\u09f8\u09bd"+
		"\u0001\u0000\u0000\u0000\u09f8\u09be\u0001\u0000\u0000\u0000\u09f8\u09bf"+
		"\u0001\u0000\u0000\u0000\u09f8\u09c0\u0001\u0000\u0000\u0000\u09f8\u09c1"+
		"\u0001\u0000\u0000\u0000\u09f8\u09c2\u0001\u0000\u0000\u0000\u09f8\u09c3"+
		"\u0001\u0000\u0000\u0000\u09f8\u09c4\u0001\u0000\u0000\u0000\u09f8\u09c5"+
		"\u0001\u0000\u0000\u0000\u09f8\u09c6\u0001\u0000\u0000\u0000\u09f8\u09c7"+
		"\u0001\u0000\u0000\u0000\u09f8\u09c8\u0001\u0000\u0000\u0000\u09f8\u09c9"+
		"\u0001\u0000\u0000\u0000\u09f8\u09ca\u0001\u0000\u0000\u0000\u09f8\u09cb"+
		"\u0001\u0000\u0000\u0000\u09f8\u09cc\u0001\u0000\u0000\u0000\u09f8\u09cd"+
		"\u0001\u0000\u0000\u0000\u09f8\u09ce\u0001\u0000\u0000\u0000\u09f8\u09cf"+
		"\u0001\u0000\u0000\u0000\u09f8\u09d0\u0001\u0000\u0000\u0000\u09f8\u09d1"+
		"\u0001\u0000\u0000\u0000\u09f8\u09d2\u0001\u0000\u0000\u0000\u09f8\u09d3"+
		"\u0001\u0000\u0000\u0000\u09f8\u09d4\u0001\u0000\u0000\u0000\u09f8\u09d5"+
		"\u0001\u0000\u0000\u0000\u09f8\u09d6\u0001\u0000\u0000\u0000\u09f8\u09d7"+
		"\u0001\u0000\u0000\u0000\u09f8\u09d8\u0001\u0000\u0000\u0000\u09f8\u09d9"+
		"\u0001\u0000\u0000\u0000\u09f8\u09da\u0001\u0000\u0000\u0000\u09f8\u09db"+
		"\u0001\u0000\u0000\u0000\u09f8\u09dc\u0001\u0000\u0000\u0000\u09f8\u09dd"+
		"\u0001\u0000\u0000\u0000\u09f8\u09de\u0001\u0000\u0000\u0000\u09f8\u09df"+
		"\u0001\u0000\u0000\u0000\u09f8\u09e0\u0001\u0000\u0000\u0000\u09f8\u09e1"+
		"\u0001\u0000\u0000\u0000\u09f8\u09e2\u0001\u0000\u0000\u0000\u09f8\u09e3"+
		"\u0001\u0000\u0000\u0000\u09f8\u09e4\u0001\u0000\u0000\u0000\u09f8\u09e5"+
		"\u0001\u0000\u0000\u0000\u09f8\u09e6\u0001\u0000\u0000\u0000\u09f8\u09e7"+
		"\u0001\u0000\u0000\u0000\u09f8\u09e8\u0001\u0000\u0000\u0000\u09f8\u09e9"+
		"\u0001\u0000\u0000\u0000\u09f8\u09ea\u0001\u0000\u0000\u0000\u09f8\u09eb"+
		"\u0001\u0000\u0000\u0000\u09f8\u09ec\u0001\u0000\u0000\u0000\u09f8\u09ed"+
		"\u0001\u0000\u0000\u0000\u09f8\u09ee\u0001\u0000\u0000\u0000\u09f8\u09ef"+
		"\u0001\u0000\u0000\u0000\u09f8\u09f0\u0001\u0000\u0000\u0000\u09f8\u09f1"+
		"\u0001\u0000\u0000\u0000\u09f8\u09f2\u0001\u0000\u0000\u0000\u09f8\u09f3"+
		"\u0001\u0000\u0000\u0000\u09f8\u09f4\u0001\u0000\u0000\u0000\u09f8\u09f5"+
		"\u0001\u0000\u0000\u0000\u09f8\u09f6\u0001\u0000\u0000\u0000\u09f8\u09f7"+
		"\u0001\u0000\u0000\u0000\u09f9\u09fd\u0001\u0000\u0000\u0000\u09fa\u09fb"+
		"\u0005\u00d4\u0000\u0000\u09fb\u09fd\u0006\u00e1\uffff\uffff\u0000\u09fc"+
		"\u09f8\u0001\u0000\u0000\u0000\u09fc\u09fa\u0001\u0000\u0000\u0000\u09fd"+
		"\u01c3\u0001\u0000\u0000\u0000\u0109\u01e0\u01e3\u01ef\u01fd\u0200\u0203"+
		"\u0206\u0209\u0211\u0217\u021c\u0220\u0226\u0231\u0238\u0241\u0249\u0251"+
		"\u025a\u025e\u0261\u0265\u026b\u0272\u0278\u0284\u0287\u0292\u0295\u029b"+
		"\u02a6\u02bb\u02be\u02c2\u02ce\u02d2\u02d6\u02df\u02f0\u02fb\u02ff\u0306"+
		"\u0309\u0310\u031b\u031f\u0329\u032e\u0338\u0342\u034d\u035a\u0365\u036a"+
		"\u0375\u0379\u037d\u0382\u0387\u0391\u0399\u03a1\u03a7\u03ac\u03ae\u03b4"+
		"\u03bb\u03c0\u03c6\u03ca\u03ce\u03d4\u03e6\u03ec\u03ee\u03f5\u03fb\u0401"+
		"\u0411\u0418\u0426\u0432\u0435\u0450\u0455\u046c\u0472\u0475\u047d\u0482"+
		"\u048b\u0492\u0495\u0499\u04a0\u04a8\u04ab\u04b0\u04b3\u04ba\u04c0\u04ca"+
		"\u04ce\u04d6\u04da\u04e2\u04e6\u04ee\u04f2\u04fb\u04ff\u0509\u050c\u0513"+
		"\u0517\u051a\u051d\u0522\u0526\u0535\u053d\u0544\u054a\u054d\u0551\u0554"+
		"\u055b\u056c\u0575\u057d\u0580\u0584\u0588\u058a\u0592\u05b3\u05bb\u05c1"+
		"\u05d0\u05d8\u05dc\u05e5\u05ea\u05f1\u05f9\u05fd\u0613\u0617\u061d\u0622"+
		"\u062b\u0631\u0635\u063f\u0642\u064a\u0658\u065d\u0661\u0669\u066b\u066e"+
		"\u067b\u0682\u0687\u0692\u0694\u06a4\u06ae\u06be\u06c0\u06c8\u06cc\u06e3"+
		"\u06ed\u06fc\u0701\u0712\u0718\u071c\u0729\u072c\u0735\u0738\u073e\u0745"+
		"\u074d\u0755\u075b\u0761\u076a\u0774\u0777\u077e\u0784\u0787\u0790\u0795"+
		"\u0798\u079d\u07a0\u07a3\u07ab\u07ae\u07b3\u07b5\u07b8\u07bf\u07cb\u07d8"+
		"\u07da\u07e7\u07f6\u07f9\u0801\u0808\u080b\u080e\u0818\u081f\u0824\u082a"+
		"\u0833\u0839\u083f\u0844\u0850\u0857\u085e\u0864\u0874\u087a\u087d\u0887"+
		"\u088a\u088d\u0890\u0893\u0899\u08a3\u08a9\u08ad\u08b5\u08b8\u08bd\u08c8"+
		"\u08da\u08ec\u08f5\u08fc\u0901\u0905\u090a\u090e\u091a\u0922\u0929\u0931"+
		"\u0938\u0945\u0950\u0955\u0960\u09f8\u09fc";
	public static final String _serializedATN = Utils.join(
		new String[] {
			_serializedATNSegment0,
			_serializedATNSegment1
		},
		""
	);
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}