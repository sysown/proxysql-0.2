#include "PgSQL_Variables.h"
#include "proxysql.h"

#include "PgSQL_Session.h"
#include "PgSQL_Data_Stream.h"
#ifndef SPOOKYV2
#include "SpookyV2.h"
#define SPOOKYV2
#endif

#include <sstream>


static inline char is_digit(char c) {
	if(c >= '0' && c <= '9')
		return 1;
	return 0;
}

#include "proxysql_find_charset.h"

pgsql_verify_var PgSQL_Variables::verifiers[PGSQL_NAME_LAST_HIGH_WM];
pgsql_update_var PgSQL_Variables::updaters[PGSQL_NAME_LAST_HIGH_WM];


PgSQL_Variables::PgSQL_Variables() {
	// add here all the variables we want proxysql to recognize, but ignore
	ignore_vars.push_back("application_name");
	//ignore_vars.push_back("interactive_timeout");
	//ignore_vars.push_back("wait_timeout");
	//ignore_vars.push_back("net_read_timeout");
	//ignore_vars.push_back("net_write_timeout");
	//ignore_vars.push_back("net_buffer_length");
	//ignore_vars.push_back("read_buffer_size");
	//ignore_vars.push_back("read_rnd_buffer_size");
	// NOTE: This variable has been temporarily ignored. Check issues #3442 and #3441.
	//ignore_vars.push_back("session_track_schema");
	variables_regexp = "";
	for (auto i = 0; i < PGSQL_NAME_LAST_HIGH_WM; i++) {
		// we initialized all the internal_variable_name if set to NULL
		if (pgsql_tracked_variables[i].internal_variable_name == NULL) {
			pgsql_tracked_variables[i].internal_variable_name = pgsql_tracked_variables[i].set_variable_name;
		}
	}
/*
   NOTE:
	make special ATTENTION that the order in pgsql_variable_name
	and pgsqll_tracked_variables[] is THE SAME
   NOTE:
	PgSQL_Variables::PgSQL_Variables() has a built-in check to make sure that the order is correct,
	and that variables are in alphabetical order
*/
	for (int i = PGSQL_NAME_LAST_LOW_WM; i < PGSQL_NAME_LAST_HIGH_WM; i++) {
		assert(i == pgsql_tracked_variables[i].idx);
		if (i > PGSQL_NAME_LAST_LOW_WM+1) {
			assert(strcmp(pgsql_tracked_variables[i].set_variable_name, pgsql_tracked_variables[i-1].set_variable_name) > 0);
		}
	}
	for (auto i = 0; i < PGSQL_NAME_LAST_HIGH_WM; i++) {
		if (i == PGSQL_CLIENT_ENCODING) {
			PgSQL_Variables::updaters[i] = NULL;
			PgSQL_Variables::verifiers[i] = NULL;
		} else {
			PgSQL_Variables::verifiers[i] = verify_server_variable;
			PgSQL_Variables::updaters[i] = update_server_variable;
		}
		if (pgsql_tracked_variables[i].status == SETTING_VARIABLE) {
			variables_regexp += pgsql_tracked_variables[i].set_variable_name;
			variables_regexp += "|";

			int idx = 0;
			while (pgsql_tracked_variables[i].alias[idx]) {
				variables_regexp += pgsql_tracked_variables[i].alias[idx];
				variables_regexp += "|";
				idx++;
			}
		}
	}
	for (std::vector<std::string>::iterator it=ignore_vars.begin(); it != ignore_vars.end(); it++) {
		variables_regexp += *it;
		variables_regexp += "|";
	}

	// Check if the last character is '|'
	if (!variables_regexp.empty() && variables_regexp.back() == '|') {
		variables_regexp.pop_back(); // Remove the last character
	}
}

PgSQL_Variables::~PgSQL_Variables() {}

bool PgSQL_Variables::client_set_hash_and_value(PgSQL_Session* session, int idx, const std::string& value, uint32_t hash) {
	if (!session || !session->client_myds || !session->client_myds->myconn) {
		proxy_warning("Session validation failed\n");
		return false;
	}

	session->client_myds->myconn->var_hash[idx] = hash;
	if (session->client_myds->myconn->variables[idx].value) {
		free(session->client_myds->myconn->variables[idx].value);
	}
	session->client_myds->myconn->variables[idx].value = strdup(value.c_str());

	return true;
}

void PgSQL_Variables::client_reset_value(PgSQL_Session* session, int idx) {
	if (!session || !session->client_myds || !session->client_myds->myconn) {
		proxy_warning("Session validation failed\n");
		return;
	}

	PgSQL_Connection *client_conn = session->client_myds->myconn;

	if (client_conn->var_hash[idx] != 0) {
		client_conn->var_hash[idx] = 0;
		if (client_conn->variables[idx].value) {
			free(client_conn->variables[idx].value);
			client_conn->variables[idx].value = NULL;
		}
		// we now regererate dynamic_variables_idx
		client_conn->reorder_dynamic_variables_idx();
	}
}
void PgSQL_Variables::server_set_hash_and_value(PgSQL_Session* session, int idx, const char* value, uint32_t hash) {
	if (!session || !session->mybe || !session->mybe->server_myds || !session->mybe->server_myds->myconn || !value) {
		proxy_warning("Session validation failed\n");
		return;
	}

	session->mybe->server_myds->myconn->var_hash[idx] = hash;
	if (session->mybe->server_myds->myconn->variables[idx].value) {
		free(session->mybe->server_myds->myconn->variables[idx].value);
	}
	session->mybe->server_myds->myconn->variables[idx].value = strdup(value);
}

bool PgSQL_Variables::client_set_value(PgSQL_Session* session, int idx, const std::string& value) {
	if (!session || !session->client_myds || !session->client_myds->myconn) {
		proxy_warning("Session validation failed\n");
		return false;
	}

	session->client_myds->myconn->var_hash[idx] = SpookyHash::Hash32(value.c_str(),strlen(value.c_str()),10);
	if (session->client_myds->myconn->variables[idx].value) {
		free(session->client_myds->myconn->variables[idx].value);
	}
	session->client_myds->myconn->variables[idx].value = strdup(value.c_str());
	// we now regererate dynamic_variables_idx
	session->client_myds->myconn->reorder_dynamic_variables_idx();
	return true;
}

const char* PgSQL_Variables::client_get_value(PgSQL_Session* session, int idx) const {
	assert(session);
	assert(session->client_myds);
	assert(session->client_myds->myconn);
	return session->client_myds->myconn->variables[idx].value;
}

uint32_t PgSQL_Variables::client_get_hash(PgSQL_Session* session, int idx) const {
	assert(session);
	assert(session->client_myds);
	assert(session->client_myds->myconn);
	return session->client_myds->myconn->var_hash[idx];
}

void PgSQL_Variables::server_set_value(PgSQL_Session* session, int idx, const char* value) {
	assert(session);
	assert(session->mybe);
	assert(session->mybe->server_myds);
	assert(session->mybe->server_myds->myconn);
	if (!value) return; // FIXME: I am not sure about this implementation . If value == NULL , show the variable be reset?
	session->mybe->server_myds->myconn->var_hash[idx] = SpookyHash::Hash32(value,strlen(value),10);

	if (session->mybe->server_myds->myconn->variables[idx].value) {
		free(session->mybe->server_myds->myconn->variables[idx].value);
	}
	session->mybe->server_myds->myconn->variables[idx].value = strdup(value);
	// we now regererate dynamic_variables_idx
	session->mybe->server_myds->myconn->reorder_dynamic_variables_idx();
}

void PgSQL_Variables::server_reset_value(PgSQL_Session* session, int idx) {
	assert(session);
	assert(session->mybe);
	assert(session->mybe->server_myds);
	assert(session->mybe->server_myds->myconn);

	PgSQL_Connection *backend_conn = session->mybe->server_myds->myconn;
	
	if (backend_conn->var_hash[idx] != 0) {
		backend_conn->var_hash[idx] = 0;
		if (backend_conn->variables[idx].value) {
			free(backend_conn->variables[idx].value);
			backend_conn->variables[idx].value = NULL;
		}
		// we now regererate dynamic_variables_idx
		backend_conn->reorder_dynamic_variables_idx();
	}
}

const char* PgSQL_Variables::server_get_value(PgSQL_Session* session, int idx) const {
	assert(session);
	assert(session->mybe);
	assert(session->mybe->server_myds);
	assert(session->mybe->server_myds->myconn);
	return session->mybe->server_myds->myconn->variables[idx].value;
}

uint32_t PgSQL_Variables::server_get_hash(PgSQL_Session* session, int idx) const {
	assert(session);
	assert(session->mybe);
	assert(session->mybe->server_myds);
	assert(session->mybe->server_myds->myconn);
	return session->mybe->server_myds->myconn->var_hash[idx];
}

bool PgSQL_Variables::update_variable(PgSQL_Session* session, session_status status, int &_rc) {
	int idx = PGSQL_NAME_LAST_HIGH_WM;
	if (session->status == SETTING_VARIABLE) {
		// if status is SETTING_VARIABLE , what variable needs to be changed is defined in changing_variable_idx
		idx = session->changing_variable_idx;
	} else {
		for (int i=0; i<PGSQL_NAME_LAST_HIGH_WM; i++) {
			if (pgsql_tracked_variables[i].status == status) {
				idx = i;
				break;
			}
		}
	}
	assert(idx != PGSQL_NAME_LAST_HIGH_WM);
	return updaters[idx](session, idx, _rc);
}

bool PgSQL_Variables::verify_variable(PgSQL_Session* session, int idx) const {
	auto ret = false;
	if (likely(verifiers[idx])) {
		auto client_hash = session->client_myds->myconn->var_hash[idx];
		auto server_hash = session->mybe->server_myds->myconn->var_hash[idx];
		if (client_hash && client_hash != server_hash) {
			ret = verifiers[idx](session, idx, client_hash, server_hash);
		}
	}
	return ret;
}

bool validate_charset(PgSQL_Session* session, int idx, int &_rc) {
	/*if (idx == PGSQL_CLIENT_ENCODING || idx == PGSQL_SET_NAMES) {
		PgSQL_Data_Stream *myds = session->mybe->server_myds;
		PgSQL_Connection *myconn = myds->myconn;
		char msg[128];
		const MARIADB_CHARSET_INFO *ci = NULL;
		const char* replace_collation = NULL;
		const char* not_supported_collation = NULL;
		unsigned int replace_collation_nr = 0;
		std::stringstream ss;
		int charset = atoi(pgsql_variables.client_get_value(session, idx));
		if (charset >= 255 && myconn->pgsql->server_version[0] != '8') {
			switch(pgsql_thread___handle_unknown_charset) {
				case HANDLE_UNKNOWN_CHARSET__DISCONNECT_CLIENT:
					snprintf(msg,sizeof(msg),"Can't initialize character set %s", pgsql_variables.client_get_value(session, idx));
					proxy_error("Can't initialize character set on %s, %d: Error %d (%s). Closing client connection %s:%d.\n",
							myconn->parent->address, myconn->parent->port, 2019, msg, session->client_myds->addr.addr, session->client_myds->addr.port);
					myds->destroy_MySQL_Connection_From_Pool(false);
					myds->fd=0;
					_rc=-1;
					return false;
				case HANDLE_UNKNOWN_CHARSET__REPLACE_WITH_DEFAULT_VERBOSE:
					ci = proxysql_find_charset_nr(charset);
					if (!ci) {
						// LCOV_EXCL_START
						proxy_error("Cannot find character set [%s]\n", pgsql_variables.client_get_value(session, idx));
						assert(0);
						// LCOV_EXCL_STOP
					}
					not_supported_collation = ci->name;

					if (idx == SQL_COLLATION_CONNECTION) {
						ci = proxysql_find_charset_collate(pgsql_thread___default_variables[idx]);
					} else {
						if (pgsql_thread___default_variables[idx]) {
							ci = proxysql_find_charset_name(pgsql_thread___default_variables[idx]);
						} else {
							ci = proxysql_find_charset_name(pgsql_thread___default_variables[SQL_CHARACTER_SET]);
						}
					}

					if (!ci) {
						// LCOV_EXCL_START
						proxy_error("Cannot find character set [%s]\n", pgsql_thread___default_variables[idx]);
						assert(0);
						// LCOV_EXCL_STOP
					}
					replace_collation = ci->name;
					replace_collation_nr = ci->nr;

					proxy_warning("Server doesn't support collation (%s) %s. Replacing it with the configured default (%d) %s. Client %s:%d\n",
							pgsql_variables.client_get_value(session, idx), not_supported_collation, 
							replace_collation_nr, replace_collation, session->client_myds->addr.addr, session->client_myds->addr.port);

					ss << replace_collation_nr;
					pgsql_variables.client_set_value(session, idx, ss.str());
					_rc=0;
					return true;
				case HANDLE_UNKNOWN_CHARSET__REPLACE_WITH_DEFAULT:
					if (idx == SQL_COLLATION_CONNECTION) {
						ci = proxysql_find_charset_collate(pgsql_thread___default_variables[idx]);
					} else {
						if (pgsql_thread___default_variables[idx]) {
							ci = proxysql_find_charset_name(pgsql_thread___default_variables[idx]);
						} else {
							ci = proxysql_find_charset_name(pgsql_thread___default_variables[SQL_CHARACTER_SET]);
						}
					}

					if (!ci) {
						// LCOV_EXCL_START
						proxy_error("Cannot filnd charset [%s]\n", pgsql_thread___default_variables[idx]);
						assert(0);
						// LCOV_EXCL_STOP
					}
					replace_collation_nr = ci->nr;

					ss << replace_collation_nr;
					pgsql_variables.client_set_value(session, idx, ss.str());
					_rc=0;
					return true;
				default:
					proxy_error("Wrong configuration of the handle_unknown_charset\n");
					_rc=-1;
					return false;
			}
		}
	}*/
	_rc=0;
	return true;
}

bool update_server_variable(PgSQL_Session* session, int idx, int &_rc) {
	bool no_quote = true;
	if (IS_PGTRACKED_VAR_OPTION_SET_QUOTE(pgsql_tracked_variables[idx])) no_quote = false;
	bool st = IS_PGTRACKED_VAR_OPTION_SET_SET_TRANSACTION(pgsql_tracked_variables[idx]);
	const char *set_var_name = pgsql_tracked_variables[idx].set_variable_name;
	bool ret = false;

	if (!validate_charset(session, idx, _rc)) {
		return false;
	}

	const char* value = pgsql_variables.client_get_value(session, idx);
	pgsql_variables.server_set_value(session, idx, value);
	ret = session->handler_again___status_SETTING_GENERIC_VARIABLE(&_rc, set_var_name, value, no_quote, st);
	return ret;
}

bool verify_set_names(PgSQL_Session* session) {
	uint32_t client_charset_hash = pgsql_variables.client_get_hash(session, PGSQL_CLIENT_ENCODING);
	if (client_charset_hash == 0)
		return false;

	if (client_charset_hash != pgsql_variables.server_get_hash(session, PGSQL_CLIENT_ENCODING)) {
		switch(session->status) { // this switch can be replaced with a simple previous_status.push(status), but it is here for readibility
			case PROCESSING_QUERY:
				session->previous_status.push(PROCESSING_QUERY);
				break;
			/*
			case PROCESSING_STMT_PREPARE:
				session->previous_status.push(PROCESSING_STMT_PREPARE);
				break;
			case PROCESSING_STMT_EXECUTE:
				session->previous_status.push(PROCESSING_STMT_EXECUTE);
				break;
			*/
			default:
				// LCOV_EXCL_START
				proxy_error("Wrong status %d\n", session->status);
				assert(0);
				break;
				// LCOV_EXCL_STOP
		}
		session->set_status(SETTING_CHARSET);
		uint32_t hash = pgsql_variables.client_get_hash(session, PGSQL_CLIENT_ENCODING);
		const char* value = pgsql_variables.client_get_value(session, PGSQL_CLIENT_ENCODING);
		pgsql_variables.server_set_hash_and_value(session, PGSQL_CLIENT_ENCODING, value, hash);
		return true;
	}
	return false;
}

inline bool verify_server_variable(PgSQL_Session* session, int idx, uint32_t client_hash, uint32_t server_hash) {
	if (client_hash && client_hash != server_hash) {
		// Edge case for set charset command, because we do not know database character set
		// for now we are setting connection and collation to empty
		//if (idx == SQL_CHARACTER_SET_CONNECTION || idx == SQL_COLLATION_CONNECTION ) {
		//	if (pgsql_variables.client_get_hash(session, idx) == 0) {
		//			pgsql_variables.server_set_hash_and_value(session, idx, "", 0);
		//			return false;
		//	}
		//}
		// this variable is relevant only if status == SETTING_VARIABLE
		session->changing_variable_idx = (enum pgsql_variable_name)idx;
		switch(session->status) { // this switch can be replaced with a simple previous_status.push(status), but it is here for readibility
			case PROCESSING_QUERY:
				session->previous_status.push(PROCESSING_QUERY);
				break;
			/*
			case PROCESSING_STMT_PREPARE:
				session->previous_status.push(PROCESSING_STMT_PREPARE);
				break;
			case PROCESSING_STMT_EXECUTE:
				session->previous_status.push(PROCESSING_STMT_EXECUTE);
				break;
			*/
			default:
				// LCOV_EXCL_START
				proxy_error("Wrong status %d\n", session->status);
				assert(0);
				break;
				// LCOV_EXCL_STOP
		}
		session->set_status(pgsql_tracked_variables[idx].status);
		pgsql_variables.server_set_value(session, idx, pgsql_variables.client_get_value(session, idx));
		return true;
	}
	return false;
}

bool PgSQL_Variables::parse_variable_boolean(PgSQL_Session *sess, int idx, string& value1, bool * lock_hostgroup) {
	proxy_debug(PROXY_DEBUG_MYSQL_COM, 5, "Processing SET %s value %s\n", pgsql_tracked_variables[idx].set_variable_name, value1.c_str());
	int __tmp_value = -1;
	if (
		(strcasecmp(value1.c_str(),(char *)"0")==0) ||
		(strcasecmp(value1.c_str(),(char *)"false")==0) ||
		(strcasecmp(value1.c_str(),(char *)"off")==0)
	) {
		__tmp_value = 0;
	} else {
		if (
			(strcasecmp(value1.c_str(),(char *)"1")==0) ||
			(strcasecmp(value1.c_str(),(char *)"true")==0) ||
			(strcasecmp(value1.c_str(),(char *)"on")==0)
		) {
			__tmp_value = 1;
		}
	}

	if (__tmp_value >= 0) {
		proxy_debug(PROXY_DEBUG_MYSQL_COM, 7, "Processing SET %s value %s\n", pgsql_tracked_variables[idx].set_variable_name, value1.c_str());
		uint32_t var_value_int=SpookyHash::Hash32(value1.c_str(),value1.length(),10);
		if (pgsql_variables.client_get_hash(sess, idx) != var_value_int) {
			if (__tmp_value == 0) {
				if (!pgsql_variables.client_set_value(sess, idx, "OFF"))
					return false;
			} else {
				if (!pgsql_variables.client_set_value(sess, idx, "ON"))
					return false;
			}
			proxy_debug(PROXY_DEBUG_MYSQL_COM, 5, "Changing connection %s to %s\n", pgsql_tracked_variables[idx].set_variable_name, value1.c_str());
		}
	} else {
		sess->unable_to_parse_set_statement(lock_hostgroup);
		return false;
	}
	return true;
}



bool PgSQL_Variables::parse_variable_number(PgSQL_Session *sess, int idx, string& value1, bool * lock_hostgroup) {
	int vl = strlen(value1.c_str());
	const char *v = value1.c_str();
	bool only_digit_chars = true;
	for (int i=0; i<vl && only_digit_chars==true; i++) {
		if (is_digit(v[i])==0) {
			only_digit_chars=false;
		}
	}
	if (!only_digit_chars) {
		if (
			(variable_name_exists(pgsql_tracked_variables[idx],"sql_select_limit") == true) // sql_select_limit allows value "default"
			||
			(variable_name_exists(pgsql_tracked_variables[idx],"max_join_size") == true) // max_join_size allows value "default"
		) {
			if (strcasecmp(v,"default")==0) {
				only_digit_chars = true;
			}
		}
	}
	if (only_digit_chars) {
		// see https://dev.pgsql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_max_join_size
		proxy_debug(PROXY_DEBUG_MYSQL_COM, 7, "Processing SET %s value %s\n", pgsql_tracked_variables[idx].set_variable_name, value1.c_str());
		uint32_t var_value_int=SpookyHash::Hash32(value1.c_str(),value1.length(),10);
		if (pgsql_variables.client_get_hash(sess, idx) != var_value_int) {
			if (!pgsql_variables.client_set_value(sess, idx, value1.c_str()))
				return false;
			proxy_debug(PROXY_DEBUG_MYSQL_COM, 5, "Changing connection %s to %s\n", pgsql_tracked_variables[idx].set_variable_name, value1.c_str());
			if (idx == SQL_MAX_JOIN_SIZE) {
				// see https://dev.pgsql.com/doc/refman/5.7/en/server-system-variables.html#sysvar_max_join_size
				if (
					(value1 == "18446744073709551615")
					||
					(strcasecmp(v,"default")==0)
				) {
					pgsql_variables.client_set_value(sess, SQL_SQL_BIG_SELECTS, "ON");
				} else {
					pgsql_variables.client_set_value(sess, SQL_SQL_BIG_SELECTS, "OFF");
				}
			}
		}
		//exit_after_SetParse = true;
	} else {
		sess->unable_to_parse_set_statement(lock_hostgroup);
		return false;
	}
	return true;
}

