#ifndef CLASS_PGSQL_PREPARED_STATEMENT_H
#define CLASS_PGSQL_PREPARED_STATEMENT_H

#include "Base_PreparedStatement.h"

#include "proxysql.h"
#include "cpp.h"

#define PROXYSQL_PS_PREFIX "proxysql_ps_"

class PgSQL_STMT_Global_info : public Base_STMT_Global_info {
	public:
	MYSQL_COM_QUERY_command MyComQueryCmd = MYSQL_COM_QUERY__UNINITIALIZED;
	PgSQL_STMT_Global_info(uint64_t id, char *u, char *s, char *q, unsigned int ql, char *fc, MYSQL_STMT *stmt, uint64_t _h);
	~PgSQL_STMT_Global_info();
};



class PgSQL_STMTs_local_v14 : public Base_STMTs_local_v14<PgSQL_STMTs_local_v14> {
	public:
	std::unordered_map<std::string, uint64_t> stmt_name_to_id = {};
	std::unordered_map<uint64_t, std::string> stmt_id_to_name = {};
//	std::map<uint64_t, MYSQL_STMT *> global_stmt_to_backend_stmt = std::map<uint64_t, MYSQL_STMT *>();


	PgSQL_Session *sess = NULL;
	PgSQL_STMTs_local_v14(bool _ic) {
	is_client_ = _ic;
	}
	void set_is_client(PgSQL_Session *_s) {
		sess=_s;
		is_client_ = true;
	}
	~PgSQL_STMTs_local_v14();
	bool is_client() {
		return is_client_;
	}
	uint32_t generate_new_backend_id();
/*
	void backend_insert(uint64_t global_statement_id, MYSQL_STMT *stmt);
	MYSQL_STMT * find_backend_stmt_by_global_id(uint32_t global_statement_id) {
		auto s=global_stmt_to_backend_stmt.find(global_statement_id);
		if (s!=global_stmt_to_backend_stmt.end()) {	// found
			return s->second;
		}
		return NULL;	// not found
	}
*/
};


class PgSQL_STMT_Manager_v14 : public Base_STMT_Manager_v14<PgSQL_STMT_Global_info> {
	public:
	PgSQL_STMT_Manager_v14();
	~PgSQL_STMT_Manager_v14();
	//PgSQL_STMT_Global_info * add_prepared_statement(char *u, char *s, char *q, unsigned int ql, char *fc, MYSQL_STMT *stmt, bool lock=true);
	//void get_memory_usage(uint64_t& prep_stmt_metadata_mem_usage, uint64_t& prep_stmt_backend_mem_usage);
};

#endif // CLASS_PGSQL_PREPARED_STATEMENT_H
