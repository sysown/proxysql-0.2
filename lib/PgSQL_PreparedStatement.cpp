#include "proxysql.h"
#include "cpp.h"

#ifndef SPOOKYV2
#include "SpookyV2.h"
#define SPOOKYV2
#endif

#include "PgSQL_PreparedStatement.h"
#include "PgSQL_Protocol.h"




extern PgSQL_STMT_Manager_v14 *GloPgStmt;

PgSQL_STMTs_local_v14::~PgSQL_STMTs_local_v14() {
	// Note: we do not free the prepared statements because we assume that
	// if we call this destructor the connection is being destroyed anyway

	if (is_client_) {
		for (std::map<uint32_t, uint64_t>::iterator it = client_stmt_to_global_ids.begin();
			it != client_stmt_to_global_ids.end(); ++it) {
			uint64_t global_stmt_id = it->second;
			GloPgStmt->ref_count_client(global_stmt_id, -1);
		}
	} else {
/*
		for (std::map<uint64_t, MYSQL_STMT *>::iterator it = global_stmt_to_backend_stmt.begin();
			it != global_stmt_to_backend_stmt.end(); ++it) {
			uint64_t global_stmt_id = it->first;
			MYSQL_STMT *stmt = it->second;
			proxy_mysql_stmt_close(stmt);
			GloPgStmt->ref_count_server(global_stmt_id, -1);
		}
*/
	}
}

PgSQL_STMT_Global_info::~PgSQL_STMT_Global_info() {
	free(username);
	free(schemaname);
	free(query);
	username = NULL;
	schemaname = NULL;
	query = NULL;
	if (first_comment) {
		free(first_comment);
		first_comment = NULL;
	}
	if (digest_text) {
		free(digest_text);
		digest_text = NULL;
	}
}

PgSQL_STMT_Manager_v14::PgSQL_STMT_Manager_v14() {
}

PgSQL_STMT_Manager_v14::~PgSQL_STMT_Manager_v14() {
}
