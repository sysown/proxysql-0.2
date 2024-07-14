#include "proxysql.h"
#include "cpp.h"

#ifndef SPOOKYV2
#include "SpookyV2.h"
#define SPOOKYV2
#endif

#include "MySQL_PreparedStatement.h"
#include "MySQL_Protocol.h"




//extern MySQL_STMT_Manager *GloMyStmt;
//static uint32_t add_prepared_statement_calls = 0;
//static uint32_t find_prepared_statement_by_hash_calls = 0;
//#else
extern MySQL_STMT_Manager_v14 *GloMyStmt;
//#endif

const int PS_GLOBAL_STATUS_FIELD_NUM = 9;

StmtLongDataHandler::StmtLongDataHandler() { long_datas = new PtrArray(); }

StmtLongDataHandler::~StmtLongDataHandler() {
	while (long_datas->len) {
		stmt_long_data_t *sld =
		    (stmt_long_data_t *)long_datas->remove_index_fast(0);
		free(sld->data);
		free(sld);
	}
	delete long_datas;
}

bool StmtLongDataHandler::add(uint32_t _stmt_id, uint16_t _param_id,
                              void *_data, unsigned long _size) {
	stmt_long_data_t *sld = NULL;
	unsigned int i;
	for (i = 0; i < long_datas->len; i++) {
		sld = (stmt_long_data_t *)long_datas->index(i);
		if (sld->stmt_id == _stmt_id && sld->param_id == _param_id) {
			// we found it!
			unsigned long _new_size = sld->size + _size;
			sld->data = realloc(sld->data, _new_size);
			memcpy((unsigned char *)sld->data + sld->size, _data, _size);
			sld->size = _new_size;
			return true;
		}
	}
	// if we reached here, we didn't find it
	sld = (stmt_long_data_t *)malloc(sizeof(stmt_long_data_t));
	sld->stmt_id = _stmt_id;
	sld->param_id = _param_id;
	sld->size = _size;
	sld->data = malloc(_size);
	sld->is_null = 0; // because the client is sending data, the field cannot be NULL
	memcpy(sld->data, _data, _size);
	long_datas->add(sld);
	return false;  // a new entry was created
}

unsigned int StmtLongDataHandler::reset(uint32_t _stmt_id) {
	unsigned int cnt = 0;
	int i;
	stmt_long_data_t *sld = NULL;
	for (i = 0; i < (int)long_datas->len;
	     i++) {  // we treat it as an int, so we can go to -1
		sld = (stmt_long_data_t *)long_datas->index(i);
		if (sld->stmt_id == _stmt_id) {
			sld = (stmt_long_data_t *)long_datas->remove_index_fast(i);
			free(sld->data);
			free(sld);
			i--;
			cnt++;
		}
	}
	return cnt;
}

void *StmtLongDataHandler::get(uint32_t _stmt_id, uint16_t _param_id,
                               unsigned long **_size, my_bool **_is_null) {
	stmt_long_data_t *sld = NULL;
	unsigned int i;
	for (i = 0; i < long_datas->len; i++) {
		sld = (stmt_long_data_t *)long_datas->index(i);
		if (sld->stmt_id == _stmt_id && sld->param_id == _param_id) {
			// we found it!
			*_size = &sld->size;
			*_is_null = &sld->is_null;
			return sld->data;
		}
	}
	return NULL;
}

MySQL_STMT_Global_info::MySQL_STMT_Global_info(uint64_t id,
                                               char *u, char *s, char *q,
                                               unsigned int ql,
                                               char *fc,
                                               MYSQL_STMT *stmt, uint64_t _h) {
	username = strdup(u);
	schemaname = strdup(s);
	query = (char *)malloc(ql + 1);
	memcpy(query, q, ql);
	query[ql] = '\0';  // add NULL byte
	query_length = ql;
	if (fc) {
		first_comment = strdup(fc);
	} else {
		first_comment = NULL;
	}
//	MyComQueryCmd = MYSQL_COM_QUERY__UNINITIALIZED;
	num_params = stmt->param_count;
	num_columns = stmt->field_count;
	warning_count = stmt->upsert_status.warning_count;
	if (_h) {
		hash = _h;
	} else {
		compute_hash();
	}

	is_select_NOT_for_update = false;
	{  // see bug #899 . Most of the code is borrowed from
	   // Query_Info::is_select_NOT_for_update()
		if (ql >= 7) {
			if (strncasecmp(q, (char *)"SELECT ", 7) == 0) {  // is a SELECT
				if (ql >= 17) {
					char *p = q;
					p += ql - 11;
					if (strncasecmp(p, " FOR UPDATE", 11) == 0) {  // is a SELECT FOR UPDATE
						__sync_fetch_and_add(&MyHGM->status.select_for_update_or_equivalent, 1);
						goto __exit_MySQL_STMT_Global_info___search_select;
					}
					p = q;
					p += ql-10;
					if (strncasecmp(p, " FOR SHARE", 10) == 0) {  // is a SELECT FOR SHARE
						__sync_fetch_and_add(&MyHGM->status.select_for_update_or_equivalent, 1);
						goto __exit_MySQL_STMT_Global_info___search_select;
					}
					if (ql >= 25) {
						p = q;
						p += ql-19;
						if (strncasecmp(p, " LOCK IN SHARE MODE", 19) == 0) {  // is a SELECT LOCK IN SHARE MODE
							__sync_fetch_and_add(&MyHGM->status.select_for_update_or_equivalent, 1);
							goto __exit_MySQL_STMT_Global_info___search_select;
						}
						p = q;
						p += ql-7;
						if (strncasecmp(p," NOWAIT",7)==0) {
							// let simplify. If NOWAIT is used, we assume FOR UPDATE|SHARE is used
							__sync_fetch_and_add(&MyHGM->status.select_for_update_or_equivalent, 1);
							goto __exit_MySQL_STMT_Global_info___search_select;
/*
							if (strcasestr(q," FOR UPDATE ")) {
								__sync_fetch_and_add(&MyHGM->status.select_for_update_or_equivalent, 1);
								goto __exit_MySQL_STMT_Global_info___search_select;
							}
							if (strcasestr(q," FOR SHARE ")) {
								__sync_fetch_and_add(&MyHGM->status.select_for_update_or_equivalent, 1);
								goto __exit_MySQL_STMT_Global_info___search_select;
							}
*/
						}
						p = q;
						p += ql-12;
						if (strncasecmp(p," SKIP LOCKED",12)==0) {
							// let simplify. If SKIP LOCKED is used, we assume FOR UPDATE|SHARE is used
							__sync_fetch_and_add(&MyHGM->status.select_for_update_or_equivalent, 1);
							goto __exit_MySQL_STMT_Global_info___search_select;
/*
							if (strcasestr(q," FOR UPDATE ")==NULL) {
								__sync_fetch_and_add(&MyHGM->status.select_for_update_or_equivalent, 1);
								goto __exit_MySQL_STMT_Global_info___search_select;
							}
							if (strcasestr(q," FOR SHARE ")==NULL) {
								__sync_fetch_and_add(&MyHGM->status.select_for_update_or_equivalent, 1);
								goto __exit_MySQL_STMT_Global_info___search_select;
							}
*/
						}
						p=q;
						char buf[129];
						if (ql>=128) { // for long query, just check the last 128 bytes
							p+=ql-128;
							memcpy(buf,p,128);
							buf[128]=0;
						} else {
							memcpy(buf,p,ql);
							buf[ql]=0;
						}
						if (strcasestr(buf," FOR ")) {
							if (strcasestr(buf," FOR UPDATE ")) {
								__sync_fetch_and_add(&MyHGM->status.select_for_update_or_equivalent, 1);
								goto __exit_MySQL_STMT_Global_info___search_select;
							}
							if (strcasestr(buf," FOR SHARE ")) {
								__sync_fetch_and_add(&MyHGM->status.select_for_update_or_equivalent, 1);
								goto __exit_MySQL_STMT_Global_info___search_select;
							}
						}
					}
				}
				is_select_NOT_for_update = true;
			}
		}
	}
__exit_MySQL_STMT_Global_info___search_select:

	// set default properties:
//	properties.cache_ttl = -1;
//	properties.timeout = -1;
//	properties.delay = -1;

	fields = NULL;
	if (num_columns) {
		fields = (MYSQL_FIELD **)malloc(num_columns * sizeof(MYSQL_FIELD *));
		uint16_t i;
		for (i = 0; i < num_columns; i++) {
			fields[i] = (MYSQL_FIELD *)malloc(sizeof(MYSQL_FIELD));
			MYSQL_FIELD *fs = &(stmt->fields[i]);
			MYSQL_FIELD *fd = fields[i];
			// first copy all fields
			memcpy(fd, fs, sizeof(MYSQL_FIELD));
			// then duplicate strings
			fd->name = (fs->name ? strdup(fs->name) : NULL);
			fd->org_name = (fs->org_name ? strdup(fs->org_name) : NULL);
			fd->table = (fs->table ? strdup(fs->table) : NULL);
			fd->org_table = (fs->org_table ? strdup(fs->org_table) : NULL);
			fd->db = (fs->db ? strdup(fs->db) : NULL);
			fd->catalog = (fs->catalog ? strdup(fs->catalog) : NULL);
			fd->def = (fs->def ? strdup(fs->def) : NULL);
		}
	}

	params = NULL;
	if (num_params == 2) {
		PROXY_TRACE();
	}
	if (num_params) {
		params = (MYSQL_BIND **)malloc(num_params * sizeof(MYSQL_BIND *));
		uint16_t i;
		for (i = 0; i < num_params; i++) {
			params[i] = (MYSQL_BIND *)malloc(sizeof(MYSQL_BIND));
			// MYSQL_BIND *ps=&(stmt->params[i]);
			// MYSQL_BIND *pd=params[i];
			// copy all params
			// memcpy(pd,ps,sizeof(MYSQL_BIND));
			memset(params[i], 0, sizeof(MYSQL_BIND));
		}
	}

	calculate_mem_usage();
}

void MySQL_STMT_Global_info::calculate_mem_usage() {
	total_mem_usage = sizeof(MySQL_STMT_Global_info) +
		(num_params * (sizeof(MYSQL_BIND) + sizeof(MYSQL_BIND*))) +
		(num_columns * (sizeof(MYSQL_FIELD) + sizeof(MYSQL_FIELD*))) +
		query_length + 1;// +
		//(ref_count_client * 24) +
		//(ref_count_server * 24);

	if (username) total_mem_usage += strlen(username) + 1;
	if (schemaname) total_mem_usage += strlen(schemaname) + 1;
	if (first_comment) total_mem_usage += strlen(first_comment) + 1;
	if (digest_text) total_mem_usage += strlen(digest_text) + 1;

	for (uint16_t i = 0; i < num_columns; i++) {
		const MYSQL_FIELD* fd = fields[i];
		if (fd->name) total_mem_usage += strlen(fd->name) + 1;
		if (fd->org_name) total_mem_usage += strlen(fd->org_name) + 1;
		if (fd->table) total_mem_usage += strlen(fd->table) + 1;
		if (fd->org_table) total_mem_usage += strlen(fd->org_table) + 1;
		if (fd->db) total_mem_usage += strlen(fd->db) + 1;
		if (fd->catalog) total_mem_usage += strlen(fd->catalog) + 1;
		if (fd->def) total_mem_usage += strlen(fd->def) + 1;
	}
}

void MySQL_STMT_Global_info::update_metadata(MYSQL_STMT *stmt) {
	int i;
	bool need_refresh = false;
	pthread_rwlock_wrlock(&rwlock_);
	if (
		(num_params != stmt->param_count)
		||
		(num_columns != stmt->field_count)
	) {
		need_refresh = true;
	}
	for (i = 0; i < num_columns; i++) {
		if (need_refresh == false) { // don't bother to check if need_refresh == true
			bool ok = true;
			MYSQL_FIELD *fs = &(stmt->fields[i]);
			MYSQL_FIELD *fd = fields[i];
			if (ok) {
				ok = false;
				if (fd->name == NULL && fs->name == NULL) {
					ok = true;
				} else {
					if (fd->name && fs->name && strcmp(fd->name,fs->name)==0) {
						ok = true;
					}
				}
			}
			if (ok) {
				ok = false;
				if (fd->org_name == NULL && fs->org_name == NULL) {
					ok = true;
				} else {
					if (fd->org_name && fs->org_name && strcmp(fd->org_name,fs->org_name)==0) {
						ok = true;
					}
				}
			}
			if (ok) {
				ok = false;
				if (fd->table == NULL && fs->table == NULL) {
					ok = true;
				} else {
					if (fd->table && fs->table && strcmp(fd->table,fs->table)==0) {
						ok = true;
					}
				}
			}
			if (ok) {
				ok = false;
				if (fd->org_table == NULL && fs->org_table == NULL) {
					ok = true;
				} else {
					if (fd->org_table && fs->org_table && strcmp(fd->org_table,fs->org_table)==0) {
						ok = true;
					}
				}
			}
			if (ok) {
				ok = false;
				if (fd->db == NULL && fs->db == NULL) {
					ok = true;
				} else {
					if (fd->db && fs->db && strcmp(fd->db,fs->db)==0) {
						ok = true;
					}
				}
			}
			if (ok) {
				ok = false;
				if (fd->catalog == NULL && fs->catalog == NULL) {
					ok = true;
				} else {
					if (fd->catalog && fs->catalog && strcmp(fd->catalog,fs->catalog)==0) {
						ok = true;
					}
				}
			}
			if (ok) {
				ok = false;
				if (fd->def == NULL && fs->def == NULL) {
					ok = true;
				} else {
					if (fd->def && fs->def && strcmp(fd->def,fs->def)==0) {
						ok = true;
					}
				}
			}
			if (ok == false) {
				need_refresh = true;
			}
		}
	}
	if (need_refresh) {
		if (digest_text && strncasecmp(digest_text, "EXPLAIN", strlen("EXPLAIN"))==0) {
			// do not print any message in case of EXPLAIN
		} else {
			proxy_warning("Updating metadata for stmt %lu , user %s, query %s\n", statement_id, username, query);
		}
// from here is copied from destructor
		if (num_columns) {
			uint16_t i;
			for (i = 0; i < num_columns; i++) {
				MYSQL_FIELD *f = fields[i];
				if (f->name) {
					free(f->name);
					f->name = NULL;
				}
				if (f->org_name) {
					free(f->org_name);
					f->org_name = NULL;
				}
				if (f->table) {
					free(f->table);
					f->table = NULL;
				}
				if (f->org_table) {
					free(f->org_table);
					f->org_table = NULL;
				}
				if (f->db) {
					free(f->db);
					f->db = NULL;
				}
				if (f->catalog) {
					free(f->catalog);
					f->catalog = NULL;
				}
				if (f->def) {
					free(f->def);
					f->def = NULL;
				}
				free(fields[i]);
			}
			free(fields);
			fields = NULL;
		}
		if (num_params) {
			uint16_t i;
			for (i = 0; i < num_params; i++) {
				free(params[i]);
			}
			free(params);
			params = NULL;
		}
// till here is copied from destructor

// from here is copied from constructor
		num_params = stmt->param_count;
		num_columns = stmt->field_count;
		fields = NULL;
		if (num_columns) {
			fields = (MYSQL_FIELD **)malloc(num_columns * sizeof(MYSQL_FIELD *));
			uint16_t i;
			for (i = 0; i < num_columns; i++) {
				fields[i] = (MYSQL_FIELD *)malloc(sizeof(MYSQL_FIELD));
				MYSQL_FIELD *fs = &(stmt->fields[i]);
				MYSQL_FIELD *fd = fields[i];
				// first copy all fields
				memcpy(fd, fs, sizeof(MYSQL_FIELD));
				// then duplicate strings
				fd->name = (fs->name ? strdup(fs->name) : NULL);
				fd->org_name = (fs->org_name ? strdup(fs->org_name) : NULL);
				fd->table = (fs->table ? strdup(fs->table) : NULL);
				fd->org_table = (fs->org_table ? strdup(fs->org_table) : NULL);
				fd->db = (fs->db ? strdup(fs->db) : NULL);
				fd->catalog = (fs->catalog ? strdup(fs->catalog) : NULL);
				fd->def = (fs->def ? strdup(fs->def) : NULL);
			}
		}
	
		params = NULL;
		if (num_params == 2) {
			PROXY_TRACE();
		}
		if (num_params) {
			params = (MYSQL_BIND **)malloc(num_params * sizeof(MYSQL_BIND *));
			uint16_t i;
			for (i = 0; i < num_params; i++) {
				params[i] = (MYSQL_BIND *)malloc(sizeof(MYSQL_BIND));
				// MYSQL_BIND *ps=&(stmt->params[i]);
				// MYSQL_BIND *pd=params[i];
				// copy all params
				// memcpy(pd,ps,sizeof(MYSQL_BIND));
				memset(params[i], 0, sizeof(MYSQL_BIND));
			}
		}
// till here is copied from constructor
		calculate_mem_usage();
	}
	pthread_rwlock_unlock(&rwlock_);
}

MySQL_STMT_Global_info::~MySQL_STMT_Global_info() {
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
	if (num_columns) {
		uint16_t i;
		for (i = 0; i < num_columns; i++) {
			MYSQL_FIELD *f = fields[i];
			if (f->name) {
				free(f->name);
				f->name = NULL;
			}
			if (f->org_name) {
				free(f->org_name);
				f->org_name = NULL;
			}
			if (f->table) {
				free(f->table);
				f->table = NULL;
			}
			if (f->org_table) {
				free(f->org_table);
				f->org_table = NULL;
			}
			if (f->db) {
				free(f->db);
				f->db = NULL;
			}
			if (f->catalog) {
				free(f->catalog);
				f->catalog = NULL;
			}
			if (f->def) {
				free(f->def);
				f->def = NULL;
			}
			free(fields[i]);
		}
		free(fields);
		fields = NULL;
	}

	if (num_params) {
		uint16_t i;
		for (i = 0; i < num_params; i++) {
			free(params[i]);
		}
		free(params);
		params = NULL;
	}
}

extern MySQL_STMT_Manager_v14 *GloMyStmt;

void MySQL_STMTs_local_v14::backend_insert(uint64_t global_statement_id, MYSQL_STMT *stmt) {
	std::pair<std::map<uint64_t, MYSQL_STMT *>::iterator, bool> ret;
	ret = global_stmt_to_backend_stmt.insert(std::make_pair(global_statement_id, stmt));
	global_stmt_to_backend_ids.insert(std::make_pair(global_statement_id,stmt->stmt_id));
	backend_stmt_to_global_ids.insert(std::make_pair(stmt->stmt_id,global_statement_id));
	// note: backend_insert() is always called after add_prepared_statement()
	// for this reason, we will the ref count increase in add_prepared_statement()
	// GloMyStmt->ref_count_client(global_statement_id, 1);
}

MySQL_STMT_Manager_v14::MySQL_STMT_Manager_v14() {
}

MySQL_STMT_Manager_v14::~MySQL_STMT_Manager_v14() {
}


MySQL_STMTs_local_v14::~MySQL_STMTs_local_v14() {
	// Note: we do not free the prepared statements because we assume that
	// if we call this destructor the connection is being destroyed anyway

	if (is_client_) {
		for (std::map<uint32_t, uint64_t>::iterator it = client_stmt_to_global_ids.begin();
			it != client_stmt_to_global_ids.end(); ++it) {
			uint64_t global_stmt_id = it->second;
			GloMyStmt->ref_count_client(global_stmt_id, -1);
		}
	} else {
		for (std::map<uint64_t, MYSQL_STMT *>::iterator it = global_stmt_to_backend_stmt.begin();
			it != global_stmt_to_backend_stmt.end(); ++it) {
			uint64_t global_stmt_id = it->first;
			MYSQL_STMT *stmt = it->second;
			proxy_mysql_stmt_close(stmt);
			GloMyStmt->ref_count_server(global_stmt_id, -1);
		}
	}
}


MySQL_STMT_Global_info *MySQL_STMT_Manager_v14::add_prepared_statement(
    char *u, char *s, char *q, unsigned int ql,
    char *fc, MYSQL_STMT *stmt, bool lock) {
	MySQL_STMT_Global_info *ret = NULL;
	uint64_t hash = Base_STMT_Global_info::stmt_compute_hash(
		u, s, q, ql);  // this identifies the prepared statement
	if (lock) {
		pthread_rwlock_wrlock(&rwlock_);
	}
	// try to find the statement
	auto f = map_stmt_hash_to_info.find(hash);
	if (f != map_stmt_hash_to_info.end()) {
		// found it!
		// MySQL_STMT_Global_info *a=f->second;
		// ret=a->statement_id;
		ret = f->second;
		ret->update_metadata(stmt);
		//*is_new = false;
	} else {
		// FIXME: add a stack here too!!!
		// we need to create a new one

		bool free_id_avail = false;
		free_id_avail = free_stmt_ids.size();

		uint64_t next_id = 0;
		if (free_id_avail) {
			next_id = free_stmt_ids.top();
			free_stmt_ids.pop();
		} else {
			next_id = next_statement_id;
			next_statement_id++;
		}

		//next_statement_id++;
		MySQL_STMT_Global_info *a =
		    new MySQL_STMT_Global_info(next_id, u, s, q, ql, fc, stmt, hash);
		// insert it in both maps
		map_stmt_id_to_info.insert(std::make_pair(a->statement_id, a));
		map_stmt_hash_to_info.insert(std::make_pair(a->hash, a));
		ret = a;
		__sync_add_and_fetch(&num_stmt_with_ref_client_count_zero,1);
		__sync_add_and_fetch(&num_stmt_with_ref_server_count_zero,1);
	}
	if (ret->ref_count_server == 0) {
		__sync_sub_and_fetch(&num_stmt_with_ref_server_count_zero,1);
	}
	ret->ref_count_server++;
	statuses.s_total++;
	if (lock) {
		pthread_rwlock_unlock(&rwlock_);
	}
	return ret;
}


void MySQL_STMT_Manager_v14::get_memory_usage(uint64_t& prep_stmt_metadata_mem_usage, uint64_t& prep_stmt_backend_mem_usage) {
	prep_stmt_backend_mem_usage = 0;
	prep_stmt_metadata_mem_usage = sizeof(MySQL_STMT_Manager_v14);
	rdlock();	
	prep_stmt_metadata_mem_usage += map_stmt_id_to_info.size() * (sizeof(uint64_t) + sizeof(MySQL_STMT_Global_info*));
	prep_stmt_metadata_mem_usage += map_stmt_hash_to_info.size() * (sizeof(uint64_t) + sizeof(MySQL_STMT_Global_info*));
	prep_stmt_metadata_mem_usage += free_stmt_ids.size() * (sizeof(uint64_t));
	for (const auto& keyval : map_stmt_id_to_info) {
		const MySQL_STMT_Global_info* stmt_global_info = keyval.second;
		prep_stmt_metadata_mem_usage += stmt_global_info->total_mem_usage;
		prep_stmt_metadata_mem_usage += stmt_global_info->ref_count_server *
			((stmt_global_info->num_params * sizeof(MYSQL_BIND)) +
			(stmt_global_info->num_columns * sizeof(MYSQL_FIELD))) + 16; // ~16 bytes of memory utilized by global_stmt_id and stmt_id mappings
		prep_stmt_metadata_mem_usage += stmt_global_info->ref_count_client *
			((stmt_global_info->num_params * sizeof(MYSQL_BIND)) +
			(stmt_global_info->num_columns * sizeof(MYSQL_FIELD))) + 16; // ~16 bytes of memory utilized by global_stmt_id and stmt_id mappings

		// backend
		prep_stmt_backend_mem_usage += stmt_global_info->ref_count_server * (sizeof(MYSQL_STMT) +
			56 + //sizeof(MADB_STMT_EXTENSION)
			(stmt_global_info->num_params * sizeof(MYSQL_BIND)) + 
			(stmt_global_info->num_columns * sizeof(MYSQL_FIELD)));
	}
	unlock();
}
