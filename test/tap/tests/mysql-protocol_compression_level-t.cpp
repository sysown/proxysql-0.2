#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <unistd.h>

#include <string>
#include <sstream>
#include <fstream>
#include "mysql.h"

#include "tap.h"
#include "command_line.h"
#include "utils.h"

unsigned long calculate_query_execution_time(MYSQL* mysql, const std::string& query) {
	MYSQL_RES *res;
	MYSQL_ROW row;
	unsigned long long begin = monotonic_time();
	unsigned long long row_count = 0;
	unsigned long ret_query = 0;

	ret_query = mysql_query(mysql, query.c_str());
	if (ret_query != 0) {
		fprintf(stderr, "Failed to execute query: Error: %s\n", mysql_error(mysql));
		return -1;
	}

	res = mysql_use_result(mysql);
	unsigned long long num_rows = mysql_num_rows(res);
	unsigned int num_fields = mysql_num_fields(res);
	while ((row = mysql_fetch_row(res))) {
		for (unsigned int i = 1; i < num_fields; i++) {
			char *field = row[i];
		}
		row_count++;
	}
	mysql_free_result(res);	
	unsigned long long end = monotonic_time();
	fprintf(stderr, "Row count: %lld\n", row_count);
	return (end - begin);
}

MYSQL* initilize_mysql_connection(char* host, char* username, char* password, int port, bool compression) {
	MYSQL* mysql = mysql_init(NULL);
	if (!mysql)
		return nullptr;

	fprintf(stderr, "MySQL connection details: %s %s %d\n", username, password, port);
	if (compression) {
		if (mysql_options(mysql, MYSQL_OPT_COMPRESS, nullptr) != 0) {
			fprintf(stderr, "Failed to set mysql compression option: Error: %s\n",
					mysql_error(mysql));
			return nullptr;
		}
	}
	if (!mysql_real_connect(mysql, host, username, password, NULL, port, NULL, 0)) {
	    fprintf(stderr, "Failed to connect to database: Error: %s\n",
	              mysql_error(mysql));
		return nullptr;
	}
	return mysql;
}

int main(int argc, char** argv) {

	CommandLine cl;
	std::string query = "SELECT t1.id id1, t1.k k1, t1.c c1, t1.pad pad1, t2.id id2, t2.k k2, t2.c c2, t2.pad pad2 FROM test.sbtest1 t1 JOIN test.sbtest1 t2 LIMIT 90000000";
	unsigned long time_proxy = 0;
	unsigned long time_proxy_compression_level_default = 0;
	unsigned long time_proxy_compression_level_8 = 0;	
	long diff = 0;
	unsigned long time_mysql_compressed = 0;
	unsigned long time_mysql_without_compressed = 0;
	std::string compression_level = {""};
	int32_t ret = 0;
	MYSQL* proxysql = nullptr;
	MYSQL* proxysql_compression = nullptr;
	MYSQL* proxysql_admin = nullptr;
	MYSQL* mysql_compression = nullptr;
	MYSQL* mysql = nullptr;
	float performance_diff = 0;

	if(cl.getEnv())
		return exit_status();

	plan(9);

	// ProxySQL connection without compression
	proxysql = initilize_mysql_connection(cl.host, cl.username, cl.password, cl.port, false);
	if (!proxysql) {
		goto cleanup;
	}

	// ProxySQL connection with compression
	proxysql_compression = initilize_mysql_connection(cl.host, cl.username, cl.password, cl.port, true);
	if (!proxysql_compression) {
		goto cleanup;
	}

	// MySQL connection without compression
	mysql = initilize_mysql_connection(cl.host, cl.username, cl.password, cl.mysql_port, false);
	if (!mysql) {
		goto cleanup;
	}

	// MySQL connection with compression
	mysql_compression = initilize_mysql_connection(cl.host, cl.username, cl.password, cl.mysql_port, true);
	if (!mysql_compression) {
		goto cleanup;
	}

	// ProxySQL admin connection
	proxysql_admin = initilize_mysql_connection(cl.host, cl.admin_username, cl.admin_password, cl.admin_port, false);
	if (!proxysql_admin) {
		goto cleanup;
	}

	// Change default query rules to avoid replication issues; This test only requires the default hostgroup
	MYSQL_QUERY(proxysql_admin, "UPDATE mysql_query_rules SET active=0");
	MYSQL_QUERY(proxysql_admin, "LOAD MYSQL QUERY RULES TO RUNTIME");

	MYSQL_QUERY(proxysql, "CREATE DATABASE IF NOT EXISTS test");
	MYSQL_QUERY(proxysql, "DROP TABLE IF EXISTS test.sbtest1");

	mysql_query(proxysql, "CREATE TABLE IF NOT EXISTS test.sbtest1 (id INT UNSIGNED NOT NULL AUTO_INCREMENT, k INT UNSIGNED NOT NULL DEFAULT 0, c CHAR(120) NOT NULL DEFAULT '', pad CHAR(60) NOT NULL DEFAULT '', PRIMARY KEY (id), KEY k_1 (k));");

	MYSQL_QUERY(proxysql, "USE test");

	for (int i = 0; i < 100; i++) {
		MYSQL_QUERY(proxysql, "INSERT INTO sbtest1 (k, c, pad) SELECT FLOOR(RAND() * 10000), REPEAT('a', 120), REPEAT('b', 60) FROM information_schema.tables LIMIT 1000;");
	}

	time_proxy = calculate_query_execution_time(proxysql, query);
	diag("Time taken for query with proxysql without compression: %ld", time_proxy);
	if (time_proxy == -1) {
		goto cleanup;
	}

	time_proxy_compression_level_default = calculate_query_execution_time(proxysql_compression, query);
	diag("Time taken for query with proxysql with default compression (3): %ld", time_proxy_compression_level_default);
	if (time_proxy_compression_level_default == -1) {
		goto cleanup;
	}

	diff = time_proxy_compression_level_default - time_proxy;
	performance_diff = (float)(diff * 100) / time_proxy;

	ok((performance_diff > 0), "proxysql without compression performed well compared to default compression level (3). Performance difference: %f percentage", performance_diff);

	time_mysql_without_compressed = calculate_query_execution_time(mysql, query);
	diag("Time taken for query with mysql without compression: %ld", time_mysql_without_compressed);
	if (time_mysql_without_compressed == -1) {
		goto cleanup;
	}

	time_mysql_compressed = calculate_query_execution_time(mysql_compression, query);
	diag("Time taken for query with mysql with compression: %ld", time_mysql_compressed);
	if (time_mysql_compressed == -1) {
		goto cleanup;
	}

	diff = time_mysql_compressed - time_mysql_without_compressed;
	performance_diff = (float)(diff * 100) / time_mysql_without_compressed;

	ok((performance_diff > 0), "MySQL without compression performed well compared to mysql with compression. Performance difference: %f percentage", performance_diff);

	ret = get_variable_value(proxysql_admin, "mysql-protocol_compression_level", compression_level, true);
	if (ret == EXIT_SUCCESS) {
		ok(compression_level == "3", "Run-time default compression level is correct: %s", compression_level.c_str());
	}
	else {
		diag("Failed to get the default compression level.");
		goto cleanup;
	}

	ret = get_variable_value(proxysql_admin, "mysql-protocol_compression_level", compression_level);
	if (ret == EXIT_SUCCESS) {
		ok(compression_level == "3", "Default compression level is correct: %s", compression_level.c_str());
	}
	else {
		diag("Failed to get the default compression level.");
		goto cleanup;
	}

	set_admin_global_variable(proxysql_admin, "mysql-protocol_compression_level", "8");
	if (mysql_query(proxysql_admin, "load mysql variables to runtime")) {
		diag("Failed to load mysql variables to runtime.");
		goto cleanup;
	}
	ret = get_variable_value(proxysql_admin, "mysql-protocol_compression_level", compression_level, true);
	if (ret == EXIT_SUCCESS) {
		ok(compression_level == "8", "Run-time Compression level is set correctly: %s", compression_level.c_str());
	}
	else {
		diag("Failed to set the Compression level is set correctly:");
		goto cleanup;
	}

	ret = get_variable_value(proxysql_admin, "mysql-protocol_compression_level", compression_level);
	if (ret == EXIT_SUCCESS) {
		ok(compression_level == "8", "Compression level is set correctly: %s", compression_level.c_str());
	}
	else {
		diag("Failed to set the Compression level is set correctly:");
		goto cleanup;
	}

	time_proxy_compression_level_8 = calculate_query_execution_time(proxysql_compression, query);
	diag("Time taken for query with proxysql with compression level 8: %ld", time_proxy_compression_level_8);
	if (time_proxy_compression_level_8 == -1) {
		goto cleanup;
	}

	diff = time_proxy_compression_level_8 - time_proxy_compression_level_default;
	performance_diff = (float)(diff * 100) / time_proxy_compression_level_default;

	ok((performance_diff > 0), "proxysql with default compression level (3) performed well compared to compression level (8). Performance difference: %f percentage", performance_diff);

	set_admin_global_variable(proxysql_admin, "mysql-protocol_compression_level", "3");
	if (mysql_query(proxysql_admin, "load mysql variables to runtime")) {
		diag("Failed to load mysql variables to runtime.");
		goto cleanup;
	}	
	ret = get_variable_value(proxysql_admin, "mysql-protocol_compression_level", compression_level, true);
	if (ret == EXIT_SUCCESS) {
		ok(compression_level == "3", "Run-time Compression level set correctly: %s", compression_level.c_str());
	}
	else {
		diag("Failed to set the Compression level set correctly:");
		goto cleanup;
	}

	ret = get_variable_value(proxysql_admin, "mysql-protocol_compression_level", compression_level);
	if (ret == EXIT_SUCCESS) {
		ok(compression_level == "3", "Compression level set correctly: %s", compression_level.c_str());
	}
	else {
		diag("Failed to set the Compression level set correctly:");
		goto cleanup;
	}

cleanup:
	// Recover default query rules
	if (proxysql_admin) {
		MYSQL_QUERY(proxysql_admin, "UPDATE mysql_query_rules SET active=1");
		MYSQL_QUERY(proxysql_admin, "LOAD MYSQL QUERY RULES TO RUNTIME");
	}

	if (proxysql)
		mysql_close(proxysql);
	if (proxysql_compression)
		mysql_close(proxysql_compression);
	if (mysql_compression)
		mysql_close(mysql_compression);
	if (mysql)
		mysql_close(mysql);
	if (proxysql_admin)
		mysql_close(proxysql_admin);

	return exit_status();
}
