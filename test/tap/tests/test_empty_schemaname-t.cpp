/**
 * @file test_empty_schemaname-t.cpp
 * @brief Simple test checking that empty schemaname are properly handled by ProxySQL.
 */

#include "mysql.h"
#include "tap.h"
#include "command_line.h"
#include "utils.h"

uint32_t EXECUTIONS = 1000;

int main(int argc, char** argv) {
	plan(1);

	CommandLine cl;

	if (cl.getEnv()) {
		diag("Failed to get the required environmental variables.");
		return EXIT_FAILURE;
	}

	for (uint32_t i = 0; i < EXECUTIONS; i++) {
	{
	MYSQL* proxy = mysql_init(NULL);

	if (!mysql_real_connect(proxy, cl.host, "sbtest", "sbtest", "sbtest", cl.port, NULL, 0)) {
		fprintf(stderr, "File %s, line %d, Error: %s\n", __FILE__, __LINE__, mysql_error(proxy));
		return EXIT_FAILURE;
	}
	mysql_query(proxy, "DO 1");
	mysql_query(proxy, "USE ``");
	mysql_query(proxy, "USE `` ");
	mysql_query(proxy, "USE  `` ");
	mysql_query(proxy, "DO 1");
/*
	mysql_close(proxy);
	}

	{
	MYSQL* proxy = mysql_init(NULL);

	if (!mysql_real_connect(proxy, cl.host, "sbtest", "sbtest", "` `", cl.port, NULL, 0)) {
		fprintf(stderr, "File %s, line %d, Error: %s\n", __FILE__, __LINE__, mysql_error(proxy));
		return EXIT_FAILURE;
	}
	mysql_query(proxy, "DO 1");
*/
	mysql_close(proxy);
	}
	}
/*
	int exp_myerr = 1065;
	int act_myerr = 0;

	for (uint32_t i = 0; i < EXECUTIONS; i++) {
		mysql_query(proxy, "");
		act_myerr = mysql_errno(proxy);

		if (exp_myerr != act_myerr) {
			break;
		}
	}

	ok(
		exp_myerr == act_myerr, "MySQL error equals expected - exp_err: '%d', act_err: '%d' error: `%s`",
		exp_myerr, act_myerr, mysql_error(proxy)
	);
*/
	//mysql_close(proxy);

	return exit_status();
}
