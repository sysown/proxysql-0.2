 /**
  * @file pgsql-reg_test_4707_threshold_resultset_size-t.cpp
  * @brief The test specifically examines the impact of different pgsql-threshold_resultset_size threshold values on query response times
  *     and addresses an identified issue caused by variable overflow, which results in slow performance.
  */

#include <string>
#include <sstream>
#include <chrono>
#include "libpq-fe.h"
#include "command_line.h"
#include "tap.h"
#include "utils.h"

CommandLine cl;

using PGConnPtr = std::unique_ptr<PGconn, decltype(&PQfinish)>;

enum ConnType {
    ADMIN,
    BACKEND
};

PGConnPtr createNewConnection(ConnType conn_type, bool with_ssl) {

    const char* host = (conn_type == BACKEND) ? cl.pgsql_host : cl.pgsql_admin_host;
    int port = (conn_type == BACKEND) ? cl.pgsql_port : cl.pgsql_admin_port;
    const char* username = (conn_type == BACKEND) ? cl.pgsql_username : cl.admin_username;
    const char* password = (conn_type == BACKEND) ? cl.pgsql_password : cl.admin_password;

    std::stringstream ss;

    ss << "host=" << host << " port=" << port;
    ss << " user=" << username << " password=" << password;
    ss << (with_ssl ? " sslmode=require" : " sslmode=disable");

    PGconn* conn = PQconnectdb(ss.str().c_str());
    if (PQstatus(conn) != CONNECTION_OK) {
        fprintf(stderr, "Connection failed to '%s': %s", (conn_type == BACKEND ? "Backend" : "Admin"), PQerrorMessage(conn));
        PQfinish(conn);
        return PGConnPtr(nullptr, &PQfinish);
    }
    return PGConnPtr(conn, &PQfinish);
}

bool executeQueries(PGconn* conn, const std::vector<std::string>& queries) {
    for (const auto& query : queries) {
        diag("Running: %s", query.c_str());
        PGresult* res = PQexec(conn, query.c_str());
        bool success = PQresultStatus(res) == PGRES_TUPLES_OK || 
            PQresultStatus(res) == PGRES_COMMAND_OK;
        if (!success) {
            fprintf(stderr, "Failed to execute query '%s': %s",
                query.c_str(), PQerrorMessage(conn));
            PQclear(res);
            return false;
        }
        PQclear(res);
    }
    return true;
}

int main(int argc, char** argv) {

    plan(6); // Total number of tests planned

    if (cl.getEnv())
        return exit_status();

    // Connnect to ProxySQL Admin
    PGConnPtr admin_conn = createNewConnection(ConnType::ADMIN, false);

    if (!admin_conn) {
        BAIL_OUT("Error: failed to connect to the database in file %s, line %d\n", __FILE__, __LINE__);
        return exit_status();
    }

    // Connnect to ProxySQL Backend
    PGConnPtr backend_conn = createNewConnection(ConnType::BACKEND, false);

    if (!backend_conn) {
        BAIL_OUT("Error: failed to connect to the database in file %s, line %d\n", __FILE__, __LINE__);
        return exit_status();
    }

	if (!executeQueries(admin_conn.get(), {
        "DELETE FROM pgsql_query_rules",
        "LOAD PGSQL QUERY RULES TO RUNTIME",
        "SET pgsql-poll_timeout=2000",
        "SET pgsql-threshold_resultset_size=8000",
        "LOAD PGSQL VARIABLES TO RUNTIME" }))
		return exit_status();

    bool success;

    auto start = std::chrono::high_resolution_clock::now();
    success = executeQueries(backend_conn.get(), { "SELECT 1" });
    auto end = std::chrono::high_resolution_clock::now();

    ok(success, "Query executed successfully. %s", PQerrorMessage(backend_conn.get()));

    std::chrono::duration<double, std::milli> duration = end - start;
	// increased threshold value in case no backend connections are available in the connection pool and a new connection is established.
    ok(duration.count() < 50.00, "Execution time should be less than 50 ms. Actual: %f ms", duration.count());

	if (!executeQueries(admin_conn.get(), {
        "SET pgsql-threshold_resultset_size=536870912",
		"LOAD PGSQL VARIABLES TO RUNTIME" }))
		return exit_status();

    start = std::chrono::high_resolution_clock::now();
    success = executeQueries(backend_conn.get(), { "SELECT 1" });
    end = std::chrono::high_resolution_clock::now();

    ok(success, "Query executed successfully. %s", PQerrorMessage(backend_conn.get()));

    duration = end - start;
    ok(duration.count() < 10.00, "Execution time should be less than 10 ms. Actual: %f ms", duration.count());

    if (!executeQueries(admin_conn.get(), {
        "SET pgsql-threshold_resultset_size=1073741824",
		"LOAD PGSQL VARIABLES TO RUNTIME" }))
		return exit_status();

    start = std::chrono::high_resolution_clock::now();
    success = executeQueries(backend_conn.get(), { "SELECT 1" });
    end = std::chrono::high_resolution_clock::now();

    ok(success, "Query executed successfully. %s", PQerrorMessage(backend_conn.get()));

    duration = end - start;
    ok(duration.count() < 10.00, "Execution time should be less than 10 ms. Actual: %f ms", duration.count());

    return exit_status();
}
