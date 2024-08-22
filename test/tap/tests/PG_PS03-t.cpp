#include <iostream>
#include <libpq-fe.h>
#include <vector>
#include <string>
#include <cstring>

const int N = 5;  // Number of connections
const std::string conninfo = "dbname=mydatabase user=sbtest password=sbtest hostaddr=127.0.0.1 port=5432";
const std::string prepareName = "test_stmt";
const std::string prepareStmt = "SELECT * FROM sbtest1 WHERE id IN ($1, $2);";

// Utility function to check the status of the query
void checkPQResult(PGresult *res, PGconn *conn) {
    if (PQresultStatus(res) != PGRES_COMMAND_OK && PQresultStatus(res) != PGRES_TUPLES_OK) {
        std::cerr << "Error: " << PQerrorMessage(conn) << std::endl;
    }
    PQclear(res);
}

int main() {
    std::vector<PGconn*> connections(N);

    // Establish multiple connections
    for (int i = 0; i < N; ++i) {
        connections[i] = PQconnectdb(conninfo.c_str());
        if (PQstatus(connections[i]) != CONNECTION_OK) {
            std::cerr << "Connection " << i << " failed: " << PQerrorMessage(connections[i]) << std::endl;
            return 1;
        }
    }

    // Prepare statements asynchronously using PQsendPrepare
    for (int i = 0; i < N; ++i) {
        if (PQsendPrepare(connections[i], prepareName.c_str(), prepareStmt.c_str(), 2, nullptr) == 0) {
            std::cerr << "Error sending prepare statement on connection " << i << ": " << PQerrorMessage(connections[i]) << std::endl;
            return 1;
        }
    }

    // Event loop to check for preparation completion
    bool all_prepared = false;
    while (!all_prepared) {
        all_prepared = true;
        for (int i = 0; i < N; ++i) {
            if (PQconsumeInput(connections[i]) == 0) {
                std::cerr << "Error consuming input on connection " << i << ": " << PQerrorMessage(connections[i]) << std::endl;
                return 1;
            }
            if (PQisBusy(connections[i])) {
                all_prepared = false;
            } else {
                PGresult *res = PQgetResult(connections[i]);
                if (res) {
                    checkPQResult(res, connections[i]);
                }
            }
        }
    }

    // Execute prepared statements asynchronously using PQsendQueryPrepared
    for (int i = 0; i < N; ++i) {
        const char *paramValues[2] = {"1", "2"};
        if (PQsendQueryPrepared(connections[i], prepareName.c_str(), 2, paramValues, NULL, NULL, 0) == 0) {
            std::cerr << "Error sending prepared statement execution on connection " << i << ": " << PQerrorMessage(connections[i]) << std::endl;
            return 1;
        }
    }

    // Event loop to check for execution completion and process results
    bool all_done = false;
    while (!all_done) {
        all_done = true;
        for (int i = 0; i < N; ++i) {
            if (PQconsumeInput(connections[i]) == 0) {
                std::cerr << "Error consuming input on connection " << i << ": " << PQerrorMessage(connections[i]) << std::endl;
                return 1;
            }
            if (PQisBusy(connections[i])) {
                all_done = false;
            } else {
                PGresult *res = PQgetResult(connections[i]);
                if (res) {
                    checkPQResult(res, connections[i]);
                }
            }
        }
    }

    // Close connections
    for (int i = 0; i < N; ++i) {
        PQfinish(connections[i]);
    }

    std::cout << "All prepared statements executed successfully." << std::endl;

    return 0;
}

