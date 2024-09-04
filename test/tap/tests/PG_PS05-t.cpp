#include <iostream>
#include <libpq-fe.h>
#include <vector>
#include <string>
#include <cstring>
#include <poll.h>

const int N = 5;  // Number of connections
const std::string conninfo = "dbname=mydatabase user=sbtest password=sbtest hostaddr=127.0.0.1 port=5432";
const std::string prepareName = "range_scan_stmt";
const std::string prepareStmt = "SELECT * FROM sbtest1 WHERE id BETWEEN $1 and $2;";

// Utility function to check the status of the query
void checkPQResult(PGresult *res, PGconn *conn) {
	ExecStatusType RS = PQresultStatus(res);
    if (RS != PGRES_SINGLE_TUPLE && RS != PGRES_TUPLES_OK && RS != PGRES_COMMAND_OK) {
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
                while (res != nullptr) {
                    checkPQResult(res, connections[i]);
                    res = PQgetResult(connections[i]);
                }
            }
        }
    }

    // Enable single-row mode and execute the query
    for (int i = 0; i < N; ++i) {
        //if (!PQsetSingleRowMode(connections[i])) {
        //    std::cerr << "Failed to set single-row mode on connection " << i << std::endl;
        //    return 1;
        //}

        const char *paramValues[2] = {"1", "100000"}; // Example range for the query
        if (PQsendQueryPrepared(connections[i], prepareName.c_str(), 2, paramValues, NULL, NULL, 0) == 0) {
            std::cerr << "Error sending prepared statement execution on connection " << i << ": " << PQerrorMessage(connections[i]) << std::endl;
            return 1;
        }
    }

    // Set up poll() structures
    std::vector<pollfd> poll_fds(N);
    for (int i = 0; i < N; ++i) {
        poll_fds[i].fd = PQsocket(connections[i]);
        poll_fds[i].events = POLLIN;
    }

    // Event loop to process rows as they become available
    bool all_done = false;
    while (!all_done) {
        all_done = true;

        // Wait for any socket to have data available
        int ret = poll(poll_fds.data(), N, -1); // -1 means to wait indefinitely
        if (ret < 0) {
            std::cerr << "poll() failed: " << strerror(errno) << std::endl;
            return 1;
        }

        // Check which connections have data ready to be read
        for (int i = 0; i < N; ++i) {
            if (poll_fds[i].revents & POLLIN) {
                if (PQconsumeInput(connections[i]) == 0) {
                    std::cerr << "Error consuming input on connection " << i << ": " << PQerrorMessage(connections[i]) << std::endl;
                    return 1;
                }
                if (!PQisBusy(connections[i])) {
                    PGresult *res = PQgetResult(connections[i]);
                    while (res != nullptr) {
                        if (PQresultStatus(res) == PGRES_SINGLE_TUPLE) {
                            // Process each row as it becomes available
                            int ncols = PQnfields(res);
                            for (int col = 0; col < ncols; ++col) {
                                std::cout << PQfname(res, col) << ": " << PQgetvalue(res, col, 0) << " ";
                            }
                            std::cout << std::endl;
                        } else if (PQresultStatus(res) == PGRES_TUPLES_OK) {
                            // Final result indicating no more rows
                            break;
                        } else {
                            checkPQResult(res, connections[i]);
                        }
                        res = PQgetResult(connections[i]);
                    }
                } else {
                    all_done = false;
                }
            }
        }
    }

    // Close connections
    for (int i = 0; i < N; ++i) {
        PQfinish(connections[i]);
    }

    std::cout << "All rows processed successfully." << std::endl;

    return 0;
}

