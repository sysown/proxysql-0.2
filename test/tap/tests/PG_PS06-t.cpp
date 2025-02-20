

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

enum class ConnectionState {
    PREPARING,
    EXECUTING,
    PROCESSING,
    DONE
};

// Utility function to check the status of the query
void checkPQResult(PGresult *res, PGconn *conn) {
    if (PQresultStatus(res) == PGRES_COMMAND_OK || PQresultStatus(res) == PGRES_SINGLE_TUPLE || PQresultStatus(res) == PGRES_TUPLES_OK) {
        return;  // These are valid statuses for different stages
    } else {
        std::cerr << "Error: " << PQerrorMessage(conn) << std::endl;
        PQclear(res);
    }
}

int main() {
    std::vector<PGconn*> connections(N);
    std::vector<ConnectionState> states(N, ConnectionState::PREPARING);

    // Establish multiple connections
    for (int i = 0; i < N; ++i) {
        connections[i] = PQconnectdb(conninfo.c_str());
        if (PQstatus(connections[i]) != CONNECTION_OK) {
            std::cerr << "Connection " << i << " failed: " << PQerrorMessage(connections[i]) << std::endl;
            return 1;
        }

        // Prepare statements asynchronously using PQsendPrepare
        if (PQsendPrepare(connections[i], prepareName.c_str(), prepareStmt.c_str(), 2, nullptr) == 0) {
            std::cerr << "Error sending prepare statement on connection " << i << ": " << PQerrorMessage(connections[i]) << std::endl;
            return 1;
        }
    }

    // Set up poll() structures
    std::vector<pollfd> poll_fds(N);
    for (int i = 0; i < N; ++i) {
        poll_fds[i].fd = PQsocket(connections[i]);
        poll_fds[i].events = POLLIN;
    }

    // Event loop to process preparation and execution
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

                if (states[i] == ConnectionState::PREPARING) {
                    // If preparing, check if preparation is done
                    if (!PQisBusy(connections[i])) {
                        PGresult *res = PQgetResult(connections[i]);
                        while (res != nullptr) {
                            checkPQResult(res, connections[i]);
                            res = PQgetResult(connections[i]);
                        }

                        // Switch to execution phase
                        states[i] = ConnectionState::EXECUTING;
                        all_done = false;  // There's more work to do
						int r1 = rand()%100000;
						int r2 = rand()%100000;
						r2 += r1;
                        const char *paramValues[2] = {std::to_string(r1).c_str(), std::to_string(r2).c_str()}; // Example range for the query
                        if (PQsendQueryPrepared(connections[i], prepareName.c_str(), 2, paramValues, NULL, NULL, 0) == 0) {
                            std::cerr << "Error sending prepared statement execution on connection " << i << ": " << PQerrorMessage(connections[i]) << std::endl;
                            return 1;
                        }

                        // Set single-row mode immediately after sending the query
                        if (!PQsetSingleRowMode(connections[i])) {
                            std::cerr << "Failed to set single-row mode on connection " << i << std::endl;
                            return 1;
                        }
                    } else {
                        all_done = false;  // Still busy with preparation
                    }
                } else if (states[i] == ConnectionState::EXECUTING) {
                    // If executing, check if execution is done
                    if (!PQisBusy(connections[i])) {
                        states[i] = ConnectionState::PROCESSING;
                        all_done = false;  // There's more work to do
                    } else {
                        all_done = false;  // Still busy with execution
                    }
                } else if (states[i] == ConnectionState::PROCESSING) {
                    // If processing, get and handle results
                    PGresult *res = PQgetResult(connections[i]);
                    while (res != nullptr) {
                        if (PQresultStatus(res) == PGRES_SINGLE_TUPLE) {
                            // Process each row as it becomes available
                            int ncols = PQnfields(res);
                            //for (int col = 0; col < ncols; ++col) {
                            //    std::cout << PQfname(res, col) << ": " << PQgetvalue(res, 0, col) << " ";
                            //}
                            //std::cout << std::endl;
                        } else if (PQresultStatus(res) == PGRES_TUPLES_OK) {
                            // Final result indicating no more rows
                            states[i] = ConnectionState::DONE;  // Mark as done
                            break;
                        } else {
                            checkPQResult(res, connections[i]);
                        }
                        res = PQgetResult(connections[i]);
                    }
                }
            }
        }

        // Check if all connections are done
        all_done = true;
        for (int i = 0; i < N; ++i) {
            if (states[i] != ConnectionState::DONE) {
                all_done = false;
                break;
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

