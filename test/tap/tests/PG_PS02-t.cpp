#include <iostream>
#include <string>
#include <vector>
#include <chrono>
#include <libpq-fe.h>
#include <unistd.h>

// Connection parameters
const std::string HOST = "127.0.0.1";
const std::string PORT = "5432";
const std::string DATABASE = "mydatabase";
const std::string USER = "sbtest";
const std::string PASSWORD = "sbtest";
const int NUM_CONNECTIONS = 5; // Number of connections to establish

// Prepared statement name
const std::string STATEMENT_NAME = "select_data";

// Function to process query results
void processQueryResult(PGconn* conn, PGresult* result) {
  if (PQresultStatus(result) != PGRES_COMMAND_OK) {
    std::cerr << "Error executing query: " << PQresultErrorMessage(result) << std::endl;
    PQclear(result);
    return;
  }

  // Process the result here
  // ...
  PQclear(result);
}

// Function to execute a prepared statement asynchronously
void executePreparedStatement(PGconn* conn, const std::string& query, const std::vector<std::string>& params) {
  // Prepare the statement
  int rc = PQsendPrepare(conn, STATEMENT_NAME.c_str(), query.c_str(), 0, NULL);
  if (rc != 1) {
    std::cerr << "Error preparing statement: " << PQerrorMessage(conn) << std::endl;
    return;
  }

  // Wait for the preparation to complete
  PGresult* prepareResult = PQgetResult(conn);
  if (PQresultStatus(prepareResult) != PGRES_COMMAND_OK) {
    std::cerr << "Error preparing statement: " << PQresultErrorMessage(prepareResult) << std::endl;
    PQclear(prepareResult);
    return;
  }
  PQclear(prepareResult);


  // Convert vector<string> to array of const char*
  std::vector<const char*> paramChars;
  for (const auto& param : params) {
    paramChars.push_back(param.c_str());
  } 

  // Execute the prepared statement
  int queryStatus = PQsendQueryPrepared(conn, STATEMENT_NAME.c_str(), params.size(), paramChars.data(), NULL, NULL, 0);
  if (queryStatus != 1) {
    std::cerr << "Error executing prepared statement: " << PQerrorMessage(conn) << std::endl;
    return;
  } 
} 

int main() {
  // Create connection string
  std::string connString = "host=" + HOST + " port=" + PORT + " dbname=" + DATABASE + " user=" + USER + " password=" + PASSWORD;


	char *conninfo = (char *)connString.c_str();

  if (getenv("PGCONN") != NULL) {
	conninfo = getenv("PGCONN");
  }

  // Create connections
  std::vector<PGconn*> connections;
  for (int i = 0; i < NUM_CONNECTIONS; ++i) {
    PGconn* conn = PQconnectdb(conninfo);
    if (PQstatus(conn) != CONNECTION_OK) {
      std::cerr << "Connection failed: " << PQerrorMessage(conn) << std::endl;
      PQfinish(conn);
    } else {
      connections.push_back(conn);
    } 
  }   

  // Prepare the statement on one connection
  PGconn* conn = connections[0];
  std::string query = "SELECT * FROM sbtest1 WHERE id = $1"; // Parameterized query
//  executePreparedStatement(conn, query, {"1"}); // Example parameter

  // Execute queries on each connection asynchronously
  for (int i = 0; i < NUM_CONNECTIONS; i++) {
    conn = connections[i];
    // Example query with different parameters
    executePreparedStatement(conn, query, {"2"});
    //executePreparedStatement(conn, query, {"3"});
    // ... Add more queries here
  }

  // Event loop to check for query completion on all connections
  while (true) {
    // Check for query completion on each connection
    for (auto conn : connections) {
      if (PQisBusy(conn)) {
        // Process other tasks or wait for a short duration
		usleep(10);
      } else {
        // Process available results
        PGresult* result = PQgetResult(conn);
        if (result) {
          processQueryResult(conn, result);
        }
      }
    }
  }

  // Close connections
  for (auto conn : connections) {
    PQfinish(conn);
  }

  return 0;



	
}

