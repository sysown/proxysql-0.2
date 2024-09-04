#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>

int main() {
  // Connection parameters
  //const char * conninfo = "host=127.0.0.1 port=15432 dbname=postgres user=postgres password=postgres";
  //const char * conninfo = "host=127.0.0.1 port=6133 dbname=postgres user=postgres password=postgres sslmode=disable";
  //const char * conninfo = "host=127.0.0.1 port=5432 dbname=postgres user=postgres password=postgres sslmode=disable";
  char * conninfo = (char *)"host=127.0.0.1 port=15432 dbname=postgres user=postgres password=postgres sslmode=disable";

  if (getenv("PGCONN") != NULL) {
	conninfo = getenv("PGCONN");
  }

  // Connect to the PostgreSQL server
  PGconn* conn = PQconnectdb(conninfo);

  if (PQstatus(conn) != CONNECTION_OK) {
    fprintf(stderr, "Connection failed: %s\n", PQerrorMessage(conn));
    PQfinish(conn);
    exit(1);
  } 

  // Prepare the statement
  const char* query = "SELECT * FROM sbtest1 WHERE id=$1";
  const char* statement_name = "my_prepared_statement";
  PGresult* prepare_result = PQprepare(conn, statement_name, query, 1, NULL);

  if (PQresultStatus(prepare_result) != PGRES_COMMAND_OK) {
    fprintf(stderr, "Prepare failed: %s\n", PQresultErrorMessage(prepare_result));
    PQclear(prepare_result);
    PQfinish(conn);
    exit(1);
  } 

  // Execute the prepared statement
  const char* params[1] = {"123"};  // Assuming id=123
  PGresult* exec_result = PQexecPrepared(conn, statement_name, 1, params, NULL, NULL, 0);

  if (PQresultStatus(exec_result) != PGRES_TUPLES_OK) {
    fprintf(stderr, "Execution failed: %s\n", PQresultErrorMessage(exec_result));
    PQclear(exec_result);
    PQclear(prepare_result);
    PQfinish(conn);
    exit(1);
  } 

  // Print the result
  int rows = PQntuples(exec_result);
  for (int i = 0; i < rows; i++) {
    printf("%s\n", PQgetvalue(exec_result, i, 0)); // Assuming the first column is text
  }

  // Clean up
  PQclear(exec_result);
  PQclear(prepare_result);
  PQfinish(conn);

  return 0;
}
