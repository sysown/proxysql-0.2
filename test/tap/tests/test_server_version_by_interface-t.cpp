#include <cstdlib>
#include <cstdio>
#include <cstring>
#include "tap.h"
#include "command_line.h"
#include "utils.h"

int main(int argc, char** argv) {
    CommandLine cl;

    if (cl.getEnv()) {
        diag("Failed to get the required environmental variables.");
        return EXIT_FAILURE;
    }

    plan(3);

    MYSQL* mysql;
    MYSQL* admin;
    
    mysql = mysql_init(NULL);
    if (!mysql) {
        fprintf(stderr, "Failed to initialize MySQL connection\n");
        return EXIT_FAILURE;
    }

    // Test case 1: Connect to port 6033
    if (!mysql_real_connect(mysql, cl.host, cl.username, cl.password, NULL, 6033, NULL, 0)) {
        fprintf(stderr, "Failed to connect to ProxySQL\n");
        return EXIT_FAILURE;
    }
    
    MYSQL_RES* res = mysql_query(mysql, "SELECT @@version");
    ok(strcmp(res->row[0], "8.0.30") == 0, "Port 6033 returns correct version");

    mysql_close(mysql);
    return exit_status();
}