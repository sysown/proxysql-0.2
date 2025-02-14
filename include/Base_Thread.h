#ifndef CLASS_BASE_THREAD_H
#define CLASS_BASE_THREAD_H
#include <type_traits>
#include "proxysql.h"

typedef struct _thr_id_username_t {
	uint32_t id;
	char *username;
} thr_id_usr;

typedef struct _kill_queue_t {
	pthread_mutex_t m;
	std::vector<thr_id_usr *> conn_ids;
	std::vector<thr_id_usr *> query_ids;
} kill_queue_t;

/**
 * @class Session_Regex
 * @brief Encapsulates regex operations for session handling.
 *
 * This class is used for matching patterns in SQL queries, specifically for
 * settings like sql_log_bin, sql_mode, and time_zone.
 * See issues #509 , #815 and #816
 */
class Session_Regex {
private:
	void* opt;
	void* re;
	char* s;
public:
	Session_Regex(char* p);
	~Session_Regex();
	bool match(char* m);
};

class MySQL_Thread;
class PgSQL_Thread;

template<typename T>
class Base_Thread {
	static_assert(std::is_same_v<T,MySQL_Thread> || std::is_same_v<T,PgSQL_Thread>,
		"Invalid Thread type");
	private:
	using TypeSession = typename std::conditional<std::is_same_v<T,MySQL_Thread>,MySQL_Session,PgSQL_Session>::type;
	using TypeDataStream = typename std::conditional<std::is_same_v<T,MySQL_Thread>,MySQL_Data_Stream,PgSQL_Data_Stream>::type;
	bool maintenance_loop;
	protected:
	std::vector<TypeSession *> mysql_sessions;
	public:
	std::mutex mysql_sessions_mutex;  // Protect access to mysql_sessions , if needed
	unsigned long long curtime;
	unsigned long long last_move_to_idle_thread_time;
	bool epoll_thread;
	int shutdown;
	Session_Regex **match_regexes;
	Base_Thread();
	~Base_Thread();
	TypeSession * create_new_session_and_client_data_stream(int _fd);
	void register_session(TypeSession *, bool up_start = true);
	/**
	 * @brief Unregisters a session from the thread's session array.
	 *
	 * @param idx The index of the session to unregister.
	 * @param lock Whatever the lock should be taken or not
	 *
	 * @details This function removes a session from the `mysql_sessions` array at the specified index.
	 * It does not delete the session object itself; it is assumed that the caller will handle
	 * the deletion.
	 *
	 * @note This function is called by various parts of the code when a session is no longer
	 * active and needs to be removed from the thread's session list.
	 *
	 */
	void unregister_session(int, bool);

	/**
	 * @brief Unregisters a session from the thread's session array.
	 *
	 * @param sess The address of the Session
	 * @param lock Whatever the lock should be taken or not
	 *
	 * @details This function removes a session from the `mysql_sessions` array.
	 * It does not delete the session object itself; it is assumed that the caller will handle
	 * the deletion.
	 *
	 * @note This function is called by various parts of the code when a session is no longer
	 * active and needs to be removed from the thread's session list.
	 *
	 */
	void unregister_session(TypeSession *, bool);

	void check_timing_out_session(unsigned int n);
	void check_for_invalid_fd(unsigned int n);
	void ProcessAllSessions_SortingSessions();
	void ProcessAllMyDS_AfterPoll();
	void read_one_byte_from_pipe(unsigned int n);
	void tune_timeout_for_myds_needs_pause(TypeDataStream * myds);
	void tune_timeout_for_session_needs_pause(TypeDataStream * myds);
	void configure_pollout(TypeDataStream * myds, unsigned int n);
	bool set_backend_to_be_skipped_if_frontend_is_slow(TypeDataStream * myds, unsigned int n);
#ifdef IDLE_THREADS
	bool move_session_to_idle_mysql_sessions(TypeDataStream * myds, unsigned int n);
#endif // IDLE_THREADS
	unsigned int find_session_idx_in_mysql_sessions(TypeSession * sess);
	void ProcessAllMyDS_BeforePoll();
	void run_SetAllSession_ToProcess0();
	const std::vector<TypeSession *>& get_mysql_sessions_ref() const { return mysql_sessions; };

#if ENABLE_TIMER
	// for now this is not accessible via Admin/Prometheus , thus useful only with gdb
	struct {
		TimerCount Sessions_Handlers;
		TimerCount Connections_Handlers;
	} Timers;
#endif // ENABLE_TIMER

	friend class MySQL_Thread;
	friend class PgSQL_Thread;
};

std::string proxysql_session_type_str(enum proxysql_session_type session_type);

#endif // CLASS_BASE_THREAD_H
