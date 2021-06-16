#define RUSTG_SQL_ASYNC_PENDING "SQL ASYNC PENDING"
#define RUSTG_SQL_ASYNC_UNKNOWN_QUERY "SQL ASYNC UNKNOWN QUERY"
#define RUSTG_SQL_ASYNC_UNKNOWN_CONNECTION "SQL ASYNC UNKNOWN CONNECTION"

#define rustg_sql_connect_pool(options) call(RUST_G, "sql_connect_pool")(options)
#define rustg_sql_query_async(handle, query, params) call(RUST_G, "sql_query_async")(handle, query, params)
#define rustg_sql_query_blocking(handle, query, params) call(RUST_G, "sql_query_blocking")(handle, query, params)
#define rustg_sql_connected(handle) call(RUST_G, "sql_connected")(handle)
#define rustg_sql_disconnect_pool(handle) call(RUST_G, "sql_disconnect_pool")(handle)
#define rustg_sql_check_query(handle, job_id) call(RUST_G, "sql_check_query")(handle, "[job_id]")
