use dashmap::DashMap;
use mysql::{
    consts::{ColumnFlags, ColumnType::*},
    prelude::Queryable,
    OptsBuilder, Params, Pool,
};
use once_cell::sync::Lazy;
use serde::Deserialize;
use serde_json::{json, map::Map, Number};
use std::{mem::ManuallyDrop, sync::{Arc, atomic::AtomicUsize}, thread::JoinHandle};
use std::{error::Error, time::Duration};

// ----------------------------------------------------------------------------
// Interface

const DEFAULT_PORT: u16 = 3306;
// The `mysql` crate defaults to 10 and 100 for these, but that is too large.
const DEFAULT_MIN_CONNECTIONS: usize = 1;
const DEFAULT_MAX_CONNECTIONS: usize = 10;


const SQL_ASYNC_PENDING: &str = "SQL ASYNC PENDING";
const SQL_ASYNC_UNKNOWN_QUERY: &str = "SQL ASYNC UNKNOWN QUERY";
const SQL_ASYNC_UNKNOWN_CONNECTION: &str = "SQL ASYNC UNKNOWN CONNECTION";

#[derive(Deserialize)]
struct ConnectOptions {
    host: Option<String>,
    port: Option<u16>,
    user: Option<String>,
    pass: Option<String>,
    db_name: Option<String>,
    read_timeout: Option<f32>,
    write_timeout: Option<f32>,
    min_threads: Option<usize>,
    max_threads: Option<usize>,
}

type QueueId = String;
type QueryId = String;

static NEXT_CONNECTION_ID: AtomicUsize = AtomicUsize::new(0);
static NEXT_QUERY_ID: AtomicUsize = AtomicUsize::new(0);
static CONNECTIONS: Lazy<DashMap<QueueId, Connection>> = Lazy::new(DashMap::new);

struct QueuedQuery {
    id: QueryId,
    query: String,
    params: String,
}

struct Connection {
    sender: ManuallyDrop<crossbeam_channel::Sender<QueuedQuery>>,
    results: Arc<DashMap<QueryId, Option<String>>>,
    workers: Vec<JoinHandle<()>>,
    pool: mysql::Pool,
}

impl Connection {
    fn create(options: ConnectOptions) -> Result<serde_json::Value, Box<dyn Error>> {
        let builder = OptsBuilder::new()
            .ip_or_hostname(options.host)
            .tcp_port(options.port.unwrap_or(DEFAULT_PORT))
            // Work around addresses like `localhost:3307` defaulting to socket as
            // if the port were the default too.
            .prefer_socket(options.port.map_or(true, |p| p == DEFAULT_PORT))
            .user(options.user)
            .pass(options.pass)
            .db_name(options.db_name)
            .read_timeout(options.read_timeout.map(Duration::from_secs_f32))
            .write_timeout(options.write_timeout.map(Duration::from_secs_f32));

        let min_connections = options.min_threads.unwrap_or(DEFAULT_MIN_CONNECTIONS);
        let max_connections = options.max_threads.unwrap_or(DEFAULT_MAX_CONNECTIONS);

        let pool = Pool::new_manual(
            min_connections,
            max_connections,
            builder,
        )?;

        let (sender, receiver) = crossbeam_channel::unbounded();
        let results = Arc::new(DashMap::new());

        let workers = (0..max_connections).map(|_| {
            let receiver = receiver.clone();
            let results = results.clone();
            let pool = pool.clone();
            std::thread::spawn(move || {
                Self::worker(receiver, results, pool)
            })
        }).collect();

        let handle = NEXT_CONNECTION_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let handle = handle.to_string();
        CONNECTIONS.insert(handle.clone(), Self {
            sender: ManuallyDrop::new(sender),
            results,
            workers,
            pool,
        });

        Ok(json!({
            "status": "ok",
            "handle": handle,
        }))
    }

    fn query_blocking(&self, query: &str, params: &str) -> Result<serde_json::Value, Box<dyn Error>> {
        let mut conn = self.pool.get_conn()?;
        let inner = conn.as_mut();
        do_query(inner, &query, &params)
    }

    fn query_async(&self, query: &str, params: &str) -> Result<String, Box<dyn Error>> {
        let id = NEXT_QUERY_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let id = id.to_string();

        self.results.insert(id.clone(), None);
        self.sender.send(QueuedQuery {
            id: id.clone(),
            query: query.to_owned(),
            params: params.to_owned(),
        }).unwrap();

        Ok(id)
    }

    fn check_async(&self, id: &str) -> String {
        match self.results.get(id) {
            Some(res) => {
                match &*res {
                    Some(_) => {
                        self.results.remove(id).unwrap().1.unwrap()
                    }
                    None => SQL_ASYNC_PENDING.to_owned(),
                }
            }

            None => SQL_ASYNC_UNKNOWN_QUERY.to_owned(),
        }
    }

    fn worker(receiver: crossbeam_channel::Receiver<QueuedQuery>, results: Arc<DashMap<QueryId, Option<String>>>, pool: mysql::Pool) {
        while let Ok(query) = receiver.recv() {
            let mut conn = match pool.get_conn() {
                Ok(conn) => conn,
                Err(e) => {
                    results.insert(query.id, Some(err_to_json(e)));
                    continue;
                }
            };
            let inner = conn.as_mut();

            let result = match do_query(inner, &query.query, &query.params) {
                Ok(o) => o.to_string(),
                Err(e) => err_to_json(e),
            };

            results.insert(query.id, Some(result));
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Wait for ongoing queries to finish
        unsafe {
            ManuallyDrop::drop(&mut self.sender);
        }

        while let Some(worker) = self.workers.pop() {
            worker.join().unwrap();
        }
    }
}

byond_fn! { sql_connect_pool(options) {
    let options = match serde_json::from_str::<ConnectOptions>(options) {
        Ok(options) => options,
        Err(e) => return Some(err_to_json(e)),
    };
    Some(match Connection::create(options) {
        Ok(o) => o.to_string(),
        Err(e) => err_to_json(e)
    })
} }

byond_fn! { sql_query_blocking(handle, query, params) {
    match CONNECTIONS.get(handle) {
        Some(connection) => {
            let res = match connection.query_blocking(query, params) {
                Ok(o) => o.to_string(),
                Err(e) => err_to_json(e)
            };

            Some(res)
        }

        None => Some(json!({"status": "offline"}).to_string()),
    }
} }

byond_fn! { sql_query_async(handle, query, params) {
    match CONNECTIONS.get(handle) {
        Some(connection) => {
            let res = match connection.query_async(query, params) {
                Ok(o) => o.to_string(),
                Err(e) => err_to_json(e)
            };

            Some(res)
        }

        None => Some(json!({"status": "offline"}).to_string()),
    }
} }

// hopefully won't panic if queries are running
byond_fn! { sql_disconnect_pool(handle) {
    match CONNECTIONS.remove(handle) {
        Some(_) => Some(json!({"status": "success"}).to_string()),
        None => Some(json!({"status": "offline"}).to_string()),
    }
} }

byond_fn! { sql_connected(handle) {
    match CONNECTIONS.remove(handle) {
        Some(_) => Some(json!({"status": "online"}).to_string()),
        None => Some(json!({"status": "offline"}).to_string()),
    }
} }

byond_fn! { sql_check_query(handle, id) {
    match CONNECTIONS.get(handle) {
        Some(connection) => {
            Some(connection.check_async(id))
        }

        None => Some(SQL_ASYNC_UNKNOWN_CONNECTION.to_owned())
    }
} }

fn do_query(conn: &mut mysql::Conn, query: &str, params: &str) -> Result<serde_json::Value, Box<dyn Error>> {
    let query_result = conn.exec_iter(query, params_from_json(params))?;
    let affected = query_result.affected_rows();
    let last_insert_id = query_result.last_insert_id();
    let mut columns = Vec::new();
    for col in query_result.columns().as_ref().iter() {
        columns.push(json! {{
            "name": col.name_str(),
            // Expansion room left for other column metadata.
        }});
    }

    let mut rows: Vec<serde_json::Value> = Vec::new();
    for row in query_result {
        let row = row?;
        let mut json_row: Vec<serde_json::Value> = Vec::new();
        for (i, col) in row.columns_ref().iter().enumerate() {
            let ctype = col.column_type();
            let value = row
                .as_ref(i)
                .ok_or("length of row was smaller than column count")?;
            let converted = match value {
                mysql::Value::Bytes(b) => match ctype {
                    MYSQL_TYPE_VARCHAR | MYSQL_TYPE_STRING | MYSQL_TYPE_VAR_STRING => {
                        serde_json::Value::String(String::from_utf8_lossy(&b).into_owned())
                    }
                    MYSQL_TYPE_BLOB
                    | MYSQL_TYPE_LONG_BLOB
                    | MYSQL_TYPE_MEDIUM_BLOB
                    | MYSQL_TYPE_TINY_BLOB => {
                        if col.flags().contains(ColumnFlags::BINARY_FLAG) {
                            serde_json::Value::Array(
                                b.iter()
                                    .map(|x| serde_json::Value::Number(Number::from(*x)))
                                    .collect(),
                            )
                        } else {
                            serde_json::Value::String(String::from_utf8_lossy(&b).into_owned())
                        }
                    }
                    _ => serde_json::Value::Null,
                },
                mysql::Value::Float(f) => serde_json::Value::Number(
                    Number::from_f64(f64::from(*f)).unwrap_or_else(|| Number::from(0)),
                ),
                mysql::Value::Double(f) => serde_json::Value::Number(
                    Number::from_f64(*f).unwrap_or_else(|| Number::from(0)),
                ),
                mysql::Value::Int(i) => serde_json::Value::Number(Number::from(*i)),
                mysql::Value::UInt(u) => serde_json::Value::Number(Number::from(*u)),
                mysql::Value::Date(year, month, day, hour, minute, second, _ms) => {
                    serde_json::Value::String(format!(
                        "{}-{:02}-{:02} {:02}:{:02}:{:02}",
                        year, month, day, hour, minute, second
                    ))
                }
                _ => serde_json::Value::Null,
            };
            json_row.push(converted)
        }
        rows.push(serde_json::Value::Array(json_row));
    }

    Ok(json! {{
        "status": "ok",
        "affected": affected,
        "last_insert_id": last_insert_id,
        "columns": columns,
        "rows": rows,
    }})
}

// ----------------------------------------------------------------------------
// Helpers

fn err_to_json<E: std::fmt::Display>(e: E) -> String {
    json!({
        "status": "err",
        "data": &e.to_string()
    })
    .to_string()
}

fn json_to_mysql(val: serde_json::Value) -> mysql::Value {
    match val {
        serde_json::Value::Bool(b) => mysql::Value::UInt(b as u64),
        serde_json::Value::Number(i) => {
            if let Some(v) = i.as_u64() {
                mysql::Value::UInt(v)
            } else if let Some(v) = i.as_i64() {
                mysql::Value::Int(v)
            } else if let Some(v) = i.as_f64() {
                mysql::Value::Float(v as f32) // Loses precision.
            } else {
                mysql::Value::NULL
            }
        }
        serde_json::Value::String(s) => mysql::Value::Bytes(s.into()),
        serde_json::Value::Array(a) => mysql::Value::Bytes(
            a.into_iter()
                .map(|x| {
                    if let serde_json::Value::Number(n) = x {
                        n.as_u64().unwrap_or(0) as u8
                    } else {
                        0
                    }
                })
                .collect(),
        ),
        _ => mysql::Value::NULL,
    }
}

fn array_to_params(params: Vec<serde_json::Value>) -> Params {
    if params.is_empty() {
        Params::Empty
    } else {
        Params::Positional(params.into_iter().map(json_to_mysql).collect())
    }
}

fn object_to_params(params: Map<std::string::String, serde_json::Value>) -> Params {
    if params.is_empty() {
        Params::Empty
    } else {
        Params::Named(
            params
                .into_iter()
                .map(|(key, val)| (key, json_to_mysql(val)))
                .collect(),
        )
    }
}

fn params_from_json(params: &str) -> Params {
    match serde_json::from_str(params) {
        Ok(serde_json::Value::Object(o)) => object_to_params(o),
        Ok(serde_json::Value::Array(a)) => array_to_params(a),
        _ => Params::Empty,
    }
}
