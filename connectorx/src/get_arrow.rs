#[cfg(feature = "src_mysql")]
use crate::sources::mysql::{BinaryProtocol as MySQLBinaryProtocol, TextProtocol};
#[cfg(feature = "src_postgres")]
use crate::sources::postgres::{
    rewrite_tls_args, BinaryProtocol as PgBinaryProtocol, CSVProtocol, CursorProtocol,
    SimpleProtocol,
};
use crate::{
    arrow_batch_iter::{ArrowBatchIter, RecordBatchIterator},
    pool::PoolVariant,
    prelude::*,
    sql::CXQuery,
};
use fehler::{throw, throws};
use log::debug;
#[cfg(feature = "src_postgres")]
use postgres::NoTls;
#[cfg(feature = "src_postgres")]
use postgres_openssl::MakeTlsConnector;
#[allow(unused_imports)]
use std::sync::Arc;

#[allow(unreachable_code, unreachable_patterns, unused_variables, unused_mut)]
#[throws(ConnectorXOutError)]
pub fn get_arrow(
    source_conn: &SourceConn,
    origin_query: Option<String>,
    queries: &[CXQuery<String>],
    pre_execution_queries: Option<&[String]>,
    pool: Option<&PoolVariant>,
) -> ArrowDestination {
    let mut destination = ArrowDestination::new();
    let protocol = source_conn.proto.as_str();
    debug!("Protocol: {}", protocol);

    match source_conn.ty {
        #[cfg(feature = "src_postgres")]
        SourceType::Postgres => {
            let (config, tls) = rewrite_tls_args(&source_conn.conn)?;
            match (protocol, tls) {
                ("csv", Some(tls_conn)) => {
                    let pg_pool = pool.map(|p| p.postgres_tls_pool());
                    let source = PostgresSource::<CSVProtocol, MakeTlsConnector>::new(
                        config, tls_conn, queries.len(), pg_pool,
                    )?;
                    let mut dispatcher = Dispatcher::<
                        _,
                        _,
                        PostgresArrowTransport<CSVProtocol, MakeTlsConnector>,
                    >::new(
                        source, &mut destination, queries, origin_query
                    );
                    dispatcher.set_pre_execution_queries(pre_execution_queries);
                    dispatcher.run()?;
                }
                ("csv", None) => {
                    let pg_pool = pool.map(|p| p.postgres_notls_pool());
                    let source = PostgresSource::<CSVProtocol, NoTls>::new(
                        config, NoTls, queries.len(), pg_pool,
                    )?;
                    let mut dispatcher = Dispatcher::<
                        _,
                        _,
                        PostgresArrowTransport<CSVProtocol, NoTls>,
                    >::new(
                        source, &mut destination, queries, origin_query
                    );
                    dispatcher.set_pre_execution_queries(pre_execution_queries);
                    dispatcher.run()?;
                }
                ("binary", Some(tls_conn)) => {
                    let pg_pool = pool.map(|p| p.postgres_tls_pool());
                    let source = PostgresSource::<PgBinaryProtocol, MakeTlsConnector>::new(
                        config, tls_conn, queries.len(), pg_pool,
                    )?;
                    let mut dispatcher = Dispatcher::<
                        _,
                        _,
                        PostgresArrowTransport<PgBinaryProtocol, MakeTlsConnector>,
                    >::new(
                        source, &mut destination, queries, origin_query
                    );
                    dispatcher.set_pre_execution_queries(pre_execution_queries);
                    dispatcher.run()?;
                }
                ("binary", None) => {
                    let pg_pool = pool.map(|p| p.postgres_notls_pool());
                    let source = PostgresSource::<PgBinaryProtocol, NoTls>::new(
                        config, NoTls, queries.len(), pg_pool,
                    )?;
                    let mut dispatcher = Dispatcher::<
                        _,
                        _,
                        PostgresArrowTransport<PgBinaryProtocol, NoTls>,
                    >::new(
                        source, &mut destination, queries, origin_query
                    );
                    dispatcher.set_pre_execution_queries(pre_execution_queries);
                    dispatcher.run()?;
                }
                ("cursor", Some(tls_conn)) => {
                    let pg_pool = pool.map(|p| p.postgres_tls_pool());
                    let source = PostgresSource::<CursorProtocol, MakeTlsConnector>::new(
                        config, tls_conn, queries.len(), pg_pool,
                    )?;
                    let mut dispatcher = Dispatcher::<
                        _,
                        _,
                        PostgresArrowTransport<CursorProtocol, MakeTlsConnector>,
                    >::new(
                        source, &mut destination, queries, origin_query
                    );
                    dispatcher.set_pre_execution_queries(pre_execution_queries);
                    dispatcher.run()?;
                }
                ("cursor", None) => {
                    let pg_pool = pool.map(|p| p.postgres_notls_pool());
                    let source = PostgresSource::<CursorProtocol, NoTls>::new(
                        config, NoTls, queries.len(), pg_pool,
                    )?;
                    let mut dispatcher = Dispatcher::<
                        _,
                        _,
                        PostgresArrowTransport<CursorProtocol, NoTls>,
                    >::new(
                        source, &mut destination, queries, origin_query
                    );
                    dispatcher.set_pre_execution_queries(pre_execution_queries);
                    dispatcher.run()?;
                }
                ("simple", Some(tls_conn)) => {
                    let pg_pool = pool.map(|p| p.postgres_tls_pool());
                    let source = PostgresSource::<SimpleProtocol, MakeTlsConnector>::new(
                        config, tls_conn, queries.len(), pg_pool,
                    )?;
                    let mut dispatcher = Dispatcher::<
                        _,
                        _,
                        PostgresArrowTransport<SimpleProtocol, MakeTlsConnector>,
                    >::new(
                        source, &mut destination, queries, origin_query
                    );
                    debug!("Running dispatcher");
                    dispatcher.set_pre_execution_queries(pre_execution_queries);
                    dispatcher.run()?;
                }
                ("simple", None) => {
                    let pg_pool = pool.map(|p| p.postgres_notls_pool());
                    let source = PostgresSource::<SimpleProtocol, NoTls>::new(
                        config, NoTls, queries.len(), pg_pool,
                    )?;
                    let mut dispatcher = Dispatcher::<
                        _,
                        _,
                        PostgresArrowTransport<SimpleProtocol, NoTls>,
                    >::new(
                        source, &mut destination, queries, origin_query
                    );
                    debug!("Running dispatcher");
                    dispatcher.set_pre_execution_queries(pre_execution_queries);
                    dispatcher.run()?;
                }
                _ => unimplemented!("{} protocol not supported", protocol),
            }
        }
        #[cfg(feature = "src_mysql")]
        SourceType::MySQL => {
            let mysql_pool = pool.map(|p| p.mysql_pool());
            match protocol {
                "binary" => {
                    let source = MySQLSource::<MySQLBinaryProtocol>::new(
                        &source_conn.conn[..], queries.len(), mysql_pool,
                    )?;
                    let mut dispatcher =
                        Dispatcher::<_, _, MySQLArrowTransport<MySQLBinaryProtocol>>::new(
                            source,
                            &mut destination,
                            queries,
                            origin_query,
                        );
                    dispatcher.set_pre_execution_queries(pre_execution_queries);
                    dispatcher.run()?;
                }
                "text" => {
                    let source = MySQLSource::<TextProtocol>::new(
                        &source_conn.conn[..], queries.len(), mysql_pool,
                    )?;
                    let mut dispatcher =
                        Dispatcher::<_, _, MySQLArrowTransport<TextProtocol>>::new(
                            source,
                            &mut destination,
                            queries,
                            origin_query,
                        );
                    dispatcher.set_pre_execution_queries(pre_execution_queries);
                    dispatcher.run()?;
                }
                _ => unimplemented!("{} protocol not supported", protocol),
            }
        }
        #[cfg(feature = "src_sqlite")]
        SourceType::SQLite => {
            // remove the first "sqlite://" manually since url.path is not correct for windows
            let path = &source_conn.conn.as_str()[9..];
            let sqlite_pool = pool.map(|p| p.sqlite_pool());
            let source = SQLiteSource::new(path, queries.len(), sqlite_pool)?;
            let dispatcher = Dispatcher::<_, _, SQLiteArrowTransport>::new(
                source,
                &mut destination,
                queries,
                origin_query,
            );
            dispatcher.run()?;
        }
        #[cfg(feature = "src_mssql")]
        SourceType::MsSQL => {
            let rt = Arc::new(tokio::runtime::Runtime::new().expect("Failed to create runtime"));
            let source = MsSQLSource::new(rt, &source_conn.conn[..], queries.len())?;
            let dispatcher = Dispatcher::<_, _, MsSQLArrowTransport>::new(
                source,
                &mut destination,
                queries,
                origin_query,
            );
            dispatcher.run()?;
        }
        #[cfg(feature = "src_oracle")]
        SourceType::Oracle => {
            let oracle_pool = pool.map(|p| p.oracle_pool());
            let source = OracleSource::new(&source_conn.conn[..], queries.len(), oracle_pool)?;
            let dispatcher = Dispatcher::<_, _, OracleArrowTransport>::new(
                source,
                &mut destination,
                queries,
                origin_query,
            );
            dispatcher.run()?;
        }
        #[cfg(feature = "src_bigquery")]
        SourceType::BigQuery => {
            let rt = Arc::new(tokio::runtime::Runtime::new().expect("Failed to create runtime"));
            let source = BigQuerySource::new(rt, &source_conn.conn[..])?;
            let dispatcher = Dispatcher::<_, _, BigQueryArrowTransport>::new(
                source,
                &mut destination,
                queries,
                origin_query,
            );
            dispatcher.run()?;
        }
        #[cfg(feature = "src_trino")]
        SourceType::Trino => {
            let rt = Arc::new(tokio::runtime::Runtime::new().expect("Failed to create runtime"));
            let source = TrinoSource::new(rt, &source_conn.conn[..])?;
            let dispatcher = Dispatcher::<_, _, TrinoArrowTransport>::new(
                source,
                &mut destination,
                queries,
                origin_query,
            );
            dispatcher.run()?;
        }
        _ => throw!(ConnectorXOutError::SourceNotSupport(format!(
            "{:?}",
            source_conn.ty
        ))),
    }

    destination
}

#[allow(unreachable_code, unreachable_patterns, unused_variables, unused_mut)]
pub fn new_record_batch_iter(
    source_conn: &SourceConn,
    origin_query: Option<String>,
    queries: &[CXQuery<String>],
    batch_size: usize,
    pre_execution_queries: Option<&[String]>,
    pool: Option<&PoolVariant>,
) -> Box<dyn RecordBatchIterator> {
    let destination = ArrowStreamDestination::new_with_batch_size(batch_size);
    let protocol = source_conn.proto.as_str();
    debug!("Protocol: {}", protocol);

    match source_conn.ty {
        #[cfg(feature = "src_postgres")]
        SourceType::Postgres => {
            let (config, tls) = rewrite_tls_args(&source_conn.conn).unwrap();
            match (protocol, tls) {
                ("csv", Some(tls_conn)) => {
                    let pg_pool = pool.map(|p| p.postgres_tls_pool());
                    let mut source = PostgresSource::<CSVProtocol, MakeTlsConnector>::new(
                        config, tls_conn, queries.len(), pg_pool,
                    ).unwrap();
                    source.set_pre_execution_queries(pre_execution_queries);
                    let batch_iter =
                        ArrowBatchIter::<
                            _,
                            PostgresArrowStreamTransport<CSVProtocol, MakeTlsConnector>,
                        >::new(source, destination, origin_query, queries)
                        .unwrap();
                    return Box::new(batch_iter);
                }
                ("csv", None) => {
                    let pg_pool = pool.map(|p| p.postgres_notls_pool());
                    let mut source = PostgresSource::<CSVProtocol, NoTls>::new(
                        config, NoTls, queries.len(), pg_pool,
                    ).unwrap();
                    source.set_pre_execution_queries(pre_execution_queries);
                    let batch_iter = ArrowBatchIter::<
                        _,
                        PostgresArrowStreamTransport<CSVProtocol, NoTls>,
                    >::new(
                        source, destination, origin_query, queries
                    )
                    .unwrap();
                    return Box::new(batch_iter);
                }
                ("binary", Some(tls_conn)) => {
                    let pg_pool = pool.map(|p| p.postgres_tls_pool());
                    let mut source = PostgresSource::<PgBinaryProtocol, MakeTlsConnector>::new(
                        config, tls_conn, queries.len(), pg_pool,
                    ).unwrap();
                    source.set_pre_execution_queries(pre_execution_queries);
                    let batch_iter =
                        ArrowBatchIter::<
                            _,
                            PostgresArrowStreamTransport<PgBinaryProtocol, MakeTlsConnector>,
                        >::new(source, destination, origin_query, queries)
                        .unwrap();
                    return Box::new(batch_iter);
                }
                ("binary", None) => {
                    let pg_pool = pool.map(|p| p.postgres_notls_pool());
                    let mut source = PostgresSource::<PgBinaryProtocol, NoTls>::new(
                        config, NoTls, queries.len(), pg_pool,
                    ).unwrap();
                    source.set_pre_execution_queries(pre_execution_queries);
                    let batch_iter = ArrowBatchIter::<
                        _,
                        PostgresArrowStreamTransport<PgBinaryProtocol, NoTls>,
                    >::new(
                        source, destination, origin_query, queries
                    )
                    .unwrap();
                    return Box::new(batch_iter);
                }
                ("cursor", Some(tls_conn)) => {
                    let pg_pool = pool.map(|p| p.postgres_tls_pool());
                    let mut source = PostgresSource::<CursorProtocol, MakeTlsConnector>::new(
                        config, tls_conn, queries.len(), pg_pool,
                    ).unwrap();
                    source.set_pre_execution_queries(pre_execution_queries);
                    let batch_iter =
                        ArrowBatchIter::<
                            _,
                            PostgresArrowStreamTransport<CursorProtocol, MakeTlsConnector>,
                        >::new(source, destination, origin_query, queries)
                        .unwrap();
                    return Box::new(batch_iter);
                }
                ("cursor", None) => {
                    let pg_pool = pool.map(|p| p.postgres_notls_pool());
                    let mut source = PostgresSource::<CursorProtocol, NoTls>::new(
                        config, NoTls, queries.len(), pg_pool,
                    ).unwrap();
                    source.set_pre_execution_queries(pre_execution_queries);
                    let batch_iter = ArrowBatchIter::<
                        _,
                        PostgresArrowStreamTransport<CursorProtocol, NoTls>,
                    >::new(
                        source, destination, origin_query, queries
                    )
                    .unwrap();
                    return Box::new(batch_iter);
                }
                _ => unimplemented!("{} protocol not supported", protocol),
            }
        }
        #[cfg(feature = "src_mysql")]
        SourceType::MySQL => {
            let mysql_pool = pool.map(|p| p.mysql_pool());
            match protocol {
                "binary" => {
                    let mut source = MySQLSource::<MySQLBinaryProtocol>::new(
                        &source_conn.conn[..], queries.len(), mysql_pool,
                    ).unwrap();
                    source.set_pre_execution_queries(pre_execution_queries);
                    let batch_iter =
                        ArrowBatchIter::<_, MySQLArrowStreamTransport<MySQLBinaryProtocol>>::new(
                            source,
                            destination,
                            origin_query,
                            queries,
                        )
                        .unwrap();
                    return Box::new(batch_iter);
                }
                "text" => {
                    let mut source = MySQLSource::<TextProtocol>::new(
                        &source_conn.conn[..], queries.len(), mysql_pool,
                    ).unwrap();
                    source.set_pre_execution_queries(pre_execution_queries);
                    let batch_iter =
                        ArrowBatchIter::<_, MySQLArrowStreamTransport<TextProtocol>>::new(
                            source,
                            destination,
                            origin_query,
                            queries,
                        )
                        .unwrap();
                    return Box::new(batch_iter);
                }
                _ => unimplemented!("{} protocol not supported", protocol),
            }
        }
        #[cfg(feature = "src_sqlite")]
        SourceType::SQLite => {
            // remove the first "sqlite://" manually since url.path is not correct for windows
            let path = &source_conn.conn.as_str()[9..];
            let sqlite_pool = pool.map(|p| p.sqlite_pool());
            let source = SQLiteSource::new(path, queries.len(), sqlite_pool).unwrap();
            let batch_iter = ArrowBatchIter::<_, SQLiteArrowStreamTransport>::new(
                source,
                destination,
                origin_query,
                queries,
            )
            .unwrap();
            return Box::new(batch_iter);
        }
        #[cfg(feature = "src_mssql")]
        SourceType::MsSQL => {
            let rt = Arc::new(tokio::runtime::Runtime::new().expect("Failed to create runtime"));
            let source = MsSQLSource::new(rt, &source_conn.conn[..], queries.len()).unwrap();
            let batch_iter = ArrowBatchIter::<_, MsSQLArrowStreamTransport>::new(
                source,
                destination,
                origin_query,
                queries,
            )
            .unwrap();
            return Box::new(batch_iter);
        }
        #[cfg(feature = "src_oracle")]
        SourceType::Oracle => {
            let oracle_pool = pool.map(|p| p.oracle_pool());
            let source =
                OracleSource::new(&source_conn.conn[..], queries.len(), oracle_pool).unwrap();
            let batch_iter = ArrowBatchIter::<_, OracleArrowStreamTransport>::new(
                source,
                destination,
                origin_query,
                queries,
            )
            .unwrap();
            return Box::new(batch_iter);
        }
        #[cfg(feature = "src_bigquery")]
        SourceType::BigQuery => {
            let rt = Arc::new(tokio::runtime::Runtime::new().expect("Failed to create runtime"));
            let source = BigQuerySource::new(rt, &source_conn.conn[..]).unwrap();
            let batch_iter = ArrowBatchIter::<_, BigQueryArrowStreamTransport>::new(
                source,
                destination,
                origin_query,
                queries,
            )
            .unwrap();
            return Box::new(batch_iter);
        }
        _ => {}
    }
    panic!("not supported!");
}
