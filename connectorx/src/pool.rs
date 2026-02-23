use crate::source_router::SourceConn;
#[cfg(any(
    feature = "src_mysql",
    feature = "src_postgres",
    feature = "src_sqlite",
    feature = "src_oracle"
))]
use crate::source_router::SourceType;
use anyhow::Result;
#[cfg(any(
    feature = "src_mysql",
    feature = "src_postgres",
    feature = "src_sqlite",
    feature = "src_oracle"
))]
use r2d2::Pool;
#[cfg(any(
    feature = "src_mysql",
    feature = "src_postgres",
    feature = "src_sqlite",
    feature = "src_oracle"
))]
use std::sync::Arc;

#[cfg(feature = "src_mysql")]
use r2d2_mysql::MySqlConnectionManager;
#[cfg(feature = "src_mysql")]
use r2d2_mysql::mysql::{Opts, OptsBuilder};

#[cfg(feature = "src_postgres")]
use r2d2_postgres::PostgresConnectionManager;
#[cfg(feature = "src_postgres")]
use postgres::NoTls;
#[cfg(feature = "src_postgres")]
use postgres_openssl::MakeTlsConnector;

#[cfg(feature = "src_sqlite")]
use r2d2_sqlite::SqliteConnectionManager;

#[cfg(feature = "src_oracle")]
use r2d2_oracle::OracleConnectionManager;

/// A unified pool type covering all pool-supported database backends.
/// Arms are feature-gated so only compiled when the relevant source is enabled.
#[derive(Clone)]
pub enum PoolVariant {
    #[cfg(feature = "src_mysql")]
    MySQL(Arc<Pool<MySqlConnectionManager>>),
    #[cfg(feature = "src_postgres")]
    PostgresNoTls(Arc<Pool<PostgresConnectionManager<NoTls>>>),
    #[cfg(feature = "src_postgres")]
    PostgresTls(Arc<Pool<PostgresConnectionManager<MakeTlsConnector>>>),
    #[cfg(feature = "src_sqlite")]
    SQLite(Arc<Pool<SqliteConnectionManager>>),
    #[cfg(feature = "src_oracle")]
    Oracle(Arc<Pool<OracleConnectionManager>>),
}

impl PoolVariant {
    /// Builds a pool from a parsed connection, returning `None` for backends that do not
    /// support connection pooling (MSSQL, BigQuery, Trino).
    #[allow(unused_variables)]
    pub fn from_source_conn(source_conn: &SourceConn, nconn: u32) -> Result<Option<Self>> {
        match source_conn.ty {
            #[cfg(feature = "src_mysql")]
            SourceType::MySQL => {
                let manager = MySqlConnectionManager::new(
                    OptsBuilder::from_opts(Opts::from_url(source_conn.conn.as_str())?),
                );
                let pool = Pool::builder().max_size(nconn).build(manager)?;
                Ok(Some(PoolVariant::MySQL(Arc::new(pool))))
            }
            #[cfg(feature = "src_postgres")]
            SourceType::Postgres => {
                use crate::sources::postgres::rewrite_tls_args;
                let (config, tls) = rewrite_tls_args(&source_conn.conn)?;
                match tls {
                    Some(tls_conn) => {
                        let manager = PostgresConnectionManager::new(config, tls_conn);
                        let pool = Pool::builder().max_size(nconn).build(manager)?;
                        Ok(Some(PoolVariant::PostgresTls(Arc::new(pool))))
                    }
                    None => {
                        let manager = PostgresConnectionManager::new(config, NoTls);
                        let pool = Pool::builder().max_size(nconn).build(manager)?;
                        Ok(Some(PoolVariant::PostgresNoTls(Arc::new(pool))))
                    }
                }
            }
            #[cfg(feature = "src_sqlite")]
            SourceType::SQLite => {
                use urlencoding::decode;
                // Strip the "sqlite://" prefix (9 chars) the same way get_arrow.rs does.
                let path = &source_conn.conn.as_str()[9..];
                let decoded = decode(path)?.into_owned();
                let manager = SqliteConnectionManager::file(decoded);
                let pool = Pool::builder().max_size(nconn).build(manager)?;
                Ok(Some(PoolVariant::SQLite(Arc::new(pool))))
            }
            #[cfg(feature = "src_oracle")]
            SourceType::Oracle => {
                use crate::sources::oracle::connect_oracle;
                let connector = connect_oracle(&source_conn.conn)
                    .map_err(anyhow::Error::from)?;
                let manager = OracleConnectionManager::from_connector(connector);
                let pool = Pool::builder().max_size(nconn).build(manager)?;
                Ok(Some(PoolVariant::Oracle(Arc::new(pool))))
            }
            // MSSQL, BigQuery, Trino, and any other type: no pool support
            _ => Ok(None),
        }
    }

    // ── Typed accessors (panic on variant mismatch — callers are always in
    //    the correct SourceType arm, so a mismatch is a bug) ──────────────────

    #[cfg(any(
        feature = "src_mysql",
        feature = "src_postgres",
        feature = "src_sqlite",
        feature = "src_oracle"
    ))]
    fn variant_name(&self) -> &'static str {
        match self {
            #[cfg(feature = "src_mysql")]
            PoolVariant::MySQL(_) => "MySQL",
            #[cfg(feature = "src_postgres")]
            PoolVariant::PostgresNoTls(_) => "PostgresNoTls",
            #[cfg(feature = "src_postgres")]
            PoolVariant::PostgresTls(_) => "PostgresTls",
            #[cfg(feature = "src_sqlite")]
            PoolVariant::SQLite(_) => "SQLite",
            #[cfg(feature = "src_oracle")]
            PoolVariant::Oracle(_) => "Oracle",
        }
    }

    #[cfg(feature = "src_mysql")]
    pub fn mysql_pool(&self) -> Arc<Pool<MySqlConnectionManager>> {
        match self {
            PoolVariant::MySQL(p) => Arc::clone(p),
            #[allow(unreachable_patterns)]
            _ => panic!(
                "PoolVariant::mysql_pool() called on {} variant",
                self.variant_name()
            ),
        }
    }

    #[cfg(feature = "src_postgres")]
    pub fn postgres_notls_pool(&self) -> Arc<Pool<PostgresConnectionManager<NoTls>>> {
        match self {
            PoolVariant::PostgresNoTls(p) => Arc::clone(p),
            #[allow(unreachable_patterns)]
            _ => panic!(
                "PoolVariant::postgres_notls_pool() called on {} variant",
                self.variant_name()
            ),
        }
    }

    #[cfg(feature = "src_postgres")]
    pub fn postgres_tls_pool(&self) -> Arc<Pool<PostgresConnectionManager<MakeTlsConnector>>> {
        match self {
            PoolVariant::PostgresTls(p) => Arc::clone(p),
            #[allow(unreachable_patterns)]
            _ => panic!(
                "PoolVariant::postgres_tls_pool() called on {} variant",
                self.variant_name()
            ),
        }
    }

    #[cfg(feature = "src_sqlite")]
    pub fn sqlite_pool(&self) -> Arc<Pool<SqliteConnectionManager>> {
        match self {
            PoolVariant::SQLite(p) => Arc::clone(p),
            #[allow(unreachable_patterns)]
            _ => panic!(
                "PoolVariant::sqlite_pool() called on {} variant",
                self.variant_name()
            ),
        }
    }

    #[cfg(feature = "src_oracle")]
    pub fn oracle_pool(&self) -> Arc<Pool<OracleConnectionManager>> {
        match self {
            PoolVariant::Oracle(p) => Arc::clone(p),
            #[allow(unreachable_patterns)]
            _ => panic!(
                "PoolVariant::oracle_pool() called on {} variant",
                self.variant_name()
            ),
        }
    }
}
