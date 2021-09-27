use postgres::types::ToSql;
use postgres::{Error, Row, ToStatement};
use std::fmt;

// pub(crate) trait DB {
//     fn execute(&mut self, sql: String);
//     fn prepare<S>(&mut self, sql: String) -> &S;
// }

pub struct Postgres {
    pub conn: Option<postgres::Client>,
}

impl fmt::Debug for Postgres {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Postgres").finish()
    }
}

// impl DB for Postgres {
impl Postgres {
    pub fn execute(&mut self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u64, Error> {
        self.conn.as_mut().unwrap().execute(sql, params)
    }

    pub fn prepare(&mut self, sql: &str) -> Result<postgres::Statement, Error> {
        self.conn.as_mut().unwrap().prepare(sql)
    }

    pub fn transaction(&mut self) -> Result<postgres::Transaction<'_>, Error> {
        self.conn.as_mut().unwrap().transaction()
    }

    pub fn batch_execute(&mut self, sql: String) -> Result<(), Error> {
        self.conn.as_mut().unwrap().batch_execute(&sql)
    }

    pub fn query_opt<T>(
        &mut self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.conn.as_mut().unwrap().query_opt(query, params)
    }

    pub fn query<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.conn.as_mut().unwrap().query(query, params)
    }

    pub fn query_one<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Row, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.conn.as_mut().unwrap().query_one(query, params)
    }
}
