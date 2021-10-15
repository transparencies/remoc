use serde::{Deserialize, Serialize};

use remoc::{
    rtc::{async_trait, remote, CallError, Server},
    rch::mpsc,
    codec
};

#[derive(Serialize, Deserialize)]
pub enum MyError {
    Error1,
    Call(CallError),
}

impl From<CallError> for MyError {
    fn from(err: CallError) -> Self {
        Self::Call(err)
    }
}

#[remote]
pub trait MyService<Codec> {
    /// Const fn docs.
    async fn const_fn(
        &self, arg1: String, arg2: u16, arg3: mpsc::Sender<String>,
    ) -> Result<u32, MyError>;

    /// Mut fn docs.
    async fn mut_fn(&mut self, arg1: Vec<String>) -> Result<(), MyError>;
}

pub struct MyObject {
    field1: String,
}

#[async_trait]
impl<Codec> MyService<Codec> for MyObject
where
    Codec: codec::Codec,
{
    async fn const_fn(
        &self, arg1: String, arg2: u16, arg3: mpsc::Sender<String>,
    ) -> Result<u32, MyError> {
        Ok(123)
    }

    async fn mut_fn(&mut self, arg1: Vec<String>) -> Result<(), MyError> {
        //self.data = String::new();
        Err(MyError::Error1)
    }
}

pub async fn do_test() {
    let obj = MyObject { field1: String::new() };
}
