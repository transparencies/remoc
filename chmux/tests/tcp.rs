use futures::stream::StreamExt;
use std::{net::Ipv4Addr, time::Duration};
use tokio::{
    io::split,
    net::{TcpListener, TcpStream},
    time::sleep,
};
use tokio_util::codec::{length_delimited::LengthDelimitedCodec, FramedRead, FramedWrite};

use chmux::{self, codec::json::JsonCodec};

async fn tcp_server() {
    let listener = TcpListener::bind((Ipv4Addr::new(127, 0, 0, 1), 9876)).await.unwrap();

    let (socket, _) = listener.accept().await.unwrap();
    let (socket_rx, socket_tx) = split(socket);
    let framed_tx = FramedWrite::new(socket_tx, LengthDelimitedCodec::new());
    let framed_rx = FramedRead::new(socket_rx, LengthDelimitedCodec::new());
    let framed_rx = framed_rx.map(|data| data.map(|b| b.freeze()));

    let mux_cfg = chmux::Cfg::default();
    let codec = JsonCodec::new();

    let (mux, _, mut server) = chmux::Multiplexer::new(&mux_cfg, codec, framed_tx, framed_rx).await.unwrap();

    let mux_run = tokio::spawn(async move { mux.run().await.unwrap() });

    while let Some((mut tx, mut rx)) = server.accept::<String, String>().await.unwrap() {
        println!("Server accepted request.");

        tx.send(&"Hi from server".to_string()).await.unwrap();
        println!("Server sent Hi message");

        println!("Server dropping sender");
        drop(tx);
        println!("Server dropped sender");

        while let Some(msg) = rx.recv().await.unwrap() {
            println!("Server received: {}", &msg);
        }
    }

    println!("Waiting for server mux to terminate...");
    mux_run.await.unwrap();
}

async fn tcp_client() {
    let socket = TcpStream::connect((Ipv4Addr::new(127, 0, 0, 1), 9876)).await.unwrap();

    let (socket_rx, socket_tx) = split(socket);
    let framed_tx = FramedWrite::new(socket_tx, LengthDelimitedCodec::new());
    let framed_rx = FramedRead::new(socket_rx, LengthDelimitedCodec::new());
    let framed_rx = framed_rx.map(|data| data.map(|b| b.freeze()));

    let mux_cfg = chmux::Cfg::default();
    let codec = JsonCodec::new();

    let (mux, client, _) = chmux::Multiplexer::new(&mux_cfg, codec, framed_tx, framed_rx).await.unwrap();
    let mux_run = tokio::spawn(async move { mux.run().await.unwrap() });

    {
        let client = client;

        println!("Client connecting...");
        let (mut tx, mut rx) = client.connect::<String, String>().await.unwrap();
        println!("Client connected");

        tx.send(&"Hi from client".to_string()).await.unwrap();
        println!("Client sent Hi message");

        println!("Client dropping sender");
        drop(tx);
        println!("Client dropped sender");

        while let Some(msg) = rx.recv().await.unwrap() {
            println!("Client received: {}", &msg);
        }

        println!("Client closing connection...");
    }

    println!("Waiting for client mux to terminate...");
    mux_run.await.unwrap();
}

#[tokio::test]
async fn tcp_test() {
    env_logger::init();

    println!("Starting server task...");
    let server_task = tokio::spawn(tcp_server());
    sleep(Duration::from_millis(100)).await;

    println!("String client thread...");
    let client_task = tokio::spawn(tcp_client());

    println!("Waiting for server task...");
    server_task.await.unwrap();
    println!("Waiting for client thread...");
    client_task.await.unwrap();
}