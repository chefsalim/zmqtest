extern crate zmq;
extern crate rand;

use std::thread;
use std::time;

const ROUTER_ADDR: &'static str = "tcp://127.0.0.1:5555";

pub struct Router {
    sock: zmq::Socket,
}

impl Router {
    fn new(sock: zmq::Socket) -> Self {
        Router { sock: sock }
    }

    fn run(&mut self) {
        let mut rng = rand::thread_rng();

        let delay = time::Duration::from_millis(1000);
        thread::sleep(delay);

        println!("Router run");
        self.sock.set_router_mandatory(true).unwrap();
        self.sock.bind(ROUTER_ADDR).unwrap();

        loop {
            let mut items = [self.sock.as_poll_item(1)];
            let mut identity: Vec<_> = vec![1];

            println!("Router polling");
            zmq::poll(&mut items, 1000).unwrap();
            println!("Router exited poll");
            if (items[0].get_revents() & zmq::POLLIN) > 0 {
                println!("Router receiving identiy");
                identity = self.sock.recv_bytes(0).unwrap();
                println!("Router received identity: {:?}", identity);
                println!("Router receiving delimiter");
                let delimiter = self.sock.recv_bytes(0).unwrap(); // Envelope
                println!("Router receive delimiter: {:?}", delimiter);
                println!("Router receiving response");
                let response = self.sock.recv_bytes(0).unwrap(); // Response from worker
                println!("Router received response: {:?}", response);
            }

            if rand::random::<u8>() > 100 {
                println!("Router sending identity: {:?}", identity);
                self.sock.send(&identity, zmq::SNDMORE).unwrap();
                println!("Router sending delimiter");
                self.sock.send(b"", zmq::SNDMORE).unwrap();
                println!("Router sending data");
                self.sock.send_str("WORK", 0).unwrap();
            }
        }
    }
}

pub struct Dealer {
    sock: zmq::Socket,
}

impl Dealer {
    fn new(sock: zmq::Socket) -> Self {
        Dealer { sock: sock }
    }

    fn run(&mut self) {
        println!("Dealer run");
        let identity: Vec<_> = vec![1];
        self.sock.set_identity(&identity).unwrap();
        self.sock.connect(ROUTER_ADDR).unwrap();

        println!("Dealer sending delimiter");
        self.sock.send(b"", zmq::SNDMORE).unwrap();
        println!("Dealer sending data");
        self.sock.send_str("IMREADY", 0).unwrap();

        loop {
            let mut items = [self.sock.as_poll_item(1)];
            println!("Dealer polling");
            zmq::poll(&mut items, 1000).unwrap();
            println!("Dealer exited poll");

            if (items[0].get_revents() & zmq::POLLIN) > 0 {
                println!("Dealer receiving delimiter");
                let delimiter = self.sock.recv_bytes(0).unwrap(); // Envelope
                println!("Dealer receive delimiter: {:?}", delimiter);
                println!("Dealer receiving response");
                let response = self.sock.recv_bytes(0).unwrap(); // Response from broker
                println!("Dealer received response: {:?}", response);
            }

            // let delay = time::Duration::from_millis(1000);
            // thread::sleep(delay);

            if rand::random::<u8>() > 100 {
                println!("Dealer sending delimiter");
                self.sock.send(b"", zmq::SNDMORE).unwrap();
                println!("Dealer sending data");
                self.sock.send_str("STATUS", 0).unwrap();
            }
        }
    }
}

fn main() {
    println!("Hello, world!");

    let ctx = zmq::Context::new();
    let r_sock = ctx.socket(zmq::ROUTER).unwrap();
    let d_sock = ctx.socket(zmq::DEALER).unwrap();

    let handle_router = thread::Builder::new()
        .name("router".to_string())
        .spawn(move || {
            let mut router = Router::new(r_sock);
            router.run();
        })
        .unwrap();

    let handle_dealer = thread::Builder::new()
        .name("dealer".to_string())
        .spawn(move || {
            let mut dealer = Dealer::new(d_sock);
            dealer.run();
        })
        .unwrap();

    let _ = handle_router.join();
    let _ = handle_dealer.join();
}
