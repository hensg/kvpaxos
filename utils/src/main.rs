use std::{
    fs::File,
    io::{self, BufRead, BufReader},
};

use clap::Parser;

#[derive(Parser)]
struct Args {
    #[arg(short, long, value_parser, value_delimiter = ',')]
    filenames: Vec<String>,

    #[arg(short, long, default_value_t = 1)]
    count: u8,
}

#[derive(Debug)]
enum ParsingType {
    RequestsByTime,
    Makespan,
}
impl ParsingType {
    fn from_str(s: &str) -> Option<ParsingType> {
        match s {
            "time,requests" => Some(ParsingType::RequestsByTime),
            "makespan" => Some(ParsingType::Makespan),
            _ => None,
        }
    }
}

fn create_buffers(filenames: &[String]) -> Result<Vec<BufReader<File>>, io::Error> {
    let mut input_readers = Vec::new();
    for filename in filenames {
        let file = File::open(filename)?;
        // TODO: handle file open/close
        let buf = BufReader::new(file);
        input_readers.push(buf);
    }
    return Ok(input_readers);
}

fn main() -> std::io::Result<()> {
    let args = Args::parse();

    let mut input_readers = self::create_buffers(&args.filenames)?;
    let mut idx = 0;
    let mut parsing_type = None;

    loop {
        let reader = &mut input_readers[idx];
        let mut buf = String::new();
        if reader.read_line(&mut buf)? == 0 {
            // Reached the end of a file, remove it from the vector
            input_readers.remove(idx);
            if input_readers.len() == 0 {
                return Ok(());
            }
        } else {
            let line = buf.trim().to_lowercase();
            let mut header_line = false;
            if let Some(pt) = ParsingType::from_str(&line) {
                parsing_type = Some(pt);
                header_line = true;
            }
            match parsing_type {
                Some(ParsingType::RequestsByTime) => {
                    if header_line {
                        print!("time,requests");
                        for i in 1..input_readers.len() {
                            print!(",requests");
                            let _ = input_readers[i].read_line(&mut buf);
                            // advances the other buffers
                        }
                        println!("");
                        continue;
                    } else {
                        if let Some((time, reqs)) = &line.split_once(",") {
                            if idx == 0 {
                                print!("{}, {}", time, reqs);
                            } else {
                                print!(",{},", reqs);
                            }
                            if idx == input_readers.len() - 1 {
                                println!("");
                            }
                        }
                    }
                },
                Some(ParsingType::Makespan) => {},
                None => {}
            }
        }
        idx += 1;
        idx %= input_readers.len();
    }
}
