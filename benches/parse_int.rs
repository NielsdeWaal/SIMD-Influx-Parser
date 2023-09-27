use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput, BenchmarkId};

use csv_parser::{gen_line, parse_int, parse_tape};

fn parse_int_bench(c: &mut Criterion) {
    let value = String::from("64i");
    c.bench_function("Parse 64i", |b| b.iter(|| parse_int(black_box(&value))));
}

fn parse_10k_lines(c: &mut Criterion) {
    let mut res: Vec<String> = Vec::new();

    for _ in 0..10000 {
	res.push(gen_line());
    }

    let line = res.concat();
    // let items = parse_tape(&line);
    c.bench_function("Parse 10k lines", |b| b.iter(|| parse_tape(black_box(&line))));
}

fn parse_influx(c: &mut Criterion) {
    let mut res: Vec<String> = Vec::new();

    let mut group = c.benchmark_group("parse_influx");
    for size  in [10, 100, 1000, 10000, 100000, 1000000].iter() {
	for _ in 0..*size {
	    res.push(gen_line());
	}
	let line = res.concat();

	group.throughput(Throughput::Bytes(line.len() as u64));
	group.bench_with_input(BenchmarkId::from_parameter(size), &line, |b, line| {
	    b.iter(|| parse_tape(black_box(&line)));
	});

	res.clear();
    }
}

criterion_group!(benches, parse_int_bench, parse_influx);
criterion_main!(benches);
