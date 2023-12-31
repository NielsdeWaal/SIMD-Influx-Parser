use influx_parser::{gen_line, parse_tape};
use std::time::SystemTime;

fn main() {
    let mut res: Vec<String> = Vec::new();

    for _ in 0..5 {
        res.push(gen_line());
    }

    let line = res.concat();
    let start = SystemTime::now();

    let line0 = String::from(",=");
    let offsets = unsafe { shuffle_lookup_avx2(&line0) };
    assert_eq!(offsets.len(), 2);

    let items = parse_tape(&line);

    let end = SystemTime::now();
    let duration = end.duration_since(start);
    println!("Took: {:?} ({} bytes)", duration, line.len());
    println!("{}B/s", line.len() as f64 / duration.unwrap().as_secs_f64());
    for item in items {
        println!("{item:?}");
    }
}

#[cfg(test)]
mod tests {
    use influx_parser::parse_int;
    use influx_parser::parse_tape;
    use influx_parser::shuffle_lookup;
    use influx_parser::shuffle_lookup_avx2;
    use influx_parser::Node;

    #[test]
    fn basic() {
        let line0 = String::from(",=");
        let offsets = unsafe { shuffle_lookup(&line0) };
        assert_eq!(offsets.len(), 3);
        assert_eq!(offsets, vec![0, 1, 2]);

        let line1 = String::from("ab,cd=ef gh=15i,jk=16i 12345678");
        let offsets = unsafe { shuffle_lookup(&line1) };
        println!("{offsets:?}");
        assert_eq!(offsets.len(), 8);
        assert_eq!(offsets, vec![2, 5, 8, 11, 15, 18, 22, 31]);

        let line2 = String::from("ab gh=15i,jk=16i 12345678");
        let offsets = unsafe { shuffle_lookup(&line2) };
        println!("{offsets:?}");
        assert_eq!(offsets.len(), 6);
        assert_eq!(offsets, vec![2, 5, 9, 12, 16, 25]);

        let line3 = String::from("test,od27r=11YaN,bHueo=zzL78,JQB4N=txYCM,uIiRV=31biD,JdqDb=PFxji e65Xk=3772672500i,7Tdmm=964201946i,VygQy=888662919i,vC0Ic=2202051695i,t3GsG=4284953162i 1695559737257");
        let res = unsafe { shuffle_lookup(&line3) };
        assert_eq!(res.len(), 22);

        let line4 = String::from("ab gh=15i,jk=16i 12345678\ncd,xe=la oiw=61i 12345678");
        let res = unsafe { shuffle_lookup(&line4) };
        assert_eq!(res.len(), 12);
    }

    #[test]
    fn parse_ints() {
        assert_eq!(parse_int("64i"), 64);
        assert_eq!(parse_int("1000000i"), 1000000);
    }

    #[test]
    fn parse_influx() {
        let line = String::from("ab,cd=ef gh=15i,jk=16i 12345678");
        let items = parse_tape(&line);
        assert_eq!(
            items,
            vec![
                Node::Measurement("ab"),
                Node::Tag {
                    key: "cd",
                    value: "ef"
                },
                Node::Field {
                    key: "gh",
                    value: 15
                },
                Node::Field {
                    key: "jk",
                    value: 16
                },
                Node::Timestamp(1234567)
            ]
        );

        let line = String::from("ab,cd=ef gh=15i,jk=16i 12345678");
        let items = parse_tape(&line);
        assert_eq!(
            items,
            vec![
                Node::Measurement("ab"),
                Node::Tag {
                    key: "cd",
                    value: "ef"
                },
                Node::Field {
                    key: "gh",
                    value: 15
                },
                Node::Field {
                    key: "jk",
                    value: 16
                },
                Node::Timestamp(1234567)
            ]
        );

        let line = String::from("ab gh=15i,jk=16i 12345678");
        let items = parse_tape(&line);
        assert_eq!(
            items,
            vec![
                Node::Measurement("ab"),
                Node::Field {
                    key: "gh",
                    value: 15
                },
                Node::Field {
                    key: "jk",
                    value: 16
                },
                Node::Timestamp(1234567)
            ]
        );
    }

    #[test]
    fn basic_avx2() {
        let line0 = String::from(",=");
        let offsets = unsafe { shuffle_lookup_avx2(&line0) };
        assert_eq!(offsets.len(), 3);
        assert_eq!(offsets, vec![0, 1, 2]);

        let line1 = String::from("ab,cd=ef gh=15i,jk=16i 12345678");
        let offsets = unsafe { shuffle_lookup_avx2(&line1) };
        println!("{offsets:?}");
        assert_eq!(offsets.len(), 8);
        assert_eq!(offsets, vec![2, 5, 8, 11, 15, 18, 22, 31]);

        let line2 = String::from("ab gh=15i,jk=16i 12345678");
        let offsets = unsafe { shuffle_lookup_avx2(&line2) };
        println!("{offsets:?}");
        assert_eq!(offsets.len(), 6);
        assert_eq!(offsets, vec![2, 5, 9, 12, 16, 25]);

        let line3 = String::from("test,od27r=11YaN,bHueo=zzL78,JQB4N=txYCM,uIiRV=31biD,JdqDb=PFxji e65Xk=3772672500i,7Tdmm=964201946i,VygQy=888662919i,vC0Ic=2202051695i,t3GsG=4284953162i 1695559737257");
        let res = unsafe { shuffle_lookup_avx2(&line3) };
        assert_eq!(res.len(), 22);

        let line4 = String::from("ab gh=15i,jk=16i 12345678\ncd,xe=la oiw=61i 12345678");
        let res = unsafe { shuffle_lookup_avx2(&line4) };
        assert_eq!(res.len(), 12);
    }
}
