use std::time::{SystemTime, UNIX_EPOCH};

use rand::distributions::{Alphanumeric, DistString};
use rand::Rng;

fn gen_line() -> String {
    let mut res = String::new();
    res.reserve(255);

    let mut rng = rand::thread_rng();
    let measurement = String::from("test,");
    res.push_str(&measurement);

    for iter in 0..5 {
	let tag = Alphanumeric.sample_string(&mut rand::thread_rng(), 5);
	let value = Alphanumeric.sample_string(&mut rand::thread_rng(), 5);
	let delim = if iter != 4 {
	    ","
	} else {
	    " "
	};
	res.push_str(&format!("{tag}={value}{delim}"));
    }

    for iter in 0..5 {
	let tag = Alphanumeric.sample_string(&mut rand::thread_rng(), 5);
	let value: u32 = rng.gen();
	let delim = if iter != 4 {
	    ","
	} else {
	    " "
	};
	res.push_str(&format!("{tag}={value}i{delim}"));
    }

    res.push_str(&format!("{}\n", SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()));

    res
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Node<'input> {
    Measurement(&'input str),
    Tag{key: &'input str, value: &'input str},
    Field{key: &'input str, value: &'input str},
    Timestamp(&'input str),
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Phase {
    Measurement,
    TagSet,
    FieldSet,
    Timestamp,
}

/// Characters: {" ", "i", "=", ",", "\n"} -> {0x20, 0x69, 0x3D, 0x2C, 0x0A}
/// lo / hi nibble
///   +--------------------------------
///   | 0 1 2 3 4 5 6 7 8 9 a b c d e f
/// --+--------------------------------
/// 0 | . .   . . . . . . . . . . . . .
/// 1 | . . . . . . . . . . . . . . . .
/// 2 | . . . . . . . . . . . . . . . .
/// 3 | . . . . . . . . . . . . . . . .
/// 4 | . . . . . . . . . . . . . . . .
/// 5 | . . . . . . . . . . . . . . . .
/// 6 | . . . . . . . . . . . . . . . .
/// 7 | . . . . . . . . . . . . . . . .
/// 8 | . . . . . . . . . . . . . . . .
/// 9 | . . . . . . i . . . . . . . . .
/// a | n . . . . . . . . . . . . . . .
/// b | . . . . . . . . . . . . . . . .
/// c | . . , . . . . . . . . . . . . .
/// d | . . . = . . . . . . . . . . . .
/// e | . . . . . . . . . . . . . . . .
/// f | . . . . . . . . . . . . . . . .
///
/// Lower nibbles:
/// 0: {" ", "\n"}
/// 9: {"i"}
/// c: {","}
/// d: {"="}
///
/// Higher nibbles:
/// 2: {" ", ","}
/// 3: {"="}
/// 6: {"i"}
/// A: {"\n"}
/// const uint8_t empty = 0x00;
/// const uint8_t " " = (1 << 0); // 0x01
/// const uint8_t , = (1 << 1); // 0x02
/// const uint8_t = = (1 << 2); // 0x04
/// const uint8_t i = (1 << 3); // 0x08
/// const uint8_t "\0" = (1 << 4); // 0x10
/// const uint8_t "\n" = (1 << 5); // 0x20

/// NOTES
/// Have separate whitespace check to determine in which of the three phases we are:
/// - Tags
/// - Data
/// - Timestamp
/// ---------------
/// Might not need the seperate check when we use a queue model
/// Process the queue from front till the end. When a whitespace is encountered
/// switch states
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.1")]
unsafe fn shuffle_lookup(record: &str) -> Vec<usize> {
    use std::arch::x86_64::*;
    const SIMD_LENGTH: usize = 16;
    let mut res_vec : Vec<usize> = Vec::new();

    println!("{record}");
    let len = record.len();
    let lenminus16: usize = if len < SIMD_LENGTH { 0 } else { len - SIMD_LENGTH };
    println!("String len: {len}, minus 16: {lenminus16}");
    let mut idx: usize = 0;

    let low_nibbles: [u8; 16] = [
	/* 0 */ 0x01 | 0x10 | 0x20, // " " | "\0" | "\n"
	/* 1 */ 0x00,
	/* 2 */ 0x00,
	/* 3 */ 0x00,
	/* 4 */ 0x00,
	/* 5 */ 0x00,
	/* 6 */ 0x00,
	/* 7 */ 0x00,
	/* 8 */ 0x00,
	// /* 9 */ 0x08, // "i"
	/* 9 */ 0x00,
	/* a */ 0x00,
	/* b */ 0x00,
	/* c */ 0x02, // ","
	/* d */ 0x04, // "="
	/* e */ 0x00,
	/* f */ 0x00,
    ];
    let high_nibbles: [u8; 16] = [
	/* 0 */ 0x10, // "\0"
	/* 1 */ 0x00,
	/* 2 */ 0x03, // " " | ","
	/* 3 */ 0x04, // "="
	/* 4 */ 0x00,
	/* 5 */ 0x00,
	// /* 6 */ 0x08, // "i"
	/* 6 */ 0x00,
	/* 7 */ 0x00,
	/* 8 */ 0x00,
	/* 9 */ 0x00,
	/* a */ 0x20, // "\n"
	/* b */ 0x00,
	/* c */ 0x00, 
	/* d */ 0x00,
	/* e */ 0x00,
	/* f */ 0x00,
    ];

    let mut dst = [0 as u8; SIMD_LENGTH];
    while idx < lenminus16 {
	//let test = b"test=2t\0\0\0\0\0\0\0\0\0";
	// let input = _mm_loadu_si128(record.as_ptr() as *const _);
	let chunk = record.get_unchecked(idx..idx + SIMD_LENGTH);
	let input = _mm_loadu_si128(chunk.as_ptr() as *const _);

	_mm_storeu_si128(dst.as_mut_ptr() as *mut _, input);
	// println!("input: {dst:#x?}");

	let lower_nibbles = _mm_and_si128(input, _mm_set1_epi8(0x0F));
	let higher_nibbles = _mm_and_si128(_mm_srli_epi16(input, 4), _mm_set1_epi8(0x0F));

	let lo_translated = _mm_shuffle_epi8(
	    _mm_load_si128(low_nibbles.as_ptr() as *const _), lower_nibbles);
	_mm_storeu_si128(dst.as_mut_ptr() as *mut _, lo_translated);
	// println!("lo_translated: {dst:#x?}");
	let hi_translated = _mm_shuffle_epi8(
	    _mm_load_si128(high_nibbles.as_ptr() as *const _), higher_nibbles);
	_mm_storeu_si128(dst.as_mut_ptr() as *mut _, hi_translated);
	// println!("hi_translated: {dst:#x?}");

	let intersection = _mm_and_si128(lo_translated, hi_translated);

	_mm_storeu_si128(dst.as_mut_ptr() as *mut _, intersection);
	// println!("intersection: {dst:#x?}");

	let t0 = _mm_cmpeq_epi8(intersection, _mm_setzero_si128());
	_mm_storeu_si128(dst.as_mut_ptr() as *mut _, t0);
	// println!("t0: {dst:#x?}");

	// let t1 = _mm_andnot_si128(t0, _mm_setzero_si128());
	let t1 = _mm_andnot_si128(t0, _mm_set1_epi16(0xFF));
	_mm_storeu_si128(dst.as_mut_ptr() as *mut _, t1);
	// println!("t1: {dst:#x?}");

	// let mask = _mm_and_si128(t1, input);
	// _mm_storeu_si128(dst.as_mut_ptr() as *mut _, mask);
	// println!("t1: {dst:#x?}");

	// let test = u64::from(static_cast_u32!())
	let mut bits = _mm_movemask_epi8(t1);
	let cnt = bits.count_ones() as usize;
	// println!("res: {:016b}", bits);
	println!("Ones count: {}", cnt);

	// let state_change = _mm_cmpeq_epi8(input, _mm_set1_epi8(0x20));
	// _mm_storeu_si128(dst.as_mut_ptr() as *mut _, state_change);
	// println!("state_change: {dst:#x?}");
	// println!("State changed: {}", _mm_testz_si128(state_change, state_change) == 0);

	while bits != 0 {
	    let v = bits.trailing_zeros() as i32;
	    bits &= bits.wrapping_sub(1);
	    let offset = v as usize + idx;
	    println!("{offset} ({v} -> '{}')", *record.as_bytes().get(offset).unwrap() as char);
	    res_vec.push(offset);
	}

	idx += SIMD_LENGTH;
    }

    if idx < len {
	// let mut dst = [0 as u8; SIMD_LENGTH];
	let mut buf: [u8; SIMD_LENGTH] = [0x00; SIMD_LENGTH];
	buf.as_mut_ptr().copy_from(record.as_ptr().add(idx), len - idx);
	println!("Remaining: {:?}", buf);
	let input = _mm_loadu_si128(buf.as_ptr() as *const _);
	// println!("input: {buf:#x?}");

	_mm_storeu_si128(dst.as_mut_ptr() as *mut _, input);
	// println!("input: {dst:#x?}");

	let lower_nibbles = _mm_and_si128(input, _mm_set1_epi8(0x0F));
	let higher_nibbles = _mm_and_si128(_mm_srli_epi16(input, 4), _mm_set1_epi8(0x0F));

	let lo_translated = _mm_shuffle_epi8(
	    _mm_load_si128(low_nibbles.as_ptr() as *const _), lower_nibbles);
	_mm_storeu_si128(dst.as_mut_ptr() as *mut _, lo_translated);
	// println!("lo_translated: {dst:#x?}");
	let hi_translated = _mm_shuffle_epi8(
	    _mm_load_si128(high_nibbles.as_ptr() as *const _), higher_nibbles);
	_mm_storeu_si128(dst.as_mut_ptr() as *mut _, hi_translated);
	// println!("hi_translated: {dst:#x?}");

	let intersection = _mm_and_si128(lo_translated, hi_translated);

	_mm_storeu_si128(dst.as_mut_ptr() as *mut _, intersection);
	// println!("intersection: {dst:#x?}");

	let t0 = _mm_cmpeq_epi8(intersection, _mm_setzero_si128());
	_mm_storeu_si128(dst.as_mut_ptr() as *mut _, t0);
	// println!("t0: {dst:#x?}");

	// let t1 = _mm_andnot_si128(t0, _mm_setzero_si128());
	let t1 = _mm_andnot_si128(t0, _mm_set1_epi16(0xFF));
	_mm_storeu_si128(dst.as_mut_ptr() as *mut _, t1);
	// println!("t1: {dst:#x?}");

	// let test = u64::from(static_cast_u32!())
	let mut bits = _mm_movemask_epi8(t1);
	let cnt = bits.count_ones() as usize;
	// println!("res: {:016b}", bits);
	println!("Ones count: {}", cnt);

	// let state_change = _mm_cmpeq_epi8(input, _mm_set1_epi8(0x20));
	// _mm_storeu_si128(dst.as_mut_ptr() as *mut _, state_change);
	// // println!("state_change: {dst:#x?}");
	// println!("State changed: {}", _mm_testz_si128(state_change, state_change) == 0);

	while bits != 0 {
	    let v = bits.trailing_zeros() as i32;
	    bits &= bits.wrapping_sub(1);
	    let offset = v as usize + idx;
	    println!("{offset} ({v})");
	    res_vec.push(offset);
	}
    }

    res_vec
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.1")]
unsafe fn _parse_test(_record: &str) {
    use std::arch::x86_64::*;

    // Ensure your input is 16 byte aligned
    //let test = b"test 2t\0\0\0\0\0\0\0\0\0";
    let test = b"test=2t\0\0\0\0\0\0\0\0\0";
    //let test = b"test 2tk4t=DtHIz";
    println!("{}", test.len());
    //let special_chars = b"!@#$%^&*()[]:;<>";
    let special_chars = b" i=,";

    // Load the input
    let a = _mm_loadu_si128(special_chars.as_ptr().add(4) as *const _);
    //let b = _mm_loadu_si128(record.as_ptr() as *const _);
    let b = _mm_loadu_si128(test.as_ptr() as *const _);

    // Use _SIDD_CMP_EQUAL_ANY to find the index of any bytes in b
    let idx = _mm_cmpistri(a.into(), b.into(), _SIDD_CMP_EQUAL_ANY);

    // if idx < 16 {
    // 	println!("Congrats! Your password contains a special character");
    // } else {
    // 	println!("Your password should contain a special character");
    // }

    //println!("{record}");
    println!("{a:?}");
    println!("{idx:b} -> val: {idx}");
}

fn main() {
    let mut res: Vec<String> = Vec::new();

    for _ in 0..1 {
	res.push(gen_line());
    }

    // if let Some(record) = res.first_mut() {
    // 	//println!("{record}");
    // 	unsafe {parse_test(record)};
    // }

    let line = res.concat();
    let start = SystemTime::now();

    unsafe {
	//let line = res.first_mut().unwrap();
	// let line = res.clone().into_iter().fold(String::new(), |acc, x| acc + &x);
	let x = shuffle_lookup(&line);
	println!("{x:?}");

	let mut items: Vec<Node> = Vec::with_capacity(x.len());
	let mut idx: usize = 0;
	let mut phase = Phase::Measurement;

	for offset in x {
	    if offset >= line.len() {
		// TODO final case
		if phase == Phase::Timestamp {
		    let item = line.get_unchecked(idx..line.len());
		    //println!("{item}");
		    items.push(Node::Timestamp(item));
		}
		break;
	    }
	    let item = line.get_unchecked(idx..offset);
	    println!("{} {} {} ", offset, line.as_bytes()[offset], item);
	    match line.as_bytes()[offset] {
		0x20 => {println!("SPACE");
			 match phase {
			     Phase::Measurement => {phase = Phase::FieldSet;},
			     Phase::TagSet => {
				 if let Node::Tag{key: _, value} = items.last_mut().unwrap()
				 {
				     *value = item;
				 } else {unreachable!();}
				 phase = Phase::FieldSet;
			     },
			     Phase::FieldSet => {
				 if let Node::Field{key: _, value} = items.last_mut().unwrap()
				 {
				     *value = item;
				 } else {unreachable!();}
				 phase = Phase::Timestamp;
			     },
			     Phase::Timestamp => unreachable!()
			 }},
		0x2C => {println!("Comma");
			 match phase {
			     Phase::Measurement => {
				 items.push(Node::Measurement(item));
				 phase = Phase::TagSet;
			     },
			     Phase::TagSet => {
				 if let Node::Tag{key: _, value} = items.last_mut().unwrap()
				 {
				     *value = item;
				 } else {unreachable!();}
			     },
			     Phase::FieldSet => {
				 // items.push(Node::Field{key : item, value : ""});
				 if let Node::Field{key: _, value} = items.last_mut().unwrap()
				 {
				     *value = item;
				 } else {unreachable!();}
			     },
			     _ => unreachable!()
			 }
			 },
		0x3D => {println!("=");
			 match phase {
			     Phase::Measurement => {
				 items.push(Node::Measurement(item));
				 phase = Phase::TagSet;
			     },
			     Phase::TagSet => {
				 items.push(Node::Tag{key : item, value : ""});
			     },
			     Phase::FieldSet => {
				 items.push(Node::Field{key : item, value : ""});
			     },
			     Phase::Timestamp => unreachable!()
			 }
		},
		0x0A => {println!{"New line"}},
		0x00 => {println!("EOL")},
		_ => unreachable!()
	    }
	    idx = offset + 1;
	}
	for item in items {
	    println!("{item:?}");
	}
    };

    let end = SystemTime::now();
    let duration = end.duration_since(start);
    // println!("Took: {:?}", duration);
    println!("{}B/s", res.first().unwrap().len() as f64 / duration.unwrap().as_secs_f64());

}

#[cfg(test)]
mod tests {
    use crate::shuffle_lookup;

    #[test]
    fn basic() {
	let line0 = String::from("ab,cd=ef gh=15i 12345678");
	unsafe {
	    let offsets = shuffle_lookup(&line0);
	    println!("{offsets:?}");
	    assert_eq!(offsets.len(), 6);
	    assert_eq!(offsets, vec![2,5,8,11,14,15]);
	}
	let line1 = String::from("test,od27r=11YaN,bHueo=zzL78,JQB4N=txYCM,uIiRV=31biD,JdqDb=PFxji e65Xk=3772672500i,7Tdmm=964201946i,VygQy=888662919i,vC0Ic=2202051695i,t3GsG=4284953162i 1695559737257");
	unsafe {
	    let res = shuffle_lookup(&line1);
	    assert_eq!(res.len(), 26);
	}
    }
}
