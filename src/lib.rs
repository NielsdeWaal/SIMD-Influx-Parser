use rand::distributions::{Alphanumeric, DistString};
use std::time::{SystemTime, UNIX_EPOCH};
use rand::Rng;

pub fn parse_int(string_ref: &str) -> u64 {
    // Can take a shortcut here
    // if string_ref.len() - 1 == 8 {
    // 	todo!();
    // } else {
       	// https://rust-malaysia.github.io/code/2020/07/11/faster-integer-parsing.html
	let data = unsafe {string_ref.get_unchecked(0..string_ref.len() - 1)};
	data.bytes().fold(0, |a, c| a * 10 + (c & 0x0f) as u64)
    // }
}


#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Node<'input> {
    Measurement(&'input str),
    Tag{key: &'input str, value: &'input str},
    // Field{key: &'input str, value: &'input str},
    Field{key: &'input str, value: u64},
    // Timestamp(&'input str),
    Timestamp(u64),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Phase {
    Measurement,
    TagSet,
    FieldSet,
    Timestamp,
}

pub fn gen_line() -> String {
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
pub unsafe fn shuffle_lookup(record: &str) -> Vec<usize> {
    use std::arch::x86_64::*;
    const SIMD_LENGTH: usize = 16;
    let mut res_vec : Vec<usize> = Vec::with_capacity(1_000_000);

    //println!("{record}");
    let len = record.len();
    let lenminus16: usize = if len < SIMD_LENGTH { 0 } else { len - SIMD_LENGTH };
    //println!("String len: {len}, minus 16: {lenminus16}");
    let mut idx: usize = 0;

    let low_nibbles: [u8; 16] = [
	// /* 0 */ 0x01 | 0x10 | 0x20, // " " | "\0" | "\n"
	// /* 0 */ 0x01 | 0x20, // " " | "\n"
	/* 0 */ 0x01, // " "
	// /* 0 */ 0x01 | 0x10, // " " | "\0"
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
	/* a */ 0x20, // "\n"
	// /* a */ 0x00,
	/* b */ 0x00,
	/* c */ 0x02, // ","
	/* d */ 0x04, // "="
	/* e */ 0x00,
	/* f */ 0x00,
    ];
    let high_nibbles: [u8; 16] = [
	// /* 0 */ 0x10, // "\0"
	// /* 0 */ 0x10 | 0x20, // "\0" | "\n"
	// /* 0 */ 0x00,
	/* 0 */ 0x20, // "\n"
	/* 1 */ 0x00,
	/* 2 */ 0x01 | 0x02, // " " | ","
	/* 3 */ 0x04, // "="
	/* 4 */ 0x00,
	/* 5 */ 0x00,
	// /* 6 */ 0x08, // "i"
	/* 6 */ 0x00,
	/* 7 */ 0x00,
	/* 8 */ 0x00,
	/* 9 */ 0x00,
	/* a */ 0x00,
	/* b */ 0x00,
	/* c */ 0x00, 
	/* d */ 0x00,
	/* e */ 0x00,
	/* f */ 0x00,
    ];

    while idx < lenminus16 {
	let mut chunk: [u8; SIMD_LENGTH] = [0x00; SIMD_LENGTH];
	chunk.as_mut_ptr().copy_from(record.as_ptr().add(idx), SIMD_LENGTH);
	let input = _mm_loadu_si128(chunk.as_ptr() as *const _);

	let lower_nibbles = _mm_and_si128(input, _mm_set1_epi8(0x0F));
	let higher_nibbles = _mm_and_si128(_mm_srli_epi16(input, 4), _mm_set1_epi8(0x0F));

	let lo_translated = _mm_shuffle_epi8(
	    _mm_load_si128(low_nibbles.as_ptr() as *const _), lower_nibbles);
	let hi_translated = _mm_shuffle_epi8(
	    _mm_load_si128(high_nibbles.as_ptr() as *const _), higher_nibbles);

	let intersection = _mm_and_si128(lo_translated, hi_translated);

	let t0 = _mm_cmpeq_epi8(intersection, _mm_setzero_si128());
	let t1 = _mm_xor_si128(t0, _mm_cmpeq_epi8(t0, t0));

	let mut bits = _mm_movemask_epi8(t1);

	while bits != 0 {
	    let v = bits.trailing_zeros() as i32;
	    bits &= bits.wrapping_sub(1);
	    let offset = v as usize + idx;
	    //println!("{offset} ({v} -> '{}')", *record.as_bytes().get(offset).unwrap() as char);
	    res_vec.push(offset);
	}

	idx += SIMD_LENGTH;
    }

    if idx < len {
	let mut buf: [u8; SIMD_LENGTH] = [0x00; SIMD_LENGTH];
	buf.as_mut_ptr().copy_from(record.as_ptr().add(idx), len - idx);
	let input = _mm_loadu_si128(buf.as_ptr() as *const _);

	let lower_nibbles = _mm_and_si128(input, _mm_set1_epi8(0x0F));
	let higher_nibbles = _mm_and_si128(_mm_srli_epi16(input, 4), _mm_set1_epi8(0x0F));

	let lo_translated = _mm_shuffle_epi8(
	    _mm_load_si128(low_nibbles.as_ptr() as *const _), lower_nibbles);
	let hi_translated = _mm_shuffle_epi8(
	    _mm_load_si128(high_nibbles.as_ptr() as *const _), higher_nibbles);

	let intersection = _mm_and_si128(lo_translated, hi_translated);

	let t0 = _mm_cmpeq_epi8(intersection, _mm_setzero_si128());
	let t1 = _mm_xor_si128(t0, _mm_cmpeq_epi8(t0, t0));

	let mut bits = _mm_movemask_epi8(t1);

	while bits != 0 {
	    let v = bits.trailing_zeros() as i32;
	    bits &= bits.wrapping_sub(1);
	    let offset = v as usize + idx;
	    //println!("{offset} ({v})");
	    //println!("{offset} ({v} -> '{}')", *record.as_bytes().get(offset).unwrap() as char);
	    res_vec.push(offset);
	    match record.as_bytes().get(offset) {
		// Some(ch) => {
		//     println!("{offset} ({v} -> '{}')", *ch as char);
		// }
		Some(_) => {}
		None => {break}
	    }
	}
    }

    res_vec
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
pub unsafe fn shuffle_lookup_avx2(record: &str) -> Vec<usize> {
    use std::arch::x86_64::*;
    const SIMD_LENGTH: usize = 32;
    let mut res_vec : Vec<usize> = Vec::with_capacity(1_000_000);

    // //println!("{record}");
    let len = record.len();
    let lenminus16: usize = if len < SIMD_LENGTH { 0 } else { len - SIMD_LENGTH };
    //println!("String len: {len}, minus 16: {lenminus16}");
    let mut idx: usize = 0;

    let low_nibbles: [u8; 32] = [
	/* 0 */ 0x01, // " "
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
	/* a */ 0x20, // "\n"
	// /* a */ 0x00,
	/* b */ 0x00,
	/* c */ 0x02, // ","
	/* d */ 0x04, // "="
	/* e */ 0x00,
	/* f */ 0x00,
	/* 0 */ 0x01, // " "
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
	/* a */ 0x20, // "\n"
	// /* a */ 0x00,
	/* b */ 0x00,
	/* c */ 0x02, // ","
	/* d */ 0x04, // "="
	/* e */ 0x00,
	/* f */ 0x00,
    ];
    let high_nibbles: [u8; 32] = [
	/* 0 */ 0x20, // "\n"
	/* 1 */ 0x00,
	/* 2 */ 0x01 | 0x02, // " " | ","
	/* 3 */ 0x04, // "="
	/* 4 */ 0x00,
	/* 5 */ 0x00,
	/* 6 */ 0x00,
	/* 7 */ 0x00,
	/* 8 */ 0x00,
	/* 9 */ 0x00,
	/* a */ 0x00,
	/* b */ 0x00,
	/* c */ 0x00, 
	/* d */ 0x00,
	/* e */ 0x00,
	/* f */ 0x00,
	/* 0 */ 0x20, // "\n"
	/* 1 */ 0x00,
	/* 2 */ 0x01 | 0x02, // " " | ","
	/* 3 */ 0x04, // "="
	/* 4 */ 0x00,
	/* 5 */ 0x00,
	/* 6 */ 0x00,
	/* 7 */ 0x00,
	/* 8 */ 0x00,
	/* 9 */ 0x00,
	/* a */ 0x00,
	/* b */ 0x00,
	/* c */ 0x00, 
	/* d */ 0x00,
	/* e */ 0x00,
	/* f */ 0x00,
    ];

    let mut dst = [0 as u8; SIMD_LENGTH];
    while idx < lenminus16 {
	let mut chunk: [u8; SIMD_LENGTH] = [0x00; SIMD_LENGTH];
	chunk.as_mut_ptr().copy_from(record.as_ptr().add(idx), SIMD_LENGTH);
	let input = _mm256_loadu_si256(chunk.as_ptr() as *const _);

	let lower_nibbles = _mm256_and_si256(input, _mm256_set1_epi8(0x0F));
	let higher_nibbles = _mm256_and_si256(_mm256_srli_epi16(input, 4), _mm256_set1_epi8(0x0F));

	let lo_translated = _mm256_shuffle_epi8(
	    _mm256_load_si256(low_nibbles.as_ptr() as *const _), lower_nibbles);
	_mm256_storeu_si256(dst.as_mut_ptr() as *mut _, lo_translated);
	let hi_translated = _mm256_shuffle_epi8(
	    _mm256_load_si256(high_nibbles.as_ptr() as *const _), higher_nibbles);
	_mm256_storeu_si256(dst.as_mut_ptr() as *mut _, hi_translated);

	let intersection = _mm256_and_si256(lo_translated, hi_translated);

	let t0 = _mm256_cmpeq_epi8(intersection, _mm256_setzero_si256());
	let t1 = _mm256_xor_si256(t0, _mm256_cmpeq_epi8(t0, t0));

	let mut bits = _mm256_movemask_epi8(t1);

	while bits != 0 {
	    let v = bits.trailing_zeros() as i32;
	    bits &= bits.wrapping_sub(1);
	    let offset = v as usize + idx;
	    // println!("{offset} ({v} -> '{}')", *record.as_bytes().get(offset).unwrap() as char);
	    res_vec.push(offset);
	}

	idx += SIMD_LENGTH;
    }

    if idx < len {
	let mut buf: [u8; SIMD_LENGTH] = [0x00; SIMD_LENGTH];
	buf.as_mut_ptr().copy_from(record.as_ptr().add(idx), len - idx);
	let input = _mm256_loadu_si256(buf.as_ptr() as *const _);

	let lower_nibbles = _mm256_and_si256(input, _mm256_set1_epi8(0x0F));
	let higher_nibbles = _mm256_and_si256(_mm256_srli_epi16(input, 4), _mm256_set1_epi8(0x0F));

	let lo_translated = _mm256_shuffle_epi8(
	    _mm256_load_si256(low_nibbles.as_ptr() as *const _), lower_nibbles);
	// _mm256_storeu_si256(dst.as_mut_ptr() as *mut _, lo_translated);
	let hi_translated = _mm256_shuffle_epi8(
	    _mm256_load_si256(high_nibbles.as_ptr() as *const _), higher_nibbles);

	let intersection = _mm256_and_si256(lo_translated, hi_translated);

	let t0 = _mm256_cmpeq_epi8(intersection, _mm256_setzero_si256());
	let t1 = _mm256_xor_si256(t0, _mm256_cmpeq_epi8(t0, t0));

	let mut bits = _mm256_movemask_epi8(t1);

	while bits != 0 {
	    let v = bits.trailing_zeros() as i32;
	    bits &= bits.wrapping_sub(1);
	    let offset = v as usize + idx;
	    // println!("{offset} ({v} -> '{}')", *record.as_bytes().get(offset).unwrap() as char);
	    res_vec.push(offset);
	}
    }
    
    res_vec
}

pub fn parse_tape(line: &str) -> Vec<Node> {
	//let line = res.first_mut().unwrap();
	// let line = res.clone().into_iter().fold(String::new(), |acc, x| acc + &x);
    let x = unsafe {shuffle_lookup(&line)};
    let mut items: Vec<Node> = Vec::with_capacity(x.len());
    //println!("{x:?}");

    let mut idx: usize = 0;
    let mut phase = Phase::Measurement;

    for offset in x {
	if offset >= line.len() {
	    // TODO final case
	    if phase == Phase::Timestamp {
		let item = unsafe {line.get_unchecked(idx..line.len())};
		//println!("{item}");
		items.push(Node::Timestamp(parse_int(item)));
	    }
	    break;
	}
	let item = unsafe {line.get_unchecked(idx..offset)};
	//println!("{} {} {} ", offset, line.as_bytes()[offset], item);
	match line.as_bytes()[offset] {
	    0x20 => {//println!("SPACE");
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
				    *value = parse_int(item);
				} else {unreachable!();}
				phase = Phase::Timestamp;
			    },
			    Phase::Timestamp => unreachable!()
			}},
	    0x2C => {//println!("Comma");
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
				    *value = parse_int(item);
				} else {unreachable!();}
			    },
			    _ => unreachable!()
			}
			},
	    0x3D => {//println!("=");
			match phase {
			    Phase::Measurement => {
				items.push(Node::Measurement(item));
				phase = Phase::TagSet;
			    },
			    Phase::TagSet => {
				items.push(Node::Tag{key : item, value : ""});
			    },
			    Phase::FieldSet => {
				items.push(Node::Field{key : item, value : 0});
			    },
			    Phase::Timestamp => unreachable!()
			}
	    },
	    0x0A => {//println!{"New line"};
			match phase {
			    Phase::Timestamp => {
			    // let item = line.get_unchecked(item);
			    //println!("{item}");
			    items.push(Node::Timestamp(parse_int(item)));
			    phase = Phase::Measurement;
			    }
			    _ => todo!("Reset phase and parse new influx line") 
			}
	    },
	    0x00 => {println!("EOL")},
	    _ => unreachable!()
	}
    idx = offset + 1;

    }
    items
}

pub fn parse_tape_avx2(line: &str) -> Vec<Node> {
	//let line = res.first_mut().unwrap();
	// let line = res.clone().into_iter().fold(String::new(), |acc, x| acc + &x);
    let x = unsafe {shuffle_lookup_avx2(&line)};
    let mut items: Vec<Node> = Vec::with_capacity(x.len());
    //println!("{x:?}");

    let mut idx: usize = 0;
    let mut phase = Phase::Measurement;

    for offset in x {
	if offset >= line.len() {
	    // TODO final case
	    if phase == Phase::Timestamp {
		let item = unsafe {line.get_unchecked(idx..line.len())};
		//println!("{item}");
		items.push(Node::Timestamp(parse_int(item)));
	    }
	    break;
	}
	let item = unsafe {line.get_unchecked(idx..offset)};
	//println!("{} {} {} ", offset, line.as_bytes()[offset], item);
	match line.as_bytes()[offset] {
	    0x20 => {//println!("SPACE");
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
				    *value = parse_int(item);
				} else {unreachable!();}
				phase = Phase::Timestamp;
			    },
			    Phase::Timestamp => unreachable!()
			}},
	    0x2C => {//println!("Comma");
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
				    *value = parse_int(item);
				} else {unreachable!();}
			    },
			    _ => unreachable!()
			}
			},
	    0x3D => {//println!("=");
			match phase {
			    Phase::Measurement => {
				items.push(Node::Measurement(item));
				phase = Phase::TagSet;
			    },
			    Phase::TagSet => {
				items.push(Node::Tag{key : item, value : ""});
			    },
			    Phase::FieldSet => {
				items.push(Node::Field{key : item, value : 0});
			    },
			    Phase::Timestamp => unreachable!()
			}
	    },
	    0x0A => {//println!{"New line"};
			match phase {
			    Phase::Timestamp => {
			    // let item = line.get_unchecked(item);
			    //println!("{item}");
			    items.push(Node::Timestamp(parse_int(item)));
			    phase = Phase::Measurement;
			    }
			    _ => todo!("Reset phase and parse new influx line") 
			}
	    },
	    0x00 => {println!("EOL")},
	    _ => unreachable!()
	}
    idx = offset + 1;

    }
    items
}
