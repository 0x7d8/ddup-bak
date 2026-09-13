use ddup_bak::varint::{decode, decode_u32, encode};
use std::io::Cursor;

fn roundtrip(value: u64) -> u64 {
    let mut buffer = Vec::new();
    encode(&mut buffer, value).unwrap();
    decode(&mut Cursor::new(buffer)).unwrap()
}

#[test]
fn roundtrips_boundaries() {
    for value in [0, 1, 0x7F, 0x80, 0x3FFF, 0x4000, u32::MAX as u64, u64::MAX] {
        assert_eq!(roundtrip(value), value);
    }
}

#[test]
fn rejects_overlong_and_overflowing_input() {
    let overflow = [0x80u8, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02];
    assert!(decode(&mut Cursor::new(overflow)).is_err());

    let too_long = [0xFFu8; 11];
    assert!(decode(&mut Cursor::new(too_long)).is_err());
    assert!(decode_u32(&mut Cursor::new([0xFFu8, 0xFF, 0xFF, 0xFF, 0x1F])).is_err());
}

#[test]
fn truncated_input_is_an_error() {
    assert!(decode(&mut Cursor::new([0x80u8])).is_err());
}
