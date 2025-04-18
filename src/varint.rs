use std::io::Read;

#[inline]
pub fn encode_u32(value: u32) -> Vec<u8> {
    let mut result = Vec::new();
    let mut value = value;

    while value > 0x7F {
        result.push((value & 0x7F) as u8 | 0x80);
        value >>= 7;
    }
    result.push(value as u8);

    result
}
#[inline]
pub fn decode_u32<S: Read>(stream: &mut S) -> u32 {
    let mut result = 0;
    let mut shift: u8 = 0;

    let mut byte = [0; 1];
    loop {
        if stream.read_exact(&mut byte).is_err() {
            break;
        }

        result |= ((byte[0] & 0x7F) as u32) << shift;
        if byte[0] & 0x80 == 0 {
            break;
        }

        shift += 7;
    }

    result
}

#[inline]
pub fn encode_u64(value: u64) -> Vec<u8> {
    let mut result = Vec::new();
    let mut value = value;

    while value > 0x7F {
        result.push((value & 0x7F) as u8 | 0x80);
        value >>= 7;
    }
    result.push(value as u8);

    result
}
#[inline]
pub fn decode_u64<S: Read>(stream: &mut S) -> u64 {
    let mut result = 0;
    let mut shift: u8 = 0;

    let mut byte = [0; 1];
    loop {
        if stream.read_exact(&mut byte).is_err() {
            break;
        }

        result |= ((byte[0] & 0x7F) as u64) << shift;
        if byte[0] & 0x80 == 0 {
            break;
        }

        shift += 7;
    }

    result
}
