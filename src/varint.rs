use std::io::{Read, Write};

#[inline]
pub fn encode<W: Write>(writer: &mut W, mut value: u64) -> std::io::Result<()> {
    let mut buffer = [0u8; 10];
    let mut len = 0;

    while value > 0x7F {
        buffer[len] = (value as u8 & 0x7F) | 0x80;
        value >>= 7;
        len += 1;
    }
    buffer[len] = value as u8;

    writer.write_all(&buffer[..=len])
}

#[inline]
pub fn decode<R: Read>(reader: &mut R) -> std::io::Result<u64> {
    let mut result = 0u64;
    let mut byte = [0u8; 1];

    for shift in (0..64).step_by(7) {
        reader.read_exact(&mut byte)?;
        let bits = (byte[0] & 0x7F) as u64;

        if shift == 63 && bits > 1 {
            return Err(invalid("varint overflows u64"));
        }

        result |= bits << shift;
        if byte[0] & 0x80 == 0 {
            return Ok(result);
        }
    }

    Err(invalid("varint is too long"))
}

#[inline]
pub fn decode_u32<R: Read>(reader: &mut R) -> std::io::Result<u32> {
    u32::try_from(decode(reader)?).map_err(|_| invalid("varint overflows u32"))
}

fn invalid(message: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
