use std::io::{Read, Write};

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

pub fn decode_u32<R: Read>(reader: &mut R) -> std::io::Result<u32> {
    u32::try_from(decode(reader)?).map_err(|_| invalid("varint overflows u32"))
}

fn invalid(message: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message)
}
