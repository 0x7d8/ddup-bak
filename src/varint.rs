use std::io::Read;

#[inline]
pub fn encode_u32(value: u32) -> Vec<u8> {
    let mut result = Vec::with_capacity(5);
    let mut value = value;

    while value > 0x7F {
        result.push((value & 0x7F) as u8 | 0x80);
        value >>= 7;
    }
    result.push(value as u8);

    result
}

#[inline]
pub fn decode_u32<S: Read>(stream: &mut S) -> std::io::Result<u32> {
    let mut result = 0;
    let mut shift: u8 = 0;

    let mut byte = [0; 1];
    loop {
        stream.read_exact(&mut byte)?;

        result |= ((byte[0] & 0x7F) as u32) << shift;
        if byte[0] & 0x80 == 0 {
            break;
        }

        shift += 7;
    }

    Ok(result)
}

#[inline]
pub fn encode_u64(value: u64) -> Vec<u8> {
    let mut result = Vec::with_capacity(10);
    let mut value = value;

    while value > 0x7F {
        result.push((value & 0x7F) as u8 | 0x80);
        value >>= 7;
    }
    result.push(value as u8);

    result
}

/// Decodes a varint, returning `None` if the stream was already at its end.
///
/// File entry bodies are a bare sequence of varints with no terminator, so
/// end-of-stream is what ends the loop. An EOF part way *through* a varint is
/// still an error: that means the data is truncated.
#[inline]
pub fn decode_u64_opt<S: Read>(stream: &mut S) -> std::io::Result<Option<u64>> {
    let mut result = 0;
    let mut shift: u8 = 0;

    let mut byte = [0; 1];
    loop {
        if shift >= 64 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "varint is too long to be a u64",
            ));
        }

        match stream.read_exact(&mut byte) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof && shift == 0 => {
                return Ok(None);
            }
            Err(err) => return Err(err),
        }

        result |= ((byte[0] & 0x7F) as u64) << shift;
        if byte[0] & 0x80 == 0 {
            break;
        }

        shift += 7;
    }

    Ok(Some(result))
}

#[inline]
pub fn decode_u64<S: Read>(stream: &mut S) -> std::io::Result<u64> {
    decode_u64_opt(stream)?.ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "failed to fill whole buffer",
        )
    })
}
