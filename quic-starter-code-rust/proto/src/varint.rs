use bytes::{Buf, BufMut, Bytes, TryGetError};

pub const VARINT_MAX_1: u8 = (1 << 6) - 1;
pub const VARINT_MAX_2: u16 = (1 << 14) - 1;
pub const VARINT_MAX_4: u32 = (1 << 30) - 1;
pub const VARINT_MAX_8: u64 = (1 << 62) - 1;
pub const VARINT_MAX: u64 = VARINT_MAX_8;

/// Returns how many bytes it would take to encode `v` as a variable-length
/// integer.
pub const fn varint_len(v: u64) -> usize {
    if v <= VARINT_MAX_1 as u64 {
        1
    } else if v <= VARINT_MAX_2 as u64 {
        2
    } else if v <= VARINT_MAX_4 as u64 {
        4
    } else if v <= VARINT_MAX_8 {
        8
    } else {
        panic!("malformed VarInt")
    }
}

/// Returns how long the variable-length integer is, given its first byte.
pub const fn varint_parse_len(first: u8) -> usize {
    1 << (first >> 6)
}

pub trait BufExt: Buf {
    /// Gets an unsigned variable-length integer from `self` in network byte
    /// order.
    ///
    /// The current position is advanced.
    ///
    /// Returns `Err(TryGetError)` when there are not enough
    /// remaining bytes to read the value.
    fn try_get_varint(&mut self) -> Result<u64, TryGetError> {
        if self.remaining() < 1 {
            return Err(TryGetError {
                requested: 1,
                available: self.remaining(),
            });
        }

        let first = self.chunk()[0];
        let len = varint_parse_len(first);

        Ok(match len {
            1 => (self.try_get_u8()? & VARINT_MAX_1) as u64,
            2 => (self.try_get_u16()? & VARINT_MAX_2) as u64,
            4 => (self.try_get_u32()? & VARINT_MAX_4) as u64,
            8 => self.try_get_u64()? & VARINT_MAX_8,
            _ => unreachable!(),
        })
    }

    /// Gets `len` bytes from `self`, where `len` is an unsigned variable-length
    /// integer prefix in network byte order.
    ///
    /// The current position is advanced.
    ///
    /// Returns `Err(TryGetError)` when there are not enough
    /// remaining bytes to read the value.
    fn try_get_bytes_with_varint_length(&mut self) -> Result<Bytes, TryGetError> {
        let len = self.try_get_varint()? as usize;
        if self.remaining() < len {
            return Err(TryGetError {
                requested: len,
                available: self.remaining(),
            });
        }
        Ok(self.copy_to_bytes(len))
    }
}

impl<B: Buf> BufExt for B {}

pub trait BufMutExt: BufMut {
    /// Puts an unsigned variable-length integer into `self` in network byte
    /// order.
    ///
    /// The current position is advanced.
    fn put_varint(&mut self, v: u64) {
        match varint_len(v) {
            1 => self.put_u8(v as u8),
            2 => self.put_u16((0b01 << 14) | (v as u16)),
            4 => self.put_u32((0b10 << 30) | (v as u32)),
            8 => self.put_u64((0b11 << 62) | v),
            _ => unreachable!(),
        }
    }

    /// Puts `bytes` into `self`, prefixed with an unsigned variable-length
    /// integer length in network byte order.
    ///
    /// The current position is advanced.
    fn put_bytes_with_varint_length(&mut self, bytes: &[u8]) {
        self.put_varint(bytes.len() as u64);
        self.put_slice(bytes);
    }
}

impl<B: BufMut> BufMutExt for B {}
