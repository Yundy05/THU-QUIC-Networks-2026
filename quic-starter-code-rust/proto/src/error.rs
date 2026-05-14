use std::{fmt, ops::Deref};

use thiserror::Error;

use crate::FrameType;

/// Transport-level errors occur when a peer violates the protocol specification
#[derive(Error, Debug, Clone, Eq, PartialEq)]
pub struct TransportError {
    /// Type of error
    pub code: TransportErrorCode,
    /// Frame type that triggered the error
    pub frame: Option<FrameType>,
    /// Human-readable explanation of the reason
    pub reason: String,
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.code.fmt(f)?;
        if let Some(frame) = self.frame {
            write!(f, " in {frame}")?;
        }
        if !self.reason.is_empty() {
            write!(f, ": {}", self.reason)?;
        }
        Ok(())
    }
}

/// Transport-level error code
#[derive(Copy, Clone, Eq, PartialEq)]
pub struct TransportErrorCode(u64);

impl TransportErrorCode {
    /// Create QUIC error code from TLS alert code
    pub fn crypto(code: u8) -> Self {
        Self(0x100 | code as u64)
    }
}

impl From<u64> for TransportErrorCode {
    fn from(code: u64) -> Self {
        Self(code)
    }
}

impl Deref for TransportErrorCode {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

macro_rules! transport_errors {
    {$($name:ident($val:expr) $desc:expr;)*} => {
        #[allow(non_snake_case, unused)]
        impl TransportError {
            $(
            pub fn $name<T>(reason: T) -> Self where T: Into<String> {
                Self {
                    code: TransportErrorCode::$name,
                    frame: None,
                    reason: reason.into(),
                }
            }
            )*
        }

        impl TransportErrorCode {
            $(#[doc = $desc] pub const $name: Self = TransportErrorCode($val);)*
        }

        impl fmt::Debug for TransportErrorCode {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self.0 {
                    $($val => f.write_str(stringify!($name)),)*
                    x if (0x100..0x200).contains(&x) => write!(f, "Code::crypto({:02x})", x as u8),
                    x => write!(f, "Code({:x})", x),
                }
            }
        }

        impl fmt::Display for TransportErrorCode {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self.0 {
                    $($val => f.write_str($desc),)*
                    // We're trying to be abstract over the crypto protocol, so human-readable descriptions here is tricky.
                    x if (0x100..0x200).contains(&x) => write!(f, "the cryptographic handshake failed: error {}", x & 0xFF),
                    _ => f.write_str("unknown error"),
                }
            }
        }
    }
}

transport_errors! {
    NO_ERROR(0x0) "the connection is being closed abruptly in the absence of any error";
    INTERNAL_ERROR(0x1) "the endpoint encountered an internal error and cannot continue with the connection";
    CONNECTION_REFUSED(0x2) "the server refused to accept a new connection";
    FLOW_CONTROL_ERROR(0x3) "received more data than permitted in advertised data limits";
    STREAM_LIMIT_ERROR(0x4) "received a frame for a stream identifier that exceeded advertised the stream limit for the corresponding stream type";
    STREAM_STATE_ERROR(0x5) "received a frame for a stream that was not in a state that permitted that frame";
    FINAL_SIZE_ERROR(0x6) "received a STREAM frame or a RESET_STREAM frame containing a different final size to the one already established";
    FRAME_ENCODING_ERROR(0x7) "received a frame that was badly formatted";
    TRANSPORT_PARAMETER_ERROR(0x8) "received transport parameters that were badly formatted, included an invalid value, was absent even though it is mandatory, was present though it is forbidden, or is otherwise in error";
    CONNECTION_ID_LIMIT_ERROR(0x9) "the number of connection IDs provided by the peer exceeds the advertised active_connection_id_limit";
    PROTOCOL_VIOLATION(0xA) "detected an error with protocol compliance that was not covered by more specific error codes";
    INVALID_TOKEN(0xB) "received an invalid Retry Token in a client Initial";
    APPLICATION_ERROR(0xC) "the application or application protocol caused the connection to be closed during the handshake";
    CRYPTO_BUFFER_EXCEEDED(0xD) "received more data in CRYPTO frames than can be buffered";
    KEY_UPDATE_ERROR(0xE) "key update error";
    AEAD_LIMIT_REACHED(0xF) "the endpoint has reached the confidentiality or integrity limit for the AEAD algorithm";
    NO_VIABLE_PATH(0x10) "no viable network path exists";
}
