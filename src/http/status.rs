use std::fmt;

use super::version::Version;
use crate::error::{ProtocolError, ProtocolErrorKind};

/// An HTTP status code in the range `100..=599`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StatusCode(u16);

impl StatusCode {
    pub const CONTINUE: StatusCode = StatusCode(100);
    pub const SWITCHING_PROTOCOLS: StatusCode = StatusCode(101);
    pub const OK: StatusCode = StatusCode(200);
    pub const CREATED: StatusCode = StatusCode(201);
    pub const ACCEPTED: StatusCode = StatusCode(202);
    pub const NO_CONTENT: StatusCode = StatusCode(204);
    pub const MOVED_PERMANENTLY: StatusCode = StatusCode(301);
    pub const FOUND: StatusCode = StatusCode(302);
    pub const NOT_MODIFIED: StatusCode = StatusCode(304);
    pub const BAD_REQUEST: StatusCode = StatusCode(400);
    pub const UNAUTHORIZED: StatusCode = StatusCode(401);
    pub const FORBIDDEN: StatusCode = StatusCode(403);
    pub const NOT_FOUND: StatusCode = StatusCode(404);
    pub const METHOD_NOT_ALLOWED: StatusCode = StatusCode(405);
    pub const REQUEST_TIMEOUT: StatusCode = StatusCode(408);
    pub const PAYLOAD_TOO_LARGE: StatusCode = StatusCode(413);
    pub const URI_TOO_LONG: StatusCode = StatusCode(414);
    pub const EXPECTATION_FAILED: StatusCode = StatusCode(417);
    pub const INTERNAL_SERVER_ERROR: StatusCode = StatusCode(500);
    pub const NOT_IMPLEMENTED: StatusCode = StatusCode(501);
    pub const BAD_GATEWAY: StatusCode = StatusCode(502);
    pub const SERVICE_UNAVAILABLE: StatusCode = StatusCode(503);
    pub const GATEWAY_TIMEOUT: StatusCode = StatusCode(504);

    /// Construct a status code, rejecting values outside `100..=599`.
    pub fn from_u16(code: u16) -> Result<Self, ProtocolError> {
        if (100..=599).contains(&code) {
            Ok(StatusCode(code))
        } else {
            Err(ProtocolError::new(
                Version::Http11,
                ProtocolErrorKind::MalformedMessage,
            ))
        }
    }

    pub fn as_u16(self) -> u16 {
        self.0
    }

    pub fn is_informational(self) -> bool {
        (100..=199).contains(&self.0)
    }

    pub fn is_success(self) -> bool {
        (200..=299).contains(&self.0)
    }

    pub fn is_redirection(self) -> bool {
        (300..=399).contains(&self.0)
    }

    pub fn is_client_error(self) -> bool {
        (400..=499).contains(&self.0)
    }

    pub fn is_server_error(self) -> bool {
        (500..=599).contains(&self.0)
    }

    /// Statuses that never carry a body, regardless of method.
    pub fn is_empty_body(self) -> bool {
        self.is_informational() || matches!(self.0, 204 | 304)
    }
}

impl fmt::Display for StatusCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<StatusCode> for u16 {
    fn from(code: StatusCode) -> Self {
        code.0
    }
}

impl TryFrom<u16> for StatusCode {
    type Error = ProtocolError;

    fn try_from(code: u16) -> Result<Self, Self::Error> {
        StatusCode::from_u16(code)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_rejects_out_of_range() {
        assert!(StatusCode::from_u16(0).is_err());
        assert!(StatusCode::from_u16(99).is_err());
        assert!(StatusCode::from_u16(600).is_err());
        assert!(StatusCode::from_u16(u16::MAX).is_err());
        assert_eq!(StatusCode::from_u16(100).unwrap().as_u16(), 100);
        assert_eq!(StatusCode::from_u16(599).unwrap().as_u16(), 599);
        assert_eq!(StatusCode::OK.as_u16(), 200);
    }

    #[test]
    fn status_classification() {
        assert!(StatusCode::CONTINUE.is_informational());
        assert!(StatusCode::OK.is_success());
        assert!(StatusCode::NO_CONTENT.is_success());
        assert!(StatusCode::NO_CONTENT.is_empty_body());
        assert!(StatusCode::NOT_MODIFIED.is_empty_body());
        assert!(!StatusCode::OK.is_empty_body());
        assert!(StatusCode::FOUND.is_redirection());
        assert!(StatusCode::NOT_FOUND.is_client_error());
        assert!(StatusCode::INTERNAL_SERVER_ERROR.is_server_error());
        assert_eq!(StatusCode::from_u16(302).unwrap().to_string(), "302");
    }
}
