use std::fmt;

/// An HTTP status code in the range 100..=599.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StatusCode(u16);

/// The error returned for an out-of-range status code.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InvalidStatusCode;

impl StatusCode {
    pub const CONTINUE: Self = Self(100);
    pub const SWITCHING_PROTOCOLS: Self = Self(101);
    pub const OK: Self = Self(200);
    pub const CREATED: Self = Self(201);
    pub const ACCEPTED: Self = Self(202);
    pub const NO_CONTENT: Self = Self(204);
    pub const PARTIAL_CONTENT: Self = Self(206);
    pub const MOVED_PERMANENTLY: Self = Self(301);
    pub const FOUND: Self = Self(302);
    pub const NOT_MODIFIED: Self = Self(304);
    pub const TEMPORARY_REDIRECT: Self = Self(307);
    pub const PERMANENT_REDIRECT: Self = Self(308);
    pub const BAD_REQUEST: Self = Self(400);
    pub const UNAUTHORIZED: Self = Self(401);
    pub const FORBIDDEN: Self = Self(403);
    pub const NOT_FOUND: Self = Self(404);
    pub const METHOD_NOT_ALLOWED: Self = Self(405);
    pub const REQUEST_TIMEOUT: Self = Self(408);
    pub const CONFLICT: Self = Self(409);
    pub const LENGTH_REQUIRED: Self = Self(411);
    pub const PAYLOAD_TOO_LARGE: Self = Self(413);
    pub const URI_TOO_LONG: Self = Self(414);
    pub const UNSUPPORTED_MEDIA_TYPE: Self = Self(415);
    pub const RANGE_NOT_SATISFIABLE: Self = Self(416);
    pub const EXPECTATION_FAILED: Self = Self(417);
    pub const IM_A_TEAPOT: Self = Self(418);
    pub const UNPROCESSABLE_CONTENT: Self = Self(422);
    pub const TOO_MANY_REQUESTS: Self = Self(429);
    pub const REQUEST_HEADER_FIELDS_TOO_LARGE: Self = Self(431);
    pub const INTERNAL_SERVER_ERROR: Self = Self(500);
    pub const NOT_IMPLEMENTED: Self = Self(501);
    pub const BAD_GATEWAY: Self = Self(502);
    pub const SERVICE_UNAVAILABLE: Self = Self(503);
    pub const GATEWAY_TIMEOUT: Self = Self(504);
    pub const HTTP_VERSION_NOT_SUPPORTED: Self = Self(505);

    /// Creates a status code if it is in the valid range.
    pub const fn from_u16(code: u16) -> Result<Self, InvalidStatusCode> {
        if code >= 100 && code <= 599 {
            Ok(Self(code))
        } else {
            Err(InvalidStatusCode)
        }
    }

    /// Creates a status code from exactly three ASCII digits.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, InvalidStatusCode> {
        if bytes.len() != 3 || !bytes.iter().all(u8::is_ascii_digit) {
            return Err(InvalidStatusCode);
        }
        let code = u16::from(bytes[0] - b'0') * 100
            + u16::from(bytes[1] - b'0') * 10
            + u16::from(bytes[2] - b'0');
        Self::from_u16(code)
    }

    /// Returns the numeric status code.
    pub const fn as_u16(self) -> u16 {
        self.0
    }

    /// Returns the canonical reason phrase, or `""` for an unknown code.
    pub const fn canonical_reason(self) -> &'static str {
        match self.0 {
            100 => "Continue",
            101 => "Switching Protocols",
            102 => "Processing",
            103 => "Early Hints",
            200 => "OK",
            201 => "Created",
            202 => "Accepted",
            203 => "Non-Authoritative Information",
            204 => "No Content",
            205 => "Reset Content",
            206 => "Partial Content",
            207 => "Multi-Status",
            208 => "Already Reported",
            226 => "IM Used",
            300 => "Multiple Choices",
            301 => "Moved Permanently",
            302 => "Found",
            303 => "See Other",
            304 => "Not Modified",
            305 => "Use Proxy",
            307 => "Temporary Redirect",
            308 => "Permanent Redirect",
            400 => "Bad Request",
            401 => "Unauthorized",
            402 => "Payment Required",
            403 => "Forbidden",
            404 => "Not Found",
            405 => "Method Not Allowed",
            406 => "Not Acceptable",
            407 => "Proxy Authentication Required",
            408 => "Request Timeout",
            409 => "Conflict",
            410 => "Gone",
            411 => "Length Required",
            412 => "Precondition Failed",
            413 => "Payload Too Large",
            414 => "URI Too Long",
            415 => "Unsupported Media Type",
            416 => "Range Not Satisfiable",
            417 => "Expectation Failed",
            418 => "I'm a Teapot",
            421 => "Misdirected Request",
            422 => "Unprocessable Content",
            423 => "Locked",
            424 => "Failed Dependency",
            425 => "Too Early",
            426 => "Upgrade Required",
            428 => "Precondition Required",
            429 => "Too Many Requests",
            431 => "Request Header Fields Too Large",
            451 => "Unavailable For Legal Reasons",
            500 => "Internal Server Error",
            501 => "Not Implemented",
            502 => "Bad Gateway",
            503 => "Service Unavailable",
            504 => "Gateway Timeout",
            505 => "HTTP Version Not Supported",
            506 => "Variant Also Negotiates",
            507 => "Insufficient Storage",
            508 => "Loop Detected",
            510 => "Not Extended",
            511 => "Network Authentication Required",
            _ => "",
        }
    }

    pub const fn is_informational(self) -> bool {
        self.0 >= 100 && self.0 < 200
    }

    pub const fn is_success(self) -> bool {
        self.0 >= 200 && self.0 < 300
    }

    pub const fn is_redirection(self) -> bool {
        self.0 >= 300 && self.0 < 400
    }

    pub const fn is_client_error(self) -> bool {
        self.0 >= 400 && self.0 < 500
    }

    pub const fn is_server_error(self) -> bool {
        self.0 >= 500 && self.0 < 600
    }

    /// Whether responses with this status have no message body.
    pub const fn is_empty_body(self) -> bool {
        self.is_informational() || self.0 == 204 || self.0 == 304
    }
}

impl Default for StatusCode {
    fn default() -> Self {
        Self::OK
    }
}

impl fmt::Display for StatusCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Debug for StatusCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StatusCode({})", self.0)
    }
}

impl fmt::Display for InvalidStatusCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HTTP status code must be in 100..=599")
    }
}

impl std::error::Error for InvalidStatusCode {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_is_enforced() {
        assert!(StatusCode::from_u16(99).is_err());
        assert!(StatusCode::from_u16(100).is_ok());
        assert!(StatusCode::from_u16(599).is_ok());
        assert!(StatusCode::from_u16(600).is_err());
    }

    #[test]
    fn three_digit_parsing() {
        assert_eq!(StatusCode::from_bytes(b"200").unwrap(), StatusCode::OK);
        assert!(StatusCode::from_bytes(b"20").is_err());
        assert!(StatusCode::from_bytes(b"2x0").is_err());
        assert!(StatusCode::from_bytes(b"099").is_err());
    }

    #[test]
    fn classes_and_empty_bodies() {
        assert!(StatusCode::CONTINUE.is_informational());
        assert!(StatusCode::OK.is_success());
        assert!(StatusCode::FOUND.is_redirection());
        assert!(StatusCode::BAD_REQUEST.is_client_error());
        assert!(StatusCode::INTERNAL_SERVER_ERROR.is_server_error());
        assert!(StatusCode::NO_CONTENT.is_empty_body());
        assert!(StatusCode::NOT_MODIFIED.is_empty_body());
        assert!(!StatusCode::OK.is_empty_body());
    }

    #[test]
    fn reason_phrases_are_stable() {
        assert_eq!(StatusCode::OK.canonical_reason(), "OK");
        assert_eq!(StatusCode::from_u16(299).unwrap().canonical_reason(), "");
    }
}
