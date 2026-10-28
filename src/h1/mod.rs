//! Strict HTTP/1.1 sans-I/O parsing and serialization.

mod encode;
mod head;

pub(crate) use encode::{
    RequestBodyEncoding, ResponseBodyEncoding, encode_request_head, encode_response_head,
};
pub(crate) use head::{
    BodyFraming, ChunkedDecoder, ChunkedStatus, RequestHead, ResponseHead, encode_chunk,
    encode_last_chunk, parse_request_head, parse_response_head,
};
