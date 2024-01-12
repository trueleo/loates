use http::StatusCode;

#[derive(Debug)]
pub struct Report {
    pub status: StatusCode,
}

impl Report {
    pub fn new(status: StatusCode) -> Self {
        Self { status }
    }
}
