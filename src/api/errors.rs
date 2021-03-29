/// API error struct for inability to access wallet
#[derive(Debug)]
pub struct ErrorCannotAccessWallet;
impl warp::reject::Reject for ErrorCannotAccessWallet {}

/// API error struct for inability to access user node
#[derive(Debug)]
pub struct ErrorCannotAccessUserNode;
impl warp::reject::Reject for ErrorCannotAccessUserNode {}

/// API error struct for inability to save addresses to wallet
#[derive(Debug)]
pub struct ErrorCannotSaveAddressesToWallet;
impl warp::reject::Reject for ErrorCannotSaveAddressesToWallet {}
