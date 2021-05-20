/// API error struct for invalid passphrase entered
#[derive(Debug)]
pub struct ErrorInvalidPassphrase;
impl warp::reject::Reject for ErrorInvalidPassphrase {}

/// API error struct for inability to access encapsulation data
#[derive(Debug)]
pub struct ErrorCannotAccessEncapsulationData;
impl warp::reject::Reject for ErrorCannotAccessEncapsulationData {}

/// API error struct for inability to generate encapsulation data
#[derive(Debug)]
pub struct ErrorCannotGenerateEncapsulationData;
impl warp::reject::Reject for ErrorCannotGenerateEncapsulationData {}

/// API error struct for inability to decrypt data encapsulated by client
#[derive(Debug)]
pub struct ErrorCannotDecryptEncapsulatedData;
impl warp::reject::Reject for ErrorCannotDecryptEncapsulatedData {}

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

/// API error struct for trying to access non-existent data
#[derive(Debug)]
pub struct ErrorNoDataFoundForKey;
impl warp::reject::Reject for ErrorNoDataFoundForKey {}

/// API error struct for a deserialization error
#[derive(Debug)]
pub struct ErrorCouldNotDeserializeData;
impl warp::reject::Reject for ErrorCouldNotDeserializeData {}

/// API error struct for a version-network mismatch error
#[derive(Debug)]
pub struct ErrorDataNetworkVersionMismatch;
impl warp::reject::Reject for ErrorDataNetworkVersionMismatch {}
