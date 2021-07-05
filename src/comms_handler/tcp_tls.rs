//! Utilities necessary to establish a TCP connection with TLS overay stream.

use super::{CommsError, Result};
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
//use tokio_stream::{Stream, StreamExt};
use std::fmt;
use tokio_rustls::rustls::internal::pemfile::{certs, pkcs8_private_keys};
use tokio_rustls::rustls::{
    AllowAnyAuthenticatedClient, Certificate, ClientConfig, NoClientAuth, PrivateKey,
    RootCertStore, ServerConfig,
};
use tokio_rustls::webpki::DNSNameRef;
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tokio_stream::Stream;

const TEST_PEM_CERTIFICATE: &str = r"-----BEGIN CERTIFICATE-----
MIIFvDCCA6SgAwIBAgIUaxSy5C/KxCfcqpivSHhDM4OaF0QwDQYJKoZIhvcNAQEL
BQAwIzELMAkGA1UEBhMCVVMxFDASBgNVBAMMC3plbm90dGEueHl6MB4XDTIxMDcw
MjE2NTAxOFoXDTIxMDgwMTE2NTAxOFowIzELMAkGA1UEBhMCVVMxFDASBgNVBAMM
C3plbm90dGEueHl6MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEA7UDU
cy20aCvGK/6rRIUFMQ+zej/oRCP1JJPNZC7aJqiKVGqZSkhOPcPCrUjiJiaZZBSI
4g7lm0LhVabZ+OYM3BNFrDbNfHK9bn4NedexhyD4cJ6CEjFLJK0ol4Ye9E7Ag7Wn
Cnyy1q2HE5yCnrN7AGuhxQjAY3HRmzmN4WS18XKch1W6UwEIVqgOmsFR1bRT/WD0
0IU+XkWYLLGxb/C29hR/75cuZZrezxwjS2xDoh6NByJwbUEUtKPD0SED8HqJ403n
IkGO5gwViyjwAUnqmCiLYGFw/svgyCqCAcinqTnVl/PsYil931C7jzMRBH3GL9Ed
XsUMgLNjY1BXhC2odPMNN5ILujddUtHpU22Z8rDIy4jJpgOaVtLkPF/CXW1iO51+
I9z7v4khYl05oztjkDgpJFN08tKyuMHN0+N4793W1aFIej8zN2b2pAq14ikV+exZ
c8cFkryrhD0Alq9L5QJrtR3T56AoTbPTq6FC7GSLU5bJIGaFxHM8ujbJwYaCnksF
KviZmmFfRV6FxsrPwj6pzjfngNvo772Cl5LfSpdAK48R+SK5BAAA5Aq1muhoLLR2
nvUdxRk+m1US8buYvW23VnbfQodNaP+Y2WZhrcTatzf5MrIYZcMwh/WNrgLRPHjp
nIcsyQJVZdV329LOluwiXSSJvkvv38y9WoHc/W8CAwEAAaOB5zCB5DAJBgNVHRME
AjAAMBEGCWCGSAGG+EIBAQQEAwIF4DALBgNVHQ8EBAMCBeAwMQYDVR0lBCowKAYI
KwYBBQUHAwEGCCsGAQUFBwMCBggrBgEFBQcDAwYIKwYBBQUHAwQwLAYJYIZIAYb4
QgENBB8WHU9wZW5TU0wgR2VuZXJhdGVkIENlcnRpZmljYXRlMB0GA1UdDgQWBBQx
GNZG6GgBqZd+Aerz+UCDShYDszAfBgNVHSMEGDAWgBQxGNZG6GgBqZd+Aerz+UCD
ShYDszAWBgNVHREEDzANggt6ZW5vdHRhLnh5ejANBgkqhkiG9w0BAQsFAAOCAgEA
jWR3mwDeXXNypcAuR5Q5Sucax4M7ckgOn+X4FCnHeRE/1dM+ME48U7AP99fAPqfs
7GCHp1l7gXRL1C2XJMeCLF16dpvm9HDASOQlRCq5zjulJ16sQR3SZmqOqeVNihzg
MIr9+C+4MF6WDKme4Qkg/31J7Xubp/f3hJb3n3BZZJDmxKvPsCpFFSlWvo1m4zfh
ZaIVLWHCsRIrWUWSHpVmFxY/RD1IX6M9cBSLWj6Zgg5yVraKF5gpTS0tIr4pH/HG
tIpwC+4s76g1xP8iL6lFwe+9yI8jv5OAK4SIz6X7fsm/BTyopHr/r0DyTbIRhBzO
8oF+iWL+LMmRoGdIPaORuZGvTcI3qi/vk+VDt73m6Mbm5NypyOM7EooaBfd6Zwkv
n786pK4g2CrB55UBHDMCszNsipl6VQHuaCD4TF9ddjsOi391y22/6AM/O+am6XpB
WC2Hd2h+n/iAPzoKXUEn0XE7QSfinPnVx3C91ln6wFIbebyA51dhe35yDQyGL6Ee
BKCDkxIwm3p3f0LetxRMSct+XtlYzLCdGBCV3kZcLBbzOHjU+gcwGBb+ENFFqRZa
H0ZiVuIFAD70/dQ6N+gRsJ1I3aI2Cszo/HS1MT6DY0iAGz/K1PLrjKem7C2d5mtv
pMyQv/04bFU3b7008Bec+D57riZUZ60hXUIPwqmv3Ao=
-----END CERTIFICATE-----";

const TEST_PEM_PRIVATE_KEY: &str = r"-----BEGIN PRIVATE KEY-----
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
9DdegeXpRZqkVmsEKE65GhcmlLPnHg==
-----END PRIVATE KEY-----";

pub type TcpTlsStream = tokio_rustls::TlsStream<TcpStream>;

pub struct TcpTlsConfig {
    pem_certs: String,
    pem_rsa_private_keys: String,
    trusted_pem_certs: Vec<String>,
}

impl TcpTlsConfig {
    pub fn new_common_config() -> Self {
        Self {
            pem_certs: TEST_PEM_CERTIFICATE.to_owned(),
            pem_rsa_private_keys: TEST_PEM_PRIVATE_KEY.to_owned(),
            trusted_pem_certs: vec![TEST_PEM_CERTIFICATE.to_owned()],
        }
    }
}

pub struct TcpTlsListner {
    tcp_listener: TcpListener,
    tls_acceptor: TlsAcceptor,
    listener_address: SocketAddr,
}

impl TcpTlsListner {
    pub async fn new(config: &TcpTlsConfig, address: SocketAddr) -> Result<Self> {
        let tls_acceptor = {
            let server_config = new_server_config(config)?;
            TlsAcceptor::from(Arc::new(server_config))
        };

        let mut bind_address = "0.0.0.0:0".parse::<SocketAddr>().unwrap();
        bind_address.set_port(address.port());

        let tcp_listener = TcpListener::bind(bind_address).await?;
        let mut listener_address = address;
        listener_address.set_port(tcp_listener.local_addr()?.port());

        Ok(Self {
            tcp_listener,
            tls_acceptor,
            listener_address,
        })
    }

    pub fn listener_address(&self) -> SocketAddr {
        self.listener_address
    }

    pub fn listener_as_stream(mut self) -> impl Stream<Item = Result<TcpTlsStream>> {
        async_stream::stream! {
            loop {
                yield self.next_tcp_tls_stream().await;
            }
        }
    }

    async fn next_tcp_tls_stream(&mut self) -> Result<TcpTlsStream> {
        let (stream, _addr) = self.tcp_listener.accept().await?;
        let stream = self.tls_acceptor.accept(stream).await?;
        let _peer_addr = stream.get_ref().0.peer_addr()?;
        Ok(TcpTlsStream::Server(stream))
    }
}

#[derive(Clone)]
pub struct TcpTlsConnector {
    tls_connector: TlsConnector,
}

impl TcpTlsConnector {
    pub fn new(config: &TcpTlsConfig) -> Result<Self> {
        let tls_connector = {
            let client_config = new_client_config(config)?;
            TlsConnector::from(Arc::new(client_config))
        };

        Ok(Self { tls_connector })
    }

    pub async fn connect(&mut self, addr: SocketAddr) -> Result<TcpTlsStream> {
        let stream = TcpStream::connect(addr).await?;

        let domain = DNSNameRef::try_from_ascii_str("zenotta.xyz")
            .map_err(|_| CommsError::ConfigError("invalid dnsname"))?;
        let stream = self.tls_connector.connect(domain, stream).await?;
        let _peer_addr = stream.get_ref().0.peer_addr()?;
        Ok(TcpTlsStream::Client(stream))
    }
}

impl fmt::Debug for TcpTlsConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TcpTlsConnector()")
    }
}

fn load_certs(pem: &str) -> Result<Vec<Certificate>> {
    certs(&mut Cursor::new(pem)).map_err(|_| CommsError::ConfigError("invalid cert"))
}

fn load_keys(pem: &str) -> Result<Vec<PrivateKey>> {
    pkcs8_private_keys(&mut Cursor::new(pem)).map_err(|_| CommsError::ConfigError("invalid key"))
}

fn add_cert_to_root(root_store: &mut RootCertStore, trusted_certs: &[Certificate]) -> Result<()> {
    for cert in trusted_certs {
        root_store
            .add(cert)
            .map_err(|_| CommsError::ConfigError("invalid root cert"))?;
    }
    Ok(())
}

fn new_root_certs(trusted_pem_certs: &[String]) -> Result<RootCertStore> {
    let mut root_store = RootCertStore::empty();
    for trusted_pem_cert in trusted_pem_certs {
        let trusted_certs = load_certs(trusted_pem_cert)?;
        add_cert_to_root(&mut root_store, &trusted_certs)?;
    }
    Ok(root_store)
}

fn new_server_config(config: &TcpTlsConfig) -> Result<ServerConfig> {
    let root_store = new_root_certs(&config.trusted_pem_certs)?;
    let certs = load_certs(&config.pem_certs)?;
    let mut keys = load_keys(&config.pem_rsa_private_keys)?;
    let _client_auth = NoClientAuth::new();
    let client_auth = AllowAnyAuthenticatedClient::new(root_store);

    let mut server_config = ServerConfig::new(client_auth);
    server_config.set_single_cert(certs, keys.remove(0))?;
    Ok(server_config)
}

fn new_client_config(config: &TcpTlsConfig) -> Result<ClientConfig> {
    let root_store = new_root_certs(&config.trusted_pem_certs)?;
    let certs = load_certs(&config.pem_certs)?;
    let mut keys = load_keys(&config.pem_rsa_private_keys)?;

    let mut client_config = ClientConfig::new();
    client_config.root_store = root_store;
    client_config.set_single_client_cert(certs, keys.remove(0))?;
    Ok(client_config)
}
