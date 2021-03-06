use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use std::net::{IpAddr, SocketAddr};

pub struct CPlaneApi {
    auth_endpoint: &'static str,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DatabaseInfo {
    pub host: IpAddr, // TODO: allow host name here too
    pub port: u16,
    pub dbname: String,
    pub user: String,
    pub password: String,
}

impl DatabaseInfo {
    pub fn socket_addr(&self) -> SocketAddr {
        SocketAddr::new(self.host, self.port)
    }

    pub fn conn_string(&self) -> String {
        format!(
            "dbname={} user={} password={}",
            self.dbname, self.user, self.password
        )
    }
}

impl CPlaneApi {
    pub fn new(auth_endpoint: &'static str) -> CPlaneApi {
        CPlaneApi { auth_endpoint }
    }

    pub fn authenticate_proxy_request(
        &self,
        user: &str,
        md5_response: &[u8],
        salt: &[u8; 4],
    ) -> Result<DatabaseInfo> {
        let mut url = reqwest::Url::parse(self.auth_endpoint)?;
        url.query_pairs_mut()
            .append_pair("login", user)
            .append_pair("md5response", std::str::from_utf8(md5_response)?)
            .append_pair("salt", &hex::encode(salt));

        println!("cplane request: {}", url.as_str());

        let resp = reqwest::blocking::get(url)?;

        if resp.status().is_success() {
            let conn_info: DatabaseInfo = serde_json::from_str(resp.text()?.as_str())?;
            println!("got conn info: #{:?}", conn_info);
            Ok(conn_info)
        } else {
            bail!("Auth failed")
        }
    }
}
