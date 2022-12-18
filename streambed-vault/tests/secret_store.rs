use std::{
    collections::HashMap,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use http::Method;
use streambed_test::server;
use streambed_vault::VaultSecretStore;

use reqwest::Url;
use std::str;
use streambed::secret_store::*;
use tokio_stream::StreamExt;

#[tokio::test]
async fn get_http_vault_secret_with_auth() {
    static HTTP_REQUESTS: AtomicUsize = AtomicUsize::new(0);

    let server = server::http(move |mut req| async move {
        let _ = HTTP_REQUESTS.fetch_add(1, Ordering::Relaxed);
        let body = match (req.method(), req.uri().to_string().as_ref()) {
            (&Method::POST, "/v1/auth/approle/login") => {
                assert!(req.headers().get("X-Vault-Token").is_none());

                let mut req_body: Vec<u8> = Vec::new();
                while let Some(item) = req.body_mut().next().await {
                    req_body.extend(&*item.unwrap());
                }

                assert_eq!(
                    str::from_utf8(&req_body),
                    Ok(
                        r#"{"role_id":"0935c840-c870-dc18-a6a3-0af0f3421df7","secret_id":"7d63c6a5-75eb-1edb-d6ee-e7c657861a5f"}"#
                    )
                );

                r#"
                {
                    "request_id": "a65a6f27-0ad7-fbcd-3987-9cb70ba179a0",
                    "lease_id": "",
                    "renewable": false,
                    "lease_duration": 0,
                    "data": null,
                    "wrap_info": null,
                    "warnings": null,
                    "auth": {
                      "client_token": "hvs.CAESICKLN-7ZJbKHLWZFcJ-XVYIUdeCyb7ZUEoFz7vqq856VGh4KHGh2cy52UnhKaVdJMmtjazV5aVNFdFdGWndJM0c",
                      "accessor": "Zd6UeKSyKIGQl3jZjGFLpj9u",
                      "policies": [
                        "default",
                        "farmo-integrator"
                      ],
                      "token_policies": [
                        "default",
                        "farmo-integrator"
                      ],
                      "metadata": {
                        "role_name": "integrator"
                      },
                      "lease_duration": 0,
                      "renewable": true,
                      "entity_id": "170c0b57-96ea-ac97-ccd7-d79a9537b642",
                      "token_type": "service",
                      "orphan": true,
                      "mfa_requirement": null,
                      "num_uses": 0
                    }
                  }
                "#
            }
            (&Method::GET, "/v1/secret/data/secrets.end-device-events.key.0000000000018879") => {
                assert_eq!(req.headers()["X-Vault-Token"], "hvs.CAESICKLN-7ZJbKHLWZFcJ-XVYIUdeCyb7ZUEoFz7vqq856VGh4KHGh2cy52UnhKaVdJMmtjazV5aVNFdFdGWndJM0c");
                r#"
                {
                    "request_id": "3b6168d5-88a4-369e-97a5-fcb217c634a1",
                    "lease_id": "",
                    "renewable": false,
                    "lease_duration": 0,
                    "data": {
                    "data": {
                        "ttl": "1m",
                        "value": "f9418ed3daaff8f808fe5f268bee6139"
                    },
                    "metadata": {
                        "created_time": "2022-05-25T05:22:24.138956Z",
                        "custom_metadata": null,
                        "deletion_time": "",
                        "destroyed": false,
                        "version": 1
                    }
                    },
                    "wrap_info": null,
                    "warnings": null,
                    "auth": null
                }
                "#
            }
            _ => panic!("Unexpected uri"),
        };
        http::Response::new(body.into())
    });

    let server_addr = server.addr();

    let cl = VaultSecretStore::new(
        Url::parse(&format!(
            "http://{}:{}",
            server_addr.ip(),
            server_addr.port()
        ))
        .unwrap(),
        None,
        false,
        Duration::from_secs(60),
        1,
        Some("ttl"),
    );

    let result = cl
        .approle_auth(
            "0935c840-c870-dc18-a6a3-0af0f3421df7",
            "7d63c6a5-75eb-1edb-d6ee-e7c657861a5f",
        )
        .await;
    assert_eq!(
        result,
        Ok(AppRoleAuthReply {
            auth: AuthToken {
                client_token: "hvs.CAESICKLN-7ZJbKHLWZFcJ-XVYIUdeCyb7ZUEoFz7vqq856VGh4KHGh2cy52UnhKaVdJMmtjazV5aVNFdFdGWndJM0c".to_string(),
                lease_duration: 0,
            }
        })
    );

    let result = cl
        .get_secret("secrets.end-device-events.key.0000000000018879")
        .await;
    assert_eq!(
        result,
        Ok(Some(GetSecretReply {
            lease_duration: 0,
            data: SecretData {
                data: HashMap::from([
                    ("ttl".to_string(), "1m".to_string()),
                    (
                        "value".to_string(),
                        "f9418ed3daaff8f808fe5f268bee6139".to_string()
                    )
                ])
            }
        }))
    );
    let result = cl
        .get_secret("secrets.end-device-events.key.0000000000018879")
        .await;
    assert!(result.is_ok());

    assert_eq!(HTTP_REQUESTS.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn userpass_auth() {
    static HTTP_REQUESTS: AtomicUsize = AtomicUsize::new(0);

    let server = server::http(move |mut req| async move {
        let _ = HTTP_REQUESTS.fetch_add(1, Ordering::Relaxed);
        let body = match (req.method(), req.uri().to_string().as_ref()) {
            (&Method::POST, "/v1/auth/userpass/login/some-user") => {
                assert!(req.headers().get("X-Vault-Token").is_none());

                let mut req_body: Vec<u8> = Vec::new();
                while let Some(item) = req.body_mut().next().await {
                    req_body.extend(&*item.unwrap());
                }

                assert_eq!(str::from_utf8(&req_body), Ok(r#"{"password":"some-pass"}"#));

                r#"
                {
                    "request_id": "a65a6f27-0ad7-fbcd-3987-9cb70ba179a0",
                    "lease_id": "",
                    "renewable": false,
                    "lease_duration": 0,
                    "data": null,
                    "wrap_info": null,
                    "warnings": null,
                    "auth": {
                      "client_token": "hvs.CAESICKLN-7ZJbKHLWZFcJ-XVYIUdeCyb7ZUEoFz7vqq856VGh4KHGh2cy52UnhKaVdJMmtjazV5aVNFdFdGWndJM0c",
                      "accessor": "Zd6UeKSyKIGQl3jZjGFLpj9u",
                      "policies": [
                        "default",
                        "farmo-integrator"
                      ],
                      "token_policies": [
                        "default",
                        "farmo-integrator"
                      ],
                      "metadata": {
                        "role_name": "integrator"
                      },
                      "lease_duration": 0,
                      "renewable": true,
                      "entity_id": "170c0b57-96ea-ac97-ccd7-d79a9537b642",
                      "token_type": "service",
                      "orphan": true,
                      "mfa_requirement": null,
                      "num_uses": 0
                    }
                  }
                "#
            }
            (&Method::POST, "/v1/auth/userpass/users/some-user") => {
                assert_eq!(req.headers()["X-Vault-Token"], "hvs.CAESICKLN-7ZJbKHLWZFcJ-XVYIUdeCyb7ZUEoFz7vqq856VGh4KHGh2cy52UnhKaVdJMmtjazV5aVNFdFdGWndJM0c");

                let mut req_body: Vec<u8> = Vec::new();
                while let Some(item) = req.body_mut().next().await {
                    req_body.extend(&*item.unwrap());
                }

                assert_eq!(
                    str::from_utf8(&req_body),
                    Ok(r#"{"username":"some-new-user","password":"some-new-pass"}"#)
                );

                r#""#
            }
            _ => panic!("Unexpected uri"),
        };
        http::Response::new(body.into())
    });

    let server_addr = server.addr();

    let cl = VaultSecretStore::new(
        Url::parse(&format!(
            "http://{}:{}",
            server_addr.ip(),
            server_addr.port()
        ))
        .unwrap(),
        None,
        false,
        Duration::from_secs(60),
        1,
        Some("ttl"),
    );

    let result = cl.userpass_auth("some-user", "some-pass").await;
    assert_eq!(
        result,
        Ok(UserPassAuthReply {
            auth: AuthToken {
                client_token: "hvs.CAESICKLN-7ZJbKHLWZFcJ-XVYIUdeCyb7ZUEoFz7vqq856VGh4KHGh2cy52UnhKaVdJMmtjazV5aVNFdFdGWndJM0c".to_string(),
                lease_duration: 0,
            }
        })
    );

    let result = cl
        .userpass_create_update_user("some-user", "some-new-user", "some-new-pass")
        .await;
    assert!(result.is_ok());

    assert_eq!(HTTP_REQUESTS.load(Ordering::Relaxed), 2);
}
