use serde_json::Value;
use std::path::Path;
use std::process::{Command, Output};

fn cli(root: &Path, args: &[&str]) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_ch"));
    cmd.current_dir(root);
    for key in [
        "CH_DATA_DIR",
        "XDG_DATA_HOME",
        "XDG_CONFIG_HOME",
        "XDG_CACHE_HOME",
        "XDG_STATE_HOME",
        "XDG_RUNTIME_DIR",
    ] {
        let path = root.join(key);
        std::fs::create_dir_all(&path).unwrap();
        cmd.env(key, path);
    }
    cmd.args(args).output().unwrap()
}

#[test]
fn scaffolds_both_languages_and_preserves_existing_files() {
    let temp = tempfile::tempdir().unwrap();
    for (id, lang, expected) in [
        ("py_demo", "python", "src/py_demo_plugin/plugin.py"),
        ("rs_demo", "rust", "src/main.rs"),
    ] {
        let args = [
            "plugin",
            "new",
            id,
            "--lang",
            lang,
            "--author",
            "Test Author",
            "--no-install",
        ];
        let output = cli(temp.path(), &args);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let dest = temp.path().join(id);
        let manifest: Value =
            serde_json::from_slice(&std::fs::read(dest.join("plugin.json")).unwrap()).unwrap();
        assert_eq!(manifest["id"], id);
        assert!(dest.join(expected).is_file());
        assert!(!cli(temp.path(), &args).status.success());
    }
    assert!(
        !cli(temp.path(), &["plugin", "new", "fake", "--no-install"])
            .status
            .success()
    );
}

#[test]
fn invalid_manifest_fails_with_a_machine_readable_report() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::write(temp.path().join("plugin.json"), "{}").unwrap();
    let output = cli(temp.path(), &["plugin", "test", ".", "--json"]);
    assert!(!output.status.success());
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(
        report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c["id"] == "manifest.schema" && c["outcome"] == "fail")
    );
}

#[cfg(unix)]
#[test]
fn reports_redact_dynamic_and_configured_secrets_in_both_formats() {
    let tmp = tempfile::tempdir().unwrap();
    let manifest = serde_json::json!({
        "id":"redact_fixture","name":"Fixture","version":"1.0.0","api_version":"1.0.0",
        "entry":["/bin/sh","plugin.sh"],"brand_color":"#112233","multi_account":false,
        "auth_methods":["none"],"settings_schema":{"type":"object","properties":{"password":{"type":"string","x-secret":true}}}
    });
    std::fs::write(tmp.path().join("plugin.json"), manifest.to_string()).unwrap();
    // Include diagnostic surfaces before a handshake succeeds, with a secret that was
    // never supplied by the CLI. stdout, stderr and schema errors must stay private.
    std::fs::write(tmp.path().join("plugin.sh"), r#"printf '%s\n' '{"jsonrpc":"2.0","method":"secrets.set","id":42,"params":{"account_id":"a","key":"x","value":"dynamic-cred"}}'
printf '%s\n' 'dynamic-cred seed-cred setting-cred auth-cred' >&2
while read -r line; do
  case "$line" in
    *'"method":"initialize"'*) printf '%s\n' '{"jsonrpc":"2.0","id":2,"error":{"code":-32603,"message":"dynamic-cred seed-cred setting-cred auth-cred","data":{"code":"INTERNAL"}}}'; exit 1 ;;
    *'"method":"ping"'*) printf '%s\n' '{"jsonrpc":"2.0","id":1,"error":{"code":-32600,"message":"before init","data":{"code":"INVALID_REQUEST"}}}' ;;
  esac
done"#).unwrap();
    for mode in ["--json", "--verbose"] {
        let output = cli(
            tmp.path(),
            &[
                "plugin",
                "test",
                ".",
                mode,
                "--secret",
                "key=seed-cred",
                "--setting",
                "password=setting-cred",
                "--auth",
                "code=auth-cred",
            ],
        );
        assert!(!output.status.success());
        let out = String::from_utf8(output.stdout).unwrap();
        assert!(out.contains("[redacted]"), "{mode}");
        for s in ["dynamic-cred", "seed-cred", "setting-cred", "auth-cred"] {
            assert!(!out.contains(s), "{mode}");
        }
    }
}
