use serde::{Deserialize, Serialize};

use crate::interface::BlockReason;
use crate::requestfields::RequestField;
use crate::{Action, ActionType, Decision};
use std::collections::HashMap;
use std::ffi::{CStr, CString};

#[repr(u8)]
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Copy)]
pub enum PrecisionLevel {
    Active,
    Passive,
    Interactive,
    MobileSdk,
    Invalid,
}

impl PrecisionLevel {
    pub fn is_human(&self) -> bool {
        *self != PrecisionLevel::Invalid
    }
}

#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum GHMode {
    Passive,
    Active,
    Interactive,
}

#[derive(Serialize, Debug)]
pub struct InputData<'t> {
    pub headers: &'t RequestField,
    pub cookies: &'t RequestField,
    pub ip: &'t str,
    pub protocol: &'t str,
}

#[derive(Deserialize, Debug)]
pub struct OutputData {
    pub precision_level: PrecisionLevel,
    pub str_response: String,
    pub headers: HashMap<String, String>,
}

pub trait Grasshopper {
    fn is_human(&self, input: InputData, mode: GHMode) -> Result<OutputData, String>;
    fn verify_challenge(&self, headers: &RequestField) -> Result<String, String>;
}

mod imported {
    use super::GHMode;
    use std::os::raw::c_char;
    extern "C" {
        pub fn is_human(c_input_data: *const c_char, mode: GHMode, success: *mut bool) -> *mut c_char;
        pub fn verify_challenge(c_headers: *const c_char, success: *mut bool) -> *mut c_char;
        pub fn free_string(s: *mut c_char);
    }
}

pub struct DummyGrasshopper {}

// use this when grasshopper can't be used
impl Grasshopper for DummyGrasshopper {
    fn is_human(&self, input: InputData, mode: GHMode) -> Result<OutputData, String> {
        Err("not implemented".into())
    }

    fn verify_challenge(&self, headers: &RequestField) -> Result<String, String> {
        Err("not implemented".into())
    }
}

#[derive(Clone)]
pub struct DynGrasshopper {}

impl Grasshopper for DynGrasshopper {
    fn is_human(&self, input: InputData, mode: GHMode) -> Result<OutputData, String> {
        unsafe {
            let encoded_input = serde_json::to_vec(&input).map_err(|rr| rr.to_string())?;
            let cinput =
                CString::new(encoded_input).map_err(|_| "null character in JSON encoded string?!?".to_string())?;
            let mut success = false;
            let r = imported::is_human(cinput.as_ptr(), mode, &mut success);
            let cstr = CStr::from_ptr(r);
            let res = if success {
                serde_json::from_slice(cstr.to_bytes()).map_err(|rr| rr.to_string())
            } else {
                let o = cstr.to_string_lossy().to_string();
                todo!()
            };
            imported::free_string(r);
            res
        }
    }

    fn verify_challenge(&self, headers: &RequestField) -> Result<String, String> {
        unsafe {
            let encoded_headers = serde_json::to_vec(headers).map_err(|rr| rr.to_string())?;
            let c_headers =
                CString::new(encoded_headers).map_err(|_| "null character in JSON encoded string?!?".to_string())?;
            let mut success = false;
            let r = imported::verify_challenge(c_headers.as_ptr(), &mut success);
            let cstr = CStr::from_ptr(r);
            let o = cstr.to_string_lossy().to_string();
            imported::free_string(r);
            if success {
                Ok(o)
            } else {
                Err(o)
            }
        }
    }
}

pub fn gh_fail_decision(reason: &str) -> Decision {
    Decision::action(
        Action {
            atype: ActionType::Block,
            block_mode: true,
            headers: None,
            status: 500,
            content: "internal_error".to_string(),
            extra_tags: None,
        },
        vec![BlockReason::phase01_unknown(reason)],
    )
}

pub fn challenge_phase01<GH: Grasshopper>(gh: &GH, ua: &str, reasons: Vec<BlockReason>) -> Decision {
    /*
    let seed = match gh.gen_new_seed(ua) {
        None => return gh_fail_decision("could not call gen_new_seed"),
        Some(s) => s,
    };
    let chall_lib = match gh.js_app() {
        None => return gh_fail_decision("could not call chall_lib"),
        Some(s) => s,
    };
    let hdrs: HashMap<String, String> = [
        ("Content-Type", "text/html; charset=utf-8"),
        ("Expires", "Thu, 01 Aug 1978 00:01:48 GMT"),
        ("Cache-Control", "no-cache, private, no-transform, no-store"),
        ("Pragma", "no-cache"),
        (
            "P3P",
            "CP=\"IDC DSP COR ADM DEVi TAIi PSA PSD IVAi IVDi CONi HIS OUR IND CNT\"",
        ),
    ]
    .iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();

    let mut content = "<html><head><meta charset=\"utf-8\"><script>".to_string();
    content += &chall_lib;
    content += ";;window.rbzns={bereshit: \"1\", seed: \"";
    content += &seed;
    content += "\", storage:\"3\"};winsocks();";
    content += "</script></head><body></body></html>";

    // here humans are accepted, as they were not denied
    // (this would have been caught by the previous guard)
    Decision::action(
        Action {
            atype: ActionType::Block,
            block_mode: true,
            headers: Some(hdrs),
            status: 247,
            content,
            extra_tags: Some(["challenge_phase01"].iter().map(|s| s.to_string()).collect()),
        },
        reasons,
    ) */
    todo!("")
}

pub fn challenge_phase02<GH: Grasshopper>(gh: &GH, uri: &str, headers: &RequestField) -> Option<Decision> {
    todo!()
    /*
    if !uri.starts_with("/7060ac19f50208cbb6b45328ef94140a612ee92387e015594234077b4d1e64f1/") {
        return None;
    }
    let ua = headers.get("user-agent")?;
    let workproof = extract_zebra(headers)?;
    let verified = gh.verify_workproof(&workproof, ua)?;
    let mut nheaders = HashMap::<String, String>::new();
    let mut cookie = "rbzid=".to_string();
    cookie += &verified.replace('=', "-");
    cookie += "; Path=/; HttpOnly";

    nheaders.insert("Set-Cookie".to_string(), cookie);

    Some(Decision::action(
        Action {
            atype: ActionType::Block,
            block_mode: true,
            headers: Some(nheaders),
            status: 248,
            content: "{}".to_string(),
            extra_tags: Some(["challenge_phase02"].iter().map(|s| s.to_string()).collect()),
        },
        vec![BlockReason::phase02()],
    ))
    */
}
