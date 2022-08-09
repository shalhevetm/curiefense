pub mod acl;
pub mod analyze;
pub mod body;
pub mod config;
pub mod contentfilter;
pub mod flow;
pub mod grasshopper;
pub mod incremental;
pub mod interface;
pub mod limit;
pub mod logs;
pub mod maxmind;
pub mod redis;
pub mod requestfields;
pub mod securitypolicy;
pub mod simple_executor;
pub mod tagging;
pub mod utils;

use analyze::CfRulesArg;
use body::body_too_large;
use config::{with_config, HSDB};
use contentfilter::content_filter_check;
use grasshopper::Grasshopper;
use interface::{Action, ActionType, Decision};
use interface::{AnalyzeResult, Tags};
use logs::Logs;
use securitypolicy::match_securitypolicy;
use simple_executor::{Executor, Progress, Task};
use tagging::tag_request;
use utils::{map_request, RawRequest, RequestInfo};

use crate::analyze::APhase0;
use crate::interface::stats::{SecpolStats, Stats, StatsCollect};
use crate::interface::{BlockReason, Location};

fn challenge_verified<GH: Grasshopper>(gh: &GH, reqinfo: &RequestInfo, logs: &mut Logs) -> bool {
    if let Some(rbzid) = reqinfo.cookies.get("rbzid") {
        if let Some(ua) = reqinfo.headers.get("user-agent") {
            logs.debug(|| format!("Checking rbzid cookie {} with user-agent {}", rbzid, ua));
            return match gh.parse_rbzid(&rbzid.replace('-', "="), ua) {
                Some(b) => b,
                None => {
                    logs.error("Something when wrong when calling parse_rbzid");
                    false
                }
            };
        } else {
            logs.debug("Could not find useragent!");
        }
    }
    false
}

/// # Safety
///
/// Steps a valid executor
pub unsafe fn inspect_async_step(ptr: *mut Executor<Task<(Decision, Tags, Logs)>>) -> Progress<(Decision, Tags, Logs)> {
    match ptr.as_ref() {
        None => Progress::Error("Null ptr".to_string()),
        Some(r) => r.step(),
    }
}

/// # Safety
///
/// Frees the executor, should be run with the output of executor_init, and only once
pub unsafe fn inspect_async_free(ptr: *mut Executor<(Decision, Tags, Logs)>) {
    if ptr.is_null() {
        return;
    }
    Box::from_raw(ptr);
}

pub fn inspect_generic_request_map<GH: Grasshopper>(
    configpath: &str,
    mgh: Option<&GH>,
    raw: RawRequest,
    logs: &mut Logs,
) -> AnalyzeResult {
    async_std::task::block_on(inspect_generic_request_map_async(configpath, mgh, raw, logs))
}

// generic entry point when the request map has already been parsed
pub fn inspect_generic_request_map_init<GH: Grasshopper>(
    configpath: &str,
    mgh: Option<&GH>,
    raw: RawRequest,
    logs: &mut Logs,
) -> Result<APhase0, AnalyzeResult> {
    let mut tags = Tags::default();

    // insert the all tag here, to make sure it is always present, even in the presence of early errors
    tags.insert("all", Location::Request);

    logs.debug(|| format!("Inspection starts (grasshopper active: {})", mgh.is_some()));

    #[allow(clippy::large_enum_variant)]
    enum RequestMappingResult<A> {
        NoSecurityPolicy,
        BodyTooLarge((Action, BlockReason), RequestInfo),
        Res(A),
    }

    // do all config queries in the lambda once
    // there is a lot of copying taking place, to minimize the lock time
    // this decision should be backed with benchmarks

    let ((nm, securitypolicy), (ntags, globalfilter_dec, stats), flows, reqinfo, is_human) =
        match with_config(configpath, logs, |slogs, cfg| {
            let mmapinfo =
                match_securitypolicy(&raw.get_host(), &raw.meta.path, cfg, slogs).map(|(nm, um)| (nm, um.clone()));
            match mmapinfo {
                Some((nm, secpolicy)) => {
                    // this part is where we use the configuration as much as possible, while we have a lock on it
                    let pmax_depth = secpolicy.content_filter_profile.max_body_depth;

                    // check if the body is too large
                    // if the body is too large, we store the "too large" action for later use, and set the max depth to 0
                    let (body_too_large, max_depth) = if let Some(body) = raw.mbody {
                        if body.len() > secpolicy.content_filter_profile.max_body_size {
                            (
                                Some(body_too_large(
                                    secpolicy.content_filter_profile.max_body_size,
                                    body.len(),
                                )),
                                0,
                            )
                        } else {
                            (None, pmax_depth)
                        }
                    } else {
                        (None, pmax_depth)
                    };

                    // if the max depth is equal to 0, the body will not be parsed
                    let reqinfo = map_request(
                        slogs,
                        &secpolicy.content_filter_profile.decoding,
                        &secpolicy.content_filter_profile.content_type,
                        secpolicy.content_filter_profile.referer_as_uri,
                        max_depth,
                        &raw,
                    );

                    if let Some(action) = body_too_large {
                        return RequestMappingResult::BodyTooLarge(action, reqinfo);
                    }

                    let nflows = cfg.flows.clone();

                    // without grasshopper, default to being human
                    let is_human = if let Some(gh) = mgh {
                        challenge_verified(gh, &reqinfo, slogs)
                    } else {
                        false
                    };

                    let stats = StatsCollect::new(cfg.revision.clone())
                        .secpol(SecpolStats::build(&secpolicy, cfg.globalfilters.len()));

                    let ntags = tag_request(stats, is_human, &cfg.globalfilters, &reqinfo);
                    RequestMappingResult::Res(((nm, secpolicy), ntags, nflows, reqinfo, is_human))
                }
                None => RequestMappingResult::NoSecurityPolicy,
            }
        }) {
            Some(RequestMappingResult::Res(x)) => x,
            Some(RequestMappingResult::BodyTooLarge((action, br), rinfo)) => {
                return Err(AnalyzeResult {
                    decision: Decision::action(action, vec![br]),
                    tags,
                    rinfo,
                    stats: Stats::default(),
                });
            }
            Some(RequestMappingResult::NoSecurityPolicy) => {
                logs.debug("No security policy found");
                return Err(AnalyzeResult {
                    decision: Decision::pass(Vec::new()),
                    tags,
                    rinfo: map_request(logs, &[], &[], false, 0, &raw),
                    stats: Stats::default(),
                });
            }
            None => {
                logs.debug("Something went wrong during security policy searching");
                return Err(AnalyzeResult {
                    decision: Decision::pass(Vec::new()),
                    tags,
                    rinfo: map_request(logs, &[], &[], false, 0, &raw),
                    stats: Stats::default(),
                });
            }
        };
    tags.extend(ntags);

    Ok(APhase0 {
        stats,
        itags: tags,
        secpolname: nm,
        securitypolicy,
        reqinfo,
        is_human,
        globalfilter_dec,
        flows,
    })
}

// generic entry point when the request map has already been parsed
pub async fn inspect_generic_request_map_async<GH: Grasshopper>(
    configpath: &str,
    mgh: Option<&GH>,
    raw: RawRequest<'_>,
    logs: &mut Logs,
) -> AnalyzeResult {
    match inspect_generic_request_map_init(configpath, mgh, raw, logs) {
        Err(res) => res,
        Ok(p0) => analyze::analyze(logs, mgh, p0, CfRulesArg::Global).await,
    }
}

// generic entry point when the request map has already been parsed
pub fn content_filter_check_generic_request_map(
    configpath: &str,
    raw: &RawRequest,
    content_filter_id: &str,
    logs: &mut Logs,
) -> (Decision, RequestInfo, Tags, Stats) {
    let mut tags = Tags::default();
    logs.debug("Content Filter inspection starts");
    let (revision, waf_profile) = match with_config(configpath, logs, |_slogs, cfg| {
        (
            cfg.revision.clone(),
            cfg.content_filter_profiles.get(content_filter_id).cloned(),
        )
    }) {
        Some((revision, Some(prof))) => (revision, prof),
        _ => {
            logs.error("Content Filter profile not found");
            return (
                Decision::pass(Vec::new()),
                map_request(logs, &[], &[], false, 25, raw),
                tags,
                Stats::default(),
            );
        }
    };

    let stats = StatsCollect::new(revision).content_filter_only();
    if let Some(body) = raw.mbody {
        if body.len() > waf_profile.max_body_size {
            logs.error("body too large, exiting early");
            let reqinfo = map_request(logs, &waf_profile.decoding, &[], waf_profile.referer_as_uri, 0, raw);
            let (a, br) = body_too_large(waf_profile.max_body_size, body.len());
            return (
                Decision::action(a, vec![br]),
                reqinfo,
                tags,
                stats.no_content_filter().cf_stage_build(),
            );
        }
    }

    let reqinfo = map_request(
        logs,
        &waf_profile.decoding,
        &[],
        waf_profile.referer_as_uri,
        waf_profile.max_body_depth,
        raw,
    );

    let (waf_result, stats) = match HSDB.read() {
        Ok(rd) => content_filter_check(
            logs,
            stats,
            &mut tags,
            &reqinfo,
            &waf_profile,
            rd.get(content_filter_id),
        ),
        Err(rr) => {
            logs.error(|| format!("Could not get lock on HSDB: {}", rr));
            (Ok(()), stats.no_content_filter())
        }
    };
    logs.debug("Content Filter checks done");

    (
        match waf_result {
            Ok(()) => Decision::pass(Vec::new()),
            Err(d) => d,
        },
        reqinfo,
        tags,
        stats.cf_stage_build(),
    )
}
