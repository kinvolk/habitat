// Copyright (c) 2017-2017 Chef Software Inc. and/or applicable contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

extern crate ansi_term;
#[macro_use]
extern crate clap;
extern crate env_logger;
extern crate habitat_common as common;
#[macro_use]
extern crate habitat_core as hcore;
extern crate habitat_launcher_client as launcher_client;
#[macro_use]
extern crate habitat_sup as sup;
extern crate habitat_sup_protocol as protocol;
extern crate libc;
#[macro_use]
extern crate log;
extern crate protobuf;
extern crate time;
extern crate tokio_core;
extern crate url;

use std::env;
use std::io::{self, Write};
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::Path;
use std::process;
use std::result;
use std::str::{self, FromStr};

use clap::{App, ArgMatches};
use common::command::package::install::InstallSource;
use common::ui::{Coloring, NONINTERACTIVE_ENVVAR, UI};
use hcore::channel;
#[cfg(windows)]
use hcore::crypto::dpapi::encrypt;
use hcore::crypto::{self, default_cache_key_path, SymKey};
use hcore::env as henv;
use hcore::url::{bldr_url_from_env, default_bldr_url};
use launcher_client::{LauncherCli, ERR_NO_RETRY_EXCODE, OK_NO_RETRY_EXCODE};
use protocol::{ctl::ServiceBindList,
               types::{ApplicationEnvironment, BindingMode, ServiceBind, ServiceGroup, Topology,
                       UpdateStrategy}};
use url::Url;

use sup::command;
use sup::config::{GossipListenAddr, GOSSIP_DEFAULT_PORT};
use sup::error::{Error, Result, SupError};
use sup::feat;
use sup::http_gateway;
use sup::manager::{Manager, ManagerConfig};
use sup::util;
use sup::VERSION;

/// Our output key
static LOGKEY: &'static str = "MN";

static RING_ENVVAR: &'static str = "HAB_RING";
static RING_KEY_ENVVAR: &'static str = "HAB_RING_KEY";

fn main() {
    if let Err(err) = start() {
        println!("{}", err);
        match err {
            SupError {
                err: Error::ProcessLocked(_),
                ..
            }
            | SupError {
                err: Error::Departed,
                ..
            }
            | SupError {
                err: Error::ButterflyError(_),
                ..
            } => process::exit(ERR_NO_RETRY_EXCODE),
            _ => process::exit(1),
        }
    }
}

fn boot() -> Option<LauncherCli> {
    env_logger::init();
    enable_features_from_env();
    if !crypto::init() {
        println!("Crypto initialization failed!");
        process::exit(1);
    }
    match launcher_client::env_pipe() {
        Some(pipe) => match LauncherCli::connect(pipe) {
            Ok(launcher) => Some(launcher),
            Err(err) => {
                println!("{}", err);
                process::exit(1);
            }
        },
        None => None,
    }
}

fn start() -> Result<()> {
    let launcher = boot();
    let app_matches = match cli().get_matches_safe() {
        Ok(matches) => matches,
        Err(err) => {
            let out = io::stdout();
            writeln!(&mut out.lock(), "{}", err.message).expect("Error writing Error to stdout");
            match launcher {
                Some(_) => process::exit(ERR_NO_RETRY_EXCODE),
                // If we weren't started by a launcher, exit 0 for
                // help and version
                None => match err.kind {
                    clap::ErrorKind::HelpDisplayed => process::exit(0),
                    clap::ErrorKind::VersionDisplayed => process::exit(0),
                    _ => process::exit(ERR_NO_RETRY_EXCODE),
                },
            }
        }
    };
    match app_matches.subcommand() {
        ("bash", Some(_)) => sub_bash(),
        ("run", Some(m)) => {
            let launcher = launcher.ok_or(sup_error!(Error::NoLauncher))?;
            sub_run(m, launcher)
        }
        ("sh", Some(_)) => sub_sh(),
        ("term", Some(m)) => sub_term(m),
        _ => unreachable!(),
    }
}

fn cli<'a, 'b>() -> App<'a, 'b> {
    clap_app!(("hab-sup") =>
        (about: "The Habitat Supervisor")
        (version: VERSION)
        (author: "\nAuthors: The Habitat Maintainers <humans@habitat.sh>\n")
        (@setting VersionlessSubcommands)
        (@setting SubcommandRequiredElseHelp)
        (@subcommand bash =>
            (about: "Start an interactive Bash-like shell")
            (aliases: &["b", "ba", "bas"])
        )
        (@subcommand run =>
            (about: "Run the Habitat Supervisor")
            (aliases: &["r", "ru"])
            (@arg LISTEN_GOSSIP: --("listen-gossip") +takes_value {valid_socket_addr}
                "The listen address for the gossip system [default: 0.0.0.0:9638]")
            (@arg LISTEN_HTTP: --("listen-http") +takes_value {valid_socket_addr}
                "The listen address for the HTTP Gateway [default: 0.0.0.0:9631]")
            (@arg LISTEN_CTL: --("listen-ctl") +takes_value {valid_socket_addr}
                "The listen address for the Control Gateway [default: 127.0.0.1:9632]")
            (@arg NAME: --("override-name") +takes_value
                "The name of the Supervisor if launching more than one [default: default]")
            (@arg ORGANIZATION: --org +takes_value
                "The organization that the Supervisor and its subsequent services are part of \
                [default: default]")
            (@arg PEER: --peer +takes_value +multiple
                "The listen address of one or more initial peers (IP[:PORT])")
            (@arg PERMANENT_PEER: --("permanent-peer") -I "If this Supervisor is a permanent peer")
            (@arg PEER_WATCH_FILE: --("peer-watch-file") +takes_value conflicts_with[peer]
                "Watch this file for connecting to the ring"
            )
            (@arg AA_WATCH_FILE: --("aa-watch-file") +takes_value
                "Watch this file for exposing the supervisor and services"
            )
            (@arg RING: --ring -r +takes_value "Ring key name")
            (@arg CHANNEL: --channel +takes_value
                "Receive Supervisor updates from the specified release channel [default: stable]")
            (@arg BLDR_URL: -u --url +takes_value {valid_url}
                "Specify an alternate Builder endpoint. If not specified, the value will \
                 be taken from the HAB_BLDR_URL environment variable if defined. (default: \
                 https://bldr.habitat.sh)")

            (@arg CONFIG_DIR: --("config-from") +takes_value {dir_exists}
                "Use package config from this path, rather than the package itself")
            (@arg AUTO_UPDATE: --("auto-update") -A "Enable automatic updates for the Supervisor \
                itself")
            (@arg EVENTS: --events -n +takes_value {valid_service_group} "Name of the service \
                group running a Habitat EventSrv to forward Supervisor and service event data to")
            // === Optional arguments to additionally load an initial service for the Supervisor
            (@arg PKG_IDENT_OR_ARTIFACT: +takes_value "Load the given Habitat package as part of \
                the Supervisor startup specified by a package identifier \
                (ex: core/redis) or filepath to a Habitat Artifact \
                (ex: /home/core-redis-3.0.7-21120102031201-x86_64-linux.hart).")
            (@arg APPLICATION: --application -a +takes_value requires[ENVIRONMENT]
                "Application name; [default: not set].")
            (@arg ENVIRONMENT: --environment -e +takes_value requires[APPLICATION]
                "Environment name; [default: not set].")
            (@arg GROUP: --group +takes_value
                "The service group; shared config and topology [default: default].")
            (@arg TOPOLOGY: --topology -t +takes_value {valid_topology}
                "Service topology; [default: none]")
            (@arg STRATEGY: --strategy -s +takes_value {valid_update_strategy}
                "The update strategy; [default: none] [values: none, at-once, rolling]")
            (@arg BIND: --bind +takes_value +multiple
                "One or more service groups to bind to a configuration")
            (@arg BINDING_MODE: --("binding-mode") +takes_value {valid_binding_mode}
                "Governs how the presence or absence of binds affects service startup. `strict` blocks \
                 startup until all binds are present. [default: strict] [values: relaxed, strict]")
            (@arg VERBOSE: -v "Verbose output; shows file and line/column numbers")
            (@arg NO_COLOR: --("no-color") "Turn ANSI color off")
            (@arg JSON: --("json-logging") "Use structured JSON logging for the Supervisor. \
                Implies NO_COLOR")
        )
        (@subcommand sh =>
            (about: "Start an interactive Bourne-like shell")
            (aliases: &[])
        )
        (@subcommand term =>
            (about: "Gracefully terminate the Habitat Supervisor and all of its running services")
            (@arg NAME: --("override-name") +takes_value
                "The name of the Supervisor if more than one is running [default: default]")
        )
    )
}

fn sub_bash() -> Result<()> {
    command::shell::bash()
}

fn sub_run(m: &ArgMatches, launcher: LauncherCli) -> Result<()> {
    set_supervisor_logging_options(m);

    let cfg = mgrcfg_from_matches(m)?;
    if Manager::is_running(&cfg)? {
        process::exit(OK_NO_RETRY_EXCODE);
    } else {
        let manager = Manager::load(cfg, launcher)?;
        // We need to determine if we have an initial service to start
        let svc = if let Some(pkg) = m.value_of("PKG_IDENT_OR_ARTIFACT") {
            let mut msg = protocol::ctl::SvcLoad::default();
            update_svc_load_from_input(m, &mut msg)?;
            // Always force - running with a package ident is a "do what I mean" operation. You
            // don't care if a service was loaded previously or not and with what options. You
            // want one loaded right now and in this way.
            msg.force = Some(true);
            let ident = match pkg.parse::<InstallSource>()? {
                source @ InstallSource::Archive(_) => {
                    // Install the archive manually then explicitly set the pkg ident to the
                    // version found in the archive. This will lock the software to this
                    // specific version.
                    let install = util::pkg::install(
                        &mut ui(),
                        msg.bldr_url
                            .as_ref()
                            .unwrap_or(&*protocol::DEFAULT_BLDR_URL),
                        &source,
                        msg.bldr_channel
                            .as_ref()
                            .unwrap_or(&*protocol::DEFAULT_BLDR_CHANNEL),
                    )?;
                    install.ident.into()
                }
                InstallSource::Ident(ident) => ident.into(),
            };
            msg.ident = Some(ident);
            Some(msg)
        } else {
            None
        };
        manager.run(svc)
    }
}

fn sub_sh() -> Result<()> {
    command::shell::sh()
}

fn sub_term(m: &ArgMatches) -> Result<()> {
    let cfg = mgrcfg_from_matches(m)?;
    match Manager::term(&cfg) {
        Err(SupError {
            err: Error::ProcessLockIO(_, _),
            ..
        }) => {
            println!("Supervisor not started.");
            Ok(())
        }
        result => result,
    }
}

// Internal Implementation Details
////////////////////////////////////////////////////////////////////////

fn mgrcfg_from_matches(m: &ArgMatches) -> Result<ManagerConfig> {
    let mut cfg = ManagerConfig::default();
    cfg.auto_update = m.is_present("AUTO_UPDATE");
    cfg.update_url = bldr_url(m);
    cfg.update_channel = channel(m);
    if let Some(addr_str) = m.value_of("LISTEN_GOSSIP") {
        cfg.gossip_listen = GossipListenAddr::from_str(addr_str)?;
    }
    if let Some(addr_str) = m.value_of("LISTEN_HTTP") {
        cfg.http_listen = http_gateway::ListenAddr::from_str(addr_str)?;
    }
    if let Some(addr_str) = m.value_of("LISTEN_CTL") {
        cfg.ctl_listen =
            SocketAddr::from_str(addr_str).unwrap_or_else(|_err| protocol::ctl::default_addr());
    }
    if let Some(name_str) = m.value_of("NAME") {
        cfg.name = Some(String::from(name_str));
        outputln!("");
        outputln!("CAUTION: Running more than one Habitat Supervisor is not recommended for most");
        outputln!("CAUTION: users in most use cases. Using one Supervisor per host for multiple");
        outputln!("CAUTION: services in one ring will yield much better performance.");
        outputln!("");
        outputln!("CAUTION: If you know what you're doing, carry on!");
        outputln!("");
    }
    cfg.organization = m.value_of("ORGANIZATION").map(|org| org.to_string());
    cfg.gossip_permanent = m.is_present("PERMANENT_PEER");
    // TODO fn: Clean this up--using a for loop doesn't feel good however an iterator was
    // causing a lot of developer/compiler type confusion
    let mut gossip_peers: Vec<SocketAddr> = Vec::new();
    if let Some(peers) = m.values_of("PEER") {
        for peer in peers {
            let peer_addr = if peer.find(':').is_some() {
                peer.to_string()
            } else {
                format!("{}:{}", peer, GOSSIP_DEFAULT_PORT)
            };
            let addrs: Vec<SocketAddr> = match peer_addr.to_socket_addrs() {
                Ok(addrs) => addrs.collect(),
                Err(e) => {
                    outputln!("Failed to resolve peer: {}", peer_addr);
                    return Err(sup_error!(Error::NameLookup(e)));
                }
            };
            let addr: SocketAddr = addrs[0];
            gossip_peers.push(addr);
        }
    }
    cfg.gossip_peers = gossip_peers;
    if let Some(watch_peer_file) = m.value_of("PEER_WATCH_FILE") {
        cfg.watch_peer_file = Some(String::from(watch_peer_file));
    }
    if let Some(aa_watch_file) = m.value_of("AA_WATCH_FILE") {
        cfg.additional_addresses_file = Some(String::from(aa_watch_file));
    }
    cfg.ring_key = match m.value_of("RING") {
        Some(val) => Some(SymKey::get_latest_pair_for(
            &val,
            &default_cache_key_path(None),
        )?),
        None => match henv::var(RING_KEY_ENVVAR) {
            Ok(val) => {
                let (key, _) = SymKey::write_file_from_str(&val, &default_cache_key_path(None))?;
                Some(key)
            }
            Err(_) => match henv::var(RING_ENVVAR) {
                Ok(val) => Some(SymKey::get_latest_pair_for(
                    &val,
                    &default_cache_key_path(None),
                )?),
                Err(_) => None,
            },
        },
    };
    if let Some(events) = m.value_of("EVENTS") {
        cfg.eventsrv_group = ServiceGroup::from_str(events).ok().map(Into::into);
    }
    Ok(cfg)
}

// Various CLI Parsing Functions
////////////////////////////////////////////////////////////////////////

/// Resolve a Builder URL. Taken from CLI args, the environment, or
/// (failing those) a default value.
fn bldr_url(m: &ArgMatches) -> String {
    match bldr_url_from_input(m) {
        Some(url) => url.to_string(),
        None => default_bldr_url(),
    }
}

/// A Builder URL, but *only* if the user specified it via CLI args or
/// the environment
fn bldr_url_from_input(m: &ArgMatches) -> Option<String> {
    m.value_of("BLDR_URL")
        .and_then(|u| Some(u.to_string()))
        .or_else(|| bldr_url_from_env())
}

/// Resolve a channel. Taken from CLI args, or (failing that), a
/// default value.
fn channel(matches: &ArgMatches) -> String {
    channel_from_input(matches).unwrap_or(channel::default())
}

/// A channel name, but *only* if the user specified via CLI args.
fn channel_from_input(m: &ArgMatches) -> Option<String> {
    m.value_of("CHANNEL").and_then(|c| Some(c.to_string()))
}

// ServiceSpec Modification Functions
////////////////////////////////////////////////////////////////////////

fn get_group_from_input(m: &ArgMatches) -> Option<String> {
    m.value_of("GROUP").map(ToString::to_string)
}

/// If the user provides both --application and --environment options,
/// parse and set the value on the spec.
fn get_app_env_from_input(m: &ArgMatches) -> Result<Option<ApplicationEnvironment>> {
    if let (Some(app), Some(env)) = (m.value_of("APPLICATION"), m.value_of("ENVIRONMENT")) {
        Ok(Some(ApplicationEnvironment {
            application: app.to_string(),
            environment: env.to_string(),
        }))
    } else {
        Ok(None)
    }
}

fn get_topology_from_input(m: &ArgMatches) -> Option<Topology> {
    m.value_of("TOPOLOGY")
        .and_then(|f| Topology::from_str(f).ok())
}

fn get_strategy_from_input(m: &ArgMatches) -> Option<UpdateStrategy> {
    m.value_of("STRATEGY")
        .and_then(|f| UpdateStrategy::from_str(f).ok())
}

fn get_binds_from_input(m: &ArgMatches) -> Result<Option<ServiceBindList>> {
    match m.values_of("BIND") {
        Some(bind_strs) => {
            let mut list = ServiceBindList::default();
            for bind_str in bind_strs {
                list.binds.push(ServiceBind::from_str(bind_str)?.into());
            }
            Ok(Some(list))
        }
        None => Ok(None),
    }
}

fn get_binding_mode_from_input(m: &ArgMatches) -> Option<BindingMode> {
    // There won't be errors, because we validate with `valid_binding_mode`
    m.value_of("BINDING_MODE")
        .and_then(|b| BindingMode::from_str(b).ok())
}

fn get_config_from_input(m: &ArgMatches) -> Option<String> {
    if let Some(ref config_from) = m.value_of("CONFIG_DIR") {
        warn!("");
        warn!(
            "WARNING: Setting '--config-from' should only be used in development, not production!"
        );
        warn!("");
        Some(config_from.to_string())
    } else {
        None
    }
}

#[cfg(target_os = "windows")]
fn get_password_from_input(m: &ArgMatches) -> Result<Option<String>> {
    if let Some(password) = m.value_of("PASSWORD") {
        Ok(Some(encrypt(password.to_string())?))
    } else {
        Ok(None)
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn get_password_from_input(_m: &ArgMatches) -> Result<Option<String>> {
    Ok(None)
}

// CLAP Validation Functions
////////////////////////////////////////////////////////////////////////

fn dir_exists(val: String) -> result::Result<(), String> {
    if Path::new(&val).is_dir() {
        Ok(())
    } else {
        Err(format!("Directory: '{}' cannot be found", &val))
    }
}

fn valid_binding_mode(val: String) -> result::Result<(), String> {
    match BindingMode::from_str(&val) {
        Ok(_) => Ok(()),
        Err(_) => Err(format!("Binding mode: '{}' is not valid", &val)),
    }
}

fn valid_service_group(val: String) -> result::Result<(), String> {
    match ServiceGroup::validate(&val) {
        Ok(()) => Ok(()),
        Err(err) => Err(err.to_string()),
    }
}

fn valid_topology(val: String) -> result::Result<(), String> {
    match Topology::from_str(&val) {
        Ok(_) => Ok(()),
        Err(_) => Err(format!("Service topology: '{}' is not valid", &val)),
    }
}

fn valid_socket_addr(val: String) -> result::Result<(), String> {
    match SocketAddr::from_str(&val) {
        Ok(_) => Ok(()),
        Err(_) => Err(format!(
            "Socket address should include both IP and port, eg: '0.0.0.0:9700'"
        )),
    }
}

fn valid_update_strategy(val: String) -> result::Result<(), String> {
    match UpdateStrategy::from_str(&val) {
        Ok(_) => Ok(()),
        Err(_) => Err(format!("Update strategy: '{}' is not valid", &val)),
    }
}

fn valid_url(val: String) -> result::Result<(), String> {
    match Url::parse(&val) {
        Ok(_) => Ok(()),
        Err(_) => Err(format!("URL: '{}' is not valid", &val)),
    }
}

////////////////////////////////////////////////////////////////////////
fn enable_features_from_env() {
    let features = vec![(feat::List, "LIST")];

    for feature in &features {
        match henv::var(format!("HAB_FEAT_{}", feature.1)) {
            Ok(ref val) if ["true", "TRUE"].contains(&val.as_str()) => {
                feat::enable(feature.0);
                outputln!("Enabling feature: {:?}", feature.0);
            }
            _ => {}
        }
    }

    if feat::is_enabled(feat::List) {
        outputln!("Listing feature flags environment variables:");
        for feature in &features {
            outputln!("     * {:?}: HAB_FEAT_{}=true", feature.0, feature.1);
        }
        outputln!("The Supervisor will start now, enjoy!");
    }
}

fn set_supervisor_logging_options(m: &ArgMatches) {
    if m.is_present("VERBOSE") {
        hcore::output::set_verbose(true);
    }
    if m.is_present("NO_COLOR") {
        hcore::output::set_no_color(true);
    }
    if m.is_present("JSON") {
        hcore::output::set_json(true)
    }
}

// Based on UI::default_with_env, but taking into account the setting
// of the global color variable.
//
// TODO: Ideally we'd have a unified way of setting color, so this
// function wouldn't be necessary. In the meantime, though, it'll keep
// the scope of change contained.
fn ui() -> UI {
    let coloring = if hcore::output::is_color() {
        Coloring::Auto
    } else {
        Coloring::Never
    };
    let isatty = if env::var(NONINTERACTIVE_ENVVAR)
        .map(|val| val == "1" || val == "true")
        .unwrap_or(false)
    {
        Some(false)
    } else {
        None
    };
    UI::default_with(coloring, isatty)
}

/// Set all fields for an `SvcLoad` message that we can from the given opts. This function
/// populates all *shared* options between `run` and `load`.
fn update_svc_load_from_input(m: &ArgMatches, msg: &mut protocol::ctl::SvcLoad) -> Result<()> {
    msg.bldr_url = Some(bldr_url(m));
    msg.bldr_channel = Some(channel(m));
    msg.application_environment = get_app_env_from_input(m)?;
    msg.binds = get_binds_from_input(m)?;
    msg.config_from = get_config_from_input(m);
    if m.is_present("FORCE") {
        msg.force = Some(true);
    }
    msg.group = get_group_from_input(m);
    msg.svc_encrypted_password = get_password_from_input(m)?;
    msg.binding_mode = get_binding_mode_from_input(m).map(|v| v as i32);
    msg.topology = get_topology_from_input(m).map(|v| v as i32);
    msg.update_strategy = get_strategy_from_input(m).map(|v| v as i32);
    Ok(())
}
