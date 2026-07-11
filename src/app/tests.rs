fn test_config_path() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "rakko-test-config-{}-{}.toml",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

use super::*;
use super::topic_detail::SeekState;
use crate::config::AuthMode;
use crate::events::SeekPageRequest;
use crate::kafka::group_offsets::PartitionLag;
use crate::raw_message::RawMessage;
use std::collections::HashMap;

fn profile(name: &str) -> Profile {
    Profile {
        name: name.into(),
        bootstrap_servers: "localhost:9092".into(),
        tls_enabled: false,
        auth: AuthMode::None,
        schema_registry_url: None,
        message_max_bytes: None,
        extra_producer_config: HashMap::new(),
    }
}

fn topic(name: &str) -> TopicSummary {
    TopicSummary {
        name: name.into(),
        partition_count: 1,
        replication_factor: 1,
        compression_type: "none".into(),
        total_message_count: 0,
    }
}

#[test]
fn quit_opens_confirm_dialog_without_exiting() {
    let mut app = App::new(Config::default(), test_config_path());
    assert!(!app.should_quit);
    assert!(!app.quit_confirm);
    app.update(Action::Quit);
    assert!(app.quit_confirm);
    assert!(!app.should_quit);
}

#[test]
fn confirm_quit_exits() {
    let mut app = App::new(Config::default(), test_config_path());
    app.update(Action::Quit);
    app.update(Action::ConfirmQuit);
    assert!(app.should_quit);
    assert!(!app.quit_confirm);
}

#[test]
fn cancel_quit_dismisses_dialog() {
    let mut app = App::new(Config::default(), test_config_path());
    app.update(Action::Quit);
    app.update(Action::CancelQuit);
    assert!(!app.should_quit);
    assert!(!app.quit_confirm);
}

#[test]
fn force_quit_exits_immediately() {
    let mut app = App::new(Config::default(), test_config_path());
    app.update(Action::ForceQuit);
    assert!(app.should_quit);
    assert!(!app.quit_confirm);
}

#[test]
fn selection_clamps_at_zero_on_empty_profile_list() {
    let mut app = App::new(Config::default(), test_config_path());
    app.update(Action::MoveSelectionDown);
    assert_eq!(app.selected_profile_index, 0);
    app.update(Action::MoveSelectionUp);
    assert_eq!(app.selected_profile_index, 0);
}

#[test]
fn selection_clamps_at_bounds_on_populated_profile_list() {
    let config = Config {
        profiles: vec![profile("a"), profile("b")],
    };
    let mut app = App::new(config, test_config_path());

    // Clamp at 0 going up from the start.
    app.update(Action::MoveSelectionUp);
    assert_eq!(app.selected_profile_index, 0);

    app.update(Action::MoveSelectionDown);
    assert_eq!(app.selected_profile_index, 1);

    // Clamp at len-1 going past the end.
    app.update(Action::MoveSelectionDown);
    assert_eq!(app.selected_profile_index, 1);
}

#[test]
fn topic_list_selection_clamps_independently_of_profile_picker() {
    let mut app = App::new(Config::default(), test_config_path());
    app.profile_create = None; // empty-config wizard would freeze navigation
    app.screen = Screen::TopicList;
    app.topics = vec![topic("t1"), topic("t2"), topic("t3")];
    app.update(Action::MoveSelectionDown);
    app.update(Action::MoveSelectionDown);
    app.update(Action::MoveSelectionDown);
    assert_eq!(app.topic_list_selected_index, 2);
}

#[test]
fn confirm_on_empty_profile_list_is_a_safe_no_op() {
    let mut app = App::new(Config::default(), test_config_path());
    // Empty config auto-opens the create wizard; cancel it to exercise the bare picker.
    app.profile_create = None;
    let commands = app.update(Action::Confirm);
    assert!(commands.is_empty());
    assert_eq!(app.screen, Screen::ProfilePicker);
    assert!(app.active_profile.is_none());
}

#[test]
fn empty_config_auto_opens_create_profile_wizard() {
    let app = App::new(Config::default(), test_config_path());
    assert!(app.profile_create.is_some());
    assert_eq!(app.screen, Screen::ProfilePicker);
}

#[test]
fn submit_profile_create_saves_and_closes_wizard() {
    let path = test_config_path();
    let mut app = App::new(Config::default(), path.clone());
    assert!(app.profile_create.is_some());
    // Defaults: name=local, bootstrap=localhost:9092
    app.update(Action::ProfileCreateSubmit);
    assert!(app.profile_create.is_none());
    assert_eq!(app.config.profiles.len(), 1);
    assert_eq!(app.config.profiles[0].name, "local");
    assert!(path.exists());
    let loaded = config::load(&path).unwrap();
    assert_eq!(loaded.profiles.len(), 1);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn auto_message_max_bytes_saved_when_profile_unset() {
    let path = test_config_path();
    let mut app = App::new(
        Config {
            profiles: vec![profile("local")],
        },
        path.clone(),
    );
    app.attach_profile(profile("local"));
    assert!(app.config.profiles[0].message_max_bytes.is_none());

    app.apply_event(AppEvent::TopicsLoaded {
        topics: vec![topic("t1")],
        auto_message_max_bytes: Some(("local".into(), 20_971_520)),
    });

    assert_eq!(app.config.profiles[0].message_max_bytes, Some(20_971_520));
    assert_eq!(
        app.active_profile.as_ref().and_then(|p| p.message_max_bytes),
        Some(20_971_520)
    );
    assert!(app
        .status_message
        .as_ref()
        .is_some_and(|s| s.contains("detected message.max.bytes=20971520")));
    let loaded = config::load(&path).unwrap();
    assert_eq!(loaded.profiles[0].message_max_bytes, Some(20_971_520));
    let _ = std::fs::remove_file(&path);
}

#[test]
fn auto_message_max_bytes_does_not_override_explicit() {
    let path = test_config_path();
    let mut explicit = profile("local");
    explicit.message_max_bytes = Some(1_000_000);
    let mut app = App::new(
        Config {
            profiles: vec![explicit.clone()],
        },
        path.clone(),
    );
    app.attach_profile(explicit);

    // Even if a buggy task sent a detect result, apply must refuse overwrite
    // when the profile already has a value (apply checks is_some).
    // First clear the "detect" path by calling apply with a name that has a value.
    let status = app.apply_auto_message_max_bytes("local", 20_971_520);
    assert!(status.is_none());
    assert_eq!(app.config.profiles[0].message_max_bytes, Some(1_000_000));
    let _ = std::fs::remove_file(&path);
}

#[test]
fn start_edit_profile_prefills_form() {
    let mut app = App::new(
        Config {
            profiles: vec![Profile {
                name: "prod".into(),
                bootstrap_servers: "kafka:9093".into(),
                tls_enabled: true,
                auth: AuthMode::None,
                schema_registry_url: Some("http://sr:8081".into()),
                message_max_bytes: Some(2_000_000),
                extra_producer_config: HashMap::from([(
                    "compression.type".into(),
                    "zstd".into(),
                )]),
            }],
        },
        test_config_path(),
    );
    app.selected_profile_index = 0;
    app.update(Action::StartEditProfile);
    let state = app.profile_create.as_ref().expect("edit form open");
    assert!(state.is_edit());
    assert_eq!(state.edit_index, Some(0));
    assert_eq!(state.name, "prod");
    assert_eq!(state.bootstrap_servers, "kafka:9093");
    assert_eq!(state.auth_choice, ProfileCreateAuthChoice::TlsSystemTrust);
    assert_eq!(state.schema_registry_url, "http://sr:8081");
}

#[test]
fn submit_profile_edit_preserves_advanced_fields() {
    let path = test_config_path();
    let mut app = App::new(
        Config {
            profiles: vec![Profile {
                name: "local".into(),
                bootstrap_servers: "localhost:9092".into(),
                tls_enabled: false,
                auth: AuthMode::Tls {
                    ca_path: "/certs/ca.pem".into(),
                },
                schema_registry_url: None,
                message_max_bytes: Some(20_971_520),
                extra_producer_config: HashMap::from([(
                    "compression.type".into(),
                    "zstd".into(),
                )]),
            }],
        },
        path.clone(),
    );
    app.selected_profile_index = 0;
    app.update(Action::StartEditProfile);
    // Auth (Tls with its ca_path) is prefilled by from_profile() and left untouched
    // here, so it round-trips through the wizard rather than being force-preserved.
    assert_eq!(
        app.profile_create.as_ref().unwrap().auth_choice,
        ProfileCreateAuthChoice::TlsCustomCa
    );
    assert_eq!(app.profile_create.as_ref().unwrap().ca_path, "/certs/ca.pem");
    {
        let state = app.profile_create.as_mut().unwrap();
        state.bootstrap_servers = "192.168.1.10:9093".into();
        state.schema_registry_url = "http://localhost:8081".into();
    }
    app.update(Action::ProfileCreateSubmit);
    assert!(app.profile_create.is_none());
    assert_eq!(app.config.profiles.len(), 1);
    let p = &app.config.profiles[0];
    assert_eq!(p.name, "local");
    assert_eq!(p.bootstrap_servers, "192.168.1.10:9093");
    assert!(p.tls_enabled);
    assert_eq!(
        p.schema_registry_url.as_deref(),
        Some("http://localhost:8081")
    );
    assert_eq!(p.message_max_bytes, Some(20_971_520));
    assert_eq!(
        p.extra_producer_config.get("compression.type").map(String::as_str),
        Some("zstd")
    );
    assert!(matches!(
        &p.auth,
        AuthMode::Tls { ca_path } if ca_path == "/certs/ca.pem"
    ));
    let loaded = config::load(&path).unwrap();
    assert_eq!(loaded.profiles[0].bootstrap_servers, "192.168.1.10:9093");
    let _ = std::fs::remove_file(&path);
}

#[test]
fn edit_rename_conflict_rejected() {
    let mut app = App::new(
        Config {
            profiles: vec![profile("a"), profile("b")],
        },
        test_config_path(),
    );
    app.selected_profile_index = 0;
    app.update(Action::StartEditProfile);
    app.profile_create.as_mut().unwrap().name = "b".into();
    app.update(Action::ProfileCreateSubmit);
    assert!(app.profile_create.is_some());
    assert!(app
        .profile_create
        .as_ref()
        .unwrap()
        .error
        .as_ref()
        .is_some_and(|e| e.contains("already exists")));
    assert_eq!(app.config.profiles[0].name, "a");
}

#[test]
fn cancel_create_with_no_profiles_quits() {
    let mut app = App::new(Config::default(), test_config_path());
    app.update(Action::ProfileCreateCancel);
    assert!(app.should_quit);
    assert!(app.profile_create.is_none());
}

#[test]
fn cancel_create_with_existing_profiles_returns_to_picker() {
    let mut app = App::new(
        Config {
            profiles: vec![profile("a")],
        },
        test_config_path(),
    );
    app.profile_create = Some(ProfileCreateState::new());
    app.update(Action::ProfileCreateCancel);
    assert!(!app.should_quit);
    assert!(app.profile_create.is_none());
    assert_eq!(app.config.profiles.len(), 1);
}

#[test]
fn profile_create_cursor_left_right_insert_and_backspace() {
    let mut state = ProfileCreateState::new();
    // name starts as "local", cursor at end
    assert_eq!(state.name, "local");
    assert_eq!(state.cursor, 5);
    state.cursor_left();
    state.cursor_left();
    assert_eq!(state.cursor, 3);
    state.insert_char('X');
    assert_eq!(state.name, "locXal");
    assert_eq!(state.cursor, 4);
    state.backspace();
    assert_eq!(state.name, "local");
    assert_eq!(state.cursor, 3);
    state.cursor_home();
    assert_eq!(state.cursor, 0);
    state.delete_forward();
    assert_eq!(state.name, "ocal");
    state.cursor_end();
    assert_eq!(state.cursor, 4);
}

#[test]
fn cycle_auth_moves_through_all_four_choices_and_wraps() {
    let mut state = ProfileCreateState::new();
    state.focus = ProfileCreateFocus::Auth;
    assert_eq!(state.auth_choice, ProfileCreateAuthChoice::Plaintext);
    state.cycle_auth();
    assert_eq!(state.auth_choice, ProfileCreateAuthChoice::TlsSystemTrust);
    state.cycle_auth();
    assert_eq!(state.auth_choice, ProfileCreateAuthChoice::TlsCustomCa);
    state.cycle_auth();
    assert_eq!(state.auth_choice, ProfileCreateAuthChoice::Mtls);
    state.cycle_auth();
    assert_eq!(state.auth_choice, ProfileCreateAuthChoice::Plaintext);
}

#[test]
fn cycle_auth_is_a_noop_when_a_different_field_is_focused() {
    let mut state = ProfileCreateState::new();
    state.focus = ProfileCreateFocus::Name;
    state.cycle_auth();
    assert_eq!(state.auth_choice, ProfileCreateAuthChoice::Plaintext);
}

#[test]
fn focus_cycle_skips_cert_key_ca_fields_for_plaintext() {
    let mut state = ProfileCreateState::new();
    state.focus = ProfileCreateFocus::Auth;
    state.focus_next();
    assert_eq!(state.focus, ProfileCreateFocus::SchemaRegistry);
    state.focus_next();
    assert_eq!(state.focus, ProfileCreateFocus::Name);
}

#[test]
fn focus_cycle_includes_ca_path_only_for_tls_custom_ca() {
    let mut state = ProfileCreateState::new();
    state.focus = ProfileCreateFocus::Auth;
    state.cycle_auth();
    state.cycle_auth(); // TlsCustomCa
    state.focus_next();
    assert_eq!(state.focus, ProfileCreateFocus::CaPath);
    state.focus_next();
    assert_eq!(state.focus, ProfileCreateFocus::SchemaRegistry);
}

#[test]
fn focus_cycle_includes_cert_key_ca_for_mtls() {
    let mut state = ProfileCreateState::new();
    state.focus = ProfileCreateFocus::Auth;
    state.cycle_auth();
    state.cycle_auth();
    state.cycle_auth(); // Mtls
    state.focus_next();
    assert_eq!(state.focus, ProfileCreateFocus::CaPath);
    state.focus_next();
    assert_eq!(state.focus, ProfileCreateFocus::CertPath);
    state.focus_next();
    assert_eq!(state.focus, ProfileCreateFocus::KeyPath);
    state.focus_next();
    assert_eq!(state.focus, ProfileCreateFocus::SchemaRegistry);
}

#[test]
fn to_profile_with_tls_custom_ca_requires_ca_path() {
    let mut state = ProfileCreateState::new();
    state.auth_choice = ProfileCreateAuthChoice::TlsCustomCa;
    assert!(state.to_profile().is_err());
    state.ca_path = "/certs/ca.pem".into();
    let profile = state.to_profile().unwrap();
    assert!(profile.tls_enabled);
    assert!(matches!(
        &profile.auth,
        AuthMode::Tls { ca_path } if ca_path == "/certs/ca.pem"
    ));
}

#[test]
fn to_profile_with_mtls_requires_cert_key_and_ca_path() {
    let mut state = ProfileCreateState::new();
    state.auth_choice = ProfileCreateAuthChoice::Mtls;
    state.ca_path = "/certs/ca.pem".into();
    // cert/key still missing.
    assert!(state.to_profile().is_err());

    state.cert_path = "/certs/client.pem".into();
    state.key_path = "/certs/client.key".into();
    let profile = state.to_profile().unwrap();
    assert!(profile.tls_enabled);
    assert!(matches!(
        &profile.auth,
        AuthMode::Mtls { cert_path, key_path, ca_path }
            if cert_path == "/certs/client.pem"
                && key_path == "/certs/client.key"
                && ca_path == "/certs/ca.pem"
    ));
}

#[test]
fn create_profile_with_mtls_end_to_end_via_actions() {
    let path = test_config_path();
    let mut app = App::new(Config::default(), path.clone());

    // Defaults: name=local, bootstrap=localhost:9092, focus starts on Name.
    app.update(Action::ProfileCreateFocusNext); // -> Bootstrap
    app.update(Action::ProfileCreateFocusNext); // -> Auth
    app.update(Action::ProfileCreateCycleAuth); // TlsSystemTrust
    app.update(Action::ProfileCreateCycleAuth); // TlsCustomCa
    app.update(Action::ProfileCreateCycleAuth); // Mtls
    app.update(Action::ProfileCreateFocusNext); // -> CaPath
    for c in "/certs/ca.pem".chars() {
        app.update(Action::ProfileCreateChar(c));
    }
    app.update(Action::ProfileCreateFocusNext); // -> CertPath
    for c in "/certs/client.pem".chars() {
        app.update(Action::ProfileCreateChar(c));
    }
    app.update(Action::ProfileCreateFocusNext); // -> KeyPath
    for c in "/certs/client.key".chars() {
        app.update(Action::ProfileCreateChar(c));
    }

    app.update(Action::ProfileCreateSubmit);
    assert!(app.profile_create.is_none(), "submit should succeed and close the wizard");
    assert_eq!(app.config.profiles.len(), 1);
    let saved = &app.config.profiles[0];
    assert!(saved.tls_enabled);
    assert!(matches!(
        &saved.auth,
        AuthMode::Mtls { cert_path, key_path, ca_path }
            if cert_path == "/certs/client.pem"
                && key_path == "/certs/client.key"
                && ca_path == "/certs/ca.pem"
    ));

    let loaded = config::load(&path).unwrap();
    assert!(matches!(loaded.profiles[0].auth, AuthMode::Mtls { .. }));
    let _ = std::fs::remove_file(&path);
}

#[test]
fn create_profile_with_mtls_missing_cert_path_shows_error_and_keeps_wizard_open() {
    let mut app = App::new(Config::default(), test_config_path());
    app.update(Action::ProfileCreateFocusNext);
    app.update(Action::ProfileCreateFocusNext); // -> Auth
    app.update(Action::ProfileCreateCycleAuth);
    app.update(Action::ProfileCreateCycleAuth);
    app.update(Action::ProfileCreateCycleAuth); // Mtls, no paths filled
    app.update(Action::ProfileCreateSubmit);
    let state = app.profile_create.as_ref().expect("wizard stays open on error");
    assert!(state.error.as_ref().is_some_and(|e| e.contains("cert")));
}

#[test]
fn toggle_banner_animation() {
    let mut app = App::new(Config::default(), test_config_path());
    assert!(app.banner_animation);
    app.update(Action::ToggleBannerAnimation);
    assert!(!app.banner_animation);
    app.update(Action::BannerTick);
    assert_eq!(app.banner_frame, 0); // frozen
    app.update(Action::ToggleBannerAnimation);
    assert!(app.banner_animation);
    app.update(Action::BannerTick);
    assert_eq!(app.banner_frame, 1);
}

#[test]
fn dismiss_splash() {
    let mut app = App::new(Config::default(), test_config_path());
    assert!(app.show_splash);
    app.update(Action::DismissSplash);
    assert!(!app.show_splash);
}

#[test]
fn confirm_on_profile_picker_transitions_to_topic_list_and_returns_load_command() {
    let config = Config {
        profiles: vec![profile("a")],
    };
    let mut app = App::new(config, test_config_path());
    let commands = app.update(Action::Confirm);
    assert_eq!(app.screen, Screen::TopicList);
    assert_eq!(app.active_profile.as_ref().map(|p| p.name.as_str()), Some("a"));
    match commands.as_slice() {
        [Command::LoadTopics(profile)] => assert_eq!(profile.name, "a"),
        other => panic!("expected exactly one LoadTopics command, got {other:?}"),
    }
}

#[test]
fn back_on_topic_list_returns_to_profile_picker() {
    let mut app = App::new(Config::default(), test_config_path());
    app.profile_create = None;
    app.screen = Screen::TopicList;
    app.update(Action::Back);
    assert_eq!(app.screen, Screen::ProfilePicker);
}

#[test]
fn apply_event_topics_loaded_populates_topics_and_clears_status() {
    let mut app = App::new(Config::default(), test_config_path());
    app.status_message = Some("loading...".into());
    app.apply_event(AppEvent::TopicsLoaded {
        topics: vec![topic("t1")],
        auto_message_max_bytes: None,
    });
    assert_eq!(app.topics.len(), 1);
    assert!(app.status_message.is_none());
}

#[test]
fn apply_event_load_failed_sets_status_without_panicking() {
    let mut app = App::new(Config::default(), test_config_path());
    app.apply_event(AppEvent::TopicsLoadFailed("boom".into()));
    assert!(app.status_message.is_some());
}

fn message(key: &str, value: &str) -> RawMessage {
    RawMessage {
        topic: "t1".into(),
        partition: 0,
        offset: 0,
        timestamp_millis: None,
        key: Some(key.as_bytes().to_vec()),
        value: Some(value.as_bytes().to_vec()),
        headers: vec![],
    }
}

fn app_in_topic_detail(topic_name: &str, partition_count: usize) -> App {
    let mut app = App::new(Config {
        profiles: vec![profile("a")],
    }, test_config_path());
    app.active_profile = Some(profile("a"));
    app.screen = Screen::TopicDetail;
    app.topic_detail = Some(TopicDetailState {
        topic: topic_name.to_string(),
        partition_count,
        mode: BrowseMode::Tail(RingBuffer::new(TAIL_BUFFER_CAPACITY)),
        selected_index: 0,
        filter_input: String::new(),
        filter_cursor: 0,
        filter_active: false,
        applied_filter: None,
        replay_phase: None,
        message_view: None,
        sort: MessageSort::default(),
    });
    app
}

fn seek_state(
    messages: Vec<RawMessage>,
    page_start_offset: i64,
    at_beginning: bool,
    at_end: bool,
) -> SeekState {
    SeekState {
        partition: 0,
        messages,
        page_start_offset,
        at_beginning,
        at_end,
        low_watermark: 0,
        high_watermark: 1000,
    }
}

#[test]
fn confirm_on_topic_list_enters_topic_detail_in_tail_mode_and_starts_tail() {
    let config = Config {
        profiles: vec![profile("a")],
    };
    let mut app = App::new(config, test_config_path());
    app.active_profile = Some(profile("a"));
    app.screen = Screen::TopicList;
    app.topics = vec![topic("orders")];

    let commands = app.update(Action::Confirm);

    assert_eq!(app.screen, Screen::TopicDetail);
    let detail = app.topic_detail.as_ref().expect("topic_detail set");
    assert_eq!(detail.topic, "orders");
    assert!(matches!(detail.mode, BrowseMode::Tail(_)));
    match commands.as_slice() {
        [Command::StartTail { topic, .. }] => assert_eq!(topic, "orders"),
        other => panic!("expected exactly one StartTail command, got {other:?}"),
    }
}

#[test]
fn message_arrived_pushes_into_tail_ring_buffer() {
    let mut app = app_in_topic_detail("orders", 1);
    app.apply_event(AppEvent::MessageArrived {
        topic: "orders".into(),
        partition: 0,
        message: message("k1", "v1"),
    });
    let detail = app.topic_detail.as_ref().unwrap();
    assert_eq!(detail.visible_messages().len(), 1);
}

#[test]
fn message_arrived_for_a_different_topic_is_ignored() {
    let mut app = app_in_topic_detail("orders", 1);
    app.apply_event(AppEvent::MessageArrived {
        topic: "other-topic".into(),
        partition: 0,
        message: message("k1", "v1"),
    });
    let detail = app.topic_detail.as_ref().unwrap();
    assert_eq!(detail.visible_messages().len(), 0);
}

#[test]
fn message_arrived_while_in_seek_mode_is_dropped_not_applied() {
    let mut app = app_in_topic_detail("orders", 1);
    // Switch to seek mode without going through toggle_browse_mode (no active
    // profile plumbing needed for this test - just testing apply_event's guard).
    if let Some(detail) = app.topic_detail.as_mut() {
        detail.mode = BrowseMode::Seek(SeekState {
            partition: 0,
            messages: vec![],
            page_start_offset: 0,
            at_beginning: false,
            at_end: false,
            low_watermark: 0,
            high_watermark: 1000,
        });
    }
    app.apply_event(AppEvent::MessageArrived {
        topic: "orders".into(),
        partition: 0,
        message: message("k1", "v1"),
    });
    let detail = app.topic_detail.as_ref().unwrap();
    assert_eq!(detail.visible_messages().len(), 0);
}

#[test]
fn toggle_browse_mode_switches_tail_to_seek_and_requests_latest_page() {
    let mut app = app_in_topic_detail("orders", 3);
    let commands = app.update(Action::ToggleBrowseMode);
    let detail = app.topic_detail.as_ref().unwrap();
    assert!(matches!(detail.mode, BrowseMode::Seek(_)));
    assert!(matches!(commands.as_slice(), [Command::StopTail, Command::LoadSeekPage { .. }]));
}

#[test]
fn toggle_browse_mode_switches_seek_back_to_tail_and_starts_tail() {
    let mut app = app_in_topic_detail("orders", 1);
    if let Some(detail) = app.topic_detail.as_mut() {
        detail.mode = BrowseMode::Seek(SeekState {
            partition: 0,
            messages: vec![message("k", "v")],
            page_start_offset: 5,
            at_beginning: false,
            at_end: true,
            low_watermark: 0,
            high_watermark: 1000,
        });
    }
    let commands = app.update(Action::ToggleBrowseMode);
    let detail = app.topic_detail.as_ref().unwrap();
    assert!(matches!(detail.mode, BrowseMode::Tail(_)));
    assert!(matches!(commands.as_slice(), [Command::StartTail { .. }]));
}

#[test]
fn page_forward_at_end_is_a_no_op() {
    let mut app = app_in_topic_detail("orders", 1);
    if let Some(detail) = app.topic_detail.as_mut() {
        detail.mode = BrowseMode::Seek(SeekState {
            partition: 0,
            messages: vec![message("k", "v")],
            page_start_offset: 0,
            at_beginning: true,
            at_end: true,
            low_watermark: 0,
            high_watermark: 1000,
        });
    }
    let commands = app.update(Action::PageForward);
    assert!(commands.is_empty());
}

#[test]
fn page_forward_requests_next_page_from_current_page_end() {
    let mut app = app_in_topic_detail("orders", 1);
    if let Some(detail) = app.topic_detail.as_mut() {
        detail.mode = BrowseMode::Seek(SeekState {
            partition: 0,
            messages: vec![message("k1", "v1"), message("k2", "v2")],
            page_start_offset: 10,
            at_beginning: false,
            at_end: false,
            low_watermark: 0,
            high_watermark: 1000,
        });
    }
    let commands = app.update(Action::PageForward);
    match commands.as_slice() {
        [Command::LoadSeekPage {
            request: SeekPageRequest::Forward { from_offset, .. },
            ..
        }] => assert_eq!(*from_offset, 12),
        other => panic!("expected one Forward LoadSeekPage command, got {other:?}"),
    }
}

#[test]
fn page_backward_at_beginning_is_a_no_op() {
    let mut app = app_in_topic_detail("orders", 1);
    if let Some(detail) = app.topic_detail.as_mut() {
        detail.mode = BrowseMode::Seek(SeekState {
            partition: 0,
            messages: vec![message("k", "v")],
            page_start_offset: 0,
            at_beginning: true,
            at_end: false,
            low_watermark: 0,
            high_watermark: 1000,
        });
    }
    let commands = app.update(Action::PageBackward);
    assert!(commands.is_empty());
}

#[test]
fn filter_input_lifecycle_apply() {
    let mut app = app_in_topic_detail("orders", 1);
    app.update(Action::StartFilterInput);
    app.update(Action::FilterChar('a'));
    app.update(Action::FilterChar('b'));
    app.update(Action::FilterBackspace);
    app.update(Action::FilterChar('c'));
    let detail = app.topic_detail.as_ref().unwrap();
    assert_eq!(detail.filter_input, "ac");
    assert!(detail.filter_active);

    app.update(Action::ApplyFilter);
    let detail = app.topic_detail.as_ref().unwrap();
    assert_eq!(detail.applied_filter.as_deref(), Some("ac"));
    assert!(!detail.filter_active);
}

#[test]
fn filter_input_cancel_discards_typed_text_without_touching_applied_filter() {
    let mut app = app_in_topic_detail("orders", 1);
    if let Some(detail) = app.topic_detail.as_mut() {
        detail.applied_filter = Some("existing".to_string());
    }
    app.update(Action::StartFilterInput);
    app.update(Action::FilterChar('x'));
    app.update(Action::CancelFilterInput);
    let detail = app.topic_detail.as_ref().unwrap();
    assert_eq!(detail.applied_filter.as_deref(), Some("existing"));
    assert!(!detail.filter_active);
    assert!(detail.filter_input.is_empty());
}

#[test]
fn clear_filter_removes_applied_filter() {
    let mut app = app_in_topic_detail("orders", 1);
    if let Some(detail) = app.topic_detail.as_mut() {
        detail.applied_filter = Some("existing".to_string());
    }
    app.update(Action::ClearFilter);
    let detail = app.topic_detail.as_ref().unwrap();
    assert!(detail.applied_filter.is_none());
}

#[test]
fn visible_messages_filters_by_key_or_value_case_insensitively() {
    let mut app = app_in_topic_detail("orders", 1);
    if let Some(detail) = app.topic_detail.as_mut() {
        if let BrowseMode::Tail(buffer) = &mut detail.mode {
            buffer.push(message("order-1", "{\"status\":\"SHIPPED\"}"));
            buffer.push(message("order-2", "{\"status\":\"pending\"}"));
        }
        detail.applied_filter = Some("shipped".to_string());
    }
    let detail = app.topic_detail.as_ref().unwrap();
    let visible = detail.visible_messages();
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].key.as_deref(), Some("order-1".as_bytes()));
}

#[test]
fn topic_detail_selection_clamps_against_filtered_not_raw_count() {
    let mut app = app_in_topic_detail("orders", 1);
    if let Some(detail) = app.topic_detail.as_mut() {
        if let BrowseMode::Tail(buffer) = &mut detail.mode {
            buffer.push(message("a", "apple"));
            buffer.push(message("b", "banana"));
            buffer.push(message("c", "apricot"));
        }
        detail.applied_filter = Some("ap".to_string());
    }
    // Only "apple" and "apricot" contain "ap" - 2 of the 3 raw messages are visible.
    let visible_len = app.topic_detail.as_ref().unwrap().visible_messages().len();
    assert_eq!(visible_len, 2);
    app.update(Action::MoveSelectionDown);
    app.update(Action::MoveSelectionDown);
    app.update(Action::MoveSelectionDown);
    app.update(Action::MoveSelectionDown);
    let detail = app.topic_detail.as_ref().unwrap();
    assert_eq!(detail.selected_index, visible_len - 1);
}

#[test]
fn back_on_topic_detail_returns_to_topic_list_and_stops_tail() {
    let mut app = app_in_topic_detail("orders", 1);
    app.screen = Screen::TopicDetail;
    let commands = app.update(Action::Back);
    assert_eq!(app.screen, Screen::TopicList);
    assert!(app.topic_detail.is_none());
    assert!(matches!(commands.as_slice(), [Command::StopTail]));
}

fn app_on_topic_list() -> App {
    let mut app = App::new(Config {
        profiles: vec![profile("a")],
    }, test_config_path());
    app.active_profile = Some(profile("a"));
    app.screen = Screen::TopicList;
    app.topics = vec![topic("orders")];
    app
}

fn sample_group_detail() -> GroupDetailState {
    GroupDetailState {
        name: "my-group".into(),
        state: "Empty".into(),
        members: vec![],
        lags: vec![PartitionLag {
            topic: "orders".into(),
            partition: 0,
            committed_offset: Some(10),
            high_watermark: 20,
            low_watermark: 0,
            lag: Some(10),
        }],
        selected_index: 0,
        has_active_members: false,
        total_lag: 10,
        reset_phase: None,
    }
}

#[test]
fn open_groups_from_topic_list_loads_groups() {
    let mut app = app_on_topic_list();
    let commands = app.update(Action::OpenGroups);
    assert_eq!(app.screen, Screen::GroupList);
    assert!(matches!(commands.as_slice(), [Command::LoadGroups(_)]));
}

#[test]
fn open_groups_from_other_screens_is_noop() {
    let mut app = app_in_topic_detail("orders", 1);
    let commands = app.update(Action::OpenGroups);
    assert!(commands.is_empty());
    assert_eq!(app.screen, Screen::TopicDetail);
}

#[test]
fn confirm_on_group_list_loads_group_detail() {
    let mut app = app_on_topic_list();
    app.screen = Screen::GroupList;
    app.groups = vec![GroupSummary {
        name: "g1".into(),
        state: "Empty".into(),
        member_count: 0,
        protocol: String::new(),
        protocol_type: "consumer".into(),
    }];
    let commands = app.update(Action::Confirm);
    assert_eq!(app.screen, Screen::GroupDetail);
    match commands.as_slice() {
        [Command::LoadGroupDetail { group, .. }] => assert_eq!(group, "g1"),
        other => panic!("expected LoadGroupDetail, got {other:?}"),
    }
}

#[test]
fn offset_reset_wizard_earliest_to_confirm_to_command() {
    let mut app = app_on_topic_list();
    app.screen = Screen::GroupDetail;
    app.group_detail = Some(sample_group_detail());

    app.update(Action::StartOffsetReset);
    assert!(matches!(
        app.group_detail.as_ref().unwrap().reset_phase,
        Some(OffsetResetPhase::ChooseMode)
    ));

    app.update(Action::OffsetResetChooseEarliest);
    assert!(matches!(
        app.group_detail.as_ref().unwrap().reset_phase,
        Some(OffsetResetPhase::Confirm {
            target: OffsetResetTarget::Earliest
        })
    ));

    let commands = app.update(Action::ConfirmOffsetReset);
    match commands.as_slice() {
        [Command::ResetGroupOffsets {
            group,
            target: OffsetResetTarget::Earliest,
            partitions,
            ..
        }] => {
            assert_eq!(group, "my-group");
            assert_eq!(partitions, &vec![("orders".into(), 0)]);
        }
        other => panic!("expected ResetGroupOffsets, got {other:?}"),
    }
    assert!(app.group_detail.as_ref().unwrap().reset_phase.is_none());
}

#[test]
fn offset_reset_absolute_input_path() {
    let mut app = app_on_topic_list();
    app.screen = Screen::GroupDetail;
    app.group_detail = Some(sample_group_detail());

    app.update(Action::StartOffsetReset);
    app.update(Action::OffsetResetChooseAbsolute);
    app.update(Action::FilterChar('4'));
    app.update(Action::FilterChar('2'));
    app.update(Action::Confirm);
    assert!(matches!(
        app.group_detail.as_ref().unwrap().reset_phase,
        Some(OffsetResetPhase::Confirm {
            target: OffsetResetTarget::Absolute(42)
        })
    ));
}

#[test]
fn cancel_offset_reset_clears_wizard() {
    let mut app = app_on_topic_list();
    app.screen = Screen::GroupDetail;
    app.group_detail = Some(sample_group_detail());
    app.update(Action::StartOffsetReset);
    app.update(Action::CancelOffsetReset);
    assert!(app.group_detail.as_ref().unwrap().reset_phase.is_none());
}

#[test]
fn back_cancels_wizard_before_leaving_screen() {
    let mut app = app_on_topic_list();
    app.screen = Screen::GroupDetail;
    app.group_detail = Some(sample_group_detail());
    app.update(Action::StartOffsetReset);
    app.update(Action::Back);
    assert_eq!(app.screen, Screen::GroupDetail);
    assert!(app.group_detail.as_ref().unwrap().reset_phase.is_none());
    app.update(Action::Back);
    assert_eq!(app.screen, Screen::GroupList);
}

#[test]
fn parse_reset_input_validates() {
    assert_eq!(
        parse_reset_input(ResetInputKind::AbsoluteOffset, "10").unwrap(),
        OffsetResetTarget::Absolute(10)
    );
    assert!(parse_reset_input(ResetInputKind::AbsoluteOffset, "-1").is_err());
    assert!(parse_reset_input(ResetInputKind::AbsoluteOffset, "").is_err());
    assert_eq!(
        parse_reset_input(ResetInputKind::TimestampMillis, "1700000000000").unwrap(),
        OffsetResetTarget::Timestamp(1700000000000)
    );
}

#[test]
fn replay_selected_message_produces_raw_bytes_same_topic() {
    let mut app = app_in_topic_detail("orders", 1);
    if let Some(detail) = app.topic_detail.as_mut() {
        if let BrowseMode::Tail(buffer) = &mut detail.mode {
            let mut msg = message("k1", "v1");
            msg.partition = 2;
            msg.offset = 99;
            msg.headers = vec![("orig".into(), b"h".to_vec())];
            buffer.push(msg);
        }
    }
    app.update(Action::RequestReplay);
    assert!(matches!(
        app.topic_detail.as_ref().unwrap().replay_phase,
        Some(ReplayPhase::Confirm { .. })
    ));
    let commands = app.update(Action::ConfirmReplay);
    match commands.as_slice() {
        [Command::ProduceMessage {
            topic,
            key,
            value,
            headers,
            ..
        }] => {
            assert_eq!(topic, "orders");
            assert_eq!(key.as_deref(), Some(b"k1".as_slice()));
            assert_eq!(value.as_deref(), Some(b"v1".as_slice()));
            assert_eq!(headers, &vec![("orig".into(), b"h".to_vec())]);
        }
        other => panic!("expected ProduceMessage, got {other:?}"),
    }
    assert!(app.topic_detail.as_ref().unwrap().replay_phase.is_none());
}

#[test]
fn replay_edit_opens_producer_with_decoded_fields() {
    let mut app = app_in_topic_detail("orders", 1);
    if let Some(detail) = app.topic_detail.as_mut() {
        if let BrowseMode::Tail(buffer) = &mut detail.mode {
            let mut msg = message("k1", r#"{"a":1}"#);
            msg.headers = vec![("x".into(), b"1".to_vec())];
            buffer.push(msg);
        }
    }
    app.update(Action::RequestReplay);
    let commands = app.update(Action::ReplayEdit);
    assert_eq!(app.screen, Screen::Producer);
    assert!(app.topic_detail.as_ref().unwrap().replay_phase.is_none());
    let state = app.producer.as_ref().unwrap();
    assert_eq!(state.topic, "orders");
    assert_eq!(state.key_input, "k1");
    assert!(state.value_input.contains("\"a\""));
    assert_eq!(state.focus, ProducerFocus::Value);
    assert!(matches!(commands.as_slice(), [Command::StopTail]));
    assert!(app
        .status_message
        .as_ref()
        .is_some_and(|s| s.contains("edit mode") && s.contains("header")));
}

#[test]
fn cancel_replay_clears_wizard() {
    let mut app = app_in_topic_detail("orders", 1);
    if let Some(detail) = app.topic_detail.as_mut() {
        if let BrowseMode::Tail(buffer) = &mut detail.mode {
            buffer.push(message("k", "v"));
        }
    }
    app.update(Action::RequestReplay);
    app.update(Action::CancelReplay);
    assert!(app.topic_detail.as_ref().unwrap().replay_phase.is_none());
}

fn app_on_producer(topic: &str) -> App {
    let mut app = app_in_topic_detail(topic, 1);
    let commands = app.update(Action::OpenProducer);
    assert!(matches!(commands.as_slice(), [Command::StopTail]));
    assert_eq!(app.screen, Screen::Producer);
    app
}

#[test]
fn open_producer_from_topic_detail_stops_tail() {
    let mut app = app_in_topic_detail("orders", 1);
    let commands = app.update(Action::OpenProducer);
    assert_eq!(app.screen, Screen::Producer);
    assert_eq!(app.producer.as_ref().unwrap().topic, "orders");
    assert!(matches!(commands.as_slice(), [Command::StopTail]));
}

#[test]
fn open_producer_from_other_screens_is_noop() {
    let mut app = app_on_topic_list();
    let commands = app.update(Action::OpenProducer);
    assert!(commands.is_empty());
    assert_eq!(app.screen, Screen::TopicList);
    assert!(app.producer.is_none());
}

#[test]
fn producer_toggle_mode_cycles_and_normalizes_focus() {
    let mut app = app_on_producer("orders");
    {
        let state = app.producer.as_mut().unwrap();
        state.focus = ProducerFocus::Value;
    }
    app.update(Action::ProducerToggleMode);
    let state = app.producer.as_ref().unwrap();
    assert_eq!(state.mode, ProducerInputMode::FilePath);
    // Value focus is invalid in FilePath mode — snaps to Key.
    assert_eq!(state.focus, ProducerFocus::Key);

    app.update(Action::ProducerToggleMode);
    assert_eq!(
        app.producer.as_ref().unwrap().mode,
        ProducerInputMode::ExternalEditor
    );
    app.update(Action::ProducerToggleMode);
    assert_eq!(app.producer.as_ref().unwrap().mode, ProducerInputMode::Inline);
}

#[test]
fn producer_focus_next_cycles_key_and_value_in_inline_mode() {
    let mut app = app_on_producer("orders");
    assert_eq!(app.producer.as_ref().unwrap().focus, ProducerFocus::Key);
    app.update(Action::ProducerFocusNext);
    assert_eq!(app.producer.as_ref().unwrap().focus, ProducerFocus::Value);
    app.update(Action::ProducerFocusNext);
    assert_eq!(app.producer.as_ref().unwrap().focus, ProducerFocus::Key);
}

#[test]
fn producer_char_backspace_newline_edit_fields() {
    let mut app = app_on_producer("orders");
    app.update(Action::ProducerChar('a'));
    app.update(Action::ProducerChar('b'));
    app.update(Action::ProducerBackspace);
    assert_eq!(app.producer.as_ref().unwrap().key_input, "a");

    // Key accepts newlines (multi-line key pane).
    app.update(Action::ProducerNewline);
    app.update(Action::ProducerChar('c'));
    assert_eq!(app.producer.as_ref().unwrap().key_input, "a\nc");

    app.update(Action::ProducerFocusNext);
    app.update(Action::ProducerChar('x'));
    app.update(Action::ProducerNewline);
    app.update(Action::ProducerChar('y'));
    assert_eq!(app.producer.as_ref().unwrap().value_input, "x\ny");
}

#[test]
fn producer_submit_returns_produce_command_with_bytes() {
    let mut app = app_on_producer("orders");
    app.update(Action::ProducerChar('k'));
    app.update(Action::ProducerFocusNext);
    app.update(Action::ProducerChar('v'));
    let commands = app.update(Action::ProducerSubmit);
    match commands.as_slice() {
        [Command::ProduceMessage {
            topic,
            key,
            value,
            headers,
            ..
        }] => {
            assert_eq!(topic, "orders");
            assert_eq!(key.as_deref(), Some(b"k".as_slice()));
            assert_eq!(value.as_deref(), Some(b"v".as_slice()));
            assert!(headers.is_empty());
        }
        other => panic!("expected ProduceMessage, got {other:?}"),
    }
}

#[test]
fn producer_submit_empty_key_and_value_are_null() {
    let mut app = app_on_producer("orders");
    let commands = app.update(Action::ProducerSubmit);
    match commands.as_slice() {
        [Command::ProduceMessage { key, value, .. }] => {
            assert!(key.is_none());
            assert!(value.is_none());
        }
        other => panic!("expected ProduceMessage, got {other:?}"),
    }
}

#[test]
fn producer_load_file_emits_command_in_file_path_mode() {
    let mut app = app_on_producer("orders");
    app.update(Action::ProducerToggleMode); // FilePath
    app.update(Action::ProducerFocusNext); // FilePath field
    for c in "/tmp/msg.json".chars() {
        app.update(Action::ProducerChar(c));
    }
    let commands = app.update(Action::ProducerLoadFile);
    match commands.as_slice() {
        [Command::LoadFileIntoProducer { path }] => assert_eq!(path, "/tmp/msg.json"),
        other => panic!("expected LoadFileIntoProducer, got {other:?}"),
    }
}

#[test]
fn producer_load_file_noop_outside_file_path_mode() {
    let mut app = app_on_producer("orders");
    let commands = app.update(Action::ProducerLoadFile);
    assert!(commands.is_empty());
}

#[test]
fn producer_open_external_editor_emits_command() {
    let mut app = app_on_producer("orders");
    app.update(Action::ProducerToggleMode); // FilePath
    app.update(Action::ProducerToggleMode); // ExternalEditor
    app.update(Action::ProducerFocusNext); // still Key
    // Seed a value then open editor with that initial content.
    if let Some(state) = app.producer.as_mut() {
        state.value_input = "seed".into();
    }
    let commands = app.update(Action::ProducerOpenExternalEditor);
    match commands.as_slice() {
        [Command::RunExternalEditor { initial }] => assert_eq!(initial, "seed"),
        other => panic!("expected RunExternalEditor, got {other:?}"),
    }
}

#[test]
fn back_from_producer_returns_to_topic_detail_and_restarts_tail() {
    let mut app = app_on_producer("orders");
    let commands = app.update(Action::Back);
    assert_eq!(app.screen, Screen::TopicDetail);
    assert!(app.producer.is_none());
    assert!(app.topic_detail.is_some());
    match commands.as_slice() {
        [Command::StartTail { topic, .. }] => assert_eq!(topic, "orders"),
        other => panic!("expected StartTail, got {other:?}"),
    }
}

#[test]
fn back_from_producer_in_seek_mode_does_not_start_tail() {
    let mut app = app_in_topic_detail("orders", 1);
    if let Some(detail) = app.topic_detail.as_mut() {
        detail.mode = BrowseMode::Seek(SeekState {
            partition: 0,
            messages: vec![],
            page_start_offset: 0,
            at_beginning: true,
            at_end: true,
            low_watermark: 0,
            high_watermark: 1000,
        });
    }
    app.update(Action::OpenProducer);
    let commands = app.update(Action::Back);
    assert_eq!(app.screen, Screen::TopicDetail);
    assert!(commands.is_empty());
}

#[test]
fn apply_event_file_loaded_updates_value() {
    let mut app = app_on_producer("orders");
    app.apply_event(AppEvent::FileLoaded {
        content: "from-file".into(),
    });
    assert_eq!(app.producer.as_ref().unwrap().value_input, "from-file");
    assert!(app.status_message.is_some());
}

#[test]
fn apply_event_external_editor_done_updates_value() {
    let mut app = app_on_producer("orders");
    app.apply_event(AppEvent::ExternalEditorDone {
        content: "from-editor".into(),
    });
    assert_eq!(app.producer.as_ref().unwrap().value_input, "from-editor");
}

#[test]
fn apply_event_produce_succeeded_sets_status() {
    let mut app = app_on_producer("orders");
    app.apply_event(AppEvent::ProduceSucceeded);
    assert_eq!(app.status_message.as_deref(), Some("message produced"));
}

#[test]
fn open_export_exports_selected_message_only() {
    let mut app = app_in_topic_detail("orders", 1);
    if let Some(detail) = app.topic_detail.as_mut() {
        if let BrowseMode::Tail(buffer) = &mut detail.mode {
            let mut m0 = message("k0", "v0");
            m0.offset = 10;
            let mut m1 = message("k1", "v1");
            m1.offset = 11;
            buffer.push(m0);
            buffer.push(m1);
        }
        // Newest-first: index 0 is offset 11.
        detail.selected_index = 0;
    }
    let commands = app.update(Action::OpenExport);
    assert_eq!(app.screen, Screen::ExportImport);
    let state = app.export_import.as_ref().unwrap();
    assert_eq!(state.mode, ExportImportMode::Export);
    assert_eq!(state.messages.len(), 1);
    assert_eq!(state.messages[0].offset, 11);
    assert_eq!(state.path_input, "orders-p0-o11.jsonl");
    assert!(matches!(commands.as_slice(), [Command::StopTail]));
}

#[test]
fn open_export_all_exports_visible_messages() {
    let mut app = app_in_topic_detail("orders", 1);
    if let Some(detail) = app.topic_detail.as_mut() {
        if let BrowseMode::Tail(buffer) = &mut detail.mode {
            buffer.push(message("k0", "v0"));
            buffer.push(message("k1", "v1"));
        }
    }
    let commands = app.update(Action::OpenExportAll);
    assert_eq!(app.screen, Screen::ExportImport);
    let state = app.export_import.as_ref().unwrap();
    assert_eq!(state.messages.len(), 2);
    assert_eq!(state.path_input, "orders.jsonl");
    assert!(matches!(commands.as_slice(), [Command::StopTail]));
}

#[test]
fn open_export_from_inspector_uses_viewed_message() {
    let mut app = app_in_topic_detail("orders", 1);
    if let Some(detail) = app.topic_detail.as_mut() {
        if let BrowseMode::Tail(buffer) = &mut detail.mode {
            let mut m0 = message("k0", "v0");
            m0.offset = 5;
            let mut m1 = message("k1", "v1");
            m1.offset = 6;
            buffer.push(m0);
            buffer.push(m1);
        }
        detail.selected_index = 0; // newest = offset 6
    }
    app.update(Action::Confirm); // open inspector on selected
    // Move list selection away — export should still use the inspector snapshot.
    app.topic_detail.as_mut().unwrap().selected_index = 1;
    app.update(Action::OpenExport);
    let state = app.export_import.as_ref().unwrap();
    assert_eq!(state.messages.len(), 1);
    assert_eq!(state.messages[0].offset, 6);
}

#[test]
fn export_submit_emits_export_command() {
    let mut app = app_in_topic_detail("orders", 1);
    if let Some(detail) = app.topic_detail.as_mut() {
        if let BrowseMode::Tail(buffer) = &mut detail.mode {
            buffer.push(message("k", "v"));
        }
    }
    app.update(Action::OpenExport);
    let commands = app.update(Action::ExportImportSubmit);
    match commands.as_slice() {
        [Command::ExportMessages { path, messages }] => {
            assert_eq!(path, "orders-p0-o0.jsonl");
            assert_eq!(messages.len(), 1);
        }
        other => panic!("expected ExportMessages, got {other:?}"),
    }
}

#[test]
fn export_path_cursor_left_right_insert_and_backspace() {
    let mut app = app_in_topic_detail("orders", 1);
    if let Some(detail) = app.topic_detail.as_mut() {
        if let BrowseMode::Tail(buffer) = &mut detail.mode {
            buffer.push(message("k", "v"));
        }
    }
    app.update(Action::OpenExport);
    {
        let state = app.export_import.as_mut().unwrap();
        state.path_input = "ab.jsonl".into();
        state.cursor = 2; // after "ab"
    }
    app.update(Action::ExportImportCursorLeft);
    app.update(Action::ExportImportChar('X'));
    assert_eq!(app.export_import.as_ref().unwrap().path_input, "aXb.jsonl");
    app.update(Action::ExportImportCursorRight);
    app.update(Action::ExportImportBackspace);
    assert_eq!(app.export_import.as_ref().unwrap().path_input, "aX.jsonl");
    app.update(Action::ExportImportCursorHome);
    assert_eq!(app.export_import.as_ref().unwrap().cursor, 0);
    app.update(Action::ExportImportCursorEnd);
    assert_eq!(
        app.export_import.as_ref().unwrap().cursor,
        "aX.jsonl".chars().count()
    );
}

#[test]
fn open_import_and_submit() {
    let mut app = app_in_topic_detail("orders", 1);
    app.update(Action::OpenImport);
    assert_eq!(app.screen, Screen::ExportImport);
    {
        let state = app.export_import.as_mut().unwrap();
        state.path_input = "/tmp/in.jsonl".into();
        state.target_topic = "other".into();
    }
    let commands = app.update(Action::ExportImportSubmit);
    match commands.as_slice() {
        [Command::ImportMessages {
            path,
            target_topic,
            ..
        }] => {
            assert_eq!(path, "/tmp/in.jsonl");
            assert_eq!(target_topic, "other");
        }
        other => panic!("expected ImportMessages, got {other:?}"),
    }
}

fn confluent_avro_bytes(schema_id: u32, payload: &[u8]) -> Vec<u8> {
    let mut bytes = vec![0x00];
    bytes.extend_from_slice(&schema_id.to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes
}

#[test]
fn avro_message_queues_schema_fetch_when_registry_configured() {
    let mut app = app_in_topic_detail("orders", 1);
    let mut profile = profile("a");
    profile.schema_registry_url = Some("http://localhost:8081".into());
    app.attach_profile(profile);
    assert!(app.schema_registry.is_some());

    let mut msg = message("k", "v");
    msg.value = Some(confluent_avro_bytes(42, b"not-real-avro-payload"));

    let commands = app.apply_event(AppEvent::MessageArrived {
        topic: "orders".into(),
        partition: 0,
        message: msg,
    });
    match commands.as_slice() {
        [Command::FetchSchema {
            registry_url,
            schema_id,
        }] => {
            assert_eq!(registry_url, "http://localhost:8081");
            assert_eq!(*schema_id, 42);
        }
        other => panic!("expected FetchSchema, got {other:?}"),
    }
    // Second arrival of same id must not re-fetch while inflight.
    let mut msg2 = message("k", "v");
    msg2.value = Some(confluent_avro_bytes(42, b"x"));
    let again = app.apply_event(AppEvent::MessageArrived {
        topic: "orders".into(),
        partition: 0,
        message: msg2,
    });
    assert!(again.is_empty());
}

#[test]
fn avro_message_without_registry_does_not_fetch() {
    let mut app = app_in_topic_detail("orders", 1);
    assert!(app.schema_registry.is_none());
    let mut msg = message("k", "v");
    msg.value = Some(confluent_avro_bytes(7, b"x"));
    let commands = app.apply_event(AppEvent::MessageArrived {
        topic: "orders".into(),
        partition: 0,
        message: msg,
    });
    assert!(commands.is_empty());
}

#[test]
fn schema_loaded_inserts_into_cache() {
    let mut app = app_in_topic_detail("orders", 1);
    let mut profile = profile("a");
    profile.schema_registry_url = Some("http://localhost:8081".into());
    app.attach_profile(profile);
    app.schema_fetch_inflight.insert(9);

    let schema = apache_avro::Schema::parse_str(
        r#"{"type":"record","name":"T","fields":[{"name":"n","type":"string"}]}"#,
    )
    .unwrap();
    app.apply_event(AppEvent::SchemaLoaded {
        schema_id: 9,
        schema,
    });
    assert!(!app.schema_fetch_inflight.contains(&9));
    assert!(app
        .schema_registry
        .as_ref()
        .unwrap()
        .cached_schema(9)
        .is_some());
}

#[test]
fn refresh_on_topic_list_reloads_topics() {
    let mut app = app_on_topic_list();
    app.topic_list_selected_index = 0;
    let commands = app.update(Action::Refresh);
    assert!(matches!(commands.as_slice(), [Command::LoadTopics(_)]));
}

#[test]
fn default_sort_is_newest_first() {
    let mut app = app_in_topic_detail("orders", 1);
    if let Some(detail) = app.topic_detail.as_mut() {
        if let BrowseMode::Tail(buffer) = &mut detail.mode {
            let mut m0 = message("old", "v0");
            m0.offset = 10;
            let mut m1 = message("new", "v1");
            m1.offset = 11;
            buffer.push(m0);
            buffer.push(m1);
        }
    }
    let visible = app
        .topic_detail
        .as_ref()
        .unwrap()
        .visible_messages();
    assert_eq!(visible[0].offset, 11);
    assert_eq!(visible[1].offset, 10);
    assert_eq!(
        String::from_utf8_lossy(visible[0].key.as_deref().unwrap()),
        "new"
    );
}

#[test]
fn toggle_sort_preserves_selected_message() {
    let mut app = app_in_topic_detail("orders", 1);
    if let Some(detail) = app.topic_detail.as_mut() {
        if let BrowseMode::Tail(buffer) = &mut detail.mode {
            let mut m0 = message("a", "1");
            m0.offset = 1;
            let mut m1 = message("b", "2");
            m1.offset = 2;
            let mut m2 = message("c", "3");
            m2.offset = 3;
            buffer.push(m0);
            buffer.push(m1);
            buffer.push(m2);
        }
        // Newest-first list: offsets 3,2,1 — select middle (offset 2).
        detail.selected_index = 1;
    }
    app.update(Action::ToggleMessageSort);
    let detail = app.topic_detail.as_ref().unwrap();
    assert_eq!(detail.sort, MessageSort::OldestFirst);
    // Oldest-first: 1,2,3 — same message (offset 2) is still selected.
    assert_eq!(detail.selected_index, 1);
    let visible = detail.visible_messages();
    assert_eq!(visible[detail.selected_index].offset, 2);
    assert_eq!(app.status_message.as_deref(), Some("sort: oldest↑"));
}

#[test]
fn seek_page_loaded_stores_watermarks() {
    let mut app = app_in_topic_detail("orders", 1);
    if let Some(detail) = app.topic_detail.as_mut() {
        detail.mode = BrowseMode::Seek(seek_state(vec![], 0, false, false));
    }
    let mut m = message("k", "v");
    m.offset = 50;
    app.apply_event(AppEvent::SeekPageLoaded {
        topic: "orders".into(),
        messages: vec![m],
        meta: crate::events::SeekPageMeta {
            partition: 0,
            page_start_offset: 50,
            at_beginning: false,
            at_end: true,
            low_watermark: 0,
            high_watermark: 51,
        },
    });
    let BrowseMode::Seek(state) = &app.topic_detail.as_ref().unwrap().mode else {
        panic!("expected seek mode");
    };
    assert_eq!(state.low_watermark, 0);
    assert_eq!(state.high_watermark, 51);
    assert_eq!(state.page_start_offset, 50);
    assert_eq!(state.messages.len(), 1);
}

#[test]
fn enter_opens_message_inspector_for_selected_row() {
    let mut app = app_in_topic_detail("orders", 1);
    if let Some(detail) = app.topic_detail.as_mut() {
        if let BrowseMode::Tail(buffer) = &mut detail.mode {
            buffer.push(message("k1", r#"{"a":1}"#));
            buffer.push(message("k2", r#"{"b":2}"#));
        }
        // Newest-first: index 0 is k2 (last pushed), index 1 is k1.
        detail.selected_index = 0;
    }
    let commands = app.update(Action::Confirm);
    assert!(commands.is_empty());
    let view = app
        .topic_detail
        .as_ref()
        .and_then(|d| d.message_view.as_ref())
        .expect("message_view should open");
    assert_eq!(
        String::from_utf8_lossy(view.message.key.as_deref().unwrap()),
        "k2"
    );
    assert_eq!(view.scroll, 0);
}

#[test]
fn enter_again_and_esc_close_message_inspector() {
    let mut app = app_in_topic_detail("orders", 1);
    if let Some(detail) = app.topic_detail.as_mut() {
        if let BrowseMode::Tail(buffer) = &mut detail.mode {
            buffer.push(message("k", "v"));
        }
    }
    app.update(Action::Confirm);
    assert!(app.topic_detail.as_ref().unwrap().message_view.is_some());

    app.update(Action::Confirm);
    assert!(app.topic_detail.as_ref().unwrap().message_view.is_none());

    app.update(Action::Confirm);
    assert!(app.topic_detail.as_ref().unwrap().message_view.is_some());
    app.update(Action::Back);
    assert!(app.topic_detail.as_ref().unwrap().message_view.is_none());
    // Still on topic detail — Esc only closed the inspector.
    assert_eq!(app.screen, Screen::TopicDetail);
}

#[test]
fn j_k_scroll_message_inspector_instead_of_list() {
    let mut app = app_in_topic_detail("orders", 1);
    if let Some(detail) = app.topic_detail.as_mut() {
        if let BrowseMode::Tail(buffer) = &mut detail.mode {
            buffer.push(message("k1", "v1"));
            buffer.push(message("k2", "v2"));
        }
        detail.selected_index = 0;
    }
    app.update(Action::Confirm);
    app.update(Action::MoveSelectionDown);
    app.update(Action::MoveSelectionDown);
    let detail = app.topic_detail.as_ref().unwrap();
    assert_eq!(detail.selected_index, 0, "list selection stays put");
    assert_eq!(detail.message_view.as_ref().unwrap().scroll, 2);
    app.update(Action::MoveSelectionUp);
    assert_eq!(
        app.topic_detail
            .as_ref()
            .unwrap()
            .message_view
            .as_ref()
            .unwrap()
            .scroll,
        1
    );
}

#[test]
fn enter_with_no_messages_sets_status() {
    let mut app = app_in_topic_detail("orders", 1);
    app.update(Action::Confirm);
    assert!(app.topic_detail.as_ref().unwrap().message_view.is_none());
    assert_eq!(app.status_message.as_deref(), Some("no message selected"));
}

#[test]
fn refresh_on_seek_reloads_current_page() {
    let mut app = app_in_topic_detail("orders", 1);
    if let Some(detail) = app.topic_detail.as_mut() {
        detail.mode = BrowseMode::Seek(SeekState {
            partition: 0,
            messages: vec![message("k1", "v1"), message("k2", "v2")],
            page_start_offset: 10,
            at_beginning: false,
            at_end: false,
            low_watermark: 0,
            high_watermark: 1000,
        });
    }
    let commands = app.update(Action::Refresh);
    match commands.as_slice() {
        [Command::LoadSeekPage {
            topic,
            partition,
            request: SeekPageRequest::Forward { from_offset, page_size },
            ..
        }] => {
            assert_eq!(topic, "orders");
            assert_eq!(*partition, 0);
            assert_eq!(*from_offset, 10);
            assert_eq!(*page_size, SEEK_PAGE_SIZE);
        }
        other => panic!("expected LoadSeekPage Forward from page_start, got {other:?}"),
    }
    assert_eq!(app.status_message.as_deref(), Some("refreshing page..."));
}

#[test]
fn refresh_on_tail_is_a_no_op_with_hint() {
    let mut app = app_in_topic_detail("orders", 1);
    assert!(matches!(
        app.topic_detail.as_ref().unwrap().mode,
        BrowseMode::Tail(_)
    ));
    let commands = app.update(Action::Refresh);
    assert!(commands.is_empty());
    assert!(app
        .status_message
        .as_ref()
        .is_some_and(|m| m.contains("tail is live")));
}

#[test]
fn refresh_on_seek_skips_while_filter_active() {
    let mut app = app_in_topic_detail("orders", 1);
    if let Some(detail) = app.topic_detail.as_mut() {
        detail.mode = BrowseMode::Seek(SeekState {
            partition: 0,
            messages: vec![message("k", "v")],
            page_start_offset: 0,
            at_beginning: true,
            at_end: false,
            low_watermark: 0,
            high_watermark: 1000,
        });
        detail.filter_active = true;
    }
    let commands = app.update(Action::Refresh);
    assert!(commands.is_empty());
}

#[test]
fn refresh_on_group_list_reloads_groups() {
    let mut app = app_on_topic_list();
    app.screen = Screen::GroupList;
    let commands = app.update(Action::Refresh);
    assert!(matches!(commands.as_slice(), [Command::LoadGroups(_)]));
}

#[test]
fn refresh_on_group_detail_reloads_group() {
    let mut app = app_on_topic_list();
    app.screen = Screen::GroupDetail;
    app.group_detail = Some(sample_group_detail());
    let commands = app.update(Action::Refresh);
    match commands.as_slice() {
        [Command::LoadGroupDetail { group, .. }] => assert_eq!(group, "my-group"),
        other => panic!("expected LoadGroupDetail, got {other:?}"),
    }
}

#[test]
fn auto_refresh_skips_during_offset_reset_wizard() {
    let mut app = app_on_topic_list();
    app.screen = Screen::GroupDetail;
    app.group_detail = Some(sample_group_detail());
    app.group_detail.as_mut().unwrap().reset_phase = Some(OffsetResetPhase::ChooseMode);
    let commands = app.update(Action::AutoRefreshGroupDetail);
    assert!(commands.is_empty());
}

#[test]
fn auto_refresh_on_idle_group_detail() {
    let mut app = app_on_topic_list();
    app.screen = Screen::GroupDetail;
    app.group_detail = Some(sample_group_detail());
    let commands = app.update(Action::AutoRefreshGroupDetail);
    assert!(matches!(
        commands.as_slice(),
        [Command::LoadGroupDetail { .. }]
    ));
}

#[test]
fn topics_loaded_preserves_selection() {
    let mut app = app_on_topic_list();
    app.topics = vec![topic("a"), topic("b"), topic("c")];
    app.topic_list_selected_index = 2;
    app.apply_event(AppEvent::TopicsLoaded {
        topics: vec![topic("a"), topic("b")],
        auto_message_max_bytes: None,
    });
    assert_eq!(app.topic_list_selected_index, 1); // clamped
}

#[test]
fn group_detail_loaded_preserves_selection_and_reset_wizard() {
    let mut app = app_on_topic_list();
    app.screen = Screen::GroupDetail;
    let mut detail = sample_group_detail();
    detail.selected_index = 0;
    detail.reset_phase = Some(OffsetResetPhase::ChooseMode);
    app.group_detail = Some(detail);

    let loaded = crate::kafka::group_offsets::GroupDetail {
        name: "my-group".into(),
        state: "Empty".into(),
        members: vec![],
        lags: sample_group_detail().lags,
    };
    app.apply_event(AppEvent::GroupDetailLoaded(loaded));
    let g = app.group_detail.as_ref().unwrap();
    assert!(matches!(g.reset_phase, Some(OffsetResetPhase::ChooseMode)));
    assert_eq!(g.selected_index, 0);
}
