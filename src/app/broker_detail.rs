//! Broker-detail screen: drill-down from the broker list into one broker's non-default
//! config values (`describe_configs`, filtered — see `kafka::admin::fetch_broker_configs`).

use super::{App, Screen};
use crate::events::Command;
use crate::kafka::admin::BrokerConfigEntry;

pub struct BrokerDetailState {
    pub broker_id: i32,
    pub host: String,
    pub port: i32,
    pub entries: Vec<BrokerConfigEntry>,
    pub selected_index: usize,
}

impl App {
    pub(super) fn open_broker_detail(&mut self) -> Vec<Command> {
        if self.screen != Screen::BrokerList {
            return vec![];
        }
        let Some(broker) = self.brokers.get(self.broker_list_selected_index).cloned() else {
            return vec![];
        };
        let Some(profile) = self.active_profile.clone() else {
            return vec![];
        };
        self.broker_detail = Some(BrokerDetailState {
            broker_id: broker.id,
            host: broker.host.clone(),
            port: broker.port,
            entries: Vec::new(),
            selected_index: 0,
        });
        self.screen = Screen::BrokerDetail;
        self.status_message = Some(format!("loading config for broker {}...", broker.id));
        vec![Command::LoadBrokerConfig {
            profile,
            broker_id: broker.id,
        }]
    }

    pub(super) fn refresh_broker_detail(&mut self, announce: bool) -> Vec<Command> {
        if self.screen != Screen::BrokerDetail {
            return vec![];
        }
        let Some(detail) = self.broker_detail.as_ref() else {
            return vec![];
        };
        let Some(profile) = self.active_profile.clone() else {
            return vec![];
        };
        let broker_id = detail.broker_id;
        if announce {
            self.status_message = Some(format!("refreshing config for broker {broker_id}..."));
        }
        vec![Command::LoadBrokerConfig { profile, broker_id }]
    }
}
