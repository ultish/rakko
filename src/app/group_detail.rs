//! Consumer-group list/detail screens: lag table, and the multi-step offset-reset wizard.

use super::{App, Screen};
use crate::events::Command;
use crate::kafka::group_offsets::{GroupMember, OffsetResetTarget, PartitionLag};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResetInputKind {
    AbsoluteOffset,
    TimestampMillis,
}

/// Multi-step offset-reset wizard on the group-detail screen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OffsetResetPhase {
    ChooseMode,
    Input {
        target_kind: ResetInputKind,
        input: String,
        cursor: usize,
    },
    Confirm {
        target: OffsetResetTarget,
    },
}

pub struct GroupDetailState {
    pub name: String,
    pub state: String,
    pub members: Vec<GroupMember>,
    pub lags: Vec<PartitionLag>,
    pub selected_index: usize,
    pub has_active_members: bool,
    pub total_lag: i64,
    pub reset_phase: Option<OffsetResetPhase>,
}

pub(super) fn parse_reset_input(kind: ResetInputKind, input: &str) -> Result<OffsetResetTarget, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err("enter a numeric value".into());
    }
    let value: i64 = trimmed
        .parse()
        .map_err(|_| format!("invalid number: {trimmed}"))?;
    match kind {
        ResetInputKind::AbsoluteOffset => {
            if value < 0 {
                return Err("offset must be >= 0".into());
            }
            Ok(OffsetResetTarget::Absolute(value))
        }
        ResetInputKind::TimestampMillis => Ok(OffsetResetTarget::Timestamp(value)),
    }
}

impl App {
    /// Reloads group lag/members if we're on group detail and not mid offset-reset wizard.
    pub(super) fn refresh_group_detail_if_idle(&mut self, announce: bool) -> Vec<Command> {
        if self.screen != Screen::GroupDetail {
            return vec![];
        }
        if self
            .group_detail
            .as_ref()
            .is_some_and(|d| d.reset_phase.is_some())
        {
            return vec![];
        }
        let Some(profile) = self.active_profile.clone() else {
            return vec![];
        };
        let Some(group) = self.group_detail.as_ref().map(|d| d.name.clone()) else {
            return vec![];
        };
        if announce {
            self.status_message = Some(format!("refreshing group {group}..."));
        }
        vec![Command::LoadGroupDetail { profile, group }]
    }

    pub(super) fn start_offset_reset(&mut self) -> Vec<Command> {
        if self.screen != Screen::GroupDetail {
            return vec![];
        }
        let Some(detail) = self.group_detail.as_mut() else {
            return vec![];
        };
        if detail.lags.is_empty() {
            self.status_message = Some("no committed offsets to reset".into());
            return vec![];
        }
        detail.reset_phase = Some(OffsetResetPhase::ChooseMode);
        vec![]
    }

    pub(super) fn choose_offset_reset_target(&mut self, target: OffsetResetTarget) -> Vec<Command> {
        let Some(detail) = self.group_detail.as_mut() else {
            return vec![];
        };
        if !matches!(detail.reset_phase, Some(OffsetResetPhase::ChooseMode)) {
            return vec![];
        }
        detail.reset_phase = Some(OffsetResetPhase::Confirm { target });
        vec![]
    }

    pub(super) fn begin_offset_reset_input(&mut self, kind: ResetInputKind) -> Vec<Command> {
        let Some(detail) = self.group_detail.as_mut() else {
            return vec![];
        };
        if !matches!(detail.reset_phase, Some(OffsetResetPhase::ChooseMode)) {
            return vec![];
        }
        detail.reset_phase = Some(OffsetResetPhase::Input {
            target_kind: kind,
            input: String::new(),
            cursor: 0,
        });
        vec![]
    }

    pub(super) fn confirm_offset_reset(&mut self) -> Vec<Command> {
        let Some(profile) = self.active_profile.clone() else {
            return vec![];
        };
        let Some(detail) = self.group_detail.as_ref() else {
            return vec![];
        };
        let Some(OffsetResetPhase::Confirm { target }) = detail.reset_phase.clone() else {
            return vec![];
        };
        let partitions: Vec<(String, i32)> = detail
            .lags
            .iter()
            .map(|lag| (lag.topic.clone(), lag.partition))
            .collect();
        let group = detail.name.clone();
        // Clear wizard; status reflects in-flight work.
        if let Some(detail) = self.group_detail.as_mut() {
            detail.reset_phase = None;
        }
        self.status_message = Some(format!("resetting offsets for {group}..."));
        vec![Command::ResetGroupOffsets {
            profile,
            group,
            target,
            partitions,
        }]
    }
}
