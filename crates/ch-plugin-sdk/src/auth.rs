//! Interactive sign-in helpers (spec §7).
//!
//! The plugin drives the flow and the core renders it: on `auth.begin` the plugin emits an
//! `auth.step`; each `auth.submit` is answered `{}` once accepted *for processing*, and the
//! outcome is the next step (or the same step again with `error`, or `status: connected`).
//! [`AuthFlow`] keeps the one pending step per flow (§7.2), hands out step ids, and tells you
//! whether a submit answers the step on screen.
//!
//! Register `auth.begin`, `auth.submit` and `auth.cancel` with
//! [`crate::PluginBuilder::method`] and declare `auth.interactive`; keep an `AuthFlow` per
//! account (e.g. in a `Mutex` in [`crate::Account::set_data`]).

use ch_protocol::Timestamp;
use ch_protocol::dto::{AuthStep, AuthStepKind};
use ch_protocol::events::AccountState;
use ch_protocol::methods::AuthSubmitParams;

use crate::error::EmitError;
use crate::host::Account;

/// The `flow` of the sign-in conversation.
pub const FLOW_SIGN_IN: &str = "sign_in";
/// The `flow` of a 2FA password change that needs an e-mail code.
pub const FLOW_TWO_FACTOR: &str = "two_factor";

/// One auth conversation (`flow`) of one account.
#[derive(Debug)]
pub struct AuthFlow {
    account: Account,
    flow: String,
    seq: u64,
    current: Option<AuthStep>,
}

impl AuthFlow {
    /// A flow named `flow` (`sign_in`, `two_factor`, or plugin-defined).
    pub fn new(account: Account, flow: impl Into<String>) -> Self {
        Self {
            account,
            flow: flow.into(),
            seq: 0,
            current: None,
        }
    }

    /// The `sign_in` flow.
    pub fn sign_in(account: Account) -> Self {
        Self::new(account, FLOW_SIGN_IN)
    }

    /// The flow name.
    pub fn flow(&self) -> &str {
        &self.flow
    }

    /// The step on screen, if any.
    pub fn current(&self) -> Option<&AuthStep> {
        self.current.as_ref()
    }

    /// Start a new step of `kind`; finish it with [`StepBuilder::send`]. Replaces the step on
    /// screen (§7.2).
    pub fn ask(&mut self, kind: AuthStepKind) -> StepBuilder<'_> {
        self.seq += 1;
        let step = AuthStep {
            flow: self.flow.clone(),
            step_id: self.seq.to_string(),
            kind,
            title: None,
            hint: None,
            placeholder: None,
            data: None,
            expires_at: None,
            can_resend: false,
            min_length: None,
            max_length: None,
            error: None,
        };
        StepBuilder { flow: self, step }
    }

    /// Does `submit` answer the step on screen (same flow and step id, and the step takes input)?
    /// A stale or foreign submit should still be answered `{}`, then ignored or re-asked.
    pub fn accepts(&self, submit: &AuthSubmitParams) -> bool {
        submit.flow == self.flow
            && self.current.as_ref().is_some_and(|s| {
                s.step_id == submit.step_id
                    && !matches!(
                        s.kind,
                        AuthStepKind::Qr | AuthStepKind::Info | AuthStepKind::Done
                    )
            })
    }

    /// The previous submit was rejected (a wrong code): ask the same step again, with `error`
    /// saying why (§7.2: never fail `auth.submit` for this). Gets a fresh step id.
    pub fn reject(&mut self, error: impl Into<String>) -> Result<(), EmitError> {
        let Some(mut step) = self.current.clone() else {
            return Ok(());
        };
        self.seq += 1;
        step.step_id = self.seq.to_string();
        step.error = Some(error.into());
        self.account.auth_step(step.clone())?;
        self.current = Some(step);
        Ok(())
    }

    /// Finish the flow with a `done` step (what closes a `two_factor` flow). For `sign_in`
    /// follow it with `status: connected` (see [`AuthFlow::signed_in`]).
    pub fn done(&mut self) -> Result<(), EmitError> {
        self.ask(AuthStepKind::Done).send()?;
        self.current = None;
        Ok(())
    }

    /// `sign_in` succeeded: emit `done`, then `status: connected`.
    pub fn signed_in(&mut self, me: Option<ch_protocol::dto::Contact>) -> Result<(), EmitError> {
        self.done()?;
        self.account.connected(me)
    }

    /// `auth.cancel`: forget the pending step; for `sign_in`, report `auth_required` again
    /// (§7.2).
    pub fn cancel(&mut self) -> Result<(), EmitError> {
        self.current = None;
        if self.flow == FLOW_SIGN_IN {
            self.account.status(AccountState::AuthRequired)?;
        }
        Ok(())
    }
}

/// A step being built by [`AuthFlow::ask`].
#[must_use = "a step is only shown once `send` is called"]
pub struct StepBuilder<'a> {
    flow: &'a mut AuthFlow,
    step: AuthStep,
}

impl StepBuilder<'_> {
    /// Heading.
    pub fn title(mut self, s: impl Into<String>) -> Self {
        self.step.title = Some(s.into());
        self
    }
    /// Explanatory text.
    pub fn hint(mut self, s: impl Into<String>) -> Self {
        self.step.hint = Some(s.into());
        self
    }
    /// Input placeholder.
    pub fn placeholder(mut self, s: impl Into<String>) -> Self {
        self.step.placeholder = Some(s.into());
        self
    }
    /// For `qr`: the payload to render.
    pub fn data(mut self, s: impl Into<String>) -> Self {
        self.step.data = Some(s.into());
        self
    }
    /// For `qr`/`code`: when the core should ask for a new one.
    pub fn expires_at(mut self, t: Timestamp) -> Self {
        self.step.expires_at = Some(t);
        self
    }
    /// For `code`/`email_code`: show "Resend".
    pub fn can_resend(mut self, yes: bool) -> Self {
        self.step.can_resend = yes;
        self
    }
    /// Input length bounds for `code`/`text`.
    pub fn length(mut self, min: Option<u64>, max: Option<u64>) -> Self {
        self.step.min_length = min;
        self.step.max_length = max;
        self
    }
    /// Show an error with this step (the previous input was rejected).
    pub fn error(mut self, s: impl Into<String>) -> Self {
        self.step.error = Some(s.into());
        self
    }

    /// Emit the step; it becomes the pending one. Returns its step id.
    pub fn send(self) -> Result<String, EmitError> {
        let id = self.step.step_id.clone();
        self.flow.account.auth_step(self.step.clone())?;
        self.flow.current = Some(self.step);
        Ok(id)
    }
}
