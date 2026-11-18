use std::io;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use parqonaut_client::{ConnectionState, ConsoleTab, WhoAmIResponseSnapshot};
use parqonaut_repair::{
    DatasetFingerprint, EffectivePolicy, RepairAction, RepairOperation, RepairPlan, RepairSafety,
    PARQONAUT_VERSION, PLAN_SCHEMA_VERSION,
};
use parqonaut_tui::terminal::TerminalOps;
use parqonaut_tui::ui::render;
use parqonaut_tui::{DialogMode, TerminalRestoreGuard, TuiAction, TuiApp};
use parqonaut_types::AuthRole;
use ratatui::backend::TestBackend;
use ratatui::Terminal;

fn key(code: KeyCode) -> KeyEvent {
    KeyEvent::new(code, KeyModifiers::NONE)
}

fn shift_tab() -> KeyEvent {
    KeyEvent::new(KeyCode::BackTab, KeyModifiers::SHIFT)
}

fn sample_plan() -> RepairPlan {
    RepairPlan {
        schema_version: PLAN_SCHEMA_VERSION,
        plan_id: "plan-tui-1".into(),
        parqonaut_version: PARQONAUT_VERSION.into(),
        dataset_root: "/tmp/in".into(),
        dataset_fingerprint: DatasetFingerprint {
            version: 1,
            root: "/tmp/in".into(),
            entries: vec![],
            digest: "a".repeat(64),
        },
        policy_fingerprint: "b".repeat(64),
        generated_at: chrono::Utc::now(),
        source_scan_id: uuid::Uuid::nil(),
        policy: EffectivePolicy::default(),
        operations: vec![
            RepairOperation {
                operation_id: "R001".into(),
                finding_ids: vec![],
                safety: RepairSafety::Safe,
                action: RepairAction::RebuildStatistics { target_paths: vec![] },
                rationale: "safe".into(),
                evidence: vec![],
                preconditions: vec![],
                depends_on: vec![],
            },
            RepairOperation {
                operation_id: "R002".into(),
                finding_ids: vec![],
                safety: RepairSafety::ReviewRequired,
                action: RepairAction::CastColumn {
                    column: "v".into(),
                    from_type: "int32".into(),
                    to_type: "int64".into(),
                },
                rationale: "review required cast".into(),
                evidence: vec![],
                preconditions: vec![],
                depends_on: vec![],
            },
            RepairOperation {
                operation_id: "R003".into(),
                finding_ids: vec![],
                safety: RepairSafety::Blocked,
                action: RepairAction::RenameColumn { from: "a".into(), to: "b".into() },
                rationale: "blocked".into(),
                evidence: vec![],
                preconditions: vec![],
                depends_on: vec![],
            },
        ],
        expected_outcomes: vec![],
        diagnosis_findings: vec![],
        schema_diff: None,
        schema_conflicts: None,
        target_schema: None,
    }
}

#[test]
fn reducer_navigation_filter_and_quit_guard() {
    let mut app = TuiApp::new(false);
    assert_eq!(app.model.tab, ConsoleTab::Overview);
    app.handle_key(key(KeyCode::Tab));
    assert_eq!(app.model.tab, ConsoleTab::Jobs);
    app.handle_key(shift_tab());
    assert_eq!(app.model.tab, ConsoleTab::Overview);

    // Filter mode captures 'q' without quitting!
    app.handle_key(key(KeyCode::Char('/')));
    assert!(matches!(app.dialog, DialogMode::FilterInput { .. }));
    assert_eq!(app.handle_key(key(KeyCode::Char('q'))), TuiAction::None);
    assert_eq!(app.handle_key(key(KeyCode::Enter)), TuiAction::None);
    assert_eq!(app.filter_query, "q");

    // Esc clears filter; q outside dialog quits.
    app.handle_key(key(KeyCode::Esc));
    assert!(app.filter_query.is_empty());
    assert_eq!(app.handle_key(key(KeyCode::Char('q'))), TuiAction::Quit);
}

#[test]
fn repair_review_and_rbac_prevent_implicit_or_unauthorized_mutations() {
    let mut app = TuiApp::new(false);
    app.model.connection = ConnectionState::Connected {
        base_url: "http://127.0.0.1:8080".into(),
        principal: WhoAmIResponseSnapshot { label: "r".into(), role: AuthRole::Reader },
    };
    app.model.tab = ConsoleTab::RepairReview;
    app.set_repair_review("/tmp/in".into(), "/tmp/out".into(), sample_plan());

    // Reader cannot submit repair.
    assert_eq!(app.handle_key(key(KeyCode::Char('s'))), TuiAction::None);
    assert_eq!(app.dialog, DialogMode::None);
    assert!(app.error_banner.is_some());

    // Operator can toggle ONLY ReviewRequired op (R002), not Safe (R001) or Blocked (R003).
    app.model.connection = ConnectionState::Connected {
        base_url: "http://127.0.0.1:8080".into(),
        principal: WhoAmIResponseSnapshot { label: "op".into(), role: AuthRole::Operator },
    };
    app.error_banner = None;
    // Index 0 = R001 (Safe) -> rejected
    app.handle_key(key(KeyCode::Char(' ')));
    assert!(app.error_banner.is_some());
    // Index 1 = R002 (ReviewRequired) -> authorized
    app.handle_key(key(KeyCode::Char('j')));
    app.handle_key(key(KeyCode::Char(' ')));
    assert_eq!(app.model.repair_review.as_ref().unwrap().authorized_list(), vec!["R002"]);
    // Index 2 = R003 (Blocked) -> rejected
    app.handle_key(key(KeyCode::Char('j')));
    app.handle_key(key(KeyCode::Char(' ')));
    assert_eq!(app.model.repair_review.as_ref().unwrap().authorized_list(), vec!["R002"]);

    // Submit requires explicit confirmation modal.
    assert_eq!(app.handle_key(key(KeyCode::Char('s'))), TuiAction::None);
    assert_eq!(app.dialog, DialogMode::ConfirmRepairSubmit);
    assert_eq!(app.handle_key(key(KeyCode::Char('y'))), TuiAction::SubmitRepair);
}

#[test]
fn test_backend_renders_all_tabs_at_ssh_80x24() {
    let mut app = TuiApp::new(false);
    app.model.connection = ConnectionState::Connected {
        base_url: "http://127.0.0.1:8080".into(),
        principal: WhoAmIResponseSnapshot { label: "op".into(), role: AuthRole::Operator },
    };
    app.set_repair_review("/tmp/in".into(), "/tmp/out".into(), sample_plan());
    let backend = TestBackend::new(80, 24);
    let mut terminal = Terminal::new(backend).unwrap();
    for tab in ConsoleTab::ALL {
        app.model.tab = tab;
        terminal.draw(|f| render(f, &app)).unwrap();
    }
}

struct MockTerminal {
    entered: AtomicBool,
    restored_count: AtomicUsize,
}

impl TerminalOps for MockTerminal {
    fn enter_raw(&self) -> io::Result<()> {
        self.entered.store(true, Ordering::SeqCst);
        Ok(())
    }
    fn leave_raw(&self) -> io::Result<()> {
        self.restored_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[test]
fn terminal_guard_restores_on_drop_and_panic_without_double_restore() {
    let mock = Arc::new(MockTerminal {
        entered: AtomicBool::new(false),
        restored_count: AtomicUsize::new(0),
    });
    {
        let guard = TerminalRestoreGuard::enter_with_ops(mock.clone()).unwrap();
        assert!(mock.entered.load(Ordering::SeqCst));
        drop(guard);
    }
    assert_eq!(mock.restored_count.load(Ordering::SeqCst), 1);

    let mock_panic = Arc::new(MockTerminal {
        entered: AtomicBool::new(false),
        restored_count: AtomicUsize::new(0),
    });
    let mp = mock_panic.clone();
    let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _guard = TerminalRestoreGuard::enter_with_ops(mp).unwrap();
        panic!("simulated TUI panic");
    }));
    assert!(res.is_err());
    assert_eq!(mock_panic.restored_count.load(Ordering::SeqCst), 1);
}
