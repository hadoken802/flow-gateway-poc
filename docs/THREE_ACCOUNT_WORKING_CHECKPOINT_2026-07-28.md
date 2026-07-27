# Three Account Working Checkpoint - 2026-07-28

## Scope

- Verification date: 2026-07-28
- Branch: feature/flow-gateway-single-machine-mvp
- Baseline commit before this checkpoint: 5bb8bf1cc7ee47a6259443fdf8450b1a702867c6
- Logical accounts: FLOW-001, FLOW-002, FLOW-003

## Verified State

The three logical accounts were verified with separate Chrome profiles and independent Runtime/Worker bindings.

- Login verification passed for FLOW-001, FLOW-002, and FLOW-003.
- Runtime startup passed for all three accounts.
- Worker health checks passed for all three accounts.
- Credits were readable for all three accounts.
- Independent task assignment worked across the three accounts.
- Three-account concurrent generation completed.
- Generated MP4 files were downloaded and playable.

Worker ports:

- FLOW-001: 8101
- FLOW-002: 8102
- FLOW-003: 8103

CDP ports:

- FLOW-001: 9300
- FLOW-002: 9301
- FLOW-003: 9302

Runtime extension state:

- Runtime loads Flow Kit only.
- Flow Site Data Reset / Cookie Reset has been removed from the active local setup.

## Known Deferred Issues

- Generated videos are currently saved under `outputs\FLOW-xxx`.
- Batch `run_dir` does not yet automatically collect MP4 files into a single batch folder.

These issues do not block this checkpoint.

## Restore Outline

1. Clone the repository.
2. Create `.venv` for the project.
3. Install project dependencies.
4. Start the Storyboard GUI with `start_storyboard_gui.bat`.
5. Initialize three clean profiles for FLOW-001, FLOW-002, and FLOW-003.
6. Have the user manually log in to three Google accounts in the three profile windows.
7. Verify Runtime, Worker health, credits, and eligibility before running any generation.

## Data Excluded From Git

This Git checkpoint does not include:

- Chrome profiles
- Cookies
- Tokens
- Flow keys
- Local databases
- Runtime logs
- Generated videos
- Google account information
- Project IDs
- Worker job IDs
- Passwords
