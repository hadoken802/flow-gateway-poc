"""Gateway model constants."""

ACCOUNT_STATUSES = {"offline", "ready", "busy", "low_credits", "needs_login", "error"}
TASK_STATUSES = {
    "queued", "assigning", "submitted", "processing", "completed",
    "waiting_recovery", "download_pending", "download_failed", "failed", "manual_review", "manual_submit_required", "manual_result_ambiguous",
}
ACTIVE_TASK_STATUSES = {"assigning", "submitted", "processing"}
