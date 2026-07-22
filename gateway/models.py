"""Gateway model constants."""

ACCOUNT_STATUSES = {"offline", "ready", "busy", "low_credits", "needs_login", "error"}
TASK_STATUSES = {
    "queued", "assigning", "submitted", "processing", "completed",
    "waiting_recovery", "failed", "manual_review",
}
ACTIVE_TASK_STATUSES = {"assigning", "submitted", "processing"}
