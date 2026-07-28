"""Gateway model constants."""

ACCOUNT_STATUSES = {"offline", "ready", "busy", "low_credits", "needs_login", "error"}
TASK_STATUSES = {
    "queued", "leased", "assigning", "project_create_pending", "project_create_in_progress",
    "project_creation_unknown", "project_created", "submit_pending", "submit_in_progress",
    "submission_unknown", "submitted", "processing", "download_pending", "downloading",
    "download_failed", "failed_before_remote_submit", "failed", "cancelled", "completed",
    "waiting_recovery", "manual_review", "manual_submit_required", "manual_result_ambiguous",
}
ACTIVE_TASK_STATUSES = {
    "leased", "assigning", "project_create_pending", "project_create_in_progress",
    "project_creation_unknown", "project_created", "submit_pending", "submit_in_progress",
    "submission_unknown", "submitted", "processing", "download_pending", "downloading",
}
