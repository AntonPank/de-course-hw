from datetime import datetime

ALLOWED_TYPES = (
    "PushEvent",
    "IssuesEvent",
    "PullRequestEvent",
    "WatchEvent",
    "IssueCommentEvent",
)


def event_filter(event: dict) -> bool:
    return event["type"] in ALLOWED_TYPES and event.get("public", True)


def flatten_event(event: dict) -> dict:
    payload = event.get("payload", {})

    if "commits" in payload:
        commit_count = len(payload["commits"])
    else:
        commit_count = None

    return {
        "id": event["id"],
        "event_type": event["type"],
        "created_at": _to_millis(event["created_at"]),
        "actor_login": event["actor"]["login"],
        "repo_name": event["repo"]["name"],
        "public": event.get("public", True),
        "payload_action": payload.get("action"),
        "payload_ref": payload.get("ref"),
        "payload_commit_count": commit_count,
    }


def _to_millis(created_at: str) -> int:
    """ISO-8601 ('2024-01-15T14:00:01Z') -> epoch-мілісекунди (int). Готова функція."""
    dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)
