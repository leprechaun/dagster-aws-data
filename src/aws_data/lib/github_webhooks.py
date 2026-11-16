"""Planning webhook creation across a user's repositories.

Pure functions only: data in, data out. The CLI in scripts/ does the GitHub calls.
"""

# GitHub's "send me everything" wildcard: every event type, including any GitHub
# adds later. The Raw mirror only copies the types listed in github_s3.py, so new
# types arrive at the receiver but are not mirrored until they are added there.
WEBHOOK_EVENTS: tuple[str, ...] = ("*",)


def hook_body(url: str, secret: str | None = None) -> dict:
    """The JSON body GitHub expects when creating a repository webhook. Without a
    secret, deliveries are not signed, so the receiver cannot verify them."""
    config = {"url": url, "content_type": "json", "insecure_ssl": "0"}
    if secret:
        config["secret"] = secret
    return {
        "name": "web",
        "active": True,
        "events": list(WEBHOOK_EVENTS),
        "config": config,
    }


def hooks_to_update(hooks: list[dict], url: str) -> list[int]:
    """Ids of the hooks on one repo that deliver to `url`."""
    return [hook["id"] for hook in hooks if hook.get("config", {}).get("url") == url]


def owned_repos(repos: list[dict], *, include_forks: bool = False) -> list[str]:
    """Full names of repos the user can administer, skipping archived repos and,
    unless asked, forks. `repos` is the `/user/repos?affiliation=owner` listing."""
    return [
        repo["full_name"]
        for repo in repos
        if repo.get("permissions", {}).get("admin")
        and not repo.get("archived")
        and (include_forks or not repo.get("fork"))
    ]


def plan_webhooks(
    repo_names: list[str], existing: dict[str, list[dict]], url: str
) -> dict[str, str]:
    """Maps each repo to "create" or "exists". A repo already has a hook when one
    of its hooks points at `url`; re-running the script then changes nothing."""
    plan = {}
    for name in repo_names:
        hooks = existing.get(name, [])
        has_hook = any(hook.get("config", {}).get("url") == url for hook in hooks)
        plan[name] = "exists" if has_hook else "create"
    return plan
