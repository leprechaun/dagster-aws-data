from aws_data.lib.github_webhooks import (
    hook_body,
    hooks_to_update,
    owned_repos,
    plan_webhooks,
)

URL = "https://hooks.example.com/github"


def repo(name, *, admin=True, archived=False, fork=False):
    return {
        "full_name": f"me/{name}",
        "permissions": {"admin": admin},
        "archived": archived,
        "fork": fork,
    }


def test_the_hook_sends_everything_including_future_event_types_and_posts_json_to_the_url():
    body = hook_body(URL, "s3cret")

    assert body["name"] == "web"
    assert body["active"] is True
    assert body["events"] == ["*"]
    assert body["config"] == {
        "url": URL,
        "content_type": "json",
        "secret": "s3cret",
        "insecure_ssl": "0",
    }


def test_without_a_secret_the_config_has_no_secret_key():
    config = hook_body(URL)["config"]

    assert "secret" not in config
    assert config["url"] == URL


def test_only_repos_the_user_can_administer_are_planned():
    repos = [repo("mine"), repo("theirs", admin=False)]

    assert owned_repos(repos) == ["me/mine"]


def test_archived_repos_are_skipped():
    assert owned_repos([repo("old", archived=True), repo("live")]) == ["me/live"]


def test_forks_are_skipped_unless_asked_for():
    repos = [repo("upstream-copy", fork=True), repo("mine")]

    assert owned_repos(repos) == ["me/mine"]
    assert owned_repos(repos, include_forks=True) == ["me/upstream-copy", "me/mine"]


def test_a_repo_with_no_hook_is_planned_for_creation():
    assert plan_webhooks(["me/a"], {"me/a": []}, URL) == {"me/a": "create"}


def test_a_repo_already_pointing_at_the_url_is_left_alone():
    existing = {"me/a": [{"config": {"url": URL}}]}

    assert plan_webhooks(["me/a"], existing, URL) == {"me/a": "exists"}


def test_only_hooks_to_the_url_are_selected_for_update():
    hooks = [
        {"id": 1, "config": {"url": URL}},
        {"id": 2, "config": {"url": "https://elsewhere.example.com"}},
        {"id": 3, "config": {"url": URL}},
    ]

    assert hooks_to_update(hooks, URL) == [1, 3]


def test_a_hook_to_a_different_url_does_not_count_as_existing():
    existing = {"me/a": [{"config": {"url": "https://elsewhere.example.com"}}]}

    assert plan_webhooks(["me/a"], existing, URL) == {"me/a": "create"}


def test_a_repo_missing_from_the_existing_map_is_planned_for_creation():
    assert plan_webhooks(["me/new"], {}, URL) == {"me/new": "create"}
