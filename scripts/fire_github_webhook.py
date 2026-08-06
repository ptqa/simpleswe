#!/usr/bin/env python3
"""Fire a fake signed GitHub webhook against a local simpleswe controller.

Supports all actionable GitHub events from internal/forge/github/webhook.go:
  issue_comment, pull_request_review_comment, pull_request_review,
  check_run, status, plus ping for smoke testing.

Example - PR review comment (the case we need for local dev):
  python3 scripts/fire_github_webhook.py \
    --event pull_request_review_comment --pr 42 --body "Please fix this"

Example - ping smoke test:
  python3 scripts/fire_github_webhook.py --event ping

The script signs the exact raw JSON body with HMAC-SHA256 using the
exact bytes from .secrets/github-simpleswe/webhook (newline-significant),
matching internal/run/webhook.go validWebhookSignature.
If --address is omitted it port-forwards svc/simpleswe-webhooks:8081.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import http.client
import json
import pathlib
import socket
import subprocess
import sys
import time
import urllib.parse
import uuid


def resolve_secret(path_str: str | None) -> bytes:
    candidates: list[pathlib.Path] = []
    if path_str:
        candidates.append(pathlib.Path(path_str))
    else:
        candidates.extend(
            [
                pathlib.Path(".secrets/github-simpleswe/webhook"),
                pathlib.Path(".secrets/github/webhook"),
            ]
        )
    for p in candidates:
        if p.is_file():
            data = p.read_bytes()
            if len(data) == 0:
                sys.exit(f"error: secret file {p} is empty")
            if len(data) > 1 << 20:
                sys.exit(f"error: secret file {p} is too large")
            return data
    sys.exit(f"error: no secret file found (tried {', '.join(str(p) for p in candidates)})")


def build_payload(args: argparse.Namespace) -> tuple[bytes, str]:
    owner = args.owner
    repo = args.repo
    # Common delivery prefix handled server-side; event name is header.
    if args.event == "ping":
        body = {"zen": "Keep it logically awesome."}
        return json.dumps(body, separators=(",", ":")).encode(), "ping"

    if args.event == "issue_comment":
        # Mirrors internal/run/webhook_test.go:448 githubActionablePayload
        body = {
            "action": "created",
            "issue": {
                "number": args.pr,
                "title": args.title,
                "html_url": f"https://github.com/{owner}/{repo}/pull/{args.pr}",
                "pull_request": {"url": f"https://api.github.com/repos/{owner}/{repo}/pulls/{args.pr}"},
            },
            "comment": {
                "id": args.comment_id,
                "body": args.body,
                "author_association": args.association,
                "user": {"login": args.author},
                "html_url": f"https://github.com/{owner}/{repo}/issues/{args.pr}#issuecomment-{args.comment_id}",
            },
            "repository": {"name": repo, "owner": {"login": owner}},
        }
        return json.dumps(body, separators=(",", ":")).encode(), "issue_comment"

    if args.event == "pull_request_review_comment":
        body = {
            "action": "created",
            "pull_request": {
                "number": args.pr,
                "title": args.title,
                "head": {"ref": args.branch, "sha": args.sha},
            },
            "comment": {
                "id": args.comment_id,
                "body": args.body,
                "author_association": args.association,
                "user": {"login": args.author},
                "html_url": f"https://github.com/{owner}/{repo}/pull/{args.pr}#discussion_r{args.comment_id}",
            },
            "repository": {"name": repo, "owner": {"login": owner}},
        }
        return json.dumps(body, separators=(",", ":")).encode(), "pull_request_review_comment"

    if args.event == "pull_request_review":
        body = {
            "action": "submitted",
            "review": {
                "id": args.comment_id,
                "body": args.body,
                "state": "changes_requested",
                "author_association": args.association,
                "user": {"login": args.author},
                "html_url": f"https://github.com/{owner}/{repo}/pull/{args.pr}#pullrequestreview-{args.comment_id}",
            },
            "pull_request": {
                "number": args.pr,
                "title": args.title,
                "head": {"ref": args.branch, "sha": args.sha},
            },
            "repository": {"name": repo, "owner": {"login": owner}},
        }
        return json.dumps(body, separators=(",", ":")).encode(), "pull_request_review"

    if args.event == "check_run":
        body = {
            "action": "completed",
            "check_run": {
                "name": args.check_name,
                "conclusion": "failure",
                "head_sha": args.sha,
                "html_url": f"https://github.com/{owner}/{repo}/checks/{args.comment_id}",
                "app": {"name": "GitHub Actions"},
                "output": {"summary": args.body},
                "check_suite": {"head_branch": args.branch},
                "pull_requests": [{"number": args.pr}] if args.pr else [],
            },
            "repository": {"name": repo, "owner": {"login": owner}},
        }
        return json.dumps(body, separators=(",", ":")).encode(), "check_run"

    if args.event == "status":
        body = {
            "state": "failure",
            "sha": args.sha,
            "context": args.check_name,
            "description": args.body,
            "target_url": f"https://github.com/{owner}/{repo}/commit/{args.sha}/checks",
            "branches": [{"name": args.branch}] if args.branch else [],
            "repository": {"name": repo, "owner": {"login": owner}},
            "sender": {"login": args.author},
        }
        return json.dumps(body, separators=(",", ":")).encode(), "status"

    sys.exit(f"error: unknown event {args.event}")


def wait_port(host: str, port: int, timeout: float = 10.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return
        except OSError:
            time.sleep(0.2)
    sys.exit(f"error: timeout waiting for {host}:{port}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Fire a fake signed GitHub webhook")
    parser.add_argument("--event", default="pull_request_review_comment",
                        choices=["ping", "issue_comment", "pull_request_review_comment",
                                 "pull_request_review", "check_run", "status"],
                        help="GitHub event name (default: pull_request_review_comment)")
    parser.add_argument("--pr", type=int, default=42, help="Pull request number")
    parser.add_argument("--comment-id", type=int, default=101, help="Comment/review/check ID")
    parser.add_argument("--body", default="Please fix this", help="Comment/check body")
    parser.add_argument("--author", default="reviewer", help="GitHub user login")
    parser.add_argument("--association", default="MEMBER",
                        choices=["OWNER", "MEMBER", "COLLABORATOR", "CONTRIBUTOR", "NONE"],
                        help="author_association (trusted: OWNER/MEMBER/COLLABORATOR)")
    parser.add_argument("--owner", default="ptqa", help="Repository owner")
    parser.add_argument("--repo", default="simpleswe", help="Repository name")
    parser.add_argument("--title", default="Fix flaky test", help="PR/issue title")
    parser.add_argument("--branch", default="simpleswe/test-branch", help="Head branch (for review/check)")
    parser.add_argument("--sha", default="abc123def456abc123def456abc123def456abcd",
                        help="Head SHA (for review/check/status)")
    parser.add_argument("--check-name", default="ci", help="Check name / status context")
    parser.add_argument("--secret-file", default=None, help="Path to webhook secret file (default: .secrets/github-simpleswe/webhook)")
    parser.add_argument("--address", default=None, help="Webhook base URL (default: port-forward svc/simpleswe-webhooks)")
    parser.add_argument("--context", default="kind-simpleswe", help="Kube context for port-forward")
    parser.add_argument("--namespace", default="simpleswe", help="Kube namespace for port-forward")
    parser.add_argument("--delivery", default=None, help="X-GitHub-Delivery value (default: random uuid)")
    args = parser.parse_args()

    secret = resolve_secret(args.secret_file)
    body_bytes, event_name = build_payload(args)
    delivery = args.delivery or str(uuid.uuid4())
    sig = "sha256=" + hmac.new(secret, body_bytes, hashlib.sha256).hexdigest()

    # Resolve address
    pf_proc: subprocess.Popen[bytes] | None = None
    if args.address:
        parsed = urllib.parse.urlparse(args.address)
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        path = parsed.path.rstrip("/") or ""
        base = f"{parsed.scheme}://{host}:{port}{path}"
        # Use host/port for http.client; keep base for display
        url_path = f"{path}/v1/webhooks/github" if path else "/v1/webhooks/github"
        if path and path.endswith("/v1/webhooks/github"):
            url_path = path
        elif path and path != "":
            url_path = f"{path}/v1/webhooks/github"
        else:
            url_path = "/v1/webhooks/github"
        conn_host, conn_port = host, port
        use_https = parsed.scheme == "https"
    else:
        # Port-forward svc/simpleswe-webhooks -> 127.0.0.1:8081
        pf_proc = subprocess.Popen(
            ["kubectl", "--context", args.context, "-n", args.namespace,
             "port-forward", "svc/simpleswe-webhooks", "8081:8081"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        try:
            wait_port("127.0.0.1", 8081, timeout=10)
        except SystemExit as e:
            if pf_proc:
                pf_proc.terminate()
            raise
        conn_host, conn_port = "127.0.0.1", 8081
        url_path = "/v1/webhooks/github"
        use_https = False
        base = "http://127.0.0.1:8081"

    # Send request
    conn_cls = http.client.HTTPSConnection if use_https else http.client.HTTPConnection
    conn = conn_cls(conn_host, conn_port, timeout=10)
    headers = {
        "Content-Type": "application/json",
        "X-GitHub-Delivery": delivery,
        "X-GitHub-Event": event_name,
        "X-Hub-Signature-256": sig,
        "Content-Length": str(len(body_bytes)),
    }
    try:
        conn.request("POST", url_path, body=body_bytes, headers=headers)
        resp = conn.getresponse()
        resp_body = resp.read()
    finally:
        conn.close()
        if pf_proc:
            pf_proc.terminate()
            try:
                pf_proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                pf_proc.kill()

    # Output
    print(f"URL: {base if args.address else 'http://127.0.0.1:8081'}{url_path}")
    print(f"Event: {event_name}  Delivery: {delivery}")
    print(f"Signature: {sig}")
    print(f"Body: {body_bytes.decode()}")
    print(f"Response: {resp.status} {resp.reason}")
    if resp_body:
        try:
            print(json.dumps(json.loads(resp_body), indent=2))
        except Exception:
            print(resp_body.decode(errors='replace'))
    else:
        print("(empty)")

    if resp.status == 202:
        print("\n✓ accepted (even if event is non-actionable, 202 means signature/header OK)")
        return 0
    if resp.status == 401:
        print("\n✗ unauthorized — check secret file exact bytes (newline matters)")
        return 1
    if resp.status == 400:
        print("\n✗ bad request — check headers/body (GitHub PR review comment requires pr/comment-id/body/association)")
        return 1
    return 0 if resp.status == 202 else 1


if __name__ == "__main__":
    raise SystemExit(main())
