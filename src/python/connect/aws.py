"""AWS sessions for the hybrid runner: explicit SSO/runner profile, fixed region, expected account.

Static access keys and the ``default`` profile are refused. Creating a session makes no call;
``assert_expected_account`` calls STS once.
"""

from __future__ import annotations

from typing import Any

from utils.errors import ConfigurationError

ALLOWED_CREDENTIAL_METHODS = frozenset({"sso", "assume-role"})


def aws_session(profile: str | None, region: str | None, *, allowed_regions: frozenset[str]) -> Any:
    import boto3  # imported here: local mode never needs AWS libraries configured

    if not profile or profile == "default":
        raise ConfigurationError("AWS_PROFILE must explicitly select a non-default SSO/runner profile")
    if region not in allowed_regions:
        raise ConfigurationError(f"AWS execution is restricted to {sorted(allowed_regions)}")
    session = boto3.Session(profile_name=profile, region_name=region)
    credentials = session.get_credentials()
    if credentials is None or credentials.method not in ALLOWED_CREDENTIAL_METHODS:
        raise ConfigurationError("AWS credentials must come from SSO or the SSO-backed runner role")
    return session


def assert_expected_account(session: Any, expected_account_id: str | None) -> dict:
    """Refuse to run against any account other than the configured one."""
    if not expected_account_id:
        raise ConfigurationError("ERL_AWS_ACCOUNT_ID must be set in aws mode")
    identity = session.client("sts").get_caller_identity()
    if identity["Account"] != expected_account_id:
        raise ConfigurationError(
            f"AWS credentials belong to account {identity['Account']}, expected {expected_account_id}"
        )
    return identity
