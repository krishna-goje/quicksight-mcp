"""Low-level AWS QuickSight client with credential management and retry.

Owns the boto3 session, handles ExpiredToken recovery via saml2aws,
and provides ``call()`` / ``paginate()`` helpers used by all services.
"""

from __future__ import annotations

import logging
import subprocess
from typing import Any, Dict, List, Optional

import boto3
from botocore.config import Config

from quicksight_mcp.config import Settings

logger = logging.getLogger(__name__)


class AwsClient:
    """Thin wrapper around boto3 QuickSight client with auto-refresh.

    Args:
        settings: Server-wide configuration.
    """

    def __init__(self, settings: Settings):
        self._settings = settings
        self.profile = settings.aws_profile or None
        self.region = settings.aws_region
        self._account_id_override = settings.aws_account_id or None

        # Populated by _init_session
        self.session: boto3.Session = None  # type: ignore[assignment]
        self.client: Any = None
        self.account_id: Optional[str] = None

        self._init_session()

    # ------------------------------------------------------------------
    # Session lifecycle
    # ------------------------------------------------------------------

    def _init_session(self) -> None:
        """Create or refresh the boto3 session and QuickSight client."""
        if self.profile:
            self.session = boto3.Session(
                profile_name=self.profile,
                region_name=self.region,
            )
        else:
            self.session = boto3.Session(region_name=self.region)

        retry_config = Config(
            retries={
                "max_attempts": self._settings.max_api_retries,
                "mode": self._settings.retry_mode,
            }
        )
        self.client = self.session.client("quicksight", config=retry_config)

        # Resolve account ID
        self.account_id = self._account_id_override
        if not self.account_id:
            try:
                sts = self.session.client("sts")
                self.account_id = sts.get_caller_identity()["Account"]
            except Exception:
                logger.warning(
                    "Could not detect account ID (credentials may be expired). "
                    "Will retry on first API call after credential refresh."
                )
                self.account_id = None

        logger.info(
            "AWS session initialized (account=%s)", self.account_id or "pending"
        )

    def ensure_account_id(self) -> str:
        """Ensure account_id is resolved.  Triggers reauth if needed."""
        if self.account_id:
            return self.account_id
        try:
            sts = self.session.client("sts")
            self.account_id = sts.get_caller_identity()["Account"]
            return self.account_id
        except Exception as e:
            if self._refresh_on_expired(e):
                if self.account_id is None:
                    raise RuntimeError(
                        "Cannot resolve AWS account ID after credential refresh."
                    )
                return self.account_id
            raise RuntimeError(
                "Cannot resolve AWS account ID. Credentials are expired. "
                "Run: saml2aws login or refresh your AWS credentials."
            ) from e

    # ------------------------------------------------------------------
    # API call helpers (with auto-retry on ExpiredToken)
    # ------------------------------------------------------------------

    def call(self, method_name: str, **kwargs: Any) -> Any:
        """Call a QuickSight API method with auto-retry on expired creds."""
        try:
            return getattr(self.client, method_name)(**kwargs)
        except Exception as e:
            if self._refresh_on_expired(e):
                if "AwsAccountId" in kwargs:
                    kwargs["AwsAccountId"] = self.account_id
                return getattr(self.client, method_name)(**kwargs)
            raise

    def paginate(self, paginator_name: str, result_key: str) -> List[Dict]:
        """Paginate a QuickSight list API with auto-retry.

        Args:
            paginator_name: Boto3 paginator name (e.g. ``'list_data_sets'``).
            result_key: Key in each page containing the result list.

        Returns:
            Combined list from all pages.
        """
        self.ensure_account_id()

        def _run() -> List[Dict]:
            paginator = self.client.get_paginator(paginator_name)
            results: List[Dict] = []
            for page in paginator.paginate(AwsAccountId=self.account_id):
                results.extend(page.get(result_key, []))
            return results

        try:
            return _run()
        except Exception as e:
            if self._refresh_on_expired(e):
                return _run()
            raise

    # ------------------------------------------------------------------
    # Authentication helpers
    # ------------------------------------------------------------------

    @staticmethod
    def check_auth(
        profile: Optional[str] = None, region: Optional[str] = None
    ) -> dict:
        """Check if AWS credentials are valid.

        Returns:
            dict with ``valid`` (bool), ``identity`` (dict | None), ``error``.
        """
        try:
            if profile:
                session = boto3.Session(
                    profile_name=profile,
                    region_name=region or "us-east-1",
                )
            else:
                session = boto3.Session(region_name=region or "us-east-1")
            sts = session.client("sts")
            identity = sts.get_caller_identity()
            return {"valid": True, "identity": identity, "error": None}
        except Exception as exc:
            return {"valid": False, "identity": None, "error": str(exc)}

    def is_authenticated(self) -> bool:
        """Check if the current session credentials are valid."""
        return self.check_auth(self.profile, self.region)["valid"]

    # ------------------------------------------------------------------
    # Credential refresh internals
    # ------------------------------------------------------------------

    def _refresh_on_expired(self, error: Exception) -> bool:
        """If *error* is an ExpiredToken, refresh credentials and return True."""
        err_str = str(error)
        if "ExpiredToken" not in err_str and "expired" not in err_str.lower():
            return False

        logger.warning("AWS credentials expired, attempting recovery...")

        # Phase 1: new session (maybe creds were refreshed externally)
        try:
            self._init_session()
            if self.account_id:
                logger.info(
                    "Session refresh successful (account=%s)", self.account_id
                )
                return True
        except Exception:
            pass

        # Phase 2: run saml2aws
        logger.info("Session refresh insufficient, running saml2aws...")
        if self._reauthenticate():
            try:
                self._init_session()
                if not self.account_id:
                    sts = self.session.client("sts")
                    self.account_id = sts.get_caller_identity()["Account"]
                logger.info("Recovery complete (account=%s)", self.account_id)
                return True
            except Exception as e:
                logger.error("Failed after saml2aws: %s", e)

        logger.error(
            "Could not refresh credentials. Run 'saml2aws login' manually "
            "or set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY."
        )
        return False

    def _reauthenticate(self) -> bool:
        """Run saml2aws login for automatic credential refresh."""
        profile = self.profile or "default"
        cmd = [
            "saml2aws",
            "login",
            "--skip-prompt",
            "--profile",
            profile,
            "--force",
            "--session-duration",
            "43200",
        ]
        saml_role = self._settings.saml_role
        if saml_role:
            cmd.extend(["--role", saml_role])

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=60,
            )
            if result.returncode == 0:
                logger.info("saml2aws re-authentication successful")
                return True
            logger.warning("saml2aws failed: %s", result.stderr.strip()[:200])
        except FileNotFoundError:
            logger.info("saml2aws not found, skipping automatic re-auth")
        except subprocess.TimeoutExpired:
            logger.warning("saml2aws timed out after 60s")
        except Exception as e:
            logger.warning("saml2aws error: %s", e)

        return False
