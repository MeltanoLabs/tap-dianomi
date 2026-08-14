"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_tap_test_class

from tap_dianomi.tap import TapDianomi

SAMPLE_CONFIG = {
    "api_key": "test-api-key",
    "email": "test@example.com",
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
}


# Run standard built-in tap tests from the SDK:
TestTapDianomi = get_tap_test_class(
    tap_class=TapDianomi,
    config=SAMPLE_CONFIG,
)
