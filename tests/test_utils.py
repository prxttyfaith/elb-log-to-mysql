import pytest
from etl_elb_log_to_mysql import to_int, to_float, parse_log_entry

def test_to_int_basic():
    assert to_int("123") == 123
    assert to_int("abc") == 0
    assert to_int("") == 0

def test_to_float_basic():
    assert to_float("3.14") == 3.14
    assert to_float("xyz") == 0.0
    assert to_float("") == 0.0

# def test_parse_log_entry_valid():
#     sample_line = (
#         'h2 2025-05-26T23:55:02.179979Z app/erank-app/xxxxxxx 1.2.3.4:5678 '
#         '5.6.7.8:80 0.001 0.303 0.000 200 200 74 1013 '
#         '"POST https://example.com:443/api/browser-ext-user HTTP/2.0" '
#         '"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
#         'Chrome/137.0.0.0 Safari/537.36" TLS_AES_128_GCM_SHA256 TLSv1.3 '
#         'arn:aws:elasticloadbalancing:region:accountid:targetgroup/example-app-v3-production/xxxxxxxx '
#         '"Root=1-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" "example.com" "session-reused" 1 2025-05-26T23:55:01.875000Z '
#         '"waf,forward" "-" "-" "5.6.7.8:80" "200" "-" "-" TID_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
#     )
#     result = parse_log_entry(sample_line, "dummy.log")
#     assert result is not None, "Should parse valid log line"
#     assert result["client_ip"] == "1.2.3.4"
#     assert result["http_method"] == "POST"
#     assert result["requested_path"] == "/api/browser-ext-user"
#     assert result["elb_status_code"] == 200
#     assert "user_agent_full" in result
#     assert result["user_agent_full"].startswith("Mozilla/")

# def test_parse_log_entry_invalid():
#     # Not enough parts
#     bad_line = "only this"
#     assert parse_log_entry(bad_line, "dummy.log") is None

#     # Invalid timestamp
#     bad_line2 = (
#         'h2 badtimestamp app/erank-app/xxxxxxx 1.2.3.4:5678 5.6.7.8:80 '
#         '0.001 0.303 0.000 200 200 74 1013 "POST /test HTTP/2.0" "-" TLS_AES_128_GCM_SHA256 TLSv1.3 x'
#     )
#     assert parse_log_entry(bad_line2, "dummy.log") is None
