import httpx

from gateway.worker_client import parse_worker_credits_payload, parse_worker_credits_response


def _json_response(status_code, payload):
    return httpx.Response(status_code, json=payload)


def test_parse_credits_primary_field():
    assert parse_worker_credits_payload({"credits": 1050})["credits"] == 1050


def test_parse_explicit_zero_as_valid():
    result = parse_worker_credits_payload({"credits": 0})
    assert result["credits"] == 0
    assert result["credits_available"] is True


def test_parse_remaining_credits_field():
    result = parse_worker_credits_payload({"remainingCredits": 1035})
    assert result["credits"] == 1035
    assert result["credits_source"] == "remainingCredits"


def test_parse_subscription_credits_field():
    result = parse_worker_credits_payload({"subscriptionCredits": 1050})
    assert result["credits"] == 1050
    assert result["credits_source"] == "subscriptionCredits"


def test_missing_credits_are_unavailable_not_zero():
    result = parse_worker_credits_payload({})
    assert result["credits"] is None
    assert result["credits_error"] == "credits_missing"


def test_null_credits_are_unavailable_not_zero():
    result = parse_worker_credits_payload({"credits": None})
    assert result["credits"] is None
    assert result["credits_error"] == "credits_invalid"


def test_empty_string_credits_are_unavailable_not_zero():
    result = parse_worker_credits_payload({"credits": ""})
    assert result["credits"] is None
    assert result["credits_error"] == "credits_invalid"


def test_string_credits_are_unavailable_not_zero():
    result = parse_worker_credits_payload({"credits": "1050"})
    assert result["credits"] is None
    assert result["credits_error"] == "credits_invalid"


def test_unknown_string_credits_are_unavailable_not_zero():
    result = parse_worker_credits_payload({"credits": "unknown"})
    assert result["credits"] is None
    assert result["credits_error"] == "credits_invalid"


def test_negative_credits_are_unavailable_not_zero():
    result = parse_worker_credits_payload({"credits": -1})
    assert result["credits"] is None
    assert result["credits_error"] == "credits_invalid"


def test_same_values_across_fields_are_valid():
    result = parse_worker_credits_payload({"credits": 1050, "remainingCredits": 1050, "subscriptionCredits": 1050})
    assert result["credits"] == 1050
    assert result["credits_source"] == "credits"


def test_conflicting_credit_fields_are_unavailable():
    result = parse_worker_credits_payload({"credits": 1050, "remainingCredits": 1035})
    assert result["credits"] is None
    assert result["credits_error"] == "credits_conflict"


def test_http_error_keeps_credits_unavailable():
    result = parse_worker_credits_response(_json_response(403, {"error": "reCAPTCHA"}))
    assert result["credits"] is None
    assert result["credits_error"] == "credits_http_403"


def test_invalid_json_keeps_credits_unavailable():
    response = httpx.Response(200, content=b"{not-json")
    result = parse_worker_credits_response(response)
    assert result["credits"] is None
    assert result["credits_error"] == "credits_json_invalid"


def test_embedded_oauth_error_is_not_successful_credits():
    result = parse_worker_credits_payload({
        "error": {
            "code": 401,
            "message": "Request had invalid authentication credentials.",
            "status": "UNAUTHENTICATED",
        }
    })
    assert result["credits"] is None
    assert result["credits_available"] is False
    assert result["credits_error"] == "oauth_unauthenticated"
