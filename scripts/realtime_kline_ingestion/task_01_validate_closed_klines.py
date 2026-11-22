"""Airflow task that validates closed Binance Spot kline event envelopes."""

from decimal import Decimal, InvalidOperation

from airflow.sdk import task


PRICE_FIELDS = ("o", "h", "l", "c")
NON_NEGATIVE_FIELDS = ("v", "q", "V", "Q")


def _finite_decimal(value, field_name, kafka_offset):
    """Parse a Binance numeric string without losing decimal precision."""
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as error:
        raise ValueError(
            f"Invalid {field_name} in Kafka offset {kafka_offset}."
        ) from error
    if not number.is_finite():
        raise ValueError(f"Non-finite {field_name} in Kafka offset {kafka_offset}.")
    return number


def _validate_envelope(envelope):
    """Check Kafka coordinates and the closed candle's required fields."""
    required_envelope_fields = {
        "payload", "kafka_topic", "kafka_partition", "kafka_offset"
    }
    if not isinstance(envelope, dict) or not required_envelope_fields.issubset(envelope):
        raise ValueError("Kafka trigger event is missing its payload or coordinates.")

    payload = envelope["payload"]
    if not isinstance(payload, dict) or payload.get("e") != "kline":
        raise ValueError("Kafka event is not a Binance kline object.")

    kline = payload.get("k")
    if not isinstance(kline, dict) or kline.get("x") is not True:
        raise ValueError("Only final (x=true) kline messages can be stored.")

    symbol = payload.get("s")
    interval = kline.get("i")
    if not isinstance(symbol, str) or not symbol.strip():
        raise ValueError("Kline event has no valid symbol.")
    if kline.get("s") != symbol:
        raise ValueError("Top-level and kline symbols do not match.")
    if not isinstance(interval, str) or not interval.strip():
        raise ValueError("Kline event has no valid interval.")

    open_time = kline.get("t")
    close_time = kline.get("T")
    if (not isinstance(open_time, int) or isinstance(open_time, bool)
            or not isinstance(close_time, int) or isinstance(close_time, bool)
            or open_time > close_time):
        raise ValueError("Kline open/close timestamps are invalid.")

    prices = {
        name: _finite_decimal(kline.get(field), field, envelope["kafka_offset"])
        for name, field in zip(("open", "high", "low", "close"), PRICE_FIELDS)
    }
    amounts = {
        field: _finite_decimal(kline.get(field), field, envelope["kafka_offset"])
        for field in NON_NEGATIVE_FIELDS
    }
    if any(value <= 0 for value in prices.values()):
        raise ValueError("Kline prices must be positive.")
    if any(value < 0 for value in amounts.values()):
        raise ValueError("Kline volumes must be zero or positive.")
    if (prices["high"] < max(prices["open"], prices["close"], prices["low"])
            or prices["low"] > min(prices["open"], prices["close"], prices["high"])):
        raise ValueError("Kline high/low values do not contain the candle prices.")

    return envelope


@task
def validate_closed_klines(triggering_asset_events):
    """Validate all Kafka events that triggered this DAG run.

    The DAG should pass its one watched asset's event list to this callable.
    Returning the original envelopes keeps Bronze payloads unchanged.
    """
    envelopes = []
    for asset_events in (triggering_asset_events or {}).values():
        for asset_event in asset_events:
            extra = getattr(asset_event, "extra", {}) or {}
            envelopes.append(_validate_envelope(extra.get("payload")))

    if not envelopes:
        raise ValueError("This DAG run has no Kafka kline events to validate.")
    return envelopes
