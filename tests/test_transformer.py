from datetime import date, datetime

from app.ingestion.transformer import BreezeTransformer


def test_transformer_normalizes_breeze_success_payload() -> None:
    payload = {
        "Success": [
            {
                "datetime": "2026-05-01 09:15:00",
                "expiry_date": "2026-05-07",
                "strike_price": "22500",
                "right": "Call",
                "open": "101.5",
                "high": "110.0",
                "low": "99.0",
                "close": "105.0",
                "volume": "1200",
                "open_interest": "23000",
            }
        ]
    }

    rows = BreezeTransformer().to_ohlcv_rows(payload, underlying="NIFTY", exchange="NFO")

    assert len(rows) == 1
    assert rows[0].datetime == datetime(2026, 5, 1, 9, 15)
    assert rows[0].expiry_date == date(2026, 5, 7)
    assert rows[0].option_type == "CE"
    assert rows[0].volume == 1200
    assert rows[0].open_interest == 23000

