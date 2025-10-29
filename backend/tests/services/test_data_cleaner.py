from datetime import date
from decimal import Decimal

from app.services.data_cleaner import TableDataCleaner


def test_clean_capital_calls_normalizes_and_deduplicates():
    cleaner = TableDataCleaner()
    tables = {
        "capital_calls": [
            {
                "call_date": "2023-01-15",
                "call_type": "  Regular ",
                "amount": "$1,000.00",
                "description": "Initial ",
            },
            {
                "call_date": date(2023, 1, 15),
                "call_type": "Regular",
                "amount": Decimal("1000.00"),
                "description": "Initial",
            },
        ],
        "distributions": [],
        "adjustments": [],
    }

    cleaned, issues = cleaner.clean(tables)

    assert len(cleaned["capital_calls"]) == 1
    row = cleaned["capital_calls"][0]
    assert row["call_date"] == date(2023, 1, 15)
    assert row["amount"] == Decimal("1000.00")
    assert row["call_type"] == "Regular"
    assert row["description"] == "Initial"
    assert issues["capital_calls"] == []


def test_clean_distributions_rejects_invalid_rows():
    cleaner = TableDataCleaner()
    tables = {
        "capital_calls": [],
        "distributions": [
            {
                "distribution_date": "2023-02-01",
                "amount": "-100.00",
            },
            {
                "distribution_date": None,
                "amount": "250.00",
            },
            {
                "distribution_date": "2023-02-05",
                "amount": "250.50",
                "distribution_type": "Return of Capital",
                "is_recallable": "yes",
                "description": " round trip ",
            },
        ],
        "adjustments": [],
    }

    cleaned, issues = cleaner.clean(tables)

    assert len(cleaned["distributions"]) == 1
    row = cleaned["distributions"][0]
    assert row["distribution_date"] == date(2023, 2, 5)
    assert row["amount"] == Decimal("250.50")
    assert row["distribution_type"] == "Return of Capital"
    assert row["is_recallable"] is True
    assert row["description"] == "round trip"
    # Two invalid rows should be reported
    assert len(issues["distributions"]) == 2


def test_clean_adjustments_allows_negative_amount():
    cleaner = TableDataCleaner()
    tables = {
        "capital_calls": [],
        "distributions": [],
        "adjustments": [
            {
                "adjustment_date": "2023-03-10",
                "amount": "-75.123",
                "adjustment_type": "Fee",
                "category": "Management",
                "is_contribution_adjustment": "true",
            }
        ],
    }

    cleaned, issues = cleaner.clean(tables)

    assert len(cleaned["adjustments"]) == 1
    row = cleaned["adjustments"][0]
    assert row["amount"] == Decimal("-75.12")
    assert row["is_contribution_adjustment"] is True
    assert issues["adjustments"] == []
