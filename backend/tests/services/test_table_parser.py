from decimal import Decimal

from app.services.table_parser import ParsedTable, TableParser


def test_parse_capital_calls_table():
    parser = TableParser()
    table = [
        ["Date", "Call Type", "Amount", "Description"],
        ["2023-01-15", "Regular", "$1,000.00", "Initial call"],
    ]

    result = parser.parse(table, page_number=1)

    assert result is not None
    assert result.table_type == "capital_calls"
    assert result.page_number == 1
    assert result.rows[0]["amount"] == Decimal("1000.00")


def test_parse_distributions_table():
    parser = TableParser()
    table = [
        ["Date", "Type", "Amount", "Recallable", "Description"],
        ["2023-02-01", "Return of Capital", "$500.50", "yes", "Distribution"],
    ]

    result = parser.parse(table, page_number=2)

    assert result is not None
    assert result.table_type == "distributions"
    row = result.rows[0]
    assert row["distribution_type"] == "Return of Capital"
    assert row["is_recallable"] is True


def test_parse_returns_none_for_unclassified_table():
    parser = TableParser()
    table = [["Col1", "Col2"], ["Value1", "Value2"]]

    result = parser.parse(table, page_number=1)

    assert result is None
