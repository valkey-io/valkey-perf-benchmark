"""Unit tests for ClientRunner._parse_csv_row and _find_csv_start."""


class TestFindCsvStart:
    """Test _find_csv_start finds correct header line index."""

    def test_finds_quoted_header(self, minimal_client_runner):
        """Finds index of a quoted CSV header line."""
        lines = [
            "some preamble output",
            "more preamble",
            '"test","rps","avg_latency_ms","min_latency_ms"',
            '"GET","150000.00","0.500","0.100"',
        ]
        assert minimal_client_runner._find_csv_start(lines) == 2

    def test_finds_unquoted_header(self, minimal_client_runner):
        """Finds index of an unquoted CSV header line."""
        lines = [
            "preamble",
            "test,rps,avg_latency_ms",
            "GET,150000.00,0.500",
        ]
        assert minimal_client_runner._find_csv_start(lines) == 1

    def test_returns_none_when_no_header(self, minimal_client_runner):
        """Returns None when no CSV header is present."""
        lines = ["no csv here", "just random output"]
        assert minimal_client_runner._find_csv_start(lines) is None

    def test_returns_none_for_empty_lines(self, minimal_client_runner):
        """Returns None for an empty list of lines."""
        assert minimal_client_runner._find_csv_start([]) is None

    def test_header_at_first_line(self, minimal_client_runner):
        """Finds header when it is the very first line."""
        lines = ['"test","rps","avg"', '"SET","100000","0.3"']
        assert minimal_client_runner._find_csv_start(lines) == 0


class TestParseCsvRow:
    """Test _parse_csv_row parses benchmark CSV output."""

    def test_valid_csv_returns_parsed_dict(self, minimal_client_runner):
        """Valid CSV output with header and data row returns a dict."""
        stdout = (
            "some preamble\n"
            '"test","rps","avg_latency_ms","min_latency_ms"\n'
            '"GET","150000.00","0.500","0.100"'
        )
        result = minimal_client_runner._parse_csv_row(stdout)
        assert result is not None
        assert result["test"] == "GET"
        assert result["rps"] == "150000.00"
        assert result["avg_latency_ms"] == "0.500"
        assert result["min_latency_ms"] == "0.100"

    def test_empty_string_returns_none(self, minimal_client_runner):
        """Empty string input returns None."""
        assert minimal_client_runner._parse_csv_row("") is None

    def test_none_input_returns_none(self, minimal_client_runner):
        """None input returns None."""
        assert minimal_client_runner._parse_csv_row(None) is None

    def test_no_csv_header_returns_none(self, minimal_client_runner):
        """Output without a CSV header returns None."""
        stdout = "just some random benchmark output\nno csv data here"
        assert minimal_client_runner._parse_csv_row(stdout) is None

    def test_header_only_no_data_returns_none(self, minimal_client_runner):
        """CSV header present but no data rows returns None."""
        stdout = '"test","rps","avg_latency_ms"'
        assert minimal_client_runner._parse_csv_row(stdout) is None

    def test_returns_first_row_only(self, minimal_client_runner):
        """When multiple data rows exist, only the first is returned."""
        stdout = (
            '"test","rps","avg_latency_ms"\n'
            '"GET","150000.00","0.500"\n'
            '"SET","120000.00","0.600"'
        )
        result = minimal_client_runner._parse_csv_row(stdout)
        assert result is not None
        assert result["test"] == "GET"
        assert result["rps"] == "150000.00"
