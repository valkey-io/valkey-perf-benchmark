"""Unit tests for ClientRunner._expand_scenario_options."""


class TestExpandScenarioNoOptions:
    """Test scenarios with no options return single-element list."""

    def test_no_options_key_returns_single_element(self, minimal_client_runner):
        """Scenario without 'options' key returns list with original scenario."""
        scenario = {"id": "test1", "command": "SET foo bar", "type": "write"}
        result = minimal_client_runner._expand_scenario_options(scenario)

        assert result == [scenario]
        assert len(result) == 1

    def test_empty_options_returns_single_element(self, minimal_client_runner):
        """Scenario with empty options dict returns list with original scenario."""
        scenario = {"id": "test1", "command": "GET key", "options": {}}
        result = minimal_client_runner._expand_scenario_options(scenario)

        assert result == [scenario]
        assert len(result) == 1

    def test_none_options_returns_single_element(self, minimal_client_runner):
        """Scenario with options=None returns list with original scenario."""
        scenario = {"id": "test1", "command": "GET key", "options": None}
        result = minimal_client_runner._expand_scenario_options(scenario)

        assert result == [scenario]
        assert len(result) == 1


class TestExpandScenarioWithOptions:
    """Test scenarios with options return correct variants."""

    def test_single_option_returns_one_variant(self, minimal_client_runner):
        """Single option produces one variant with id suffix and command flag."""
        scenario = {
            "id": "test1",
            "command": "SET foo bar",
            "options": {"--threads 4": "_4t"},
        }
        result = minimal_client_runner._expand_scenario_options(scenario)

        assert len(result) == 1
        assert result[0]["id"] == "test1_4t"
        assert result[0]["command"] == "SET foo bar --threads 4"

    def test_multiple_options_returns_correct_count(self, minimal_client_runner):
        """Multiple options produce one variant per option."""
        scenario = {
            "id": "bench",
            "command": "GET key",
            "options": {
                "--threads 1": "_1t",
                "--threads 4": "_4t",
                "--threads 8": "_8t",
            },
        }
        result = minimal_client_runner._expand_scenario_options(scenario)

        assert len(result) == 3
        ids = {v["id"] for v in result}
        assert ids == {"bench_1t", "bench_4t", "bench_8t"}

    def test_variant_command_has_flag_appended(self, minimal_client_runner):
        """Each variant's command has the flag appended."""
        scenario = {
            "id": "s1",
            "command": "SET k v",
            "options": {"--pipeline 10": "_p10", "--pipeline 20": "_p20"},
        }
        result = minimal_client_runner._expand_scenario_options(scenario)

        commands = {v["command"] for v in result}
        assert "SET k v --pipeline 10" in commands
        assert "SET k v --pipeline 20" in commands

    def test_empty_flag_key_no_extra_space(self, minimal_client_runner):
        """An empty-string flag key doesn't append extra text to command."""
        scenario = {
            "id": "base",
            "command": "GET key",
            "options": {"": "_default"},
        }
        result = minimal_client_runner._expand_scenario_options(scenario)

        assert len(result) == 1
        assert result[0]["id"] == "base_default"
        assert result[0]["command"] == "GET key"

    def test_description_updated_when_flag_present(self, minimal_client_runner):
        """Variant description gets ' + flag' appended when flag is non-empty."""
        scenario = {
            "id": "s1",
            "command": "SET k v",
            "description": "Set test",
            "options": {"--threads 4": "_4t"},
        }
        result = minimal_client_runner._expand_scenario_options(scenario)

        assert result[0]["description"] == "Set test + --threads 4"

    def test_description_not_updated_for_empty_flag(self, minimal_client_runner):
        """Variant description is unchanged when flag is empty string."""
        scenario = {
            "id": "s1",
            "command": "SET k v",
            "description": "Set test",
            "options": {"": "_default"},
        }
        result = minimal_client_runner._expand_scenario_options(scenario)

        assert result[0]["description"] == "Set test"

    def test_original_scenario_not_mutated(self, minimal_client_runner):
        """Expanding options does not modify the original scenario dict."""
        scenario = {
            "id": "orig",
            "command": "GET key",
            "options": {"--threads 2": "_2t"},
        }
        original_id = scenario["id"]
        original_cmd = scenario["command"]

        minimal_client_runner._expand_scenario_options(scenario)

        assert scenario["id"] == original_id
        assert scenario["command"] == original_cmd
