"""Unit tests for small AQUA console helpers."""

import io
import sys

import pytest

from aqua.core.console.util import query_yes_no


@pytest.fixture
def run_query_with_input():
    def _run_query(input_text, default_answer):
        old_stdin = sys.stdin
        try:
            content = input_text if input_text.endswith("\n") else input_text + "\n"
            sys.stdin = io.StringIO(content)
            result = query_yes_no("Question?", default_answer)
        finally:
            sys.stdin = old_stdin
        return result

    return _run_query


@pytest.mark.console
class TestQueryYesNo:
    """Tests for query_yes_no."""

    def test_query_yes_no_invalid_input(self, run_query_with_input):
        result = run_query_with_input("invalid\nyes", "yes")
        assert result is True

    def test_query_yes_no_explicit_yes(self, run_query_with_input):
        result = run_query_with_input("yes", "no")
        assert result is True

    def test_query_yes_no_explicit_no(self, run_query_with_input):
        result = run_query_with_input("no", "yes")
        assert result is False

    @pytest.mark.parametrize("default_answer, expected", [("yes", True), ("no", False)])
    def test_query_yes_no_default(self, run_query_with_input, default_answer, expected):
        """Just pressing Enter gives the default answer"""
        assert run_query_with_input("", default_answer) is expected

    @pytest.mark.parametrize("answer, expected", [("y", True), ("ye", True), ("n", False), ("YES", True), ("No", False)])
    def test_query_yes_no_short_and_uppercase_answers(self, run_query_with_input, answer, expected):
        assert run_query_with_input(answer, "yes") is expected

    def test_query_yes_no_without_default_asks_again_on_enter(self, run_query_with_input):
        """With no default an empty answer is not accepted: the question is asked again"""
        assert run_query_with_input("\nno", None) is False

    def test_query_yes_no_invalid_default(self, run_query_with_input):
        with pytest.raises(ValueError, match="invalid default answer: maybe"):
            run_query_with_input("yes", "maybe")
