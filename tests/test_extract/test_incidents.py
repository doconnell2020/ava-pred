"""Tests for the extract.incidents module."""

from unittest.mock import MagicMock, patch

import pytest
from extract.incidents import generate_incident_urls


class TestGenerateIncidentUrls:
    """Tests for generate_incident_urls function."""

    def test_single_page(self) -> None:
        """Test URL generation when there's only one page.

        Note: The code uses (count // per_page) + 1, so for a single page we need
        count < per_page (e.g., 3 items with 5 per page returns 1 page).
        """
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "count": 3,
            "results": [{"id": i} for i in range(5)],  # 5 results per page
        }

        with patch("requests.get", return_value=mock_response):
            urls = generate_incident_urls("https://test.api/incidents/?format=json")

        assert len(urls) == 1
        assert urls[0] == "https://test.api/incidents/?format=json"

    def test_multiple_pages(self) -> None:
        """Test URL generation with pagination."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "count": 25,
            "results": [{"id": i} for i in range(10)],
        }

        with patch("requests.get", return_value=mock_response):
            urls = generate_incident_urls("https://test.api/incidents/?format=json")

        assert len(urls) == 3
        assert urls[0] == "https://test.api/incidents/?format=json"
        assert urls[1] == "https://test.api/incidents/?format=json&page=2"
        assert urls[2] == "https://test.api/incidents/?format=json&page=3"

    def test_empty_results(self) -> None:
        """Test URL generation with empty results."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "count": 0,
            "results": [],
        }

        with patch("requests.get", return_value=mock_response):
            urls = generate_incident_urls("https://test.api/incidents/?format=json")

        assert len(urls) == 1

    def test_api_error(self) -> None:
        """Test that API errors raise ExtractionError."""
        from common.exceptions import ExtractionError

        mock_response = MagicMock()
        mock_response.status_code = 500

        with (
            patch("requests.get", return_value=mock_response),
            pytest.raises(ExtractionError),
        ):
            generate_incident_urls("https://test.api/incidents/?format=json")
