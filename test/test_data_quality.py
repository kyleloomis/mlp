"""
API and Database tests for MLP pipeline
"""

import sqlite3
import tempfile

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from api.service import APIService
from pipeline.config.run_configuration import RunConfiguration
from pipeline.sink.database_sink import DatabaseSink


@pytest.fixture(scope="module")
def test_config():
    """Fixture to set up test configuration"""
    temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    config = RunConfiguration(
        firm_crd_numbers=[160882],
        db_path=temp_db.name,
        input_dir="./temp",
        output_dir="./temp",
        verbose=False
    )
    yield config
    temp_db.close()


@pytest.fixture(scope="module")
def db_manager(test_config):
    """Fixture to set up database manager"""
    db_manager = DatabaseSink(test_config)
    yield db_manager


@pytest.fixture(scope="module")
def api_client(test_config):
    """Fixture to set up FastAPI client"""
    api_service = APIService(test_config)
    client = TestClient(api_service.app)
    yield client


@pytest.fixture(scope="module", autouse=True)
def setup_test_data(db_manager):
    """Populate test database with sample data and verify insertion"""
    firm_data = {
        160882: {
            'firm_crd_nb': 160882,
            "sec_nb": "801-12345",
            "business_name": "Test Firm 1",
            "full_legal_name": "Test Firm One LLC",
            "address": "123 Test St, Test City, TS 12345",
            "phone_number": "555-1234",
            "employee_count": 50,
            "signatory": "John Doe",
            "compensation_arrangements": ["Percentage of AUM", "Performance-based fees"],
            "client_types": {"Individuals": 5000000, "Corporations": 10000000},
            "private_funds": {
                "XYZ Fund": "805-123456"
            },
        }
    }

    db_manager.write(firm_data)

    # Verify that the firm was inserted
    with sqlite3.connect(db_manager.db_path) as conn:
        df = pd.read_sql_query("SELECT * FROM firms", conn)
        assert not df.empty, "Firm data was not written to the database"
        assert "160882" in df["firm_crd_nb"].astype(str).values, "Firm CRD 160882 missing"


# --- TEST DATABASE INTEGRITY ---
class TestDatabaseIntegrity:
    """Test database structure and data integrity"""

    def test_database_tables_exist(self, db_manager):
        """Ensure required tables exist"""
        with sqlite3.connect(db_manager.db_path) as conn:
            tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", conn)
            assert set(["firms", "compensation_arrangements", "client_types", "private_funds"]).issubset(
                set(tables["name"])
            )

    def test_firm_data_exists(self, db_manager):
        """Ensure firms table contains expected data"""
        with sqlite3.connect(db_manager.db_path) as conn:
            firms = pd.read_sql_query("SELECT * FROM firms", conn)
            assert len(firms) == 1
            assert "160882" in firms["firm_crd_nb"].astype(str).values

    def test_compensation_arrangements(self, db_manager):
        """Ensure compensation arrangements are stored correctly"""
        data = db_manager.fetch_compensation(160882)
        assert set(data) == {"Percentage of AUM", "Performance-based fees"}

    def test_client_types(self, db_manager):
        """Ensure client types and AUM are correctly stored"""
        data = db_manager.fetch_client_types(160882)
        assert data == {"Individuals": 5000000, "Corporations": 10000000}

    def test_private_funds(self, db_manager):
        """Ensure private funds are correctly stored"""
        data = db_manager.fetch_private_funds(160882)
        assert data == [{"name": "XYZ Fund", "identification_number": "805-123456"}]


# --- TEST API ENDPOINTS ---
class TestAPIEndpoints:
    """Test API endpoints for fetching firm data"""

    def test_get_firms(self, api_client):
        """Test GET /firms endpoint"""
        response = api_client.get("/firms")
        assert response.status_code == 200
        assert isinstance(response.json(), list)

    def test_get_firm_by_id(self, api_client):
        """Test GET /firms/{firm_crd_nb} endpoint"""
        response = api_client.get("/firms/160882")
        assert response.status_code == 200
        assert response.json()["firm_crd_nb"] == 160882

    def test_get_nonexistent_firm(self, api_client):
        """Test GET /firms/{firm_crd_nb} with invalid firm"""
        response = api_client.get("/firms/999999")
        assert response.status_code == 404

    def test_get_compensation(self, api_client):
        """Test GET /compensation/{firm_crd_nb}"""
        response = api_client.get("/compensation/160882")
        assert response.status_code == 200
        assert set(response.json()["compensation"]) == {"Percentage of AUM", "Performance-based fees"}

    def test_get_client_types(self, api_client):
        """Test GET /client_types/{firm_crd_nb}"""
        response = api_client.get("/client_types/160882")
        assert response.status_code == 200
        assert response.json()["client_types"] == {"Individuals": 5000000, "Corporations": 10000000}

    def test_get_private_funds(self, api_client):
        """Test GET /private_funds/{firm_crd_nb}"""
        response = api_client.get("/private_funds/160882")
        assert response.status_code == 200
        assert response.json()["private_funds"] == [{"name": "XYZ Fund", "identification_number": "805-123456"}]


# --- TEST API RESPONSE VALIDATION ---
class TestAPIResponseValidation:
    """Validate response data structure"""

    def test_firm_response_format(self, api_client):
        """Ensure firm data response structure matches expected format"""
        response = api_client.get("/firms/160882")
        assert response.status_code == 200
        firm_data = response.json()

        expected_fields = {
            "firm_crd_nb",
            "sec_nb",
            "business_name",
            "full_legal_name",
            "address",
            "phone_number",
            "employee_count",
            "signatory",
        }
        assert expected_fields.issubset(firm_data.keys())

    def test_compensation_format(self, api_client):
        """Ensure compensation response structure is valid"""
        response = api_client.get("/compensation/160882")
        assert response.status_code == 200
        assert "compensation" in response.json()
        assert isinstance(response.json()["compensation"], list)

    def test_client_types_format(self, api_client):
        """Ensure client types response format is correct"""
        response = api_client.get("/client_types/160882")
        assert response.status_code == 200
        assert "client_types" in response.json()
        assert isinstance(response.json()["client_types"], dict)

    def test_private_funds_format(self, api_client):
        """Ensure private funds response format is valid"""
        response = api_client.get("/private_funds/160882")
        assert response.status_code == 200
        assert "private_funds" in response.json()
        assert isinstance(response.json()["private_funds"], list)
        assert "name" in response.json()["private_funds"][0]
        assert "identification_number" in response.json()["private_funds"][0]


# --- RUN TESTS ---
if __name__ == "__main__":
    pytest.main()
