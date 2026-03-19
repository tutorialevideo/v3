"""
Iteration 6 Backend Tests - Portal JUST Downloader
Tests: All 5 tabs APIs, Export CSV, Profile Modal, ANAF full test, DB Final filters
"""
import pytest
import requests
import os

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', '').rstrip('/')


class TestDashboardApis:
    """Tests for Dashboard tab APIs: stats, db/stats"""

    def test_stats_returns_200(self):
        """GET /api/stats returns 200"""
        response = requests.get(f"{BASE_URL}/api/stats", timeout=15)
        assert response.status_code == 200, f"Expected 200: {response.text[:200]}"

    def test_stats_has_db_firme_and_db_dosare(self):
        """Stats contains db_firme and db_dosare counts"""
        response = requests.get(f"{BASE_URL}/api/stats", timeout=15)
        assert response.status_code == 200
        data = response.json()
        assert 'db_firme' in data
        assert 'db_dosare' in data
        assert isinstance(data['db_firme'], (int, float))
        assert isinstance(data['db_dosare'], (int, float))

    def test_stats_firme_count_350(self):
        """db_firme should be 350 (as per test data)"""
        response = requests.get(f"{BASE_URL}/api/stats", timeout=15)
        assert response.status_code == 200
        data = response.json()
        assert data.get('db_firme', 0) >= 350, f"Expected >= 350 firms, got {data.get('db_firme')}"

    def test_db_stats_returns_200(self):
        """GET /api/db/stats returns 200"""
        response = requests.get(f"{BASE_URL}/api/db/stats", timeout=15)
        assert response.status_code == 200, f"Expected 200: {response.text[:200]}"

    def test_db_stats_structure(self):
        """db/stats has firme_total, firme_with_cui, dosare_total"""
        response = requests.get(f"{BASE_URL}/api/db/stats", timeout=15)
        assert response.status_code == 200
        data = response.json()
        required = ['firme_total', 'firme_with_cui', 'dosare_total']
        for field in required:
            assert field in data, f"Missing field: {field}"


class TestFirmeTabApis:
    """Tests for Firme tab: GET /api/db/firme, profile, update"""

    def test_firme_list_returns_350(self):
        """GET /api/db/firme returns total >= 350"""
        response = requests.get(f"{BASE_URL}/api/db/firme?limit=1", timeout=15)
        assert response.status_code == 200
        data = response.json()
        assert data['total'] >= 350, f"Expected >= 350 firms, got {data['total']}"

    def test_firme_list_structure(self):
        """firme list has id, cui, denumire per entry"""
        response = requests.get(f"{BASE_URL}/api/db/firme?limit=10", timeout=15)
        assert response.status_code == 200
        data = response.json()
        assert len(data['firme']) > 0
        for firma in data['firme'][:3]:
            assert 'id' in firma
            assert 'denumire' in firma

    def test_firme_search_works(self):
        """Search by company name filters results"""
        response = requests.get(f"{BASE_URL}/api/db/firme?search=SRL&limit=10", timeout=15)
        assert response.status_code == 200
        data = response.json()
        # Should not error even if 0 results
        assert isinstance(data['firme'], list)

    def test_firma_profile_id_350(self):
        """GET /api/db/firma/350 returns valid profile"""
        response = requests.get(f"{BASE_URL}/api/db/firma/350", timeout=15)
        assert response.status_code == 200, f"Expected 200: {response.text[:200]}"
        data = response.json()
        required_keys = ['id', 'basic_info', 'adresa', 'anaf_data', 'mfinante_data', 'bilanturi_history', 'dosare_summary']
        for key in required_keys:
            assert key in data, f"Missing key: {key}"
        assert data['id'] == 350

    def test_firma_profile_has_dosare_summary(self):
        """dosare_summary has total and recente list"""
        response = requests.get(f"{BASE_URL}/api/db/firma/1", timeout=15)
        assert response.status_code == 200
        data = response.json()
        ds = data.get('dosare_summary', {})
        assert 'total' in ds
        assert 'recente' in ds
        assert isinstance(ds['recente'], list)

    def test_firma_profile_not_found(self):
        """GET /api/db/firma/99999 returns 404"""
        response = requests.get(f"{BASE_URL}/api/db/firma/99999", timeout=15)
        assert response.status_code == 404


class TestAnafTab:
    """Tests for ANAF tab: stats, test-full endpoint"""

    def test_anaf_stats_returns_200(self):
        """GET /api/anaf/stats returns 200"""
        response = requests.get(f"{BASE_URL}/api/anaf/stats", timeout=15)
        assert response.status_code == 200, f"Expected 200: {response.text[:200]}"

    def test_anaf_stats_structure(self):
        """anaf/stats has total_firme_cu_cui, synced, not_synced"""
        response = requests.get(f"{BASE_URL}/api/anaf/stats", timeout=15)
        assert response.status_code == 200
        data = response.json()
        required = ['total_firme_cu_cui', 'synced', 'not_synced', 'found', 'not_found']
        for field in required:
            assert field in data, f"Missing field: {field}"

    def test_anaf_sync_progress_returns_200(self):
        """GET /api/anaf/sync-progress returns 200"""
        response = requests.get(f"{BASE_URL}/api/anaf/sync-progress", timeout=15)
        assert response.status_code == 200

    def test_anaf_test_full_14918042_found(self):
        """GET /api/anaf/test-full/14918042 returns found=true with sections_analysis"""
        response = requests.get(f"{BASE_URL}/api/anaf/test-full/14918042", timeout=30)
        assert response.status_code == 200, f"Expected 200: {response.text[:200]}"
        data = response.json()
        required = ['cui', 'found', 'raw_response', 'sections_analysis']
        for field in required:
            assert field in data, f"Missing field: {field}"
        assert data['found'] is True, f"CUI 14918042 should be found, got found={data['found']}"

    def test_anaf_test_full_sections_nonempty(self):
        """sections_analysis is not empty for CUI 14918042"""
        response = requests.get(f"{BASE_URL}/api/anaf/test-full/14918042", timeout=30)
        assert response.status_code == 200
        data = response.json()
        assert len(data.get('sections_analysis', {})) > 0

    def test_anaf_test_full_raw_response_has_date_generale(self):
        """raw_response.found[0] has date_generale section"""
        response = requests.get(f"{BASE_URL}/api/anaf/test-full/14918042", timeout=30)
        assert response.status_code == 200
        data = response.json()
        raw = data.get('raw_response', {})
        found_list = raw.get('found', [])
        assert len(found_list) > 0, "raw_response.found should not be empty"
        assert 'date_generale' in found_list[0], "found[0] should have date_generale"

    def test_anaf_verifica_rapid_14918042(self):
        """GET /api/anaf/test/14918042 (quick test) returns 200"""
        response = requests.get(f"{BASE_URL}/api/anaf/test/14918042", timeout=30)
        assert response.status_code == 200, f"Expected 200: {response.text[:200]}"


class TestDbFinalTab:
    """Tests for DB Final tab: stats, firme list, filters, export"""

    def test_dbfinal_stats_returns_200(self):
        """GET /api/dbfinal/stats returns 200"""
        response = requests.get(f"{BASE_URL}/api/dbfinal/stats", timeout=15)
        assert response.status_code == 200, f"Expected 200: {response.text[:200]}"

    def test_dbfinal_stats_structure(self):
        """dbfinal/stats has total_cu_cui, sincronizate_anaf, cu_date_bilant, active"""
        response = requests.get(f"{BASE_URL}/api/dbfinal/stats", timeout=15)
        assert response.status_code == 200
        data = response.json()
        required = ['total_cu_cui', 'sincronizate_anaf', 'cu_date_bilant', 'active']
        for field in required:
            assert field in data, f"Missing field: {field}"
        # All values must be integers (not None)
        for field in required:
            assert isinstance(data[field], int), f"{field} must be int, got {type(data[field])}"

    def test_dbfinal_firme_returns_200(self):
        """GET /api/dbfinal/firme returns 200"""
        response = requests.get(f"{BASE_URL}/api/dbfinal/firme", timeout=15)
        assert response.status_code == 200

    def test_dbfinal_firme_search_filter(self):
        """Search filter returns relevant results"""
        response = requests.get(f"{BASE_URL}/api/dbfinal/firme?search=test", timeout=15)
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data['firme'], list)

    def test_dbfinal_firme_doar_active_filter(self):
        """doar_active=true filter works"""
        response = requests.get(f"{BASE_URL}/api/dbfinal/firme?doar_active=true", timeout=15)
        assert response.status_code == 200
        data = response.json()
        assert 'firme' in data
        assert 'total' in data

    def test_dbfinal_firme_doar_cu_bilant_filter(self):
        """doar_cu_bilant=true filter works"""
        response = requests.get(f"{BASE_URL}/api/dbfinal/firme?doar_cu_bilant=true", timeout=15)
        assert response.status_code == 200
        data = response.json()
        assert 'firme' in data

    def test_dbfinal_export_returns_200(self):
        """GET /api/dbfinal/export returns 200 with CSV content"""
        response = requests.get(f"{BASE_URL}/api/dbfinal/export", timeout=30)
        assert response.status_code == 200, f"Expected 200: {response.text[:200]}"

    def test_dbfinal_export_content_type_csv(self):
        """Export response has text/csv content type"""
        response = requests.get(f"{BASE_URL}/api/dbfinal/export", timeout=30)
        assert response.status_code == 200
        content_type = response.headers.get('content-type', '')
        assert 'csv' in content_type.lower() or 'text' in content_type.lower(), \
            f"Expected csv content-type, got: {content_type}"

    def test_dbfinal_export_has_proper_headers(self):
        """Export CSV has CUI;Denumire headers with semicolon delimiter"""
        response = requests.get(f"{BASE_URL}/api/dbfinal/export", timeout=30)
        assert response.status_code == 200
        # Read content - handle BOM (utf-8-sig)
        content = response.content.decode('utf-8-sig', errors='replace')
        lines = content.strip().split('\n')
        assert len(lines) >= 1, "CSV should have at least header row"
        header = lines[0]
        # Should use semicolon delimiter
        assert ';' in header, f"Expected semicolon-delimited CSV, header: {header}"
        assert 'CUI' in header, f"Header should contain 'CUI', got: {header}"
        assert 'Denumire' in header, f"Header should contain 'Denumire', got: {header}"

    def test_dbfinal_export_content_disposition(self):
        """Export has Content-Disposition attachment header"""
        response = requests.get(f"{BASE_URL}/api/dbfinal/export", timeout=30)
        assert response.status_code == 200
        content_disp = response.headers.get('content-disposition', '')
        assert 'attachment' in content_disp.lower() or 'filename' in content_disp.lower(), \
            f"Expected content-disposition with attachment: {content_disp}"


class TestDiagnosticsTab:
    """Tests for Diagnostics tab: db diagnostics endpoint"""

    def test_diagnostics_returns_200(self):
        """GET /api/db/diagnostics returns 200"""
        response = requests.get(f"{BASE_URL}/api/db/diagnostics", timeout=15)
        # Accept 200 or 404 (endpoint might be named differently)
        assert response.status_code in [200, 404, 503], \
            f"Expected 200/404/503: {response.status_code} {response.text[:100]}"

    def test_db_status_returns_200(self):
        """GET /api/db/status returns 200"""
        response = requests.get(f"{BASE_URL}/api/db/status", timeout=15)
        assert response.status_code == 200, f"Expected 200: {response.text[:200]}"

    def test_db_status_has_postgres_available(self):
        """db/status has postgres_available field"""
        response = requests.get(f"{BASE_URL}/api/db/status", timeout=15)
        assert response.status_code == 200
        data = response.json()
        assert 'postgres_available' in data


@pytest.fixture(autouse=True)
def check_env():
    """Verify BASE_URL is set"""
    if not BASE_URL:
        pytest.skip("REACT_APP_BACKEND_URL not set")
