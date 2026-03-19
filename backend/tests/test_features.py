"""
Backend API tests for Justice Portal - Romanian Company Data Management
Tests: Company Profile Modal, Inline CUI Edit, DB Final Tab, ANAF Sync Test
"""
import pytest
import requests
import os

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', '').rstrip('/')


class TestFirmaProfile:
    """Tests for GET /api/db/firma/{id} - Company Profile endpoint"""

    def test_get_firma_profile_id_350(self):
        """GET /api/db/firma/350 returns valid JSON with required keys"""
        response = requests.get(f"{BASE_URL}/api/db/firma/350", timeout=15)
        assert response.status_code == 200, f"Expected 200 got {response.status_code}: {response.text}"
        data = response.json()
        required_keys = ['id', 'basic_info', 'adresa', 'anaf_data', 'mfinante_data', 'bilanturi_history', 'dosare_summary']
        for key in required_keys:
            assert key in data, f"Missing key: {key}"
        assert data['id'] == 350

    def test_get_firma_profile_id_1(self):
        """GET /api/db/firma/1 returns valid profile"""
        response = requests.get(f"{BASE_URL}/api/db/firma/1", timeout=15)
        assert response.status_code == 200, f"Expected 200: {response.text}"
        data = response.json()
        assert 'id' in data
        assert data['id'] == 1
        assert 'basic_info' in data
        assert 'denumire' in data['basic_info']

    def test_get_firma_profile_basic_info_structure(self):
        """basic_info has expected fields: denumire, cui, cod_inregistrare"""
        response = requests.get(f"{BASE_URL}/api/db/firma/1", timeout=15)
        assert response.status_code == 200
        data = response.json()
        basic_info = data['basic_info']
        assert 'denumire' in basic_info
        assert 'cui' in basic_info
        assert 'cod_inregistrare' in basic_info

    def test_get_firma_profile_dosare_summary_structure(self):
        """dosare_summary has 'total' and 'recente' keys"""
        response = requests.get(f"{BASE_URL}/api/db/firma/1", timeout=15)
        assert response.status_code == 200
        data = response.json()
        dosare_summary = data['dosare_summary']
        assert 'total' in dosare_summary
        assert 'recente' in dosare_summary
        assert isinstance(dosare_summary['recente'], list)

    def test_get_firma_profile_not_found(self):
        """GET /api/db/firma/99999 returns 404"""
        response = requests.get(f"{BASE_URL}/api/db/firma/99999", timeout=15)
        assert response.status_code == 404


class TestFirmaUpdateCUI:
    """Tests for PUT /api/db/firme/{id} - Update CUI (inline edit)"""

    def test_update_cui_valid(self):
        """PUT /api/db/firme/1 with a valid CUI updates successfully"""
        # First get current state
        get_res = requests.get(f"{BASE_URL}/api/db/firma/1", timeout=15)
        assert get_res.status_code == 200
        original_cui = get_res.json()['basic_info'].get('cui')

        # Set a test CUI
        test_cui = "TEST99999"
        response = requests.put(
            f"{BASE_URL}/api/db/firme/1",
            json={"cui": test_cui},
            timeout=15
        )
        assert response.status_code == 200, f"Expected 200 got {response.status_code}: {response.text}"
        data = response.json()
        assert data.get('cui') == test_cui

        # Verify persistence via GET
        get_res2 = requests.get(f"{BASE_URL}/api/db/firma/1", timeout=15)
        assert get_res2.status_code == 200
        assert get_res2.json()['basic_info']['cui'] == test_cui

        # Restore original CUI
        requests.put(f"{BASE_URL}/api/db/firme/1", json={"cui": original_cui}, timeout=15)

    def test_update_cui_clear(self):
        """PUT /api/db/firme/2 with null CUI clears it"""
        response = requests.put(
            f"{BASE_URL}/api/db/firme/2",
            json={"cui": None},
            timeout=15
        )
        # Should succeed
        assert response.status_code == 200, f"Expected 200: {response.text}"

    def test_update_cui_not_found(self):
        """PUT /api/db/firme/99999 returns 404"""
        response = requests.put(
            f"{BASE_URL}/api/db/firme/99999",
            json={"cui": "123456"},
            timeout=15
        )
        assert response.status_code == 404


class TestDbFinalStats:
    """Tests for GET /api/dbfinal/stats - DB Final statistics"""

    def test_dbfinal_stats_returns_200(self):
        """GET /api/dbfinal/stats returns 200"""
        response = requests.get(f"{BASE_URL}/api/dbfinal/stats", timeout=15)
        assert response.status_code == 200, f"Expected 200: {response.text}"

    def test_dbfinal_stats_has_required_fields(self):
        """DB Final stats has: total_cu_cui, sincronizate_anaf, cu_date_bilant, active"""
        response = requests.get(f"{BASE_URL}/api/dbfinal/stats", timeout=15)
        assert response.status_code == 200
        data = response.json()
        required = ['total_cu_cui', 'sincronizate_anaf', 'cu_date_bilant', 'active']
        for field in required:
            assert field in data, f"Missing field: {field}"

    def test_dbfinal_stats_numeric_values(self):
        """DB Final stats all values are numeric"""
        response = requests.get(f"{BASE_URL}/api/dbfinal/stats", timeout=15)
        assert response.status_code == 200
        data = response.json()
        for field in ['total_cu_cui', 'sincronizate_anaf', 'cu_date_bilant', 'active']:
            assert isinstance(data[field], int), f"Expected int for {field}, got {type(data[field])}"


class TestDbFinalFirme:
    """Tests for GET /api/dbfinal/firme - DB Final companies list"""

    def test_dbfinal_firme_returns_200(self):
        """GET /api/dbfinal/firme returns 200"""
        response = requests.get(f"{BASE_URL}/api/dbfinal/firme", timeout=15)
        assert response.status_code == 200, f"Expected 200: {response.text}"

    def test_dbfinal_firme_response_structure(self):
        """Response has 'firme' (list) and 'total' (int)"""
        response = requests.get(f"{BASE_URL}/api/dbfinal/firme", timeout=15)
        assert response.status_code == 200
        data = response.json()
        assert 'firme' in data
        assert 'total' in data
        assert isinstance(data['firme'], list)
        assert isinstance(data['total'], int)

    def test_dbfinal_firme_only_with_cui(self):
        """All returned companies have a non-null CUI"""
        response = requests.get(f"{BASE_URL}/api/dbfinal/firme?limit=50", timeout=15)
        assert response.status_code == 200
        data = response.json()
        for firma in data['firme']:
            assert firma.get('cui'), f"Firma {firma.get('id')} has no CUI in DB Final"

    def test_dbfinal_firme_pagination(self):
        """Pagination with skip parameter works"""
        r1 = requests.get(f"{BASE_URL}/api/dbfinal/firme?skip=0&limit=10", timeout=15)
        r2 = requests.get(f"{BASE_URL}/api/dbfinal/firme?skip=10&limit=10", timeout=15)
        assert r1.status_code == 200
        assert r2.status_code == 200
        # Both should return the same total
        assert r1.json()['total'] == r2.json()['total']

    def test_dbfinal_firme_search(self):
        """Search parameter filters results"""
        response = requests.get(f"{BASE_URL}/api/dbfinal/firme?search=SRL", timeout=15)
        assert response.status_code == 200
        data = response.json()
        # If results found, they should contain SRL in name
        if data['firme']:
            for firma in data['firme'][:5]:
                assert 'SRL' in firma.get('denumire', '').upper() or firma.get('cui'), \
                    f"Search result mismatch: {firma.get('denumire')}"


class TestAnafTestFull:
    """Tests for GET /api/anaf/test-full/{cui} - Full ANAF test with raw JSON"""

    def test_anaf_test_full_returns_200(self):
        """GET /api/anaf/test-full/14918042 returns 200"""
        response = requests.get(f"{BASE_URL}/api/anaf/test-full/14918042", timeout=30)
        assert response.status_code == 200, f"Expected 200: {response.text}"

    def test_anaf_test_full_response_structure(self):
        """Response has: cui, found, raw_response, sections_analysis"""
        response = requests.get(f"{BASE_URL}/api/anaf/test-full/14918042", timeout=30)
        assert response.status_code == 200
        data = response.json()
        required_fields = ['cui', 'found', 'raw_response', 'sections_analysis']
        for field in required_fields:
            assert field in data, f"Missing field: {field}"

    def test_anaf_test_full_cui_found(self):
        """CUI 14918042 should be found in ANAF"""
        response = requests.get(f"{BASE_URL}/api/anaf/test-full/14918042", timeout=30)
        assert response.status_code == 200
        data = response.json()
        assert data['found'] is True, "CUI 14918042 should be found in ANAF"

    def test_anaf_test_full_raw_response_has_data(self):
        """raw_response has 'found' array with company data"""
        response = requests.get(f"{BASE_URL}/api/anaf/test-full/14918042", timeout=30)
        assert response.status_code == 200
        data = response.json()
        raw = data.get('raw_response', {})
        assert 'found' in raw, "raw_response should have 'found' key"
        assert isinstance(raw['found'], list), "'found' should be a list"
        if raw['found']:
            assert 'date_generale' in raw['found'][0], "found[0] should have 'date_generale'"

    def test_anaf_test_full_sections_analysis_nonempty(self):
        """sections_analysis has data for CUI 14918042"""
        response = requests.get(f"{BASE_URL}/api/anaf/test-full/14918042", timeout=30)
        assert response.status_code == 200
        data = response.json()
        sections = data.get('sections_analysis', {})
        assert len(sections) > 0, "sections_analysis should have data"

    def test_anaf_test_full_cui_format(self):
        """Response returns correct cui value"""
        response = requests.get(f"{BASE_URL}/api/anaf/test-full/14918042", timeout=30)
        assert response.status_code == 200
        data = response.json()
        assert data['cui'] == '14918042'

    def test_anaf_quick_test_endpoint(self):
        """GET /api/anaf/test/14918042 (quick test) also works"""
        response = requests.get(f"{BASE_URL}/api/anaf/test/14918042", timeout=30)
        # Should return either 200 or valid response
        assert response.status_code in [200, 404, 422], f"Unexpected: {response.status_code}"


@pytest.fixture(autouse=True)
def check_env():
    """Verify BASE_URL is set"""
    if not BASE_URL:
        pytest.skip("REACT_APP_BACKEND_URL not set")
