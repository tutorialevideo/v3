"""
BPI Exports and Import Tests
Tests for:
- GET /api/bpi/exports - list CSV export files
- GET /api/bpi/exports/{filename} - download specific CSV
- POST /api/bpi/import-to-firme - import CUIs from CSV or bpi_records
"""
import pytest
import requests
import os

BASE_URL = os.environ.get('REACT_APP_BACKEND_URL', '').rstrip('/')

class TestBpiExports:
    """Tests for BPI export file listing and download"""
    
    def test_list_exports_returns_200(self):
        """GET /api/bpi/exports should return 200"""
        response = requests.get(f"{BASE_URL}/api/bpi/exports")
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        print(f"✓ GET /api/bpi/exports returns 200")
    
    def test_list_exports_returns_array(self):
        """GET /api/bpi/exports should return an array"""
        response = requests.get(f"{BASE_URL}/api/bpi/exports")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list), f"Expected list, got {type(data)}"
        print(f"✓ GET /api/bpi/exports returns array with {len(data)} files")
    
    def test_list_exports_file_structure(self):
        """Each export file should have name, size_kb, created fields"""
        response = requests.get(f"{BASE_URL}/api/bpi/exports")
        assert response.status_code == 200
        data = response.json()
        
        if len(data) > 0:
            file_info = data[0]
            assert "name" in file_info, "Missing 'name' field"
            assert "size_kb" in file_info, "Missing 'size_kb' field"
            assert "created" in file_info, "Missing 'created' field"
            assert file_info["name"].startswith("bpi_cuis_"), f"Filename should start with 'bpi_cuis_', got {file_info['name']}"
            assert file_info["name"].endswith(".csv"), f"Filename should end with '.csv', got {file_info['name']}"
            print(f"✓ Export file structure correct: {file_info}")
        else:
            pytest.skip("No export files to test structure")
    
    def test_download_specific_export(self):
        """GET /api/bpi/exports/{filename} should download the CSV file"""
        # First get list of exports
        list_response = requests.get(f"{BASE_URL}/api/bpi/exports")
        assert list_response.status_code == 200
        exports = list_response.json()
        
        if len(exports) == 0:
            pytest.skip("No export files available")
        
        filename = exports[0]["name"]
        download_response = requests.get(f"{BASE_URL}/api/bpi/exports/{filename}")
        assert download_response.status_code == 200, f"Expected 200, got {download_response.status_code}"
        
        # Check content type
        content_type = download_response.headers.get("content-type", "")
        assert "text/csv" in content_type or "application/octet-stream" in content_type, f"Unexpected content-type: {content_type}"
        
        # Check content has CSV structure (semicolon delimiter, UTF-8 BOM)
        content = download_response.text
        assert "CUI" in content, "CSV should contain 'CUI' header"
        assert "Denumire Firma" in content, "CSV should contain 'Denumire Firma' header"
        assert ";" in content, "CSV should use semicolon delimiter"
        print(f"✓ Downloaded {filename}: {len(content)} bytes, CSV structure valid")
    
    def test_download_nonexistent_file_returns_404(self):
        """GET /api/bpi/exports/nonexistent.csv should return 404"""
        response = requests.get(f"{BASE_URL}/api/bpi/exports/nonexistent_file_12345.csv")
        assert response.status_code == 404, f"Expected 404, got {response.status_code}"
        print(f"✓ GET /api/bpi/exports/nonexistent returns 404")
    
    def test_download_invalid_filename_returns_404(self):
        """GET /api/bpi/exports/invalid.csv (not starting with bpi_cuis_) should return 404"""
        response = requests.get(f"{BASE_URL}/api/bpi/exports/invalid.csv")
        assert response.status_code == 404, f"Expected 404, got {response.status_code}"
        print(f"✓ GET /api/bpi/exports/invalid.csv returns 404 (security check)")


class TestBpiImportToFirme:
    """Tests for importing BPI CUIs into firme collection"""
    
    def test_import_from_csv_returns_200(self):
        """POST /api/bpi/import-to-firme?filename=... should return 200"""
        # Get first available export file
        list_response = requests.get(f"{BASE_URL}/api/bpi/exports")
        exports = list_response.json()
        
        if len(exports) == 0:
            pytest.skip("No export files available")
        
        filename = exports[0]["name"]
        response = requests.post(f"{BASE_URL}/api/bpi/import-to-firme?filename={filename}&only_new=true")
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        print(f"✓ POST /api/bpi/import-to-firme?filename={filename} returns 200")
    
    def test_import_from_csv_response_structure(self):
        """Import response should have message, added, skipped, total_cuis"""
        list_response = requests.get(f"{BASE_URL}/api/bpi/exports")
        exports = list_response.json()
        
        if len(exports) == 0:
            pytest.skip("No export files available")
        
        filename = exports[0]["name"]
        response = requests.post(f"{BASE_URL}/api/bpi/import-to-firme?filename={filename}&only_new=true")
        assert response.status_code == 200
        
        data = response.json()
        assert "message" in data, "Missing 'message' field"
        assert "added" in data, "Missing 'added' field"
        assert "skipped" in data, "Missing 'skipped' field"
        assert "total_cuis" in data, "Missing 'total_cuis' field"
        
        # Verify types
        assert isinstance(data["added"], int), f"'added' should be int, got {type(data['added'])}"
        assert isinstance(data["skipped"], int), f"'skipped' should be int, got {type(data['skipped'])}"
        assert isinstance(data["total_cuis"], int), f"'total_cuis' should be int, got {type(data['total_cuis'])}"
        
        print(f"✓ Import response structure valid: added={data['added']}, skipped={data['skipped']}, total={data['total_cuis']}")
    
    def test_import_skips_existing_cuis(self):
        """Re-importing same CSV should skip all CUIs (already exist)"""
        list_response = requests.get(f"{BASE_URL}/api/bpi/exports")
        exports = list_response.json()
        
        if len(exports) == 0:
            pytest.skip("No export files available")
        
        filename = exports[0]["name"]
        
        # First import
        response1 = requests.post(f"{BASE_URL}/api/bpi/import-to-firme?filename={filename}&only_new=true")
        assert response1.status_code == 200
        data1 = response1.json()
        
        # Second import - should skip all
        response2 = requests.post(f"{BASE_URL}/api/bpi/import-to-firme?filename={filename}&only_new=true")
        assert response2.status_code == 200
        data2 = response2.json()
        
        # Second import should have 0 added (all skipped)
        assert data2["added"] == 0, f"Expected 0 added on re-import, got {data2['added']}"
        assert data2["skipped"] == data2["total_cuis"], f"Expected all {data2['total_cuis']} to be skipped"
        
        print(f"✓ Re-import correctly skips existing CUIs: {data2['skipped']} skipped")
    
    def test_import_nonexistent_file_returns_404(self):
        """POST /api/bpi/import-to-firme?filename=nonexistent.csv should return 404"""
        response = requests.post(f"{BASE_URL}/api/bpi/import-to-firme?filename=nonexistent_file.csv&only_new=true")
        assert response.status_code == 404, f"Expected 404, got {response.status_code}"
        print(f"✓ Import nonexistent file returns 404")
    
    def test_import_from_bpi_records_returns_200(self):
        """POST /api/bpi/import-to-firme?only_new=true (no filename) should return 200"""
        response = requests.post(f"{BASE_URL}/api/bpi/import-to-firme?only_new=true")
        assert response.status_code == 200, f"Expected 200, got {response.status_code}"
        
        data = response.json()
        # Should have message field even if no CUIs found
        assert "message" in data, "Missing 'message' field"
        print(f"✓ Import from bpi_records returns 200: {data['message']}")


class TestBpiExportTestFile:
    """Tests specific to the test CSV file created for testing"""
    
    def test_test_file_exists(self):
        """Test CSV file bpi_cuis_test_export_20260320_120000.csv should exist"""
        response = requests.get(f"{BASE_URL}/api/bpi/exports")
        exports = response.json()
        
        test_file = next((f for f in exports if f["name"] == "bpi_cuis_test_export_20260320_120000.csv"), None)
        assert test_file is not None, "Test file bpi_cuis_test_export_20260320_120000.csv not found"
        print(f"✓ Test file exists: {test_file}")
    
    def test_test_file_has_3_cuis(self):
        """Test CSV should have 3 CUIs: 12345678, 87654321, 99887766"""
        response = requests.get(f"{BASE_URL}/api/bpi/exports/bpi_cuis_test_export_20260320_120000.csv")
        assert response.status_code == 200
        
        content = response.text
        assert "12345678" in content, "Missing CUI 12345678"
        assert "87654321" in content, "Missing CUI 87654321"
        assert "99887766" in content, "Missing CUI 99887766"
        print(f"✓ Test file contains all 3 expected CUIs")
    
    def test_import_test_file_shows_3_total(self):
        """Importing test file should show total_cuis=3"""
        response = requests.post(f"{BASE_URL}/api/bpi/import-to-firme?filename=bpi_cuis_test_export_20260320_120000.csv&only_new=true")
        assert response.status_code == 200
        
        data = response.json()
        assert data["total_cuis"] == 3, f"Expected total_cuis=3, got {data['total_cuis']}"
        print(f"✓ Test file import shows 3 total CUIs")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
