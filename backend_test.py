#!/usr/bin/env python3
"""
Backend API Testing for Portal JUST Downloader - PostgreSQL Edition
Tests all API endpoints and functionality for Romanian company data extraction
"""

import requests
import sys
import json
import time
from datetime import datetime, timezone
from typing import Dict, List, Any

class JustPortalAPITester:
    def __init__(self, base_url="https://auto-portal-fetch.preview.emergentagent.com"):
        self.base_url = base_url
        self.api_url = f"{base_url}/api"
        self.tests_run = 0
        self.tests_passed = 0
        self.test_results = []
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'Portal-JUST-API-Tester-PostgreSQL/1.0'
        })

    def log_result(self, name: str, success: bool, details: str = "", response_data: Any = None):
        """Log test result"""
        self.tests_run += 1
        if success:
            self.tests_passed += 1
            status = "✅ PASSED"
        else:
            status = "❌ FAILED"
        
        result = {
            "test_name": name,
            "success": success,
            "details": details,
            "response_data": response_data,
            "timestamp": datetime.now().isoformat()
        }
        self.test_results.append(result)
        print(f"{status} - {name}: {details}")

    def test_api_health(self) -> bool:
        """Test API root endpoint"""
        try:
            response = self.session.get(f"{self.api_url}/", timeout=30)
            success = response.status_code == 200
            
            if success:
                data = response.json()
                expected_message = "Portal JUST Downloader API"
                if data.get("message") == expected_message:
                    self.log_result("API Health Check", True, f"Status: {response.status_code}, Message: {data.get('message')}")
                    return True
                else:
                    self.log_result("API Health Check", False, f"Wrong message: {data.get('message')}")
                    return False
            else:
                self.log_result("API Health Check", False, f"Status code: {response.status_code}")
                return False
                
        except Exception as e:
            self.log_result("API Health Check", False, f"Exception: {str(e)}")
            return False

    def test_get_config(self) -> Dict[str, Any]:
        """Test getting configuration"""
        try:
            response = self.session.get(f"{self.api_url}/config", timeout=30)
            success = response.status_code == 200
            
            if success:
                data = response.json()
                required_fields = ['id', 'schedule_hour', 'schedule_minute', 'search_term', 'is_active']
                missing_fields = [field for field in required_fields if field not in data]
                
                if not missing_fields:
                    self.log_result("Get Config", True, f"Config loaded with all required fields", data)
                    return data
                else:
                    self.log_result("Get Config", False, f"Missing fields: {missing_fields}")
                    return {}
            else:
                self.log_result("Get Config", False, f"Status code: {response.status_code}")
                return {}
                
        except Exception as e:
            self.log_result("Get Config", False, f"Exception: {str(e)}")
            return {}

    def test_update_config(self, config: Dict[str, Any]) -> bool:
        """Test updating configuration"""
        try:
            # Test data
            update_data = {
                "search_term": "TEST COMPANY SRL",
                "schedule_hour": 10,
                "schedule_minute": 30,
                "is_active": True
            }
            
            response = self.session.put(f"{self.api_url}/config", json=update_data, timeout=30)
            success = response.status_code == 200
            
            if success:
                data = response.json()
                # Verify the update worked
                if (data.get("search_term") == update_data["search_term"] and 
                    data.get("schedule_hour") == update_data["schedule_hour"]):
                    self.log_result("Update Config", True, f"Config updated successfully", data)
                    return True
                else:
                    self.log_result("Update Config", False, f"Config not updated properly: {data}")
                    return False
            else:
                self.log_result("Update Config", False, f"Status code: {response.status_code}")
                return False
                
        except Exception as e:
            self.log_result("Update Config", False, f"Exception: {str(e)}")
            return False

    def test_search_preview(self) -> bool:
        """Test search preview functionality"""
        try:
            search_data = {"company_name": "DEDEMAN"}
            
            response = self.session.post(f"{self.api_url}/search", json=search_data, timeout=60)
            success = response.status_code == 200
            
            if success:
                data = response.json()
                if "total" in data and "dosare" in data:
                    total_results = data.get("total", 0)
                    dosare_count = len(data.get("dosare", []))
                    self.log_result("Search Preview", True, f"Found {total_results} results, returned {dosare_count} dosare")
                    return True
                else:
                    self.log_result("Search Preview", False, f"Invalid response structure: {data}")
                    return False
            else:
                self.log_result("Search Preview", False, f"Status code: {response.status_code}")
                return False
                
        except Exception as e:
            self.log_result("Search Preview", False, f"Exception: {str(e)}")
            return False

    def test_get_stats(self) -> Dict[str, Any]:
        """Test getting statistics"""
        try:
            response = self.session.get(f"{self.api_url}/stats", timeout=30)
            success = response.status_code == 200
            
            if success:
                data = response.json()
                required_fields = ['total_runs', 'completed_runs', 'failed_runs', 'total_files', 'total_size_mb', 'cron_enabled']
                missing_fields = [field for field in required_fields if field not in data]
                
                if not missing_fields:
                    cron_status = "Active" if data.get('cron_enabled') else "Inactive"
                    self.log_result("Get Stats", True, f"Stats loaded: {data['total_runs']} runs, {data['total_files']} files, Cron: {cron_status}", data)
                    return data
                else:
                    self.log_result("Get Stats", False, f"Missing fields: {missing_fields}")
                    return {}
            else:
                self.log_result("Get Stats", False, f"Status code: {response.status_code}")
                return {}
                
        except Exception as e:
            self.log_result("Get Stats", False, f"Exception: {str(e)}")
            return {}

    def test_get_files(self) -> List[Dict[str, Any]]:
        """Test getting file list"""
        try:
            response = self.session.get(f"{self.api_url}/files", timeout=30)
            success = response.status_code == 200
            
            if success:
                data = response.json()
                if isinstance(data, list):
                    file_count = len(data)
                    self.log_result("Get Files", True, f"Retrieved {file_count} files")
                    return data
                else:
                    self.log_result("Get Files", False, f"Expected list, got: {type(data)}")
                    return []
            else:
                self.log_result("Get Files", False, f"Status code: {response.status_code}")
                return []
                
        except Exception as e:
            self.log_result("Get Files", False, f"Exception: {str(e)}")
            return []

    def test_get_runs(self) -> List[Dict[str, Any]]:
        """Test getting run history"""
        try:
            response = self.session.get(f"{self.api_url}/runs", timeout=30)
            success = response.status_code == 200
            
            if success:
                data = response.json()
                if isinstance(data, list):
                    run_count = len(data)
                    self.log_result("Get Runs", True, f"Retrieved {run_count} runs")
                    return data
                else:
                    self.log_result("Get Runs", False, f"Expected list, got: {type(data)}")
                    return []
            else:
                self.log_result("Get Runs", False, f"Status code: {response.status_code}")
                return []
                
        except Exception as e:
            self.log_result("Get Runs", False, f"Exception: {str(e)}")
            return []

    def test_get_current_run(self) -> Dict[str, Any]:
        """Test getting current running job"""
        try:
            response = self.session.get(f"{self.api_url}/runs/current", timeout=30)
            success = response.status_code == 200
            
            if success:
                data = response.json()
                if data is None:
                    self.log_result("Get Current Run", True, "No job currently running")
                else:
                    status = data.get("status", "unknown")
                    records = data.get("records_downloaded", 0)
                    self.log_result("Get Current Run", True, f"Current job status: {status}, records: {records}")
                return data or {}
            else:
                self.log_result("Get Current Run", False, f"Status code: {response.status_code}")
                return {}
                
        except Exception as e:
            self.log_result("Get Current Run", False, f"Exception: {str(e)}")
            return {}

    def test_get_institutions(self) -> List[str]:
        """Test getting institutions list"""
        try:
            response = self.session.get(f"{self.api_url}/institutions", timeout=30)
            success = response.status_code == 200
            
            if success:
                data = response.json()
                if isinstance(data, list) and len(data) > 0:
                    self.log_result("Get Institutions", True, f"Retrieved {len(data)} institutions")
                    return data
                else:
                    self.log_result("Get Institutions", False, f"Expected non-empty list, got: {type(data)} with {len(data) if isinstance(data, list) else 'unknown'} items")
                    return []
            else:
                self.log_result("Get Institutions", False, f"Status code: {response.status_code}")
                return []
                
        except Exception as e:
            self.log_result("Get Institutions", False, f"Exception: {str(e)}")
            return []

    def test_file_download(self, files: List[Dict[str, Any]]) -> bool:
        """Test downloading a file (if available)"""
        if not files:
            self.log_result("File Download", True, "No files available to test download - skipping")
            return True
            
        try:
            # Test with first file
            filename = files[0]["name"]
            response = self.session.get(f"{self.api_url}/files/{filename}", timeout=60)
            success = response.status_code == 200
            
            if success:
                content_type = response.headers.get('content-type', '')
                content_length = len(response.content)
                self.log_result("File Download", True, f"Downloaded {filename} ({content_length} bytes, {content_type})")
                return True
            else:
                self.log_result("File Download", False, f"Status code: {response.status_code} for {filename}")
                return False
                
        except Exception as e:
            self.log_result("File Download", False, f"Exception: {str(e)}")
            return False

    def test_trigger_run(self, skip_if_running: bool = True) -> bool:
        """Test triggering a manual run (be careful - this actually starts a job)"""
        try:
            # Check if job is already running
            current_run = self.test_get_current_run()
            if current_run and current_run.get("status") == "running":
                if skip_if_running:
                    self.log_result("Trigger Run", True, "Job already running - skipping trigger test")
                    return True
                else:
                    self.log_result("Trigger Run", False, "Job already running - cannot start another")
                    return False
            
            response = self.session.post(f"{self.api_url}/run", timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                job_id = data.get("job_id")
                message = data.get("message")
                self.log_result("Trigger Run", True, f"Job started: {message} (ID: {job_id})")
                return True
            elif response.status_code == 400:
                # Probably no search term configured
                data = response.json()
                detail = data.get("detail", "Unknown error")
                self.log_result("Trigger Run", False, f"Bad request: {detail}")
                return False
            elif response.status_code == 409:
                # Job already running
                self.log_result("Trigger Run", True, "Job already running - expected behavior")
                return True
            else:
                self.log_result("Trigger Run", False, f"Status code: {response.status_code}")
                return False
                
        except Exception as e:
            self.log_result("Trigger Run", False, f"Exception: {str(e)}")
            return False

    def test_cron_status(self) -> Dict[str, Any]:
        """Test GET /api/cron/status endpoint"""
        try:
            response = self.session.get(f"{self.api_url}/cron/status", timeout=30)
            success = response.status_code == 200
            
            if success:
                data = response.json()
                required_fields = ['enabled', 'schedule_hour', 'schedule_minute', 'job_active']
                missing_fields = [field for field in required_fields if field not in data]
                
                if not missing_fields:
                    cron_status = "Active" if data.get('enabled') else "Inactive"
                    next_run = data.get('next_run', 'Not scheduled')
                    schedule = f"{data.get('schedule_hour', 0):02d}:{data.get('schedule_minute', 0):02d}"
                    self.log_result("Cron Status", True, f"Cron: {cron_status}, Schedule: {schedule}, Next: {next_run}", data)
                    return data
                else:
                    self.log_result("Cron Status", False, f"Missing fields: {missing_fields}")
                    return {}
            else:
                self.log_result("Cron Status", False, f"Status code: {response.status_code}")
                return {}
                
        except Exception as e:
            self.log_result("Cron Status", False, f"Exception: {str(e)}")
            return {}

    def test_config_cron_enable_disable(self) -> bool:
        """Test enabling and disabling cron via PUT /api/config"""
        try:
            # First enable cron
            enable_data = {"cron_enabled": True}
            response = self.session.put(f"{self.api_url}/config", json=enable_data, timeout=30)
            if response.status_code != 200:
                self.log_result("Config Cron Enable", False, f"Status code: {response.status_code}")
                return False
            
            data = response.json()
            if not data.get('cron_enabled'):
                self.log_result("Config Cron Enable", False, f"Cron not enabled: {data.get('cron_enabled')}")
                return False
            
            # Test disable cron
            disable_data = {"cron_enabled": False}
            response = self.session.put(f"{self.api_url}/config", json=disable_data, timeout=30)
            if response.status_code != 200:
                self.log_result("Config Cron Disable", False, f"Status code: {response.status_code}")
                return False
                
            data = response.json()
            if data.get('cron_enabled'):
                self.log_result("Config Cron Disable", False, f"Cron not disabled: {data.get('cron_enabled')}")
                return False
            
            self.log_result("Config Cron Enable/Disable", True, "Successfully enabled and disabled cron via config")
            return True
                
        except Exception as e:
            self.log_result("Config Cron Enable/Disable", False, f"Exception: {str(e)}")
            return False

    def test_config_date_range(self) -> bool:
        """Test setting date range via PUT /api/config"""
        try:
            from datetime import date, timedelta
            
            today = date.today()
            start_date = (today - timedelta(days=30)).isoformat()
            end_date = today.isoformat()
            
            date_data = {
                "date_start": start_date,
                "date_end": end_date
            }
            
            response = self.session.put(f"{self.api_url}/config", json=date_data, timeout=30)
            success = response.status_code == 200
            
            if success:
                data = response.json()
                if (data.get('date_start') == start_date and 
                    data.get('date_end') == end_date):
                    self.log_result("Config Date Range", True, f"Date range set: {start_date} to {end_date}")
                    return True
                else:
                    self.log_result("Config Date Range", False, f"Date range not set properly")
                    return False
            else:
                self.log_result("Config Date Range", False, f"Status code: {response.status_code}")
                return False
                
        except Exception as e:
            self.log_result("Config Date Range", False, f"Exception: {str(e)}")
            return False

    def test_search_with_date_filtering(self) -> bool:
        """Test POST /api/search with date_start and date_end parameters"""
        try:
            from datetime import date, timedelta
            
            today = date.today()
            start_date = (today - timedelta(days=60)).isoformat()
            end_date = today.isoformat()
            
            # Test search with date filtering
            search_data = {
                "company_name": "SRL",
                "date_start": start_date,
                "date_end": end_date
            }
            
            response = self.session.post(f"{self.api_url}/search", json=search_data, timeout=60)
            success = response.status_code == 200
            
            if success:
                data = response.json()
                if "total" in data and "dosare" in data:
                    total_results = data.get("total", 0)
                    dosare_count = len(data.get("dosare", []))
                    self.log_result("Search with Date Filter", True, f"Found {total_results} results with date filter ({start_date} to {end_date}), returned {dosare_count} dosare")
                    return True
                else:
                    self.log_result("Search with Date Filter", False, f"Invalid response structure: {data}")
                    return False
            else:
                self.log_result("Search with Date Filter", False, f"Status code: {response.status_code}")
                return False
                
        except Exception as e:
            self.log_result("Search with Date Filter", False, f"Exception: {str(e)}")
            return False

    def test_db_stats(self) -> bool:
        """Test GET /api/db/stats - PostgreSQL statistics"""
        try:
            response = self.session.get(f"{self.api_url}/db/stats", timeout=30)
            success = response.status_code == 200
            
            if success:
                data = response.json()
                required_fields = ['firme_total', 'firme_with_cui', 'firme_without_cui', 
                                  'dosare_total', 'timeline_events']
                missing_fields = [f for f in required_fields if f not in data]
                
                if not missing_fields:
                    details = f"Firme: {data['firme_total']}, Dosare: {data['dosare_total']}, Timeline: {data['timeline_events']}"
                    self.log_result("DB Stats", True, details)
                    return True
                else:
                    self.log_result("DB Stats", False, f"Missing fields: {missing_fields}")
                    return False
            else:
                self.log_result("DB Stats", False, f"Status code: {response.status_code}")
                return False
                
        except Exception as e:
            self.log_result("DB Stats", False, f"Exception: {str(e)}")
            return False

    def test_db_firme_list(self) -> Dict[str, Any]:
        """Test GET /api/db/firme - List companies from PostgreSQL"""
        try:
            response = self.session.get(f"{self.api_url}/db/firme?limit=10", timeout=30)
            success = response.status_code == 200
            
            if success:
                data = response.json()
                if 'total' in data and 'firme' in data:
                    total = data['total']
                    returned = len(data['firme'])
                    details = f"Total: {total}, Returned: {returned}"
                    
                    # Check structure of first company
                    if data['firme']:
                        first_company = data['firme'][0]
                        required_fields = ['id', 'cui', 'denumire', 'created_at']
                        missing_fields = [f for f in required_fields if f not in first_company]
                        if missing_fields:
                            self.log_result("DB Firme List", False, f"Missing fields in company: {missing_fields}")
                            return {}
                    
                    self.log_result("DB Firme List", True, details)
                    return data
                else:
                    self.log_result("DB Firme List", False, "Invalid response structure")
                    return {}
            else:
                self.log_result("DB Firme List", False, f"Status code: {response.status_code}")
                return {}
                
        except Exception as e:
            self.log_result("DB Firme List", False, f"Exception: {str(e)}")
            return {}

    def test_db_firma_details_and_update(self) -> bool:
        """Test GET /api/db/firme/{id} and PUT /api/db/firme/{id}"""
        try:
            # First get a company to test
            firme_data = self.test_db_firme_list()
            if not firme_data.get('firme'):
                self.log_result("DB Firma Details & Update", False, "No companies available to test")
                return False
            
            company_id = firme_data['firme'][0]['id']
            
            # Test GET company details
            response = self.session.get(f"{self.api_url}/db/firme/{company_id}", timeout=30)
            if response.status_code != 200:
                self.log_result("DB Firma Details & Update", False, f"GET failed: {response.status_code}")
                return False
            
            company_data = response.json()
            required_fields = ['id', 'cui', 'denumire', 'dosare_count', 'dosare']
            missing_fields = [f for f in required_fields if f not in company_data]
            if missing_fields:
                self.log_result("DB Firma Details & Update", False, f"Missing fields: {missing_fields}")
                return False
            
            # Test PUT company update
            original_cui = company_data.get('cui')
            test_cui = f"TEST{company_id}123"
            
            update_response = self.session.put(
                f"{self.api_url}/db/firme/{company_id}",
                json={"cui": test_cui},
                timeout=30
            )
            
            if update_response.status_code != 200:
                self.log_result("DB Firma Details & Update", False, f"PUT failed: {update_response.status_code}")
                return False
            
            updated_data = update_response.json()
            if updated_data.get('cui') != test_cui:
                self.log_result("DB Firma Details & Update", False, "CUI not updated correctly")
                return False
            
            # Restore original CUI
            if original_cui:
                self.session.put(
                    f"{self.api_url}/db/firme/{company_id}",
                    json={"cui": original_cui},
                    timeout=30
                )
            
            details = f"Company ID: {company_id}, Dosare: {company_data['dosare_count']}, CUI updated successfully"
            self.log_result("DB Firma Details & Update", True, details)
            return True
            
        except Exception as e:
            self.log_result("DB Firma Details & Update", False, f"Exception: {str(e)}")
            return False

    def test_db_dosar_details(self) -> bool:
        """Test GET /api/db/dosare/{id} - File details with timeline"""
        try:
            # First get a company with dosare
            firme_data = self.test_db_firme_list()
            if not firme_data.get('firme'):
                self.log_result("DB Dosar Details", False, "No companies available to test")
                return False
            
            # Find a company with dosare
            company_with_dosare = None
            for company in firme_data['firme'][:5]:  # Check first 5
                company_details_response = self.session.get(f"{self.api_url}/db/firme/{company['id']}", timeout=30)
                if company_details_response.status_code == 200:
                    company_details = company_details_response.json()
                    if company_details.get('dosare') and len(company_details['dosare']) > 0:
                        company_with_dosare = company_details
                        break
            
            if not company_with_dosare:
                self.log_result("DB Dosar Details", False, "No companies with dosare found")
                return False
            
            dosar_id = company_with_dosare['dosare'][0]['id']
            
            # Test dosar details
            response = self.session.get(f"{self.api_url}/db/dosare/{dosar_id}", timeout=30)
            if response.status_code != 200:
                self.log_result("DB Dosar Details", False, f"Status code: {response.status_code}")
                return False
            
            dosar_data = response.json()
            required_fields = ['id', 'numar_dosar', 'firma', 'institutie', 'timeline']
            missing_fields = [f for f in required_fields if f not in dosar_data]
            
            if missing_fields:
                self.log_result("DB Dosar Details", False, f"Missing fields: {missing_fields}")
                return False
            
            timeline_count = len(dosar_data['timeline']) if dosar_data['timeline'] else 0
            details = f"Dosar: {dosar_data['numar_dosar']}, Timeline events: {timeline_count}"
            self.log_result("DB Dosar Details", True, details)
            return True
            
        except Exception as e:
            self.log_result("DB Dosar Details", False, f"Exception: {str(e)}")
            return False

    def test_company_filtering(self) -> bool:
        """Test that only companies (SC, SRL, SA, etc.) are extracted, not individuals"""
        try:
            response = self.session.get(f"{self.api_url}/db/firme?limit=10", timeout=30)
            if response.status_code != 200:
                self.log_result("Company Filtering", False, f"Failed to get companies: {response.status_code}")
                return False
            
            data = response.json()
            companies = data.get('firme', [])
            
            if not companies:
                self.log_result("Company Filtering", False, "No companies found to test filtering")
                return False
            
            # Company patterns that indicate business entities (not individuals)
            company_patterns = [
                'SC', 'SRL', 'SA', 'SCS', 'SNC', 'SCA', 'SPRL', 'GMBH', 'LTD', 'LLC', 'INC',
                'PFA', 'II', 'IF', 'ONG', 'ASSOC', 'S.R.L.', 'S.A.', 'S.C.'
            ]
            
            valid_companies = 0
            total_checked = 0
            
            for company in companies[:5]:  # Check first 5
                name = company.get('denumire', '').upper()
                total_checked += 1
                
                # Check if name contains any company pattern
                if any(pattern in name for pattern in company_patterns):
                    valid_companies += 1
            
            if total_checked == 0:
                self.log_result("Company Filtering", False, "No companies to check")
                return False
            
            # We expect most companies to have valid patterns
            success_rate = valid_companies / total_checked
            if success_rate >= 0.8:  # At least 80% should be valid companies
                details = f"Valid companies: {valid_companies}/{total_checked} ({success_rate:.0%})"
                self.log_result("Company Filtering", True, details)
                return True
            else:
                details = f"Too many individuals found: {valid_companies}/{total_checked} ({success_rate:.0%}) valid companies"
                self.log_result("Company Filtering", False, details)
                return False
                
        except Exception as e:
            self.log_result("Company Filtering", False, f"Exception: {str(e)}")
            return False

    def run_all_tests(self) -> Dict[str, Any]:
        """Run all API tests"""
        print("🚀 Starting Portal JUST API Tests")
        print(f"🔗 Base URL: {self.base_url}")
        print("=" * 60)
        
        start_time = time.time()
        
        # Core API tests
        self.test_api_health()
        
        # Configuration tests
        config = self.test_get_config()
        self.test_update_config(config)
        
        # Cron-specific tests (as required in test specs)
        self.test_cron_status()  # GET /api/cron/status
        self.test_config_cron_enable_disable()  # PUT /api/config with cron_enabled
        self.test_config_date_range()  # PUT /api/config with date_start and date_end
        
        # Search and data tests
        self.test_search_preview()
        self.test_search_with_date_filtering()  # POST /api/search with date filtering
        stats = self.test_get_stats()
        files = self.test_get_files()
        runs = self.test_get_runs()
        self.test_get_current_run()
        self.test_get_institutions()
        
        # PostgreSQL Database Tests (new requirements)
        self.test_db_stats()  # GET /api/db/stats
        self.test_db_firme_list()  # GET /api/db/firme  
        self.test_db_firma_details_and_update()  # GET /api/db/firme/{id} and PUT /api/db/firme/{id}
        self.test_db_dosar_details()  # GET /api/db/dosare/{id}
        self.test_company_filtering()  # Verify only companies extracted
        
        # File operations (if files exist)
        self.test_file_download(files)
        
        # Job execution (careful - actually starts a job)
        # Only run if no search term is set or explicitly requested
        if config.get("search_term"):
            self.test_trigger_run(skip_if_running=True)
        else:
            self.log_result("Trigger Run", True, "No search term configured - skipping job trigger test")
        
        end_time = time.time()
        duration = round(end_time - start_time, 2)
        
        print("=" * 60)
        print(f"📊 Test Results: {self.tests_passed}/{self.tests_run} passed ({duration}s)")
        
        # Summary
        summary = {
            "total_tests": self.tests_run,
            "passed_tests": self.tests_passed,
            "failed_tests": self.tests_run - self.tests_passed,
            "success_rate": round((self.tests_passed / self.tests_run * 100), 2) if self.tests_run > 0 else 0,
            "duration_seconds": duration,
            "test_details": self.test_results
        }
        
        return summary

def main():
    """Main test execution"""
    tester = JustPortalAPITester()
    
    try:
        results = tester.run_all_tests()
        
        # Save detailed results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = f"/tmp/api_test_results_{timestamp}.json"
        
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        print(f"📄 Detailed results saved to: {results_file}")
        
        # Return exit code based on success
        if results["failed_tests"] == 0:
            print("🎉 All tests passed!")
            return 0
        else:
            print(f"⚠️  {results['failed_tests']} tests failed")
            return 1
            
    except KeyboardInterrupt:
        print("\n⏹️  Tests interrupted by user")
        return 130
    except Exception as e:
        print(f"💥 Test execution failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())