#!/usr/bin/env python3
"""
Backend API Testing for Portal JUST Downloader
Tests all API endpoints and functionality
"""

import requests
import sys
import json
import time
from datetime import datetime, timezone
from typing import Dict, List, Any

class PortalJustAPITester:
    def __init__(self, base_url="https://auto-portal-fetch.preview.emergentagent.com"):
        self.base_url = base_url
        self.api_url = f"{base_url}/api"
        self.tests_run = 0
        self.tests_passed = 0
        self.test_results = []
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'Portal-JUST-API-Tester/1.0'
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
                required_fields = ['total_runs', 'completed_runs', 'failed_runs', 'total_files', 'total_size_mb']
                missing_fields = [field for field in required_fields if field not in data]
                
                if not missing_fields:
                    self.log_result("Get Stats", True, f"Stats loaded: {data['total_runs']} runs, {data['total_files']} files", data)
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
        
        # Search and data tests
        self.test_search_preview()
        stats = self.test_get_stats()
        files = self.test_get_files()
        runs = self.test_get_runs()
        self.test_get_current_run()
        self.test_get_institutions()
        
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
    tester = PortalJustAPITester()
    
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