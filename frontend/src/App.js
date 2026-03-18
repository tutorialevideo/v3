import { useState, useEffect, useCallback } from "react";
import "@/App.css";
import axios from "axios";
import { Button } from "./components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "./components/ui/card";
import { Input } from "./components/ui/input";
import { Label } from "./components/ui/label";
import { Badge } from "./components/ui/badge";
import { Switch } from "./components/ui/switch";
import { ScrollArea } from "./components/ui/scroll-area";
import { Separator } from "./components/ui/separator";
import { Progress } from "./components/ui/progress";
import { Calendar } from "./components/ui/calendar";
import { Popover, PopoverContent, PopoverTrigger } from "./components/ui/popover";
import { Toaster, toast } from "sonner";
import { format } from "date-fns";
import { ro } from "date-fns/locale";
import { 
  Play, 
  Settings, 
  FileJson, 
  Clock, 
  Download, 
  Trash2, 
  Search,
  RefreshCw,
  CheckCircle2,
  XCircle,
  Loader2,
  Building2,
  FolderDown,
  Activity,
  CalendarIcon,
  Timer,
  Zap,
  Upload,
  FileSpreadsheet,
  AlertCircle,
  Database,
  HardDrive,
  AlertTriangle,
  Wrench,
  BarChart3
} from "lucide-react";

// Use relative URL - nginx will proxy to backend
// This eliminates CORS issues completely
const BACKEND_URL = "";
const API = "/api";

// Debug: Log the API URL (remove in production)
console.log("[DEBUG] API URL:", API);

function App() {
  const [config, setConfig] = useState(null);
  const [stats, setStats] = useState(null);
  const [dbStats, setDbStats] = useState(null);
  const [files, setFiles] = useState([]);
  const [runs, setRuns] = useState([]);
  const [currentRun, setCurrentRun] = useState(null);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState("");
  const [scheduleHour, setScheduleHour] = useState(2);
  const [scheduleMinute, setScheduleMinute] = useState(0);
  const [cronEnabled, setCronEnabled] = useState(false);
  const [dateStart, setDateStart] = useState(null);
  const [dateEnd, setDateEnd] = useState(null);
  const [searchPreview, setSearchPreview] = useState(null);
  const [searchLoading, setSearchLoading] = useState(false);
  const [importLoading, setImportLoading] = useState(false);
  const [importResult, setImportResult] = useState(null);
  const [importLog, setImportLog] = useState([]);
  const [importError, setImportError] = useState(null);
  
  // Firme viewer state
  const [activeTab, setActiveTab] = useState('dashboard'); // 'dashboard', 'firme', or 'diagnostics'
  const [firmeList, setFirmeList] = useState([]);
  const [firmeTotal, setFirmeTotal] = useState(0);
  const [firmePage, setFirmePage] = useState(0);
  const [firmeSearch, setFirmeSearch] = useState("");
  const [firmeLoading, setFirmeLoading] = useState(false);
  
  // Diagnostics state
  const [diagnosticsData, setDiagnosticsData] = useState(null);
  const [duplicateDenumiri, setDuplicateDenumiri] = useState([]);
  const [duplicateCui, setDuplicateCui] = useState([]);
  const [indexes, setIndexes] = useState([]);
  const [diagnosticsLoading, setDiagnosticsLoading] = useState(false);
  const [cleanupLoading, setCleanupLoading] = useState(false);

  // ANAF sync state
  const [anafStats, setAnafStats] = useState(null);
  const [anafProgress, setAnafProgress] = useState(null);
  const [anafLoading, setAnafLoading] = useState(false);
  const [anafSyncRunning, setAnafSyncRunning] = useState(false);
  const [anafTestResult, setAnafTestResult] = useState(null);
  const [anafTestCui, setAnafTestCui] = useState("");

  // MFinante sync state
  const [mfStats, setMfStats] = useState(null);
  const [mfProgress, setMfProgress] = useState(null);
  const [mfLoading, setMfLoading] = useState(false);
  const [mfSession, setMfSession] = useState("");
  const [mfTestCui, setMfTestCui] = useState("");
  const [mfTestResult, setMfTestResult] = useState(null);

  const fetchData = useCallback(async () => {
    try {
      const [configRes, statsRes, filesRes, runsRes, currentRes, dbStatsRes] = await Promise.all([
        axios.get(`${API}/config`),
        axios.get(`${API}/stats`),
        axios.get(`${API}/files`),
        axios.get(`${API}/runs`),
        axios.get(`${API}/runs/current`),
        axios.get(`${API}/db/stats`)
      ]);
      
      setConfig(configRes.data);
      setStats(statsRes.data);
      setFiles(filesRes.data);
      setRuns(runsRes.data);
      setCurrentRun(currentRes.data);
      setDbStats(dbStatsRes.data);
      
      if (configRes.data) {
        setSearchTerm(configRes.data.search_term || "");
        setScheduleHour(configRes.data.schedule_hour || 2);
        setScheduleMinute(configRes.data.schedule_minute || 0);
        setCronEnabled(configRes.data.cron_enabled || false);
        if (configRes.data.date_start) {
          setDateStart(new Date(configRes.data.date_start));
        }
        if (configRes.data.date_end) {
          setDateEnd(new Date(configRes.data.date_end));
        }
      }
    } catch (error) {
      console.error("Error fetching data:", error);
      toast.error("Eroare la încărcarea datelor");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 5000);
    return () => clearInterval(interval);
  }, [fetchData]);

  const saveConfig = async () => {
    try {
      await axios.put(`${API}/config`, {
        search_term: searchTerm,
        schedule_hour: scheduleHour,
        schedule_minute: scheduleMinute,
        cron_enabled: cronEnabled,
        date_start: dateStart ? format(dateStart, 'yyyy-MM-dd') : null,
        date_end: dateEnd ? format(dateEnd, 'yyyy-MM-dd') : null
      });
      toast.success("Configurație salvată cu succes!");
      fetchData();
    } catch (error) {
      toast.error("Eroare la salvarea configurației");
    }
  };

  const triggerRun = async () => {
    try {
      await axios.post(`${API}/run`);
      toast.success("Job-ul de descărcare a fost pornit!");
      fetchData();
    } catch (error) {
      toast.error(error.response?.data?.detail || "Eroare la pornirea job-ului");
    }
  };

  const searchPreviewDosare = async () => {
    if (!searchTerm.trim()) {
      toast.error("Introduceți un termen de căutare");
      return;
    }
    
    setSearchLoading(true);
    try {
      const res = await axios.post(`${API}/search`, { 
        company_name: searchTerm,
        date_start: dateStart ? format(dateStart, 'yyyy-MM-dd') : null,
        date_end: dateEnd ? format(dateEnd, 'yyyy-MM-dd') : null
      });
      setSearchPreview(res.data);
      toast.success(`Găsite ${res.data.total} dosare`);
    } catch (error) {
      toast.error("Eroare la căutare");
    } finally {
      setSearchLoading(false);
    }
  };

  const downloadFile = (filename) => {
    window.open(`${API}/files/${filename}`, '_blank');
  };

  const deleteFile = async (filename) => {
    try {
      await axios.delete(`${API}/files/${filename}`);
      toast.success("Fișier șters");
      fetchData();
    } catch (error) {
      toast.error("Eroare la ștergerea fișierului");
    }
  };

  const handleCsvImport = async (event) => {
    const files = event.target.files;
    if (!files || files.length === 0) return;

    setImportLoading(true);
    setImportResult(null);
    setImportError(null);
    
    const fileList = Array.from(files);
    const totalSize = fileList.reduce((acc, f) => acc + f.size, 0);
    
    setImportLog([
      `[${new Date().toLocaleTimeString()}] 📁 ${fileList.length} fișier(e) selectat(e)`,
      `[${new Date().toLocaleTimeString()}] 📊 Dimensiune totală: ${(totalSize / 1024 / 1024).toFixed(2)} MB`,
      `[${new Date().toLocaleTimeString()}] ─────────────────────────────`
    ]);

    let grandTotal = {
      total_rows: 0,
      processed: 0,
      created_new: 0,
      updated: 0,
      skipped_not_company: 0,
      skipped_no_cui: 0
    };

    for (let i = 0; i < fileList.length; i++) {
      const file = fileList[i];
      
      setImportLog(prev => [
        ...prev,
        ``,
        `[${new Date().toLocaleTimeString()}] 📄 Fișier ${i + 1}/${fileList.length}: ${file.name}`,
        `[${new Date().toLocaleTimeString()}]    Dimensiune: ${(file.size / 1024 / 1024).toFixed(2)} MB`,
        `[${new Date().toLocaleTimeString()}]    Se încarcă...`
      ]);
      
      const formData = new FormData();
      formData.append('file', file);

      try {
        // Start progress polling
        const progressInterval = setInterval(async () => {
          try {
            const progressRes = await axios.get(`${API}/db/import-progress`);
            if (progressRes.data && progressRes.data.processed > 0) {
              setImportLog(prev => {
                // Update the last progress line or add new one
                const lastLine = prev[prev.length - 1];
                const progressLine = `[${new Date().toLocaleTimeString()}]    ⏳ Procesat: ${progressRes.data.processed.toLocaleString()} rânduri, ${progressRes.data.created_new.toLocaleString()} firme create...`;
                
                if (lastLine && lastLine.includes('⏳ Procesat:')) {
                  return [...prev.slice(0, -1), progressLine];
                } else {
                  return [...prev, progressLine];
                }
              });
            }
          } catch (e) {
            // Progress endpoint might not exist or no active import
          }
        }, 2000);
        
        const res = await axios.post(`${API}/db/import-cui`, formData, {
          headers: { 'Content-Type': 'multipart/form-data' },
          timeout: 1800000 // 30 minute timeout pentru fișiere foarte mari
        });
        
        clearInterval(progressInterval);
        
        // Accumulate totals
        grandTotal.total_rows += res.data.total_rows || 0;
        grandTotal.processed += res.data.processed || 0;
        grandTotal.created_new += res.data.created_new || 0;
        grandTotal.updated += res.data.updated || 0;
        grandTotal.skipped_not_company += res.data.skipped_not_company || 0;
        grandTotal.skipped_no_cui += res.data.skipped_no_cui || 0;
        
        setImportLog(prev => [
          ...prev.filter(line => !line.includes('⏳ Procesat:')),
          `[${new Date().toLocaleTimeString()}]    ✅ ${file.name} - GATA!`,
          `[${new Date().toLocaleTimeString()}]       Rânduri: ${res.data.total_rows?.toLocaleString()}`,
          `[${new Date().toLocaleTimeString()}]       Create: ${res.data.created_new?.toLocaleString()} | Actualizate: ${res.data.updated?.toLocaleString()}`,
          `[${new Date().toLocaleTimeString()}]       Sărite (PFA/II): ${res.data.skipped_not_company?.toLocaleString()}`
        ]);
        
      } catch (error) {
        const errorMsg = error.response?.data?.detail || error.message || "Eroare necunoscută";
        setImportLog(prev => [
          ...prev.filter(line => !line.includes('⏳ Procesat:')),
          `[${new Date().toLocaleTimeString()}]    ❌ ${file.name} - EROARE: ${errorMsg}`
        ]);
      }
    }

    // Final summary
    setImportLog(prev => [
      ...prev,
      ``,
      `[${new Date().toLocaleTimeString()}] ═══════════════════════════════`,
      `[${new Date().toLocaleTimeString()}] 📊 SUMAR TOTAL:`,
      `[${new Date().toLocaleTimeString()}]    Rânduri procesate: ${grandTotal.total_rows.toLocaleString()}`,
      `[${new Date().toLocaleTimeString()}]    Firme create: ${grandTotal.created_new.toLocaleString()}`,
      `[${new Date().toLocaleTimeString()}]    Firme actualizate: ${grandTotal.updated.toLocaleString()}`,
      `[${new Date().toLocaleTimeString()}]    Sărite (PFA/II): ${grandTotal.skipped_not_company.toLocaleString()}`,
      `[${new Date().toLocaleTimeString()}]    Sărite (fără CUI): ${grandTotal.skipped_no_cui.toLocaleString()}`,
      `[${new Date().toLocaleTimeString()}] ═══════════════════════════════`
    ]);
    
    setImportResult(grandTotal);
    toast.success(`Import finalizat: ${grandTotal.created_new.toLocaleString()} firme create din ${fileList.length} fișier(e)`);
    fetchData();
    
    setImportLoading(false);
    event.target.value = '';
  };

  const exportFirme = () => {
    window.open(`${API}/db/firme/export`, '_blank');
  };

  const loadFirme = useCallback(async (page = 0, search = "") => {
    setFirmeLoading(true);
    try {
      const res = await axios.get(`${API}/db/firme`, {
        params: { skip: page * 50, limit: 50, search: search || undefined }
      });
      setFirmeList(res.data.firme);
      setFirmeTotal(res.data.total);
    } catch (error) {
      toast.error("Eroare la încărcarea firmelor");
    } finally {
      setFirmeLoading(false);
    }
  }, []);

  useEffect(() => {
    if (activeTab === 'firme') {
      loadFirme(firmePage, firmeSearch);
    }
  }, [activeTab, firmePage, loadFirme]);

  const handleFirmeSearch = () => {
    setFirmePage(0);
    loadFirme(0, firmeSearch);
  };

  // Diagnostics functions
  const loadDiagnostics = useCallback(async () => {
    setDiagnosticsLoading(true);
    try {
      const [overviewRes, denumiriRes, cuiRes, indexesRes] = await Promise.all([
        axios.get(`${API}/diagnostics/overview`),
        axios.get(`${API}/diagnostics/duplicates/denumire?limit=20`),
        axios.get(`${API}/diagnostics/duplicates/cui?limit=20`),
        axios.get(`${API}/diagnostics/indexes`)
      ]);
      setDiagnosticsData(overviewRes.data);
      setDuplicateDenumiri(denumiriRes.data);
      setDuplicateCui(cuiRes.data);
      setIndexes(indexesRes.data);
    } catch (error) {
      console.error("Error loading diagnostics:", error);
      toast.error("Eroare la încărcarea diagnosticelor");
    } finally {
      setDiagnosticsLoading(false);
    }
  }, []);

  useEffect(() => {
    if (activeTab === 'diagnostics') {
      loadDiagnostics();
    }
  }, [activeTab, loadDiagnostics]);

  const cleanupDuplicateDenumiri = async () => {
    if (!window.confirm("Sigur vrei să ștergi duplicatele după denumire? Această acțiune este ireversibilă!")) {
      return;
    }
    setCleanupLoading(true);
    try {
      const res = await axios.post(`${API}/diagnostics/cleanup/duplicates-denumire`);
      toast.success(res.data.message);
      loadDiagnostics();
    } catch (error) {
      toast.error("Eroare la curățarea duplicatelor");
    } finally {
      setCleanupLoading(false);
    }
  };

  const cleanupDuplicateCui = async () => {
    if (!window.confirm("Sigur vrei să ștergi duplicatele după CUI? Această acțiune este ireversibilă!")) {
      return;
    }
    setCleanupLoading(true);
    try {
      const res = await axios.post(`${API}/diagnostics/cleanup/duplicates-cui`);
      toast.success(res.data.message);
      loadDiagnostics();
    } catch (error) {
      toast.error("Eroare la curățarea duplicatelor CUI");
    } finally {
      setCleanupLoading(false);
    }
  };

  const cleanupOrphanedDosare = async () => {
    if (!window.confirm("Sigur vrei să ștergi dosarele orfane? Această acțiune este ireversibilă!")) {
      return;
    }
    setCleanupLoading(true);
    try {
      const res = await axios.post(`${API}/diagnostics/cleanup/orphaned-dosare`);
      toast.success(res.data.message);
      loadDiagnostics();
    } catch (error) {
      toast.error("Eroare la curățarea dosarelor orfane");
    } finally {
      setCleanupLoading(false);
    }
  };

  const optimizeDatabase = async () => {
    setCleanupLoading(true);
    try {
      const res = await axios.post(`${API}/diagnostics/optimize`);
      toast.success(res.data.message);
    } catch (error) {
      toast.error("Eroare la optimizarea bazei de date");
    } finally {
      setCleanupLoading(false);
    }
  };

  const createIndexes = async () => {
    setCleanupLoading(true);
    try {
      const res = await axios.post(`${API}/diagnostics/create-indexes`);
      toast.success(`${res.data.message}: ${res.data.created_indexes.join(', ')}`);
      loadDiagnostics();
    } catch (error) {
      toast.error("Eroare la crearea indexurilor");
    } finally {
      setCleanupLoading(false);
    }
  };

  const migrateSchema = async () => {
    setCleanupLoading(true);
    try {
      const res = await axios.post(`${API}/diagnostics/migrate-schema`);
      toast.success(res.data.message);
      loadDiagnostics();
    } catch (error) {
      toast.error("Eroare la migrarea schemei");
    } finally {
      setCleanupLoading(false);
    }
  };

  // ANAF Functions
  const loadAnafStats = useCallback(async () => {
    try {
      const res = await axios.get(`${API}/anaf/stats`);
      setAnafStats(res.data);
    } catch (error) {
      console.error("Error loading ANAF stats:", error);
    }
  }, []);

  const loadAnafProgress = useCallback(async () => {
    try {
      const res = await axios.get(`${API}/anaf/sync-progress`);
      setAnafProgress(res.data);
      setAnafSyncRunning(res.data.active);
    } catch (error) {
      console.error("Error loading ANAF progress:", error);
    }
  }, []);

  useEffect(() => {
    if (activeTab === 'anaf') {
      loadAnafStats();
      loadAnafProgress();
    }
  }, [activeTab, loadAnafStats, loadAnafProgress]);

  // Poll for ANAF progress while sync is running
  useEffect(() => {
    let interval;
    if (anafSyncRunning) {
      interval = setInterval(() => {
        loadAnafProgress();
        loadAnafStats();
      }, 3000);
    }
    return () => clearInterval(interval);
  }, [anafSyncRunning, loadAnafProgress, loadAnafStats]);

  const startAnafSync = async (options = {}) => {
    setAnafLoading(true);
    try {
      const params = new URLSearchParams();
      if (options.limit) params.append('limit', options.limit);
      if (options.only_unsynced !== undefined) params.append('only_unsynced', options.only_unsynced);
      if (options.judet) params.append('judet', options.judet);
      
      await axios.post(`${API}/anaf/sync?${params.toString()}`);
      toast.success("Sincronizare ANAF pornită!");
      setAnafSyncRunning(true);
      loadAnafProgress();
    } catch (error) {
      toast.error(error.response?.data?.detail || "Eroare la pornirea sincronizării");
    } finally {
      setAnafLoading(false);
    }
  };

  const stopAnafSync = async () => {
    try {
      await axios.post(`${API}/anaf/sync-stop`);
      toast.info("Sincronizare oprită");
      setAnafSyncRunning(false);
    } catch (error) {
      toast.error("Eroare la oprirea sincronizării");
    }
  };

  const testAnafCui = async () => {
    if (!anafTestCui) {
      toast.error("Introdu un CUI");
      return;
    }
    setAnafLoading(true);
    try {
      const res = await axios.get(`${API}/anaf/test/${anafTestCui}`);
      setAnafTestResult(res.data);
      toast.success("Test ANAF reușit!");
    } catch (error) {
      toast.error("Eroare la testarea ANAF API");
      setAnafTestResult({ error: error.message });
    } finally {
      setAnafLoading(false);
    }
  };

  const formatEta = (seconds) => {
    if (!seconds) return '-';
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const secs = seconds % 60;
    if (hours > 0) return `${hours}h ${minutes}m`;
    if (minutes > 0) return `${minutes}m ${secs}s`;
    return `${secs}s`;
  };

  // MFinante Functions
  const loadMfStats = useCallback(async () => {
    try {
      const res = await axios.get(`${API}/mfinante/stats`);
      setMfStats(res.data);
    } catch (error) {
      console.error("Error loading MFinante stats:", error);
    }
  }, []);

  const loadMfProgress = useCallback(async () => {
    try {
      const res = await axios.get(`${API}/mfinante/session-status`);
      setMfProgress(res.data);
    } catch (error) {
      console.error("Error loading MFinante progress:", error);
    }
  }, []);

  useEffect(() => {
    if (activeTab === 'anaf') {
      loadMfStats();
      loadMfProgress();
    }
  }, [activeTab, loadMfStats, loadMfProgress]);

  const setMfSessionId = async () => {
    if (!mfSession) {
      toast.error("Introdu JSESSIONID");
      return;
    }
    setMfLoading(true);
    try {
      const res = await axios.post(`${API}/mfinante/set-session?jsessionid=${encodeURIComponent(mfSession)}`);
      if (res.data.session_valid) {
        toast.success("Sesiune MFinante validă!");
      } else {
        toast.warning("Sesiune setată, dar poate fi invalidă. Încearcă să rezolvi CAPTCHA din nou.");
      }
      loadMfProgress();
    } catch (error) {
      toast.error("Eroare la setarea sesiunii");
    } finally {
      setMfLoading(false);
    }
  };

  const testMfCui = async () => {
    if (!mfTestCui) {
      toast.error("Introdu un CUI");
      return;
    }
    setMfLoading(true);
    try {
      const res = await axios.get(`${API}/mfinante/test/${mfTestCui}`);
      setMfTestResult(res.data);
      toast.success("Test MFinante reușit!");
    } catch (error) {
      toast.error(error.response?.data?.detail || "Eroare la testare");
      setMfTestResult({ error: error.response?.data?.detail || error.message });
    } finally {
      setMfLoading(false);
    }
  };

  const startMfSync = async (limit = 100) => {
    setMfLoading(true);
    try {
      await axios.post(`${API}/mfinante/sync?limit=${limit}`);
      toast.success("Sincronizare MFinante pornită!");
      loadMfProgress();
    } catch (error) {
      toast.error(error.response?.data?.detail || "Eroare la pornirea sincronizării");
    } finally {
      setMfLoading(false);
    }
  };

  const stopMfSync = async () => {
    try {
      await axios.post(`${API}/mfinante/sync-stop`);
      toast.info("Sincronizare oprită");
    } catch (error) {
      toast.error("Eroare la oprire");
    }
  };

  const formatBytes = (bytes) => {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  const formatDate = (dateStr) => {
    if (!dateStr) return '-';
    return new Date(dateStr).toLocaleString('ro-RO');
  };

  if (loading) {
    return (
      <div className="app-container" data-testid="loading-screen">
        <div className="loading-spinner">
          <Loader2 className="animate-spin" size={48} />
          <p>Se încarcă...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="app-container" data-testid="main-dashboard">
      <Toaster position="top-right" richColors />
      
      <header className="app-header">
        <div className="header-content">
          <div className="header-title">
            <FolderDown size={32} />
            <h1>Portal JUST Downloader</h1>
          </div>
          <p className="header-subtitle">Descărcare automată dosare firme din portalquery.just.ro • 246 instituții</p>
        </div>
        
        {/* Tab Navigation */}
        <div className="tab-navigation">
          <button 
            className={`tab-btn ${activeTab === 'dashboard' ? 'active' : ''}`}
            onClick={() => setActiveTab('dashboard')}
            data-testid="tab-dashboard"
          >
            <Activity size={18} />
            Dashboard
          </button>
          <button 
            className={`tab-btn ${activeTab === 'firme' ? 'active' : ''}`}
            onClick={() => setActiveTab('firme')}
            data-testid="tab-firme"
          >
            <Building2 size={18} />
            Firme ({dbStats?.firme_total?.toLocaleString() || 0})
          </button>
          <button 
            className={`tab-btn ${activeTab === 'anaf' ? 'active' : ''}`}
            onClick={() => setActiveTab('anaf')}
            data-testid="tab-anaf"
          >
            <Download size={18} />
            Sync ANAF
          </button>
          <button 
            className={`tab-btn ${activeTab === 'diagnostics' ? 'active' : ''}`}
            onClick={() => setActiveTab('diagnostics')}
            data-testid="tab-diagnostics"
          >
            <Database size={18} />
            Diagnosticare DB
          </button>
        </div>
      </header>

      <main className="main-content">
        {activeTab === 'dashboard' ? (
          <>
        {/* Import CUI Section */}
        <Card className="import-card" data-testid="import-section">
          <CardHeader>
            <div className="card-header-with-action">
              <div>
                <CardTitle className="card-title">
                  <FileSpreadsheet size={20} />
                  Import CUI pentru Firme
                </CardTitle>
                <CardDescription>
                  Încarcă un fișier cu coloanele: DENUMIRE, CUI (orice format)
                </CardDescription>
              </div>
              <div className="import-stats">
                <Badge variant="outline" className="badge-info">
                  {dbStats?.firme_with_cui || 0} / {dbStats?.firme_total || 0} cu CUI
                </Badge>
              </div>
            </div>
          </CardHeader>
          <CardContent>
            <div className="import-content">
              <div className="import-actions">
                <label className="import-btn">
                  <input
                    type="file"
                    accept="*/*"
                    multiple
                    onChange={handleCsvImport}
                    disabled={importLoading}
                    style={{ display: 'none' }}
                    data-testid="csv-file-input"
                  />
                  <Button variant="default" disabled={importLoading} asChild>
                    <span>
                      {importLoading ? (
                        <Loader2 className="animate-spin" size={16} />
                      ) : (
                        <Upload size={16} />
                      )}
                      {importLoading ? 'Se importă...' : 'Import CSV (multiple)'}
                    </span>
                  </Button>
                </label>
                <Button variant="outline" onClick={exportFirme} data-testid="export-firme-btn">
                  <Download size={16} />
                  Export Firme
                </Button>
                <Button variant="ghost" onClick={() => { setImportLog([]); setImportResult(null); setImportError(null); }}>
                  Șterge log
                </Button>
              </div>

              {/* Import Log Box */}
              {(importLog.length > 0 || importError) && (
                <div className="import-log-box" data-testid="import-log">
                  <div className="import-log-header">
                    <span>Log Import</span>
                    {importLoading && <Loader2 className="animate-spin" size={14} />}
                  </div>
                  <ScrollArea className="import-log-scroll">
                    {importLog.map((log, idx) => (
                      <div key={idx} className={`log-line ${log.includes('EROARE') ? 'error' : log.includes('✓') ? 'success' : ''}`}>
                        {log}
                      </div>
                    ))}
                  </ScrollArea>
                  {importError && (
                    <div className="import-error-box">
                      <AlertCircle size={16} />
                      <span>{importError}</span>
                    </div>
                  )}
                </div>
              )}
              
              {importResult && (
                <div className="import-result" data-testid="import-result">
                  <div className="import-stats-grid">
                    <div className="import-stat">
                      <span className="import-stat-value">{importResult.total_rows?.toLocaleString()}</span>
                      <span className="import-stat-label">Rânduri totale</span>
                    </div>
                    <div className="import-stat">
                      <span className="import-stat-value">{importResult.processed?.toLocaleString() || 0}</span>
                      <span className="import-stat-label">Firme procesate</span>
                    </div>
                    <div className="import-stat success">
                      <span className="import-stat-value">{(importResult.created_new || 0).toLocaleString()}</span>
                      <span className="import-stat-label">Firme noi create</span>
                    </div>
                    <div className="import-stat warning">
                      <span className="import-stat-value">{(importResult.skipped_not_company || 0).toLocaleString()}</span>
                      <span className="import-stat-label">PFA/II sărite</span>
                    </div>
                  </div>
                  
                  {(importResult.already_exists > 0 || importResult.updated_cui > 0) && (
                    <p className="import-info">
                      Existau deja: <strong>{importResult.already_exists?.toLocaleString() || 0}</strong> | 
                      CUI actualizat: <strong>{importResult.updated_cui?.toLocaleString() || 0}</strong> |
                      Fără CUI valid: <strong>{importResult.skipped_no_cui?.toLocaleString() || 0}</strong>
                    </p>
                  )}
                  
                  {importResult.sample_created?.length > 0 && (
                    <div className="import-not-found">
                      <p className="not-found-title" style={{color: 'var(--success)'}}>
                        <CheckCircle2 size={14} />
                        Exemple firme create:
                      </p>
                      <ScrollArea className="not-found-scroll">
                        {importResult.sample_created.map((firma, idx) => (
                          <span key={idx} className="not-found-item" style={{background: 'var(--success-bg)'}}>
                            {firma.denumire} ({firma.cui})
                          </span>
                        ))}
                      </ScrollArea>
                    </div>
                  )}
                </div>
              )}
            </div>
          </CardContent>
        </Card>

        {/* Stats Cards */}
        <div className="stats-grid" data-testid="stats-section">
          <Card className="stat-card">
            <CardContent className="stat-content">
              <div className="stat-icon green">
                <CheckCircle2 size={24} />
              </div>
              <div className="stat-info">
                <span className="stat-value">{stats?.completed_runs || 0}</span>
                <span className="stat-label">Rulări complete</span>
              </div>
            </CardContent>
          </Card>
          
          <Card className="stat-card">
            <CardContent className="stat-content">
              <div className="stat-icon purple">
                <Building2 size={24} />
              </div>
              <div className="stat-info">
                <span className="stat-value">{stats?.db_firme || 0}</span>
                <span className="stat-label">Firme în DB</span>
              </div>
            </CardContent>
          </Card>
          
          <Card className="stat-card">
            <CardContent className="stat-content">
              <div className="stat-icon blue">
                <FileJson size={24} />
              </div>
              <div className="stat-info">
                <span className="stat-value">{stats?.db_dosare || 0}</span>
                <span className="stat-label">Dosare în DB</span>
              </div>
            </CardContent>
          </Card>
          
          <Card className="stat-card">
            <CardContent className="stat-content">
              <div className={`stat-icon ${cronEnabled ? 'green' : 'orange'}`}>
                <Timer size={24} />
              </div>
              <div className="stat-info">
                <span className="stat-value">{cronEnabled ? 'Activ' : 'Inactiv'}</span>
                <span className="stat-label">Cron zilnic</span>
              </div>
            </CardContent>
          </Card>
        </div>

        <div className="content-grid">
          {/* Configuration Card */}
          <Card className="config-card" data-testid="config-section">
            <CardHeader>
              <CardTitle className="card-title">
                <Settings size={20} />
                Configurare
              </CardTitle>
              <CardDescription>Setează parametrii pentru descărcarea automată</CardDescription>
            </CardHeader>
            <CardContent className="config-content">
              <div className="form-group">
                <Label htmlFor="searchTerm">Nume Firmă</Label>
                <div className="search-input-group">
                  <Input
                    id="searchTerm"
                    data-testid="search-term-input"
                    value={searchTerm}
                    onChange={(e) => setSearchTerm(e.target.value)}
                    placeholder="Ex: SC EXEMPLU SRL"
                  />
                  <Button 
                    variant="outline" 
                    onClick={searchPreviewDosare}
                    disabled={searchLoading}
                    data-testid="preview-search-btn"
                  >
                    {searchLoading ? <Loader2 className="animate-spin" size={16} /> : <Search size={16} />}
                  </Button>
                </div>
              </div>

              {/* Date Range Selection */}
              <div className="form-group">
                <Label>Perioada dosarelor</Label>
                <div className="date-range-group">
                  <Popover>
                    <PopoverTrigger asChild>
                      <Button variant="outline" className="date-picker-btn" data-testid="date-start-btn">
                        <CalendarIcon size={16} />
                        {dateStart ? format(dateStart, 'dd.MM.yyyy') : 'Data început'}
                      </Button>
                    </PopoverTrigger>
                    <PopoverContent className="calendar-popover" align="start">
                      <Calendar
                        mode="single"
                        selected={dateStart}
                        onSelect={setDateStart}
                        locale={ro}
                        initialFocus
                      />
                      {dateStart && (
                        <Button 
                          variant="ghost" 
                          size="sm" 
                          className="clear-date-btn"
                          onClick={() => setDateStart(null)}
                        >
                          Șterge data
                        </Button>
                      )}
                    </PopoverContent>
                  </Popover>

                  <span className="date-separator">→</span>

                  <Popover>
                    <PopoverTrigger asChild>
                      <Button variant="outline" className="date-picker-btn" data-testid="date-end-btn">
                        <CalendarIcon size={16} />
                        {dateEnd ? format(dateEnd, 'dd.MM.yyyy') : 'Data sfârșit'}
                      </Button>
                    </PopoverTrigger>
                    <PopoverContent className="calendar-popover" align="start">
                      <Calendar
                        mode="single"
                        selected={dateEnd}
                        onSelect={setDateEnd}
                        locale={ro}
                        initialFocus
                      />
                      {dateEnd && (
                        <Button 
                          variant="ghost" 
                          size="sm" 
                          className="clear-date-btn"
                          onClick={() => setDateEnd(null)}
                        >
                          Șterge data
                        </Button>
                      )}
                    </PopoverContent>
                  </Popover>
                </div>
              </div>

              <Separator className="my-4" />

              {/* Cron Configuration */}
              <div className="cron-section">
                <div className="form-group switch-group cron-toggle">
                  <div className="cron-label">
                    <Zap size={18} />
                    <div>
                      <Label>Cron Zilnic Activ</Label>
                      <p className="cron-description">Rulează automat în fiecare zi</p>
                    </div>
                  </div>
                  <Switch
                    id="cronEnabled"
                    data-testid="cron-enabled-switch"
                    checked={cronEnabled}
                    onCheckedChange={setCronEnabled}
                  />
                </div>

                <div className={`cron-time-config ${!cronEnabled ? 'disabled' : ''}`}>
                  <div className="form-row">
                    <div className="form-group">
                      <Label htmlFor="hour">Ora</Label>
                      <Input
                        id="hour"
                        data-testid="schedule-hour-input"
                        type="number"
                        min="0"
                        max="23"
                        value={scheduleHour}
                        onChange={(e) => setScheduleHour(parseInt(e.target.value) || 0)}
                        disabled={!cronEnabled}
                      />
                    </div>
                    <div className="form-group">
                      <Label htmlFor="minute">Minut</Label>
                      <Input
                        id="minute"
                        data-testid="schedule-minute-input"
                        type="number"
                        min="0"
                        max="59"
                        value={scheduleMinute}
                        onChange={(e) => setScheduleMinute(parseInt(e.target.value) || 0)}
                        disabled={!cronEnabled}
                      />
                    </div>
                  </div>
                  {cronEnabled && stats?.next_scheduled_run && (
                    <p className="next-run-info">
                      <Clock size={14} />
                      Următoarea rulare: {formatDate(stats.next_scheduled_run)}
                    </p>
                  )}
                </div>
              </div>

              <div className="button-group">
                <Button onClick={saveConfig} data-testid="save-config-btn">
                  Salvează Configurația
                </Button>
                <Button 
                  variant="secondary" 
                  onClick={triggerRun}
                  disabled={!!currentRun}
                  data-testid="run-now-btn"
                >
                  <Play size={16} />
                  Rulează Acum
                </Button>
              </div>

              {searchPreview && (
                <div className="preview-section" data-testid="search-preview">
                  <Separator className="my-4" />
                  <h4 className="preview-title">
                    <Building2 size={16} />
                    Rezultate căutare: {searchPreview.total} dosare
                  </h4>
                  <ScrollArea className="preview-scroll">
                    {searchPreview.dosare.slice(0, 5).map((dosar, idx) => (
                      <div key={idx} className="preview-item">
                        <span className="preview-number">{dosar.numar || '-'}</span>
                        <span className="preview-object">{dosar.obiect || '-'}</span>
                      </div>
                    ))}
                    {searchPreview.total > 5 && (
                      <p className="preview-more">...și încă {searchPreview.total - 5} dosare</p>
                    )}
                  </ScrollArea>
                </div>
              )}
            </CardContent>
          </Card>

          {/* Current Run Status */}
          {currentRun && (
            <Card className="status-card running" data-testid="current-run-section">
              <CardHeader>
                <CardTitle className="card-title">
                  <Loader2 className="animate-spin" size={20} />
                  Job în Execuție
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="run-status">
                  <p>Descărcare în curs...</p>
                  <p className="run-count">{currentRun.records_downloaded} dosare descărcate</p>
                  <p className="run-progress">{currentRun.progress_message || ''}</p>
                  <Progress value={50} className="mt-2" />
                </div>
              </CardContent>
            </Card>
          )}

          {/* Files List */}
          <Card className="files-card" data-testid="files-section">
            <CardHeader>
              <div className="card-header-with-action">
                <div>
                  <CardTitle className="card-title">
                    <FileJson size={20} />
                    Fișiere Descărcate
                  </CardTitle>
                  <CardDescription>{files.length} fișiere salvate local</CardDescription>
                </div>
                <Button variant="ghost" size="icon" onClick={fetchData} data-testid="refresh-files-btn">
                  <RefreshCw size={16} />
                </Button>
              </div>
            </CardHeader>
            <CardContent>
              <ScrollArea className="files-scroll">
                {files.length === 0 ? (
                  <div className="empty-state">
                    <FileJson size={48} />
                    <p>Niciun fișier descărcat încă</p>
                  </div>
                ) : (
                  files.map((file, idx) => (
                    <div key={idx} className="file-item" data-testid={`file-item-${idx}`}>
                      <div className="file-info">
                        <span className="file-name">{file.name}</span>
                        <span className="file-meta">
                          {formatBytes(file.size)} • {formatDate(file.created)}
                        </span>
                      </div>
                      <div className="file-actions">
                        <Button 
                          variant="ghost" 
                          size="icon"
                          onClick={() => downloadFile(file.name)}
                          data-testid={`download-file-${idx}`}
                        >
                          <Download size={16} />
                        </Button>
                        <Button 
                          variant="ghost" 
                          size="icon"
                          onClick={() => deleteFile(file.name)}
                          data-testid={`delete-file-${idx}`}
                        >
                          <Trash2 size={16} />
                        </Button>
                      </div>
                    </div>
                  ))
                )}
              </ScrollArea>
            </CardContent>
          </Card>

          {/* Run History */}
          <Card className="history-card" data-testid="history-section">
            <CardHeader>
              <CardTitle className="card-title">
                <Clock size={20} />
                Istoric Rulări
              </CardTitle>
              <CardDescription>Ultimele 10 rulări</CardDescription>
            </CardHeader>
            <CardContent>
              <ScrollArea className="history-scroll">
                {runs.length === 0 ? (
                  <div className="empty-state">
                    <Clock size={48} />
                    <p>Nicio rulare înregistrată</p>
                  </div>
                ) : (
                  runs.slice(0, 10).map((run, idx) => (
                    <div key={idx} className="history-item" data-testid={`run-item-${idx}`}>
                      <div className="history-status">
                        {run.status === 'completed' ? (
                          <Badge variant="default" className="badge-success">
                            <CheckCircle2 size={12} />
                            Complet
                          </Badge>
                        ) : run.status === 'running' ? (
                          <Badge variant="secondary" className="badge-running">
                            <Loader2 className="animate-spin" size={12} />
                            În execuție
                          </Badge>
                        ) : (
                          <Badge variant="destructive" className="badge-failed">
                            <XCircle size={12} />
                            Eșuat
                          </Badge>
                        )}
                        {run.triggered_by === 'cron' && (
                          <Badge variant="outline" className="badge-cron">
                            <Timer size={10} />
                            Cron
                          </Badge>
                        )}
                      </div>
                      <div className="history-info">
                        <span className="history-date">{formatDate(run.started_at)}</span>
                        <span className="history-count">{run.total_records} dosare</span>
                      </div>
                    </div>
                  ))
                )}
              </ScrollArea>
            </CardContent>
          </Card>
        </div>
          </>
        ) : activeTab === 'firme' ? (
          /* Firme Tab */
          <div className="firme-container" data-testid="firme-section">
            <Card className="firme-card">
              <CardHeader>
                <div className="card-header-with-action">
                  <div>
                    <CardTitle className="card-title">
                      <Building2 size={20} />
                      Firme în Baza de Date
                    </CardTitle>
                    <CardDescription>
                      {firmeTotal.toLocaleString()} firme totale • {dbStats?.firme_with_cui?.toLocaleString() || 0} cu CUI
                    </CardDescription>
                  </div>
                </div>
              </CardHeader>
              <CardContent>
                {/* Search */}
                <div className="firme-search">
                  <Input
                    placeholder="Caută după denumire sau CUI..."
                    value={firmeSearch}
                    onChange={(e) => setFirmeSearch(e.target.value)}
                    onKeyDown={(e) => e.key === 'Enter' && handleFirmeSearch()}
                    data-testid="firme-search-input"
                  />
                  <Button onClick={handleFirmeSearch} disabled={firmeLoading}>
                    <Search size={16} />
                    Caută
                  </Button>
                  <Button variant="outline" onClick={() => { setFirmeSearch(""); setFirmePage(0); loadFirme(0, ""); }}>
                    Reset
                  </Button>
                </div>

                {/* Firme Table */}
                <div className="firme-table-container">
                  {firmeLoading ? (
                    <div className="firme-loading">
                      <Loader2 className="animate-spin" size={32} />
                      <p>Se încarcă...</p>
                    </div>
                  ) : (
                    <table className="firme-table">
                      <thead>
                        <tr>
                          <th>CUI</th>
                          <th>Denumire</th>
                          <th>Formă</th>
                          <th>Județ</th>
                          <th>Localitate</th>
                          <th>Adresă</th>
                          <th>Înreg.</th>
                          <th>Dosare</th>
                        </tr>
                      </thead>
                      <tbody>
                        {firmeList.map((firma) => (
                          <tr key={firma.id} data-testid={`firma-row-${firma.id}`}>
                            <td className="col-cui">
                              {firma.cui ? (
                                <Badge variant="outline" className="badge-cui">{firma.cui}</Badge>
                              ) : (
                                <span className="no-cui">-</span>
                              )}
                            </td>
                            <td className="col-denumire" title={firma.denumire}>{firma.denumire}</td>
                            <td className="col-forma">
                              {firma.forma_juridica && (
                                <Badge variant="secondary" className="badge-forma">{firma.forma_juridica}</Badge>
                              )}
                            </td>
                            <td className="col-judet">{firma.judet || '-'}</td>
                            <td className="col-localitate">{firma.localitate || '-'}</td>
                            <td className="col-adresa" title={`${firma.strada || ''} ${firma.numar || ''}`}>
                              {firma.strada ? `${firma.strada} ${firma.numar || ''}`.trim() : '-'}
                            </td>
                            <td className="col-inreg">
                              {firma.cod_inregistrare && (
                                <span className="cod-inreg" title={firma.data_inregistrare}>{firma.cod_inregistrare}</span>
                              )}
                            </td>
                            <td className="col-dosare">{firma.dosare_count || 0}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  )}
                </div>

                {/* Pagination */}
                <div className="firme-pagination">
                  <Button 
                    variant="outline" 
                    disabled={firmePage === 0 || firmeLoading}
                    onClick={() => setFirmePage(p => Math.max(0, p - 1))}
                  >
                    ← Anterior
                  </Button>
                  <span className="pagination-info">
                    Pagina {firmePage + 1} din {Math.ceil(firmeTotal / 50) || 1}
                    <small>({(firmePage * 50) + 1} - {Math.min((firmePage + 1) * 50, firmeTotal)} din {firmeTotal.toLocaleString()})</small>
                  </span>
                  <Button 
                    variant="outline" 
                    disabled={(firmePage + 1) * 50 >= firmeTotal || firmeLoading}
                    onClick={() => setFirmePage(p => p + 1)}
                  >
                    Următor →
                  </Button>
                </div>
              </CardContent>
            </Card>
          </div>
        ) : activeTab === 'anaf' ? (
          /* ANAF Sync Tab */
          <div className="anaf-section" data-testid="anaf-section">
            {/* ANAF Stats Card */}
            <Card className="anaf-stats-card" data-testid="anaf-stats">
              <CardHeader>
                <div className="card-header-with-action">
                  <div>
                    <CardTitle className="card-title">
                      <Download size={20} />
                      Sincronizare ANAF
                    </CardTitle>
                    <CardDescription>
                      Descarcă date de la ANAF pentru firmele din baza de date (TVA, stare, e-Factura)
                    </CardDescription>
                  </div>
                  <div className="header-actions">
                    <Button variant="outline" onClick={() => { loadAnafStats(); loadAnafProgress(); }}>
                      <RefreshCw size={16} />
                      Reîncarcă
                    </Button>
                  </div>
                </div>
              </CardHeader>
              <CardContent>
                {anafStats && (
                  <div className="anaf-stats-grid">
                    <div className="anaf-stat-card">
                      <span className="stat-value">{anafStats.total_firme_cu_cui?.toLocaleString() || 0}</span>
                      <span className="stat-label">Firme cu CUI</span>
                    </div>
                    <div className="anaf-stat-card synced">
                      <span className="stat-value">{anafStats.synced?.toLocaleString() || 0}</span>
                      <span className="stat-label">Sincronizate</span>
                    </div>
                    <div className="anaf-stat-card not-synced">
                      <span className="stat-value">{anafStats.not_synced?.toLocaleString() || 0}</span>
                      <span className="stat-label">Nesincronizate</span>
                    </div>
                    <div className="anaf-stat-card found">
                      <span className="stat-value">{anafStats.found?.toLocaleString() || 0}</span>
                      <span className="stat-label">Găsite în ANAF</span>
                    </div>
                    <div className="anaf-stat-card not-found">
                      <span className="stat-value">{anafStats.not_found?.toLocaleString() || 0}</span>
                      <span className="stat-label">Negăsite</span>
                    </div>
                    <div className="anaf-stat-card active">
                      <span className="stat-value">{anafStats.active?.toLocaleString() || 0}</span>
                      <span className="stat-label">Active</span>
                    </div>
                    <div className="anaf-stat-card radiate">
                      <span className="stat-value">{anafStats.radiate?.toLocaleString() || 0}</span>
                      <span className="stat-label">Radiate</span>
                    </div>
                    <div className="anaf-stat-card tva">
                      <span className="stat-value">{anafStats.platitori_tva?.toLocaleString() || 0}</span>
                      <span className="stat-label">Plătitori TVA</span>
                    </div>
                    <div className="anaf-stat-card efactura">
                      <span className="stat-value">{anafStats.e_factura?.toLocaleString() || 0}</span>
                      <span className="stat-label">e-Factura</span>
                    </div>
                  </div>
                )}
              </CardContent>
            </Card>

            {/* Sync Progress Card */}
            <Card className="anaf-progress-card" data-testid="anaf-progress">
              <CardHeader>
                <CardTitle className="card-title">
                  {anafSyncRunning ? <Loader2 className="animate-spin" size={20} /> : <Zap size={20} />}
                  Progres Sincronizare
                </CardTitle>
              </CardHeader>
              <CardContent>
                {anafProgress && anafProgress.active ? (
                  <div className="progress-info">
                    <div className="progress-bar-container">
                      <div 
                        className="progress-bar" 
                        style={{ width: `${anafProgress.total_firms > 0 ? (anafProgress.processed / anafProgress.total_firms * 100) : 0}%` }}
                      />
                    </div>
                    <div className="progress-stats">
                      <span>Procesat: {anafProgress.processed?.toLocaleString()} / {anafProgress.total_firms?.toLocaleString()}</span>
                      <span>Găsite: {anafProgress.found?.toLocaleString()}</span>
                      <span>Negăsite: {anafProgress.not_found?.toLocaleString()}</span>
                      <span>Erori: {anafProgress.errors?.toLocaleString()}</span>
                      <span>ETA: {formatEta(anafProgress.eta_seconds)}</span>
                    </div>
                    <Button variant="destructive" onClick={stopAnafSync} className="stop-btn">
                      <XCircle size={16} />
                      Oprește
                    </Button>
                  </div>
                ) : (
                  <div className="sync-actions">
                    <div className="sync-options">
                      <Button 
                        onClick={() => startAnafSync({ only_unsynced: true })}
                        disabled={anafLoading || anafSyncRunning}
                        className="sync-btn"
                      >
                        <Download size={16} />
                        Sync Nesincronizate ({anafStats?.not_synced?.toLocaleString() || 0})
                      </Button>
                      <Button 
                        variant="outline"
                        onClick={() => startAnafSync({ only_unsynced: true, limit: 1000 })}
                        disabled={anafLoading || anafSyncRunning}
                      >
                        Test 1,000 firme
                      </Button>
                      <Button 
                        variant="outline"
                        onClick={() => startAnafSync({ only_unsynced: false })}
                        disabled={anafLoading || anafSyncRunning}
                      >
                        Re-sync toate
                      </Button>
                    </div>
                    <p className="sync-note">
                      ⚠️ ANAF permite max 100 CUI/request și 1 request/secundă. 
                      Pentru 1 milion de firme va dura ~3 ore.
                    </p>
                  </div>
                )}
              </CardContent>
            </Card>

            {/* Test ANAF API Card */}
            <Card className="anaf-test-card" data-testid="anaf-test">
              <CardHeader>
                <CardTitle className="card-title">
                  <Search size={20} />
                  Test API ANAF
                </CardTitle>
                <CardDescription>
                  Testează API-ul ANAF cu un singur CUI
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="test-form">
                  <input
                    type="text"
                    placeholder="Introdu CUI (ex: 14918042)"
                    value={anafTestCui}
                    onChange={(e) => setAnafTestCui(e.target.value)}
                    className="test-input"
                  />
                  <Button onClick={testAnafCui} disabled={anafLoading}>
                    {anafLoading ? <Loader2 className="animate-spin" size={16} /> : <Search size={16} />}
                    Verifică
                  </Button>
                </div>
                {anafTestResult && (
                  <div className="test-result">
                    <pre>{JSON.stringify(anafTestResult, null, 2)}</pre>
                  </div>
                )}
              </CardContent>
            </Card>

            {/* MFinante Section */}
            <Card className="mfinante-card" data-testid="mfinante-section">
              <CardHeader>
                <div className="card-header-with-action">
                  <div>
                    <CardTitle className="card-title">
                      <FileSpreadsheet size={20} />
                      MFinante - Bilanțuri
                    </CardTitle>
                    <CardDescription>
                      Date financiare de pe mfinante.gov.ro (cifră afaceri, profit, nr. angajați)
                    </CardDescription>
                  </div>
                  <Button variant="outline" onClick={() => { loadMfStats(); loadMfProgress(); }}>
                    <RefreshCw size={16} />
                  </Button>
                </div>
              </CardHeader>
              <CardContent>
                {/* Session Setup */}
                <div className="mf-session-section">
                  <h4>1. Rezolvă CAPTCHA și setează sesiunea</h4>
                  <ol className="mf-instructions">
                    <li>Deschide <a href="https://mfinante.gov.ro/apps/infocodfiscal.html" target="_blank" rel="noopener noreferrer">mfinante.gov.ro/apps/infocodfiscal.html</a></li>
                    <li>Rezolvă CAPTCHA și trimite cu orice CUI (ex: 14918042)</li>
                    <li>Apasă F12 → Network → Găsește request-ul → Copiază <code>jsessionid</code> din URL</li>
                    <li>Lipește mai jos și apasă "Setează Sesiune"</li>
                  </ol>
                  <div className="mf-session-form">
                    <input
                      type="text"
                      placeholder="jsessionid (ex: Gtjp5kQJsBZ7Kd4ZU07zCai6IAzJ...)"
                      value={mfSession}
                      onChange={(e) => setMfSession(e.target.value)}
                      className="test-input"
                    />
                    <Button onClick={setMfSessionId} disabled={mfLoading}>
                      {mfLoading ? <Loader2 className="animate-spin" size={16} /> : <CheckCircle2 size={16} />}
                      Setează Sesiune
                    </Button>
                  </div>
                  {mfProgress && (
                    <div className={`session-status ${mfProgress.session_valid ? 'valid' : 'invalid'}`}>
                      {mfProgress.session_valid ? (
                        <><CheckCircle2 size={16} /> Sesiune validă</>
                      ) : (
                        <><XCircle size={16} /> Sesiune invalidă sau expirată</>
                      )}
                    </div>
                  )}
                </div>

                {/* Test CUI */}
                <div className="mf-test-section">
                  <h4>2. Testează cu un CUI</h4>
                  <div className="test-form">
                    <input
                      type="text"
                      placeholder="CUI (ex: 14918042)"
                      value={mfTestCui}
                      onChange={(e) => setMfTestCui(e.target.value)}
                      className="test-input"
                    />
                    <Button onClick={testMfCui} disabled={mfLoading || !mfProgress?.session_valid}>
                      {mfLoading ? <Loader2 className="animate-spin" size={16} /> : <Search size={16} />}
                      Test
                    </Button>
                  </div>
                  {mfTestResult && (
                    <div className="test-result">
                      <pre>{JSON.stringify(mfTestResult, null, 2)}</pre>
                    </div>
                  )}
                </div>

                {/* Sync Actions */}
                <div className="mf-sync-section">
                  <h4>3. Sincronizează firme</h4>
                  {mfStats && (
                    <div className="mf-stats-mini">
                      <span>Total: {mfStats.total_firme?.toLocaleString()}</span>
                      <span>Sincronizate: {mfStats.synced_mfinante?.toLocaleString()}</span>
                      <span>Cu cifră afaceri: {mfStats.with_cifra_afaceri?.toLocaleString()}</span>
                    </div>
                  )}
                  <div className="sync-options">
                    <Button 
                      onClick={() => startMfSync(50)}
                      disabled={mfLoading || !mfProgress?.session_valid}
                    >
                      Test 50 firme
                    </Button>
                    <Button 
                      variant="outline"
                      onClick={() => startMfSync(500)}
                      disabled={mfLoading || !mfProgress?.session_valid}
                    >
                      Sync 500 firme
                    </Button>
                    <Button 
                      variant="destructive"
                      onClick={stopMfSync}
                      disabled={!mfProgress?.progress?.active}
                    >
                      <XCircle size={16} />
                      Stop
                    </Button>
                  </div>
                  {mfProgress?.progress?.active && (
                    <div className="mf-progress">
                      <div className="progress-bar-container">
                        <div 
                          className="progress-bar" 
                          style={{ width: `${mfProgress.progress.total_firms > 0 ? (mfProgress.progress.processed / mfProgress.progress.total_firms * 100) : 0}%` }}
                        />
                      </div>
                      <div className="progress-stats">
                        <span>Procesat: {mfProgress.progress.processed} / {mfProgress.progress.total_firms}</span>
                        <span>Găsite: {mfProgress.progress.found}</span>
                        <span>Erori: {mfProgress.progress.errors}</span>
                        <span>Ultimul CUI: {mfProgress.progress.last_cui}</span>
                      </div>
                    </div>
                  )}
                  <p className="sync-note">
                    ⚠️ MFinante este lent (~2 sec/firmă). Pentru 1000 de firme = ~30 minute.
                    Sesiunea expiră după ~15-30 min inactivitate.
                  </p>
                </div>
              </CardContent>
            </Card>
          </div>
        ) : activeTab === 'diagnostics' ? (
          /* Diagnostics Tab */
          <div className="diagnostics-section" data-testid="diagnostics-section">
            {/* Overview Card */}
            <Card className="diagnostics-overview" data-testid="diagnostics-overview">
              <CardHeader>
                <div className="card-header-with-action">
                  <div>
                    <CardTitle className="card-title">
                      <Database size={20} />
                      Diagnosticare Bază de Date
                    </CardTitle>
                    <CardDescription>
                      Verifică starea bazei de date, duplicatele și optimizează performanța
                    </CardDescription>
                  </div>
                  <div className="header-actions">
                    <Button 
                      variant="outline" 
                      onClick={loadDiagnostics}
                      disabled={diagnosticsLoading}
                    >
                      <RefreshCw size={16} className={diagnosticsLoading ? 'animate-spin' : ''} />
                      Reîncarcă
                    </Button>
                  </div>
                </div>
              </CardHeader>
              <CardContent>
                {diagnosticsLoading && !diagnosticsData ? (
                  <div className="loading-center">
                    <Loader2 className="animate-spin" size={32} />
                    <p>Se încarcă diagnosticele...</p>
                  </div>
                ) : diagnosticsData ? (
                  <div className="diagnostics-grid">
                    {/* Table Counts */}
                    <div className="diag-card">
                      <h4><BarChart3 size={18} /> Statistici Tabele</h4>
                      <div className="diag-stats">
                        <div className="diag-stat">
                          <span className="stat-value">{diagnosticsData.counts.firme?.toLocaleString() || 0}</span>
                          <span className="stat-label">Firme</span>
                        </div>
                        <div className="diag-stat">
                          <span className="stat-value">{diagnosticsData.counts.dosare?.toLocaleString() || 0}</span>
                          <span className="stat-label">Dosare</span>
                        </div>
                        <div className="diag-stat">
                          <span className="stat-value">{diagnosticsData.counts.timeline_events?.toLocaleString() || 0}</span>
                          <span className="stat-label">Evenimente</span>
                        </div>
                      </div>
                    </div>

                    {/* Table Sizes */}
                    <div className="diag-card">
                      <h4><HardDrive size={18} /> Dimensiune pe Disc</h4>
                      <div className="diag-sizes">
                        {diagnosticsData.table_sizes?.map((t, i) => (
                          <div key={i} className="size-row">
                            <span className="table-name">{t.table}</span>
                            <span className="table-size">{t.size}</span>
                          </div>
                        ))}
                      </div>
                    </div>

                    {/* Issues */}
                    <div className="diag-card issues-card">
                      <h4><AlertTriangle size={18} /> Probleme Detectate</h4>
                      <div className="issues-list">
                        <div className={`issue-row ${diagnosticsData.issues.duplicate_denumiri > 0 ? 'has-issue' : 'no-issue'}`}>
                          <span>Denumiri duplicate:</span>
                          <Badge variant={diagnosticsData.issues.duplicate_denumiri > 0 ? "destructive" : "secondary"}>
                            {diagnosticsData.issues.duplicate_denumiri}
                          </Badge>
                        </div>
                        <div className={`issue-row ${diagnosticsData.issues.duplicate_cui > 0 ? 'has-issue' : 'no-issue'}`}>
                          <span>CUI duplicate:</span>
                          <Badge variant={diagnosticsData.issues.duplicate_cui > 0 ? "destructive" : "secondary"}>
                            {diagnosticsData.issues.duplicate_cui}
                          </Badge>
                        </div>
                        <div className={`issue-row ${diagnosticsData.issues.firme_without_cui > 0 ? 'has-warning' : 'no-issue'}`}>
                          <span>Firme fără CUI:</span>
                          <Badge variant={diagnosticsData.issues.firme_without_cui > 0 ? "outline" : "secondary"}>
                            {diagnosticsData.issues.firme_without_cui}
                          </Badge>
                        </div>
                        <div className={`issue-row ${diagnosticsData.issues.orphaned_dosare > 0 ? 'has-issue' : 'no-issue'}`}>
                          <span>Dosare orfane:</span>
                          <Badge variant={diagnosticsData.issues.orphaned_dosare > 0 ? "destructive" : "secondary"}>
                            {diagnosticsData.issues.orphaned_dosare}
                          </Badge>
                        </div>
                      </div>
                    </div>

                    {/* Actions */}
                    <div className="diag-card actions-card">
                      <h4><Wrench size={18} /> Acțiuni de Întreținere</h4>
                      <div className="actions-list">
                        <Button 
                          variant="default" 
                          onClick={migrateSchema}
                          disabled={cleanupLoading}
                          className="action-btn"
                        >
                          <Database size={16} />
                          Migrare Schemă (coloane noi)
                        </Button>
                        <Button 
                          variant="outline" 
                          onClick={createIndexes}
                          disabled={cleanupLoading}
                          className="action-btn"
                        >
                          <Zap size={16} />
                          Creează Indexuri
                        </Button>
                        <Button 
                          variant="outline" 
                          onClick={optimizeDatabase}
                          disabled={cleanupLoading}
                          className="action-btn"
                        >
                          <RefreshCw size={16} />
                          Optimizează DB
                        </Button>
                        <Button 
                          variant="destructive" 
                          onClick={cleanupDuplicateDenumiri}
                          disabled={cleanupLoading || !diagnosticsData?.issues?.duplicate_denumiri}
                          className="action-btn"
                        >
                          <Trash2 size={16} />
                          Șterge Duplicate Denumiri
                        </Button>
                        <Button 
                          variant="destructive" 
                          onClick={cleanupDuplicateCui}
                          disabled={cleanupLoading || !diagnosticsData?.issues?.duplicate_cui}
                          className="action-btn"
                        >
                          <Trash2 size={16} />
                          Șterge Duplicate CUI
                        </Button>
                        <Button 
                          variant="destructive" 
                          onClick={cleanupOrphanedDosare}
                          disabled={cleanupLoading || !diagnosticsData?.issues?.orphaned_dosare}
                          className="action-btn"
                        >
                          <Trash2 size={16} />
                          Șterge Dosare Orfane
                        </Button>
                      </div>
                      {cleanupLoading && (
                        <div className="cleanup-loading">
                          <Loader2 className="animate-spin" size={16} />
                          <span>Se procesează...</span>
                        </div>
                      )}
                    </div>
                  </div>
                ) : (
                  <p>Nu s-au putut încărca diagnosticele.</p>
                )}
              </CardContent>
            </Card>

            {/* Duplicate Denumiri */}
            {duplicateDenumiri.length > 0 && (
              <Card className="duplicates-card" data-testid="duplicate-denumiri-card">
                <CardHeader>
                  <CardTitle className="card-title">
                    <AlertTriangle size={20} className="text-warning" />
                    Denumiri Duplicate ({duplicateDenumiri.length})
                  </CardTitle>
                  <CardDescription>
                    Firme cu aceeași denumire normalizată
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="duplicates-table-container">
                    <table className="duplicates-table">
                      <thead>
                        <tr>
                          <th>Denumire</th>
                          <th>Duplicări</th>
                          <th>IDs</th>
                          <th>CUI-uri</th>
                        </tr>
                      </thead>
                      <tbody>
                        {duplicateDenumiri.map((dup, i) => (
                          <tr key={i}>
                            <td className="denumire-cell">{dup.denumire_normalized}</td>
                            <td><Badge variant="destructive">{dup.count}</Badge></td>
                            <td className="ids-cell">{dup.ids?.slice(0, 5).join(', ')}{dup.ids?.length > 5 ? '...' : ''}</td>
                            <td className="cui-cell">
                              {dup.cui_list?.filter(c => c).slice(0, 3).map((c, j) => (
                                <Badge key={j} variant="outline" className="mr-1">{c}</Badge>
                              ))}
                              {dup.cui_list?.filter(c => c).length > 3 ? '...' : ''}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </CardContent>
              </Card>
            )}

            {/* Duplicate CUI */}
            {duplicateCui.length > 0 && (
              <Card className="duplicates-card" data-testid="duplicate-cui-card">
                <CardHeader>
                  <CardTitle className="card-title">
                    <AlertTriangle size={20} className="text-danger" />
                    CUI Duplicate ({duplicateCui.length})
                  </CardTitle>
                  <CardDescription>
                    Firme cu același CUI (ar trebui să fie unic)
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="duplicates-table-container">
                    <table className="duplicates-table">
                      <thead>
                        <tr>
                          <th>CUI</th>
                          <th>Duplicări</th>
                          <th>IDs</th>
                          <th>Denumiri</th>
                        </tr>
                      </thead>
                      <tbody>
                        {duplicateCui.map((dup, i) => (
                          <tr key={i}>
                            <td><Badge variant="outline">{dup.cui}</Badge></td>
                            <td><Badge variant="destructive">{dup.count}</Badge></td>
                            <td className="ids-cell">{dup.ids?.slice(0, 5).join(', ')}{dup.ids?.length > 5 ? '...' : ''}</td>
                            <td className="denumire-cell">
                              {dup.denumiri?.slice(0, 2).join(', ')}{dup.denumiri?.length > 2 ? '...' : ''}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </CardContent>
              </Card>
            )}

            {/* Indexes */}
            <Card className="indexes-card" data-testid="indexes-card">
              <CardHeader>
                <CardTitle className="card-title">
                  <Zap size={20} />
                  Indexuri ({indexes.length})
                </CardTitle>
                <CardDescription>
                  Indexurile existente în baza de date pentru performanță
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="indexes-table-container">
                  <table className="indexes-table">
                    <thead>
                      <tr>
                        <th>Tabel</th>
                        <th>Index</th>
                        <th>Definiție</th>
                      </tr>
                    </thead>
                    <tbody>
                      {indexes.map((idx, i) => (
                        <tr key={i}>
                          <td><Badge variant="secondary">{idx.table}</Badge></td>
                          <td className="index-name">{idx.name}</td>
                          <td className="index-def"><code>{idx.definition}</code></td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </CardContent>
            </Card>
          </div>
        ) : null}
      </main>

      <footer className="app-footer">
        <p>Portal JUST Downloader • Descărcare automată dosare firme • 246 instituții</p>
      </footer>
    </div>
  );
}

export default App;
