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
  BarChart3,
  Shield,
  ChevronDown
} from "lucide-react";
import DashboardPage from "./pages/DashboardPage";
import FirmePage from "./pages/FirmePage";
import AnafPage from "./pages/AnafPage";
import DbFinalPage from "./pages/DbFinalPage";
import DiagnosticsPage from "./pages/DiagnosticsPage";
import FirmaProfileModal from "./components/FirmaProfileModal";
import CaptchaModal from "./components/CaptchaModal";


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
  const [activeTab, setActiveTab] = useState('dashboard'); // 'dashboard', 'firme', 'dbfinal', 'anaf', 'diagnostics'
  const [firmeList, setFirmeList] = useState([]);
  const [firmeTotal, setFirmeTotal] = useState(0);
  const [firmePage, setFirmePage] = useState(0);
  const [firmeSearch, setFirmeSearch] = useState("");
  const [firmeLoading, setFirmeLoading] = useState(false);
  
  // Company profile modal
  const [selectedFirma, setSelectedFirma] = useState(null);
  const [firmaProfile, setFirmaProfile] = useState(null);
  const [profileLoading, setProfileLoading] = useState(false);
  const [profileModalOpen, setProfileModalOpen] = useState(false);
  const [editingCui, setEditingCui] = useState(false);
  const [editCuiValue, setEditCuiValue] = useState("");
  const [savingCui, setSavingCui] = useState(false);
  // Inline CUI edit for firma table (not modal)
  const [inlineEditId, setInlineEditId] = useState(null);
  const [inlineEditValue, setInlineEditValue] = useState("");
  
  // DB Final state
  const [dbFinalStats, setDbFinalStats] = useState(null);
  const [dbFinalList, setDbFinalList] = useState([]);
  const [dbFinalTotal, setDbFinalTotal] = useState(0);
  const [dbFinalPage, setDbFinalPage] = useState(0);
  const [dbFinalSearch, setDbFinalSearch] = useState("");
  const [dbFinalLoading, setDbFinalLoading] = useState(false);
  const [dbFinalFilters, setDbFinalFilters] = useState({ doarActive: false, doarCuBilant: false });
  
  // Diagnostics state
  const [diagnosticsData, setDiagnosticsData] = useState(null);
  const [duplicateDenumiri, setDuplicateDenumiri] = useState([]);
  const [duplicateCui, setDuplicateCui] = useState([]);
  const [indexes, setIndexes] = useState([]);
  const [diagnosticsLoading, setDiagnosticsLoading] = useState(false);
  const [cleanupLoading, setCleanupLoading] = useState(false);
  const [dbAvailable, setDbAvailable] = useState(true);
  const [reconnecting, setReconnecting] = useState(false);

  // ANAF sync state
  const [anafStats, setAnafStats] = useState(null);
  const [anafProgress, setAnafProgress] = useState(null);
  const [anafLoading, setAnafLoading] = useState(false);
  const [anafSyncRunning, setAnafSyncRunning] = useState(false);
  const [anafTestResult, setAnafTestResult] = useState(null);
  const [anafTestCui, setAnafTestCui] = useState("");
  const [anafLogs, setAnafLogs] = useState([]);

  // MFinante sync state
  const [mfStats, setMfStats] = useState(null);
  const [mfProgress, setMfProgress] = useState(null);
  const [mfLoading, setMfLoading] = useState(false);
  const [mfSession, setMfSession] = useState("");
  const [mfTestCui, setMfTestCui] = useState("");
  const [mfTestResult, setMfTestResult] = useState(null);
  
  // CAPTCHA popup state
  const [captchaModalOpen, setCaptchaModalOpen] = useState(false);
  const [captchaLoading, setCaptchaLoading] = useState(false);
  const [captchaImageUrl, setCaptchaImageUrl] = useState(null);
  const [captchaCode, setCaptchaCode] = useState("");
  const [captchaError, setCaptchaError] = useState(null);
  const [showAdvancedConfig, setShowAdvancedConfig] = useState(false);
  const [downloadLogs, setDownloadLogs] = useState([]);
  const [downloadProgress, setDownloadProgress] = useState(null);

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
      // Only show error toast once, not repeatedly
      if (!window._dataErrorShown) {
        window._dataErrorShown = true;
        // Don't show error toast if it's just PostgreSQL unavailable
        // This allows CAPTCHA and other non-DB features to work
      }
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchData();
    // Poll less frequently to avoid spamming errors when DB is unavailable
    const interval = setInterval(fetchData, 10000);
    return () => clearInterval(interval);
  }, [fetchData]);

  // Poll download logs when a job is running
  useEffect(() => {
    let logInterval = null;
    if (currentRun) {
      const pollLogs = async () => {
        try {
          const res = await axios.get(`${API}/run/logs`);
          setDownloadLogs(res.data.logs || []);
          setDownloadProgress(res.data);
          if (!res.data.active) {
            // Job finished — refresh data and stop polling
            fetchData();
          }
        } catch (e) {}
      };
      pollLogs();
      logInterval = setInterval(pollLogs, 2000);
    } else {
      setDownloadProgress(null);
    }
    return () => { if (logInterval) clearInterval(logInterval); };
  }, [currentRun, fetchData]);

  const reconnectDatabase = async () => {
    setReconnecting(true);
    try {
      await axios.post(`${API}/db/reconnect`);
      toast.success("Conexiune la baza de date restabilită!");
      setDbAvailable(true);
      window._dataErrorShown = false; // Reset error flag
      fetchData();
    } catch (error) {
      toast.error(error.response?.data?.detail || "Nu s-a putut conecta la baza de date");
      setDbAvailable(false);
    } finally {
      setReconnecting(false);
    }
  };

  const checkDbStatus = async () => {
    try {
      const res = await axios.get(`${API}/db/status`);
      setDbAvailable(res.data.postgres_available);
      return res.data.postgres_available;
    } catch (error) {
      setDbAvailable(false);
      return false;
    }
  };

  // Check DB status on mount
  useEffect(() => {
    checkDbStatus();
  }, []);

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
    if (!dateStart && !dateEnd && !searchTerm.trim()) {
      toast.error("Selectați cel puțin o perioadă sau un nume de firmă");
      return;
    }
    try {
      // Auto-save config then run
      await axios.put(`${API}/config`, {
        search_term: searchTerm,
        schedule_hour: scheduleHour,
        schedule_minute: scheduleMinute,
        cron_enabled: cronEnabled,
        date_start: dateStart ? format(dateStart, 'yyyy-MM-dd') : null,
        date_end: dateEnd ? format(dateEnd, 'yyyy-MM-dd') : null
      });
      await axios.post(`${API}/run`);
      setDownloadLogs([]);
      toast.success("Job-ul de descărcare a fost pornit!");
      fetchData();
    } catch (error) {
      toast.error(error.response?.data?.detail || "Eroare la pornirea job-ului");
    }
  };

  const stopDownloadJob = async () => {
    try {
      await axios.post(`${API}/run/stop`);
      toast.success("Oprire solicitată. Job-ul se va opri după instituția curentă.");
      fetchData();
    } catch (error) {
      toast.error("Eroare la oprire");
    }
  };

  const searchPreviewDosare = async () => {
    if (!searchTerm.trim() && !dateStart && !dateEnd) {
      toast.error("Introduceți un nume de firmă sau o perioadă de timp");
      return;
    }
    
    setSearchLoading(true);
    try {
      const res = await axios.post(`${API}/search`, { 
        company_name: searchTerm.trim() || null,
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

  // Company Profile functions
  const openFirmaProfile = async (firmaId) => {
    setProfileLoading(true);
    setProfileModalOpen(true);
    setEditingCui(false);
    try {
      const res = await axios.get(`${API}/db/firma/${firmaId}`);
      setFirmaProfile(res.data);
      setEditCuiValue(res.data.basic_info?.cui || "");
    } catch (error) {
      toast.error("Eroare la încărcarea profilului firmei");
      setProfileModalOpen(false);
    } finally {
      setProfileLoading(false);
    }
  };

  const closeFirmaProfile = () => {
    setProfileModalOpen(false);
    setFirmaProfile(null);
    setEditingCui(false);
  };

  const saveCuiInProfile = async () => {
    if (!firmaProfile) return;
    setSavingCui(true);
    try {
      await axios.put(`${API}/db/firme/${firmaProfile.id}`, { cui: editCuiValue.trim() || null });
      toast.success("CUI actualizat cu succes!");
      setFirmaProfile(prev => ({ ...prev, basic_info: { ...prev.basic_info, cui: editCuiValue.trim() || null } }));
      setEditingCui(false);
      fetchData();
    } catch (error) {
      toast.error("Eroare la salvarea CUI-ului");
    } finally {
      setSavingCui(false);
    }
  };

  const saveInlineCui = async (firmaId) => {
    try {
      await axios.put(`${API}/db/firme/${firmaId}`, { cui: inlineEditValue.trim() || "" });
      toast.success("CUI salvat!");
      setInlineEditId(null);
      setInlineEditValue("");
      fetchData();
      if (activeTab === 'firme') loadFirme(firmePage, firmeSearch);
      if (activeTab === 'dbfinal') loadDbFinal(dbFinalPage, dbFinalSearch);
    } catch (error) {
      toast.error("Eroare la salvarea CUI-ului");
    }
  };

  // DB Final functions
  const loadDbFinalStats = useCallback(async () => {
    try {
      const res = await axios.get(`${API}/dbfinal/stats`);
      setDbFinalStats(res.data);
    } catch (error) {
      console.error("Error loading DB Final stats:", error);
    }
  }, []);

  const loadDbFinal = useCallback(async (page = 0, search = "") => {
    setDbFinalLoading(true);
    try {
      const res = await axios.get(`${API}/dbfinal/firme`, {
        params: { 
          skip: page * 50, 
          limit: 50, 
          search: search || undefined,
          doar_active: dbFinalFilters.doarActive || undefined,
          doar_cu_bilant: dbFinalFilters.doarCuBilant || undefined
        }
      });
      setDbFinalList(res.data.firme);
      setDbFinalTotal(res.data.total);
    } catch (error) {
      toast.error("Eroare la încărcarea firmelor");
    } finally {
      setDbFinalLoading(false);
    }
  }, [dbFinalFilters]);

  useEffect(() => {
    if (activeTab === 'dbfinal') {
      loadDbFinalStats();
      loadDbFinal(dbFinalPage, dbFinalSearch);
    }
  }, [activeTab, dbFinalPage, loadDbFinal, loadDbFinalStats]);

  const handleDbFinalSearch = () => {
    setDbFinalPage(0);
    loadDbFinal(0, dbFinalSearch);
  };

  const handleDbFinalFilterChange = (filter) => {
    setDbFinalFilters(prev => ({ ...prev, ...filter }));
    setDbFinalPage(0);
  };

  useEffect(() => {
    if (activeTab === 'dbfinal') {
      loadDbFinal(0, dbFinalSearch);
    }
  }, [dbFinalFilters]);

  // Enhanced ANAF test with logging
  const testAnafCuiFull = async () => {
    if (!anafTestCui) {
      toast.error("Introdu un CUI");
      return;
    }
    setAnafLoading(true);
    const timestamp = new Date().toLocaleTimeString('ro-RO');
    setAnafLogs(prev => [...prev, `[${timestamp}] 🔍 Test complet CUI: ${anafTestCui}...`].slice(-50));
    
    try {
      const res = await axios.get(`${API}/anaf/test-full/${anafTestCui}`);
      setAnafTestResult(res.data);
      
      // Log detailed analysis
      if (res.data.found) {
        const sections = Object.keys(res.data.sections_analysis || {});
        setAnafLogs(prev => [...prev, 
          `[${timestamp}] ✓ CUI ${anafTestCui}: Găsit!`,
          `[${timestamp}]    Secțiuni: ${sections.join(', ')}`,
        ].slice(-50));
        
        // Log each section's data
        for (const [section, data] of Object.entries(res.data.sections_analysis || {})) {
          if (data.total_fields) {
            setAnafLogs(prev => [...prev, 
              `[${timestamp}]    → ${section}: ${data.non_empty_fields}/${data.total_fields} câmpuri cu date`
            ].slice(-50));
          }
        }
      } else {
        setAnafLogs(prev => [...prev, `[${timestamp}] ✗ CUI ${anafTestCui}: Negăsit`].slice(-50));
      }
      
      toast.success("Test complet ANAF reușit!");
    } catch (error) {
      toast.error("Eroare la testarea ANAF API");
      setAnafLogs(prev => [...prev, `[${timestamp}] ✗ Eroare: ${error.message}`].slice(-50));
    } finally {
      setAnafLoading(false);
    }
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
      
      // Update logs from backend if available
      if (res.data.logs && res.data.logs.length > 0) {
        setAnafLogs(res.data.logs);
      }
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
    const timestamp = new Date().toLocaleTimeString('ro-RO');
    setAnafLogs(prev => [...prev, `[${timestamp}] Pornire sincronizare ANAF...`].slice(-50));
    
    try {
      const params = new URLSearchParams();
      if (options.limit) params.append('limit', options.limit);
      if (options.only_unsynced !== undefined) params.append('only_unsynced', options.only_unsynced);
      if (options.judet) params.append('judet', options.judet);
      
      await axios.post(`${API}/anaf/sync?${params.toString()}`);
      toast.success("Sincronizare ANAF pornită!");
      setAnafSyncRunning(true);
      setAnafLogs(prev => [...prev, `[${timestamp}] ✓ Sincronizare pornită cu succes`].slice(-50));
      loadAnafProgress();
    } catch (error) {
      const errMsg = error.response?.data?.detail || "Eroare la pornirea sincronizării";
      toast.error(errMsg);
      setAnafLogs(prev => [...prev, `[${timestamp}] ✗ Eroare: ${errMsg}`].slice(-50));
    } finally {
      setAnafLoading(false);
    }
  };

  const stopAnafSync = async () => {
    const timestamp = new Date().toLocaleTimeString('ro-RO');
    try {
      await axios.post(`${API}/anaf/sync-stop`);
      toast.info("Sincronizare oprită");
      setAnafSyncRunning(false);
      setAnafLogs(prev => [...prev, `[${timestamp}] ⏹ Sincronizare oprită de utilizator`].slice(-50));
    } catch (error) {
      toast.error("Eroare la oprirea sincronizării");
    }
  };

  const clearAnafLogs = () => {
    setAnafLogs([]);
  };

  const fixAnafTimestamps = async () => {
    try {
      const res = await axios.post(`${API}/anaf/fix-timestamps`);
      toast.success(`Timestamps reparate pentru ${res.data.fixed?.toLocaleString()} firme!`);
      loadAnafStats();
    } catch (error) {
      toast.error("Eroare la repararea timestamps");
    }
  };

  const resetAnafSyncStatus = async (judet = null) => {
    if (!window.confirm(`Ești sigur? Vei reseta statusul ANAF pentru TOATE firmele${judet ? ` din ${judet}` : ''}. Vor trebui re-sincronizate.`)) return;
    try {
      const params = judet ? `?judet=${encodeURIComponent(judet)}` : '';
      const res = await axios.post(`${API}/anaf/reset-sync-status${params}`);
      toast.success(res.data.message);
      loadAnafStats();
    } catch (error) {
      toast.error("Eroare la resetarea statusului");
    }
  };

  const testAnafCui = async () => {
    if (!anafTestCui) {
      toast.error("Introdu un CUI");
      return;
    }
    setAnafLoading(true);
    const timestamp = new Date().toLocaleTimeString('ro-RO');
    setAnafLogs(prev => [...prev, `[${timestamp}] Test CUI: ${anafTestCui}...`].slice(-50));
    
    try {
      const res = await axios.get(`${API}/anaf/test/${anafTestCui}`);
      setAnafTestResult(res.data);
      const found = res.data.found?.length > 0;
      toast.success("Test ANAF reușit!");
      setAnafLogs(prev => [...prev, `[${timestamp}] ✓ CUI ${anafTestCui}: ${found ? 'Găsit' : 'Negăsit'}`].slice(-50));
    } catch (error) {
      toast.error("Eroare la testarea ANAF API");
      setAnafTestResult({ error: error.message });
      setAnafLogs(prev => [...prev, `[${timestamp}] ✗ Eroare test CUI: ${error.message}`].slice(-50));
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

  // CAPTCHA Functions
  const openCaptchaModal = async () => {
    setCaptchaModalOpen(true);
    setCaptchaLoading(true);
    setCaptchaError(null);
    setCaptchaCode("");
    setCaptchaImageUrl(null);
    
    try {
      // Initialize CAPTCHA session
      const initRes = await axios.get(`${API}/mfinante/captcha/init`);
      if (initRes.data.success) {
        // Set image URL with timestamp to prevent caching
        // captcha_url already includes /api/, so don't add API prefix
        setCaptchaImageUrl(`${initRes.data.captcha_url}&r=${Date.now()}`);
      } else {
        setCaptchaError("Nu s-a putut inițializa sesiunea CAPTCHA");
      }
    } catch (error) {
      setCaptchaError(error.response?.data?.detail || "Eroare la încărcarea CAPTCHA");
    } finally {
      setCaptchaLoading(false);
    }
  };

  const refreshCaptcha = async () => {
    setCaptchaLoading(true);
    setCaptchaError(null);
    setCaptchaCode("");
    
    try {
      // Just reload the image with the SAME session - don't reinitialize!
      // The backend will serve a new captcha image for the existing session
      setCaptchaImageUrl(`${API}/mfinante/captcha/image?r=${Date.now()}`);
    } catch (error) {
      setCaptchaError("Eroare la reîncărcarea CAPTCHA");
    } finally {
      setCaptchaLoading(false);
    }
  };

  const submitCaptcha = async () => {
    if (!captchaCode || captchaCode.length < 3) {
      setCaptchaError("Introdu codul din imagine (minim 3 caractere)");
      return;
    }
    
    setCaptchaLoading(true);
    setCaptchaError(null);
    
    try {
      const res = await axios.post(`${API}/mfinante/captcha/solve?captcha_code=${encodeURIComponent(captchaCode)}`);
      
      if (res.data.success) {
        toast.success("CAPTCHA rezolvat! Sesiunea a fost setată automat.");
        setCaptchaModalOpen(false);
        loadMfProgress();
      } else {
        setCaptchaError(res.data.error || "CAPTCHA incorect");
        // DON'T refresh - let user see their error and try again with same image
        // Only clear the input
        setCaptchaCode("");
      }
    } catch (error) {
      const errorMsg = error.response?.data?.detail || "Eroare la verificarea CAPTCHA";
      setCaptchaError(errorMsg);
      
      // If session expired, we need to reinitialize
      if (errorMsg.includes("session") || errorMsg.includes("Session") || errorMsg.includes("expired")) {
        toast.error("Sesiunea a expirat. Se reinițializează...");
        openCaptchaModal();
      }
    } finally {
      setCaptchaLoading(false);
    }
  };

  const closeCaptchaModal = () => {
    setCaptchaModalOpen(false);
    setCaptchaCode("");
    setCaptchaError(null);
    setCaptchaImageUrl(null);
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

  // Build context object for page components
  const ctx = {
    // Global state
    config, stats, dbStats, files, runs, currentRun, loading,
    searchTerm, setSearchTerm, scheduleHour, setScheduleHour,
    scheduleMinute, setScheduleMinute, cronEnabled, setCronEnabled,
    dateStart, setDateStart, dateEnd, setDateEnd,
    searchPreview, setSearchPreview, searchLoading, setSearchLoading,
    showAdvancedConfig, setShowAdvancedConfig,
    downloadLogs, downloadProgress,
    importLoading, importResult, importLog, importError,
    setImportLog, setImportResult, setImportError,
    // Firme
    firmeList, firmeTotal, firmePage, setFirmePage, firmeSearch, setFirmeSearch, firmeLoading,
    inlineEditId, setInlineEditId, inlineEditValue, setInlineEditValue,
    // DB Final
    dbFinalStats, dbFinalList, dbFinalTotal, dbFinalPage, setDbFinalPage,
    dbFinalSearch, setDbFinalSearch, dbFinalLoading, dbFinalFilters,
    // ANAF
    anafStats, anafProgress, anafLoading, setAnafLoading, anafSyncRunning, setAnafSyncRunning,
    anafTestResult, setAnafTestResult, anafTestCui, setAnafTestCui, anafLogs, setAnafLogs,
    // MFinante
    mfStats, mfProgress, mfLoading, mfSession, setMfSession,
    mfTestCui, setMfTestCui, mfTestResult, setMfTestResult,
    // CAPTCHA
    captchaModalOpen, setCaptchaModalOpen, captchaLoading, setCaptchaLoading,
    captchaImageUrl, setCaptchaImageUrl, captchaCode, setCaptchaCode, captchaError, setCaptchaError,
    // Profile modal
    profileModalOpen, firmaProfile, profileLoading,
    editingCui, setEditingCui, editCuiValue, setEditCuiValue, savingCui,
    // Diagnostics
    diagnosticsData, duplicateDenumiri, duplicateCui, indexes,
    diagnosticsLoading, cleanupLoading, dbAvailable, reconnecting,
    // Handlers
    saveConfig, triggerRun, stopDownloadJob, searchPreviewDosare,
    downloadFile, deleteFile, handleCsvImport, exportFirme,
    loadFirme, handleFirmeSearch, openFirmaProfile, closeFirmaProfile,
    saveInlineCui, saveCuiInProfile,
    loadDbFinalStats, loadDbFinal, handleDbFinalSearch, handleDbFinalFilterChange,
    testAnafCui, testAnafCuiFull,
    startAnafSync, stopAnafSync, clearAnafLogs, loadAnafStats, loadAnafProgress, formatEta,
    fixAnafTimestamps, resetAnafSyncStatus,
    loadMfStats, loadMfProgress, setMfSessionId, openCaptchaModal, refreshCaptcha,
    submitCaptcha, closeCaptchaModal, testMfCui, startMfSync, stopMfSync,
    cleanupDuplicateDenumiri, cleanupDuplicateCui, cleanupOrphanedDosare,
    optimizeDatabase, createIndexes, migrateSchema,
    loadDiagnostics, reconnectDatabase, fetchData,
    // Helpers
    formatDate: (d) => d ? new Date(d).toLocaleString('ro-RO') : '-',
    formatBytes: (b) => b > 1024*1024 ? `${(b/1024/1024).toFixed(1)} MB` : `${(b/1024).toFixed(0)} KB`,
  };

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
        
        <div className="tab-navigation">
          <button className={`tab-btn ${activeTab === 'dashboard' ? 'active' : ''}`} onClick={() => setActiveTab('dashboard')} data-testid="tab-dashboard">
            <Activity size={18} /> Dashboard
          </button>
          <button className={`tab-btn ${activeTab === 'firme' ? 'active' : ''}`} onClick={() => setActiveTab('firme')} data-testid="firme-tab">
            <Building2 size={18} /> Firme ({dbStats?.firme_total?.toLocaleString() || 0})
          </button>
          <button className={`tab-btn ${activeTab === 'anaf' ? 'active' : ''}`} onClick={() => setActiveTab('anaf')} data-testid="tab-anaf">
            <Download size={18} /> Sync ANAF
          </button>
          <button className={`tab-btn ${activeTab === 'dbfinal' ? 'active' : ''}`} onClick={() => setActiveTab('dbfinal')} data-testid="tab-dbfinal">
            <CheckCircle2 size={18} /> DB Final
          </button>
          <button className={`tab-btn ${activeTab === 'diagnostics' ? 'active' : ''}`} onClick={() => setActiveTab('diagnostics')} data-testid="tab-diagnostics">
            <Database size={18} /> Diagnosticare DB
          </button>
        </div>
      </header>

      {!dbAvailable && (
        <div className="db-warning-banner" data-testid="db-warning">
          <AlertTriangle size={20} />
          <span>Baza de date PostgreSQL nu este disponibilă. Unele funcții nu vor funcționa.</span>
          <Button size="sm" onClick={reconnectDatabase} disabled={reconnecting}>
            {reconnecting ? <Loader2 className="animate-spin" size={16} /> : <RefreshCw size={16} />}
            {reconnecting ? "Se conectează..." : "Reconectare"}
          </Button>
        </div>
      )}

      <main className="main-content">
        {activeTab === 'dashboard' && <DashboardPage ctx={ctx} />}
        {activeTab === 'firme' && <FirmePage ctx={ctx} />}
        {activeTab === 'anaf' && <AnafPage ctx={ctx} />}
        {activeTab === 'dbfinal' && <DbFinalPage ctx={ctx} />}
        {activeTab === 'diagnostics' && <DiagnosticsPage ctx={ctx} />}
      </main>

      <footer className="app-footer">
        <p>Portal JUST Downloader • Descărcare automată dosare firme • 246 instituții</p>
      </footer>

      <FirmaProfileModal ctx={ctx} />
      <CaptchaModal ctx={ctx} />
    </div>
  );
}

export default App;
