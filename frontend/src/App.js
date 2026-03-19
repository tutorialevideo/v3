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
      await axios.put(`${API}/db/firme/${firmaId}`, { cui: inlineEditValue.trim() || null });
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
            className={`tab-btn ${activeTab === 'dbfinal' ? 'active' : ''}`}
            onClick={() => setActiveTab('dbfinal')}
            data-testid="tab-dbfinal"
          >
            <CheckCircle2 size={18} />
            DB Final
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

      {/* Database Connection Warning Banner */}
      {!dbAvailable && (
        <div className="db-warning-banner" data-testid="db-warning">
          <AlertTriangle size={20} />
          <span>Baza de date PostgreSQL nu este disponibilă. Unele funcții nu vor funcționa.</span>
          <Button 
            size="sm" 
            onClick={reconnectDatabase}
            disabled={reconnecting}
          >
            {reconnecting ? <Loader2 className="animate-spin" size={16} /> : <RefreshCw size={16} />}
            {reconnecting ? "Se conectează..." : "Reconectare"}
          </Button>
        </div>
      )}

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
              <CardDescription>Selectează perioada și pornește descărcarea</CardDescription>
            </CardHeader>
            <CardContent className="config-content">

              {/* PRIMARY ACTION: Date range + Start button */}
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
                        <Button variant="ghost" size="sm" className="clear-date-btn" onClick={() => setDateStart(null)}>
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
                        <Button variant="ghost" size="sm" className="clear-date-btn" onClick={() => setDateEnd(null)}>
                          Șterge data
                        </Button>
                      )}
                    </PopoverContent>
                  </Popover>
                </div>
              </div>

              {/* Start / Stop buttons */}
              <div style={{display: 'flex', gap: '8px', marginTop: '8px', marginBottom: '4px'}}>
                <Button
                  className="run-primary-btn"
                  onClick={triggerRun}
                  disabled={!!currentRun}
                  data-testid="run-now-btn"
                  style={{flex: 1}}
                >
                  {currentRun
                    ? <><Loader2 className="animate-spin" size={16} style={{marginRight: 8}} />Descărcare în curs...</>
                    : <><Play size={16} style={{marginRight: 8}} />Start Descărcare</>
                  }
                </Button>
                {currentRun && (
                  <Button
                    variant="destructive"
                    onClick={stopDownloadJob}
                    data-testid="stop-run-btn"
                    style={{flexShrink: 0}}
                  >
                    <XCircle size={16} style={{marginRight: 6}} />
                    Oprește
                  </Button>
                )}
              </div>

              {/* Live log panel — shown only when running or logs available */}
              {(currentRun || downloadLogs.length > 0) && (
                <div className="download-log-panel" data-testid="download-log-panel">
                  <div className="download-log-header">
                    <Activity size={14} />
                    <span>Log descărcare</span>
                    {downloadProgress && (
                      <span className="download-log-stats">
                        {downloadProgress.processed}/{downloadProgress.total} instituții
                        {downloadProgress.dosare_found > 0 && ` • ${downloadProgress.dosare_found} dosare`}
                        {downloadProgress.firme_new > 0 && ` • ${downloadProgress.firme_new} firme noi`}
                      </span>
                    )}
                  </div>
                  {downloadProgress && downloadProgress.total > 0 && (
                    <Progress
                      value={(downloadProgress.processed / downloadProgress.total) * 100}
                      className="download-log-progress"
                    />
                  )}
                  <ScrollArea className="download-log-scroll">
                    <div className="download-log-content">
                      {downloadLogs.length === 0
                        ? <span className="download-log-empty">Se inițializează...</span>
                        : downloadLogs.map((line, i) => (
                            <div key={i} className="download-log-line">{line}</div>
                          ))
                      }
                    </div>
                  </ScrollArea>
                </div>
              )}

              {/* Advanced settings toggle */}
              <button
                className="advanced-toggle"
                onClick={() => setShowAdvancedConfig(v => !v)}
                data-testid="advanced-config-toggle"
                style={{
                  background: 'none', border: 'none', cursor: 'pointer',
                  color: 'var(--text-muted)', fontSize: '0.82rem',
                  display: 'flex', alignItems: 'center', gap: '4px',
                  marginTop: '8px', padding: '2px 0'
                }}
              >
                <ChevronDown size={14} style={{transform: showAdvancedConfig ? 'rotate(180deg)' : 'none', transition: 'transform 0.2s'}} />
                {showAdvancedConfig ? 'Ascunde setări avansate' : 'Setări avansate (firmă, cron)'}
              </button>

              {showAdvancedConfig && (
                <div style={{marginTop: '12px'}}>
                  <div className="form-group">
                    <Label htmlFor="searchTerm">Nume Firmă <span style={{fontWeight: 'normal', opacity: 0.6, fontSize: '0.85em'}}>(opțional)</span></Label>
                    <div className="search-input-group">
                      <Input
                        id="searchTerm"
                        data-testid="search-term-input"
                        value={searchTerm}
                        onChange={(e) => setSearchTerm(e.target.value)}
                        placeholder="Ex: SC EXEMPLU SRL — lasă gol pentru toate firmele"
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
                            type="number" min="0" max="23"
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
                            type="number" min="0" max="59"
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

                  <div className="button-group" style={{marginTop: '12px'}}>
                    <Button onClick={saveConfig} data-testid="save-config-btn" variant="outline" size="sm">
                      Salvează Configurația
                    </Button>
                  </div>
                </div>
              )}

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
                          <tr key={firma.id} data-testid={`firma-row-${firma.id}`} className="clickable-row" onClick={() => openFirmaProfile(firma.id)}>
                            <td className="col-cui" onClick={e => e.stopPropagation()}>
                              {inlineEditId === firma.id ? (
                                <div className="cui-edit-row" style={{display:'flex',gap:'4px',alignItems:'center'}}>
                                  <input
                                    className="cui-edit-input"
                                    value={inlineEditValue}
                                    onChange={e => setInlineEditValue(e.target.value)}
                                    placeholder="CUI"
                                    autoFocus
                                    style={{width:'90px',padding:'2px 6px',fontSize:'0.8rem'}}
                                    onKeyDown={e => { if (e.key === 'Enter') saveInlineCui(firma.id); if (e.key === 'Escape') setInlineEditId(null); }}
                                  />
                                  <button onClick={() => saveInlineCui(firma.id)} style={{background:'none',border:'none',cursor:'pointer',color:'var(--primary)',padding:'2px'}} title="Salvează"><CheckCircle2 size={14}/></button>
                                  <button onClick={() => setInlineEditId(null)} style={{background:'none',border:'none',cursor:'pointer',color:'var(--text-muted)',padding:'2px'}} title="Anulează"><XCircle size={14}/></button>
                                </div>
                              ) : (
                                <div style={{display:'flex',gap:'4px',alignItems:'center'}}>
                                  {firma.cui ? <Badge variant="outline" className="badge-cui">{firma.cui}</Badge> : <span className="no-cui">-</span>}
                                  <button
                                    onClick={e => { e.stopPropagation(); setInlineEditId(firma.id); setInlineEditValue(firma.cui || ""); }}
                                    style={{background:'none',border:'none',cursor:'pointer',color:'var(--text-muted)',padding:'2px',opacity:0.6}}
                                    title="Editează CUI"
                                    data-testid={`edit-cui-inline-${firma.id}`}
                                  ><Wrench size={11}/></button>
                                </div>
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
                      <span>Batch: {anafProgress.current_batch} / {anafProgress.total_batches}</span>
                      <span>Găsite: {anafProgress.found?.toLocaleString()}</span>
                      <span>Negăsite: {anafProgress.not_found?.toLocaleString()}</span>
                      <span>Erori: {anafProgress.errors?.toLocaleString()}</span>
                      <span>ETA: {formatEta(anafProgress.eta_seconds)}</span>
                    </div>
                    <Button variant="destructive" onClick={stopAnafSync} className="stop-btn">
                      <XCircle size={16} />
                      Oprește Sincronizarea
                    </Button>
                  </div>
                ) : (
                  <div className="sync-actions">
                    <div className="sync-batch-controls">
                      <h4>Sincronizare în Batch-uri</h4>
                      <div className="batch-buttons">
                        <Button 
                          onClick={() => startAnafSync({ only_unsynced: true, limit: 100 })}
                          disabled={anafLoading || anafSyncRunning}
                          variant="outline"
                          size="sm"
                        >
                          100 firme
                        </Button>
                        <Button 
                          onClick={() => startAnafSync({ only_unsynced: true, limit: 500 })}
                          disabled={anafLoading || anafSyncRunning}
                          variant="outline"
                          size="sm"
                        >
                          500 firme
                        </Button>
                        <Button 
                          onClick={() => startAnafSync({ only_unsynced: true, limit: 1000 })}
                          disabled={anafLoading || anafSyncRunning}
                          variant="outline"
                          size="sm"
                        >
                          1.000 firme
                        </Button>
                        <Button 
                          onClick={() => startAnafSync({ only_unsynced: true, limit: 5000 })}
                          disabled={anafLoading || anafSyncRunning}
                          variant="outline"
                          size="sm"
                        >
                          5.000 firme
                        </Button>
                      </div>
                    </div>
                    <Separator className="my-4" />
                    <div className="sync-options">
                      <Button 
                        onClick={() => startAnafSync({ only_unsynced: true })}
                        disabled={anafLoading || anafSyncRunning}
                        className="sync-btn"
                      >
                        <Download size={16} />
                        Sync Toate Nesincronizate ({anafStats?.not_synced?.toLocaleString() || 0})
                      </Button>
                      <Button 
                        variant="secondary"
                        onClick={() => startAnafSync({ only_unsynced: false })}
                        disabled={anafLoading || anafSyncRunning}
                      >
                        Re-sync toate firmele
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

            {/* Logs Card */}
            <Card className="anaf-logs-card" data-testid="anaf-logs">
              <CardHeader>
                <div className="card-header-with-action">
                  <CardTitle className="card-title">
                    <Activity size={20} />
                    Loguri Sincronizare
                  </CardTitle>
                  <Button variant="ghost" size="sm" onClick={clearAnafLogs}>
                    <Trash2 size={14} />
                    Șterge
                  </Button>
                </div>
              </CardHeader>
              <CardContent>
                <div className="logs-container">
                  {anafLogs.length === 0 ? (
                    <p className="no-logs">Niciun log încă. Pornește o sincronizare pentru a vedea logurile.</p>
                  ) : (
                    <pre className="logs-content">
                      {anafLogs.map((log, i) => (
                        <div key={i} className={`log-line ${log.includes('✗') ? 'error' : log.includes('✓') ? 'success' : ''}`}>
                          {log}
                        </div>
                      ))}
                    </pre>
                  )}
                </div>
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
                    onKeyDown={(e) => e.key === 'Enter' && testAnafCuiFull()}
                    className="test-input"
                  />
                  <Button onClick={testAnafCui} disabled={anafLoading} variant="outline">
                    {anafLoading ? <Loader2 className="animate-spin" size={16} /> : <Search size={16} />}
                    Verifică rapid
                  </Button>
                  <Button onClick={testAnafCuiFull} disabled={anafLoading}>
                    {anafLoading ? <Loader2 className="animate-spin" size={16} /> : <BarChart3 size={16} />}
                    Date complete
                  </Button>
                </div>
                {anafTestResult && (
                  <div className="test-result">
                    {anafTestResult.found !== undefined && (
                      <div style={{marginBottom:'8px',display:'flex',gap:'8px',alignItems:'center',flexWrap:'wrap'}}>
                        <Badge variant={anafTestResult.found ? 'success' : 'secondary'}>
                          {anafTestResult.found ? 'Găsit' : 'Negăsit'}
                        </Badge>
                        {anafTestResult.sections_analysis && Object.entries(anafTestResult.sections_analysis).map(([section, data]) => (
                          data.non_empty_fields > 0 && (
                            <Badge key={section} variant="outline" style={{fontSize:'0.7rem'}}>
                              {section}: {data.non_empty_fields}/{data.total_fields}
                            </Badge>
                          )
                        ))}
                      </div>
                    )}
                    {anafTestResult.raw_response?.found?.[0]?.date_generale && (
                      <div className="anaf-quick-summary">
                        {(() => {
                          const dg = anafTestResult.raw_response.found[0].date_generale;
                          return (
                            <div className="profile-grid" style={{marginBottom:'8px'}}>
                              <div className="profile-field"><label>Denumire:</label><span className="value">{dg.denumire}</span></div>
                              <div className="profile-field"><label>Stare:</label><span className="value">{dg.stare_inregistrare}</span></div>
                              <div className="profile-field"><label>Nr. Reg. Com.:</label><span className="value">{dg.nrRegCom}</span></div>
                              <div className="profile-field"><label>Cod CAEN:</label><span className="value">{dg.cod_CAEN}</span></div>
                              <div className="profile-field"><label>Adresă:</label><span className="value">{dg.adresa}</span></div>
                              <div className="profile-field"><label>Telefon:</label><span className="value">{dg.telefon || '-'}</span></div>
                            </div>
                          );
                        })()}
                      </div>
                    )}
                    <details>
                      <summary style={{cursor:'pointer',fontSize:'0.8rem',color:'var(--text-muted)',marginBottom:'4px'}}>JSON Raw complet</summary>
                      <ScrollArea style={{height:'220px',border:'1px solid var(--border)',borderRadius:'4px',padding:'8px'}}>
                        <pre style={{fontSize:'0.72rem',lineHeight:'1.4',whiteSpace:'pre-wrap',wordBreak:'break-all'}}>{JSON.stringify(anafTestResult, null, 2)}</pre>
                      </ScrollArea>
                    </details>
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
                  
                  {/* New CAPTCHA Button */}
                  <div className="captcha-button-section">
                    <Button 
                      onClick={openCaptchaModal} 
                      className="captcha-main-btn"
                      disabled={captchaLoading}
                    >
                      {captchaLoading ? (
                        <Loader2 className="animate-spin" size={16} />
                      ) : (
                        <Shield size={16} />
                      )}
                      Rezolvă CAPTCHA aici
                    </Button>
                    <span className="captcha-hint">
                      sau folosește metoda manuală de mai jos
                    </span>
                  </div>

                  {/* Manual method (collapsed by default) */}
                  <details className="manual-session-method">
                    <summary>Metodă alternativă (manuală)</summary>
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
                  </details>

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
        ) : activeTab === 'dbfinal' ? (
          /* DB Final Tab - Companies with CUI */
          <div className="dbfinal-section" data-testid="dbfinal-section">
            <Card className="dbfinal-stats-card">
              <CardHeader>
                <div className="card-header-with-action">
                  <div>
                    <CardTitle className="card-title">
                      <CheckCircle2 size={20} />
                      DB Final - Firme cu CUI
                    </CardTitle>
                    <CardDescription>
                      Firme validate cu CUI, pregătite pentru procesare avansată
                    </CardDescription>
                  </div>
                  <Button variant="outline" onClick={() => { loadDbFinalStats(); loadDbFinal(dbFinalPage, dbFinalSearch); }}>
                    <RefreshCw size={16} />
                    Reîncarcă
                  </Button>
                </div>
              </CardHeader>
              <CardContent>
                {dbFinalStats && (
                  <div className="stats-grid-4">
                    <div className="stat-card primary">
                      <span className="stat-value">{dbFinalStats.total_cu_cui?.toLocaleString() || 0}</span>
                      <span className="stat-label">Total cu CUI</span>
                    </div>
                    <div className="stat-card success">
                      <span className="stat-value">{dbFinalStats.sincronizate_anaf?.toLocaleString() || 0}</span>
                      <span className="stat-label">Sincronizate ANAF</span>
                    </div>
                    <div className="stat-card info">
                      <span className="stat-value">{dbFinalStats.cu_date_bilant?.toLocaleString() || 0}</span>
                      <span className="stat-label">Cu Bilanț</span>
                    </div>
                    <div className="stat-card warning">
                      <span className="stat-value">{dbFinalStats.active?.toLocaleString() || 0}</span>
                      <span className="stat-label">Active</span>
                    </div>
                  </div>
                )}
              </CardContent>
            </Card>

            {/* Filters and Search */}
            <Card className="dbfinal-filters-card">
              <CardContent>
                <div className="dbfinal-controls">
                  <div className="search-box">
                    <input
                      type="text"
                      placeholder="Caută după denumire sau CUI..."
                      value={dbFinalSearch}
                      onChange={(e) => setDbFinalSearch(e.target.value)}
                      onKeyPress={(e) => e.key === 'Enter' && handleDbFinalSearch()}
                    />
                    <Button onClick={handleDbFinalSearch}>
                      <Search size={16} />
                      Caută
                    </Button>
                  </div>
                  <div className="filter-toggles">
                    <label className="filter-toggle">
                      <input
                        type="checkbox"
                        checked={dbFinalFilters.doarActive}
                        onChange={(e) => handleDbFinalFilterChange({ doarActive: e.target.checked })}
                      />
                      <span>Doar Active</span>
                    </label>
                    <label className="filter-toggle">
                      <input
                        type="checkbox"
                        checked={dbFinalFilters.doarCuBilant}
                        onChange={(e) => handleDbFinalFilterChange({ doarCuBilant: e.target.checked })}
                      />
                      <span>Doar cu Bilanț</span>
                    </label>
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* Companies List */}
            <Card className="dbfinal-list-card">
              <CardHeader>
                <CardTitle className="card-title">
                  Firme ({dbFinalTotal.toLocaleString()})
                </CardTitle>
              </CardHeader>
              <CardContent>
                {dbFinalLoading ? (
                  <div className="loading-spinner">
                    <Loader2 className="animate-spin" size={32} />
                    <span>Se încarcă...</span>
                  </div>
                ) : (
                  <div className="table-container">
                    <table className="dbfinal-table">
                      <thead>
                        <tr>
                          <th>CUI</th>
                          <th>Denumire</th>
                          <th>Județ</th>
                          <th>Stare</th>
                          <th>Cifră Afaceri</th>
                          <th>Profit</th>
                          <th>Angajați</th>
                          <th>An Bilanț</th>
                          <th>TVA</th>
                          <th>Sync</th>
                        </tr>
                      </thead>
                      <tbody>
                        {dbFinalList.map((firma) => (
                          <tr key={firma.id} className="clickable-row" onClick={() => openFirmaProfile(firma.id)}>
                            <td onClick={e => e.stopPropagation()}>
                              {inlineEditId === firma.id ? (
                                <div style={{display:'flex',gap:'4px',alignItems:'center'}}>
                                  <input className="cui-edit-input" value={inlineEditValue} onChange={e => setInlineEditValue(e.target.value)} placeholder="CUI" autoFocus style={{width:'90px',padding:'2px 6px',fontSize:'0.8rem'}} onKeyDown={e => { if (e.key === 'Enter') saveInlineCui(firma.id); if (e.key === 'Escape') setInlineEditId(null); }} />
                                  <button onClick={() => saveInlineCui(firma.id)} style={{background:'none',border:'none',cursor:'pointer',color:'var(--primary)',padding:'2px'}}><CheckCircle2 size={14}/></button>
                                  <button onClick={() => setInlineEditId(null)} style={{background:'none',border:'none',cursor:'pointer',color:'var(--text-muted)',padding:'2px'}}><XCircle size={14}/></button>
                                </div>
                              ) : (
                                <div style={{display:'flex',gap:'4px',alignItems:'center'}}>
                                  <Badge variant="outline">{firma.cui}</Badge>
                                  <button onClick={e => { e.stopPropagation(); setInlineEditId(firma.id); setInlineEditValue(firma.cui || ""); }} style={{background:'none',border:'none',cursor:'pointer',color:'var(--text-muted)',padding:'2px',opacity:0.6}} title="Editează CUI"><Wrench size={11}/></button>
                                </div>
                              )}
                            </td>
                            <td className="col-denumire" title={firma.denumire}>{firma.denumire}</td>
                            <td>{firma.judet || '-'}</td>
                            <td>
                              {firma.stare && (
                                <Badge variant={firma.stare.includes('ACTIV') && !firma.stare.includes('INACTIV') ? 'success' : 'secondary'}>
                                  {firma.stare.substring(0, 20)}
                                </Badge>
                              )}
                            </td>
                            <td className="col-number">{firma.cifra_afaceri ? `${(firma.cifra_afaceri / 1000).toFixed(0)}k` : '-'}</td>
                            <td className="col-number">{firma.profit ? `${(firma.profit / 1000).toFixed(0)}k` : '-'}</td>
                            <td className="col-number">{firma.angajati || '-'}</td>
                            <td>{firma.an_bilant || '-'}</td>
                            <td>{firma.platitor_tva ? <CheckCircle2 size={16} className="text-success" /> : <XCircle size={16} className="text-muted" />}</td>
                            <td>
                              {firma.anaf_sync && <Badge variant="outline" className="sync-badge">ANAF</Badge>}
                              {firma.mf_sync && <Badge variant="outline" className="sync-badge">MF</Badge>}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}

                {/* Pagination */}
                <div className="firme-pagination">
                  <Button 
                    variant="outline" 
                    disabled={dbFinalPage === 0 || dbFinalLoading}
                    onClick={() => setDbFinalPage(p => Math.max(0, p - 1))}
                  >
                    ← Anterior
                  </Button>
                  <span className="pagination-info">
                    Pagina {dbFinalPage + 1} din {Math.ceil(dbFinalTotal / 50) || 1}
                  </span>
                  <Button 
                    variant="outline" 
                    disabled={(dbFinalPage + 1) * 50 >= dbFinalTotal || dbFinalLoading}
                    onClick={() => setDbFinalPage(p => p + 1)}
                  >
                    Următor →
                  </Button>
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

      {/* Firma Profile Modal */}
      {profileModalOpen && (
        <div className="modal-overlay" onClick={closeFirmaProfile}>
          <div className="profile-modal" onClick={e => e.stopPropagation()}>
            <div className="profile-modal-header">
              <h3>
                <Building2 size={20} />
                {firmaProfile?.basic_info?.denumire || 'Profil Firmă'}
              </h3>
              <button className="close-btn" onClick={closeFirmaProfile}>
                <XCircle size={20} />
              </button>
            </div>

            <div className="profile-modal-body">
              {profileLoading ? (
                <div className="profile-loading">
                  <Loader2 className="animate-spin" size={32} />
                  <p>Se încarcă profilul...</p>
                </div>
              ) : firmaProfile ? (
                <div className="profile-content">

                  {/* CUI + Identificare */}
                  <div className="profile-section">
                    <h4>Identificare</h4>
                    <div className="profile-grid">
                      {/* CUI with inline edit */}
                      <div className="profile-field cui-field">
                        <label>CUI:</label>
                        {editingCui ? (
                          <div className="cui-edit-row">
                            <input
                              className="cui-edit-input"
                              value={editCuiValue}
                              onChange={e => setEditCuiValue(e.target.value)}
                              placeholder="Ex: 14918042"
                              autoFocus
                              onKeyDown={e => { if (e.key === 'Enter') saveCuiInProfile(); if (e.key === 'Escape') setEditingCui(false); }}
                            />
                            <Button size="sm" onClick={saveCuiInProfile} disabled={savingCui} data-testid="save-cui-profile-btn">
                              {savingCui ? <Loader2 className="animate-spin" size={14} /> : <CheckCircle2 size={14} />}
                            </Button>
                            <Button size="sm" variant="ghost" onClick={() => setEditingCui(false)}>
                              <XCircle size={14} />
                            </Button>
                          </div>
                        ) : (
                          <div className="cui-display-row">
                            <span className="value highlight">{firmaProfile.basic_info?.cui || <em style={{opacity:0.5}}>Nedefinit</em>}</span>
                            <button className="edit-cui-btn" onClick={() => { setEditingCui(true); setEditCuiValue(firmaProfile.basic_info?.cui || ""); }} data-testid="edit-cui-btn" title="Editează CUI">
                              <Wrench size={13} />
                            </button>
                          </div>
                        )}
                      </div>
                      <div className="profile-field"><label>Denumire:</label><span className="value">{firmaProfile.basic_info?.denumire || '-'}</span></div>
                      <div className="profile-field"><label>Forma Juridică:</label><span className="value">{firmaProfile.basic_info?.forma_juridica || '-'}</span></div>
                      <div className="profile-field"><label>Nr. Înregistrare:</label><span className="value">{firmaProfile.basic_info?.cod_inregistrare || firmaProfile.basic_info?.cod_onrc || '-'}</span></div>
                      <div className="profile-field"><label>Data Înregistrare:</label><span className="value">{firmaProfile.basic_info?.data_inregistrare || '-'}</span></div>
                      {firmaProfile.adresa?.judet && <div className="profile-field"><label>Județ:</label><span className="value">{firmaProfile.adresa.judet}</span></div>}
                      {firmaProfile.adresa?.localitate && <div className="profile-field"><label>Localitate:</label><span className="value">{firmaProfile.adresa.localitate}</span></div>}
                      {firmaProfile.adresa?.strada && (
                        <div className="profile-field profile-field-wide">
                          <label>Adresă:</label>
                          <span className="value">{[firmaProfile.adresa.strada, firmaProfile.adresa.numar, firmaProfile.adresa.bloc && `Bl.${firmaProfile.adresa.bloc}`, firmaProfile.adresa.scara && `Sc.${firmaProfile.adresa.scara}`, firmaProfile.adresa.apartament && `Ap.${firmaProfile.adresa.apartament}`].filter(Boolean).join(', ')}</span>
                        </div>
                      )}
                    </div>
                  </div>

                  {/* ANAF Data */}
                  {firmaProfile.anaf_data?.anaf_sync_status && (
                    <div className="profile-section">
                      <h4>Date ANAF <span className={`profile-badge ${firmaProfile.anaf_data.anaf_sync_status === 'success' ? 'success' : 'warning'}`}>{firmaProfile.anaf_data.anaf_sync_status}</span></h4>
                      <div className="profile-grid">
                        <div className="profile-field profile-field-wide"><label>Stare:</label><span className="value">{firmaProfile.anaf_data.anaf_stare || '-'}</span></div>
                        <div className="profile-field"><label>Nr. Reg. Com.:</label><span className="value">{firmaProfile.anaf_data.anaf_nr_reg_com || '-'}</span></div>
                        <div className="profile-field"><label>Cod CAEN:</label><span className="value">{firmaProfile.anaf_data.anaf_cod_caen || '-'}</span></div>
                        <div className="profile-field"><label>Plătitor TVA:</label><span className={`value ${firmaProfile.anaf_data.anaf_platitor_tva ? 'text-success' : ''}`}>{firmaProfile.anaf_data.anaf_platitor_tva ? 'DA' : 'NU'}</span></div>
                        <div className="profile-field"><label>e-Factură:</label><span className={`value ${firmaProfile.anaf_data.anaf_e_factura ? 'text-success' : ''}`}>{firmaProfile.anaf_data.anaf_e_factura ? 'DA' : 'NU'}</span></div>
                        <div className="profile-field"><label>Inactiv:</label><span className="value">{firmaProfile.anaf_data.anaf_inactiv ? 'DA' : 'NU'}</span></div>
                        {firmaProfile.anaf_data.anaf_organ_fiscal && <div className="profile-field profile-field-wide"><label>Organ Fiscal:</label><span className="value">{firmaProfile.anaf_data.anaf_organ_fiscal}</span></div>}
                        {firmaProfile.anaf_data.anaf_forma_proprietate && <div className="profile-field profile-field-wide"><label>Formă Proprietate:</label><span className="value">{firmaProfile.anaf_data.anaf_forma_proprietate}</span></div>}
                        <div className="profile-field"><label>Sync ANAF:</label><span className="value muted">{firmaProfile.anaf_data.anaf_last_sync ? new Date(firmaProfile.anaf_data.anaf_last_sync).toLocaleDateString('ro-RO') : 'Nesincronizat'}</span></div>
                      </div>
                    </div>
                  )}

                  {/* MFinante Summary */}
                  {firmaProfile.mfinante_data?.mf_sync_status === 'success' && (
                    <div className="profile-section">
                      <h4>Date MFinante — Bilanț {firmaProfile.mfinante_data.mf_an_bilant || '-'}</h4>
                      <div className="profile-grid">
                        <div className="profile-field"><label>Cifra Afaceri:</label><span className="value highlight">{firmaProfile.mfinante_data.mf_cifra_afaceri?.toLocaleString('ro-RO') || '-'} RON</span></div>
                        <div className="profile-field"><label>Venituri Totale:</label><span className="value">{firmaProfile.mfinante_data.mf_venituri_totale?.toLocaleString('ro-RO') || '-'} RON</span></div>
                        <div className="profile-field"><label>Profit Net:</label><span className={`value ${firmaProfile.mfinante_data.mf_profit_net > 0 ? 'text-success' : ''}`}>{firmaProfile.mfinante_data.mf_profit_net?.toLocaleString('ro-RO') || (firmaProfile.mfinante_data.mf_pierdere_neta ? `-${firmaProfile.mfinante_data.mf_pierdere_neta?.toLocaleString('ro-RO')}` : '-')} RON</span></div>
                        <div className="profile-field"><label>Nr. Angajați:</label><span className="value">{firmaProfile.mfinante_data.mf_numar_angajati || '-'}</span></div>
                        <div className="profile-field"><label>Active Imob.:</label><span className="value">{firmaProfile.mfinante_data.mf_active_imobilizate?.toLocaleString('ro-RO') || '-'} RON</span></div>
                        <div className="profile-field"><label>Active Circ.:</label><span className="value">{firmaProfile.mfinante_data.mf_active_circulante?.toLocaleString('ro-RO') || '-'} RON</span></div>
                        <div className="profile-field"><label>Capitaluri Proprii:</label><span className="value">{firmaProfile.mfinante_data.mf_capitaluri_proprii?.toLocaleString('ro-RO') || '-'} RON</span></div>
                        <div className="profile-field"><label>Datorii Totale:</label><span className="value">{firmaProfile.mfinante_data.mf_datorii?.toLocaleString('ro-RO') || '-'} RON</span></div>
                        {firmaProfile.mfinante_data.mf_ani_disponibili && <div className="profile-field"><label>Ani disponibili:</label><span className="value muted">{firmaProfile.mfinante_data.mf_ani_disponibili}</span></div>}
                      </div>
                    </div>
                  )}

                  {/* Bilanturi History Table */}
                  {firmaProfile.bilanturi_history?.length > 0 && (
                    <div className="profile-section">
                      <h4>Istoric Bilanțuri ({firmaProfile.bilanturi_history.length} ani)</h4>
                      <div className="bilanturi-table-container">
                        <table className="bilanturi-table">
                          <thead>
                            <tr>
                              <th>An</th>
                              <th>Cifra Afaceri</th>
                              <th>Profit/Pierdere Net</th>
                              <th>Angajați</th>
                              <th>Active Totale</th>
                              <th>Capitaluri Proprii</th>
                              <th>Datorii</th>
                            </tr>
                          </thead>
                          <tbody>
                            {firmaProfile.bilanturi_history.map((b, i) => (
                              <tr key={i}>
                                <td><strong>{b.an}</strong></td>
                                <td>{b.cifra_afaceri_neta?.toLocaleString('ro-RO') || '-'}</td>
                                <td className={b.profit_net > 0 ? 'text-success' : b.pierdere_neta > 0 ? 'text-danger' : ''}>
                                  {b.profit_net ? `+${b.profit_net.toLocaleString('ro-RO')}` : b.pierdere_neta ? `-${b.pierdere_neta.toLocaleString('ro-RO')}` : '-'}
                                </td>
                                <td>{b.numar_angajati || '-'}</td>
                                <td>{b.active_imobilizate || b.active_circulante ? ((b.active_imobilizate||0) + (b.active_circulante||0)).toLocaleString('ro-RO') : '-'}</td>
                                <td>{b.capitaluri_proprii?.toLocaleString('ro-RO') || '-'}</td>
                                <td>{b.datorii?.toLocaleString('ro-RO') || '-'}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  )}

                  {/* Dosare */}
                  {firmaProfile.dosare_summary?.total > 0 && (
                    <div className="profile-section">
                      <h4>Dosare ({firmaProfile.dosare_summary.total} total)</h4>
                      <div className="dosare-list">
                        {firmaProfile.dosare_summary.recente.map((d, i) => (
                          <div key={i} className="dosar-item">
                            <div className="dosar-nr">{d.numar_dosar}</div>
                            <div className="dosar-details">
                              <span className="dosar-inst">{d.institutie}</span>
                              {d.obiect && <span className="dosar-obiect">{d.obiect}</span>}
                              {d.stadiu && <Badge variant="outline" className="dosar-stadiu">{d.stadiu}</Badge>}
                            </div>
                            <div className="dosar-data">{d.data_dosar || '-'}</div>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}

                </div>
              ) : (
                <p>Nu s-a putut încărca profilul.</p>
              )}
            </div>

            <div className="profile-modal-footer">
              <Button variant="outline" onClick={closeFirmaProfile}>Închide</Button>
            </div>
          </div>
        </div>
      )}

      {/* CAPTCHA Modal */}
      {captchaModalOpen && (
        <div className="modal-overlay" onClick={closeCaptchaModal}>
          <div className="captcha-modal" onClick={e => e.stopPropagation()}>
            <div className="captcha-modal-header">
              <h3>
                <Shield size={20} />
                Rezolvă CAPTCHA - MFinante
              </h3>
              <button className="close-btn" onClick={closeCaptchaModal}>
                <XCircle size={20} />
              </button>
            </div>
            
            <div className="captcha-modal-body">
              {captchaLoading ? (
                <div className="captcha-loading">
                  <Loader2 className="animate-spin" size={32} />
                  <p>Se încarcă CAPTCHA...</p>
                </div>
              ) : captchaImageUrl ? (
                <>
                  <div className="captcha-image-container">
                    <img 
                      src={captchaImageUrl} 
                      alt="CAPTCHA" 
                      className="captcha-image"
                      onError={() => setCaptchaError("Nu s-a putut încărca imaginea CAPTCHA")}
                    />
                    <button 
                      className="refresh-captcha-btn" 
                      onClick={() => {
                        // Reinitialize to get a completely new session and image
                        openCaptchaModal();
                      }}
                      title="Generează un nou CAPTCHA"
                    >
                      <RefreshCw size={16} />
                    </button>
                  </div>
                  
                  <div className="captcha-input-section">
                    <label htmlFor="captcha-input">Introdu codul EXACT din imagine (atenție la majuscule/minuscule):</label>
                    <input
                      id="captcha-input"
                      type="text"
                      value={captchaCode}
                      onChange={(e) => setCaptchaCode(e.target.value)}
                      placeholder="Ex: aB12cD"
                      className="captcha-input"
                      autoFocus
                      onKeyPress={(e) => e.key === 'Enter' && submitCaptcha()}
                    />
                    <span className="captcha-hint-text">Codul este case-sensitive (diferență între litere mari/mici)</span>
                  </div>
                  
                  {captchaError && (
                    <div className="captcha-error">
                      <XCircle size={16} />
                      {captchaError}
                    </div>
                  )}
                </>
              ) : (
                <div className="captcha-error">
                  <XCircle size={16} />
                  {captchaError || "Eroare la încărcarea CAPTCHA"}
                </div>
              )}
            </div>
            
            <div className="captcha-modal-footer">
              <Button variant="outline" onClick={closeCaptchaModal}>
                Anulează
              </Button>
              <Button 
                onClick={submitCaptcha} 
                disabled={captchaLoading || !captchaCode}
              >
                {captchaLoading ? (
                  <Loader2 className="animate-spin" size={16} />
                ) : (
                  <CheckCircle2 size={16} />
                )}
                Verifică CAPTCHA
              </Button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default App;
