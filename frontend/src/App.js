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
  AlertCircle
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
  const [activeTab, setActiveTab] = useState('dashboard'); // 'dashboard' or 'firme'
  const [firmeList, setFirmeList] = useState([]);
  const [firmeTotal, setFirmeTotal] = useState(0);
  const [firmePage, setFirmePage] = useState(0);
  const [firmeSearch, setFirmeSearch] = useState("");
  const [firmeLoading, setFirmeLoading] = useState(false);

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
    const file = event.target.files?.[0];
    if (!file) return;

    setImportLoading(true);
    setImportResult(null);
    setImportError(null);
    setImportLog([
      `[${new Date().toLocaleTimeString()}] Fișier selectat: ${file.name}`,
      `[${new Date().toLocaleTimeString()}] Dimensiune: ${(file.size / 1024 / 1024).toFixed(2)} MB`,
      `[${new Date().toLocaleTimeString()}] Se încarcă fișierul...`
    ]);
    
    const formData = new FormData();
    formData.append('file', file);

    try {
      setImportLog(prev => [...prev, `[${new Date().toLocaleTimeString()}] Se procesează pe server...`]);
      
      const res = await axios.post(`${API}/db/import-cui`, formData, {
        headers: { 'Content-Type': 'multipart/form-data' },
        timeout: 600000 // 10 minute timeout pentru fișiere mari
      });
      
      setImportResult(res.data);
      setImportLog(prev => [
        ...prev,
        `[${new Date().toLocaleTimeString()}] ✓ Import finalizat cu succes!`,
        `[${new Date().toLocaleTimeString()}] Rânduri procesate: ${res.data.total_rows?.toLocaleString()}`,
        `[${new Date().toLocaleTimeString()}] Firme create: ${res.data.created_new?.toLocaleString()}`,
        `[${new Date().toLocaleTimeString()}] PFA/II sărite: ${res.data.skipped_not_company?.toLocaleString()}`
      ]);
      toast.success(`Import finalizat: ${res.data.created_new || 0} firme create`);
      fetchData();
    } catch (error) {
      const errorMsg = error.response?.data?.detail || error.message || "Eroare necunoscută";
      setImportError(errorMsg);
      setImportLog(prev => [
        ...prev,
        `[${new Date().toLocaleTimeString()}] ✗ EROARE: ${errorMsg}`,
        `[${new Date().toLocaleTimeString()}] Status: ${error.response?.status || 'N/A'}`,
        `[${new Date().toLocaleTimeString()}] Verifică dacă fișierul are format corect (delimitator ^)`
      ]);
      toast.error("Eroare la importul fișierului");
    } finally {
      setImportLoading(false);
      event.target.value = '';
    }
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
                      {importLoading ? 'Se importă...' : 'Import CSV'}
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
        ) : (
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
                          <th>ID</th>
                          <th>CUI</th>
                          <th>Denumire</th>
                          <th>Dosare</th>
                          <th>Creat</th>
                        </tr>
                      </thead>
                      <tbody>
                        {firmeList.map((firma) => (
                          <tr key={firma.id} data-testid={`firma-row-${firma.id}`}>
                            <td className="col-id">{firma.id}</td>
                            <td className="col-cui">
                              {firma.cui ? (
                                <Badge variant="outline" className="badge-cui">{firma.cui}</Badge>
                              ) : (
                                <span className="no-cui">-</span>
                              )}
                            </td>
                            <td className="col-denumire">{firma.denumire}</td>
                            <td className="col-dosare">{firma.dosare_count || 0}</td>
                            <td className="col-date">{firma.created_at ? new Date(firma.created_at).toLocaleDateString('ro-RO') : '-'}</td>
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
        )}
      </main>

      <footer className="app-footer">
        <p>Portal JUST Downloader • Descărcare automată dosare firme • 246 instituții</p>
      </footer>
    </div>
  );
}

export default App;
