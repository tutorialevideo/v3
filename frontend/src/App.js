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
  Zap
} from "lucide-react";

const BACKEND_URL = process.env.REACT_APP_BACKEND_URL;
const API = `${BACKEND_URL}/api`;

function App() {
  const [config, setConfig] = useState(null);
  const [stats, setStats] = useState(null);
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

  const fetchData = useCallback(async () => {
    try {
      const [configRes, statsRes, filesRes, runsRes, currentRes] = await Promise.all([
        axios.get(`${API}/config`),
        axios.get(`${API}/stats`),
        axios.get(`${API}/files`),
        axios.get(`${API}/runs`),
        axios.get(`${API}/runs/current`)
      ]);
      
      setConfig(configRes.data);
      setStats(statsRes.data);
      setFiles(filesRes.data);
      setRuns(runsRes.data);
      setCurrentRun(currentRes.data);
      
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
      </header>

      <main className="main-content">
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
      </main>

      <footer className="app-footer">
        <p>Portal JUST Downloader • Descărcare automată dosare firme • 246 instituții</p>
      </footer>
    </div>
  );
}

export default App;
