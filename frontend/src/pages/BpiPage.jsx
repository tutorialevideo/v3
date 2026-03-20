import { useState, useRef } from "react";
import { Button } from "../components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "../components/ui/card";
import { Badge } from "../components/ui/badge";
import { ScrollArea } from "../components/ui/scroll-area";
import { Loader2, FileJson, Upload, CheckCircle2, XCircle, Building2, Search, ChevronDown } from "lucide-react";

export default function BpiPage({ ctx }) {
  const {
    bpiParsing, bpiResults, bpiHistory, bpiStats,
    parseBpiPdf, saveBpiRecord, loadBpiHistory,
    openFirmaProfile
  } = ctx;

  const [dragOver, setDragOver] = useState(false);
  const [expandedIdx, setExpandedIdx] = useState(null);
  const fileInputRef = useRef(null);

  const handleFile = (file) => {
    if (file && file.name.toLowerCase().endsWith('.pdf')) {
      parseBpiPdf(file);
    }
  };

  const handleDrop = (e) => {
    e.preventDefault();
    setDragOver(false);
    const file = e.dataTransfer.files[0];
    handleFile(file);
  };

  const tipBadgeStyle = (tip) => {
    if (!tip) return {};
    if (tip.includes('Faliment')) return { background: 'rgba(239,68,68,0.15)', color: '#ef4444' };
    if (tip.includes('Insolventa') || tip.includes('Insolventa')) return { background: 'rgba(234,179,8,0.15)', color: '#eab308' };
    if (tip.includes('Reorganizare')) return { background: 'rgba(99,102,241,0.15)', color: '#6366f1' };
    return {};
  };

  return (
    <div className="tab-content">
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '20px' }}>

        <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>

          {bpiStats && bpiStats.total_records > 0 && (
            <div style={{ display: 'flex', gap: '10px' }}>
              <div className="anaf-stat-card" style={{ flex: 1 }}>
                <span className="stat-value">{bpiStats.total_records?.toLocaleString()}</span>
                <span className="stat-label">Total BPI salvate</span>
              </div>
            </div>
          )}

          <Card>
            <CardHeader>
              <CardTitle className="card-title">
                <FileJson size={20} />
                BPI Parser
              </CardTitle>
              <CardDescription>
                Parsare locala PDF-uri BPI. Extrage: firma, CUI, dosar, tribunal, tip procedura, termen, administrator.
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div
                data-testid="bpi-upload-zone"
                onDragOver={(e) => { e.preventDefault(); setDragOver(true); }}
                onDragLeave={() => setDragOver(false)}
                onDrop={handleDrop}
                onClick={() => fileInputRef.current?.click()}
                style={{
                  border: `2px dashed ${dragOver ? 'var(--primary)' : 'var(--border)'}`,
                  borderRadius: '10px', padding: '32px 20px', textAlign: 'center',
                  cursor: 'pointer', background: 'var(--bg-secondary)', marginBottom: '12px',
                  transition: 'all 0.2s'
                }}
              >
                {bpiParsing ? (
                  <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '8px' }}>
                    <Loader2 className="animate-spin" size={32} style={{ color: 'var(--primary)' }} />
                    <p style={{ color: 'var(--text-muted)', fontSize: '0.9rem' }}>Se parseaza PDF-ul...</p>
                  </div>
                ) : (
                  <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '8px' }}>
                    <Upload size={32} style={{ color: 'var(--text-muted)' }} />
                    <p style={{ fontWeight: 600, fontSize: '0.95rem' }}>Drag and Drop PDF BPI</p>
                    <p style={{ color: 'var(--text-muted)', fontSize: '0.82rem' }}>sau click pentru a selecta fisierul</p>
                    <p style={{ color: 'var(--text-muted)', fontSize: '0.75rem' }}>Max 50MB · Doar PDF</p>
                  </div>
                )}
              </div>

              <input ref={fileInputRef} type="file" accept=".pdf" style={{ display: 'none' }}
                data-testid="bpi-file-input"
                onChange={(e) => handleFile(e.target.files[0])} />

              <Button onClick={() => fileInputRef.current?.click()} disabled={bpiParsing}
                style={{ width: '100%' }} data-testid="bpi-upload-btn">
                {bpiParsing
                  ? <><Loader2 className="animate-spin" size={16} style={{ marginRight: 6 }} />Se proceseaza...</>
                  : <><Upload size={16} style={{ marginRight: 6 }} />Selecteaza PDF BPI</>
                }
              </Button>
            </CardContent>
          </Card>

          {bpiResults && (
            <Card data-testid="bpi-results-card">
              <CardHeader>
                <CardTitle className="card-title" style={{ fontSize: '1rem' }}>
                  {bpiResults.success
                    ? <><CheckCircle2 size={18} style={{ color: '#22c55e' }} /> {bpiResults.records_count} inregistrari extrase</>
                    : <><XCircle size={18} style={{ color: '#ef4444' }} /> Eroare parsare</>
                  }
                </CardTitle>
                <CardDescription>
                  {bpiResults.filename} &middot; {bpiResults.pages} pagini &middot; {bpiResults.text_length?.toLocaleString()} caractere
                </CardDescription>
              </CardHeader>
              <CardContent>
                {!bpiResults.success && (
                  <p style={{ color: '#ef4444', fontSize: '0.85rem' }}>{bpiResults.error}</p>
                )}

                {bpiResults.records?.length > 0 && (
                  <div style={{ display: 'flex', flexDirection: 'column', gap: '10px' }}>
                    {bpiResults.records.map((rec, i) => (
                      <div key={i} data-testid={`bpi-record-${i}`} style={{
                        border: '1px solid var(--border)', borderRadius: '8px',
                        overflow: 'hidden', background: 'var(--bg-secondary)'
                      }}>
                        <div style={{ padding: '10px 14px', cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '8px' }}
                          onClick={() => setExpandedIdx(expandedIdx === i ? null : i)}>
                          <ChevronDown size={14} style={{ transform: expandedIdx === i ? 'rotate(180deg)' : 'none', transition: '0.2s', flexShrink: 0 }} />
                          <div style={{ flex: 1 }}>
                            <div style={{ fontWeight: 600, fontSize: '0.88rem' }}>
                              {rec.denumire_firma || <em style={{ opacity: 0.5 }}>Firma neidentificata</em>}
                              {rec.cui && <span style={{ marginLeft: 8, fontSize: '0.75rem', color: 'var(--primary)' }}>CUI: {rec.cui}</span>}
                            </div>
                            <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)', marginTop: '2px' }}>
                              {rec.dosar && <span>Dosar: {rec.dosar} &middot; </span>}
                              {rec.tribunal && <span>{rec.tribunal}</span>}
                            </div>
                          </div>
                          {rec.tip_procedura && (
                            <Badge variant="outline" style={{ fontSize: '0.68rem', flexShrink: 0, ...tipBadgeStyle(rec.tip_procedura) }}>
                              {rec.tip_procedura}
                            </Badge>
                          )}
                          {rec.firma_match && (
                            <Badge variant="outline" style={{ fontSize: '0.68rem', background: 'rgba(34,197,94,0.15)', color: '#22c55e', flexShrink: 0 }}>
                              in DB
                            </Badge>
                          )}
                        </div>

                        {expandedIdx === i && (
                          <div style={{ padding: '0 14px 12px', borderTop: '1px solid var(--border)' }}>
                            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '6px 16px', marginTop: '10px', fontSize: '0.8rem' }}>
                              {[
                                ['Firma', rec.denumire_firma],
                                ['CUI', rec.cui],
                                ['Nr. Reg. Com.', rec.nr_reg_com],
                                ['Tribunal', rec.tribunal],
                                ['Dosar', rec.dosar],
                                ['Tip Procedura', rec.tip_procedura],
                                ['Judecator Sindic', rec.judecator_sindic],
                                ['Administrator Judiciar', rec.administrator_judiciar],
                                ['Lichidator', rec.lichidator],
                                ['Data Publicare', rec.data_publicare],
                                ['Termen', rec.termen],
                                ['Adresa', rec.adresa],
                              ].filter(([, v]) => v).map(([label, value]) => (
                                <div key={label}>
                                  <span style={{ color: 'var(--text-muted)' }}>{label}: </span>
                                  <strong>{value}</strong>
                                </div>
                              ))}
                            </div>

                            {rec.firma_match && (
                              <div style={{ marginTop: '8px', padding: '8px 10px', background: 'rgba(34,197,94,0.08)', borderRadius: '6px', fontSize: '0.78rem' }}>
                                <span style={{ color: '#22c55e' }}>Firma gasita in DB: </span>
                                <button onClick={() => openFirmaProfile(rec.firma_match.id)}
                                  style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--primary)', textDecoration: 'underline', fontSize: '0.78rem' }}>
                                  {rec.firma_match.denumire} (CUI: {rec.firma_match.cui})
                                </button>
                              </div>
                            )}

                            <div style={{ display: 'flex', gap: '6px', marginTop: '10px' }}>
                              <Button size="sm" variant="outline" onClick={() => saveBpiRecord(rec)} data-testid={`save-bpi-${i}`}>
                                Salveaza in istoric
                              </Button>
                              {rec.firma_match && (
                                <Button size="sm" variant="outline" onClick={() => openFirmaProfile(rec.firma_match.id)}>
                                  <Building2 size={12} style={{ marginRight: 4 }} />
                                  Profil firma
                                </Button>
                              )}
                            </div>
                          </div>
                        )}
                      </div>
                    ))}
                  </div>
                )}

                {bpiResults.success && bpiResults.records_count === 0 && (
                  <div style={{ textAlign: 'center', padding: '20px', color: 'var(--text-muted)' }}>
                    <p>Nu s-au putut extrage date structurate.</p>
                    <details style={{ marginTop: '8px', fontSize: '0.78rem' }}>
                      <summary style={{ cursor: 'pointer' }}>Preview text extras</summary>
                      <pre style={{ marginTop: '8px', whiteSpace: 'pre-wrap', textAlign: 'left', fontSize: '0.72rem', maxHeight: '200px', overflow: 'auto' }}>
                        {bpiResults.raw_text_preview}
                      </pre>
                    </details>
                  </div>
                )}
              </CardContent>
            </Card>
          )}
        </div>

        <div>
          <Card>
            <CardHeader>
              <CardTitle className="card-title" style={{ fontSize: '1rem' }}>
                <Search size={18} />
                Istoric BPI Salvate
                {bpiStats && <span style={{ marginLeft: 8, fontSize: '0.78rem', color: 'var(--text-muted)', fontWeight: 400 }}>({bpiStats.total_records} total)</span>}
              </CardTitle>
            </CardHeader>
            <CardContent>
              {bpiHistory.length === 0 ? (
                <p style={{ color: 'var(--text-muted)', fontSize: '0.85rem', textAlign: 'center', padding: '20px' }}>
                  Niciun record salvat inca.<br />
                  Parseaza un PDF si salveaza inregistrarile.
                </p>
              ) : (
                <ScrollArea style={{ height: '600px' }}>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                    {bpiHistory.map((rec, i) => (
                      <div key={i} style={{
                        padding: '10px 12px', border: '1px solid var(--border)',
                        borderRadius: '8px', background: 'var(--bg-secondary)', fontSize: '0.8rem'
                      }}>
                        <div style={{ fontWeight: 600, marginBottom: '3px' }}>
                          {rec.denumire_firma || <em style={{ opacity: 0.5 }}>Firma neidentificata</em>}
                        </div>
                        <div style={{ color: 'var(--text-muted)', display: 'flex', gap: '10px', flexWrap: 'wrap' }}>
                          {rec.cui && <span>CUI: {rec.cui}</span>}
                          {rec.dosar && <span>Dosar: {rec.dosar}</span>}
                          {rec.tip_procedura && <Badge variant="outline" style={{ fontSize: '0.65rem' }}>{rec.tip_procedura}</Badge>}
                        </div>
                        {rec.saved_at && (
                          <div style={{ fontSize: '0.7rem', color: 'var(--text-muted)', marginTop: '3px' }}>
                            {new Date(rec.saved_at).toLocaleString('ro-RO')}
                          </div>
                        )}
                      </div>
                    ))}
                  </div>
                </ScrollArea>
              )}
            </CardContent>
          </Card>
        </div>

      </div>
    </div>
  );
}
