import { Button } from "../components/ui/button";
import { Badge } from "../components/ui/badge";
import { ScrollArea } from "../components/ui/scroll-area";
import { Loader2, Building2, XCircle, CheckCircle2, Wrench } from "lucide-react";

export default function FirmaProfileModal({ ctx }) {
  const {
    profileModalOpen, firmaProfile, profileLoading,
    editingCui, setEditingCui, editCuiValue, setEditCuiValue, savingCui,
    closeFirmaProfile, saveCuiInProfile,
  } = ctx;

  if (!profileModalOpen) return null;

  return (
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
  );
}
