import { useCallback, useEffect, useState } from 'react';
import type { ReactNode } from 'react';
import axios from 'axios';
import { Activity, AlertTriangle, Check, CheckCircle2, ChevronLeft, Clock3, Database, Radio, RefreshCw, Shield, X } from 'lucide-react';
import { format } from 'date-fns';

type Incident = { id: string; title: string; severity: string; status: string; trigger: string; trigger_metrics: Record<string, number>; root_cause?: Record<string, any> | null; outcome?: string | null; created_at: string; resolved_at?: string | null };
type Event = { id: number; incident_id: string; event_type: string; payload: Record<string, any>; timestamp: string };
type Proposal = { id: string; action: string; parameters: Record<string, any>; risk: string; status: string; rationale: string; expected_outcome: string; execution?: { status: string; result: any; pre_metrics: any; post_metrics: any } | null };
type Provider = { active: string; demo_available: boolean; nemotron_available: boolean; model?: string | null };
type TelemetryStatus = { provider: string; service_id: string; service_name: string; prometheus: { available: boolean | null }; loki: { available: boolean | null }; service: { available: boolean; ready: boolean; source: string }; ai_provider: string };
const API = '/api';
const activeStatuses = new Set(['active', 'investigating', 'investigated', 'awaiting_approval', 'remediating']);

function Badge({ value }: { value: string }) {
  const style = value === 'resolved' || value === 'healthy' || value === 'executed' ? 'good' : value === 'awaiting_approval' || value === 'pending_approval' ? 'warn' : value === 'active' || value === 'escalated' || value === 'failed' ? 'bad' : 'neutral';
  return <span className={`badge ${style}`}>{value.replaceAll('_', ' ')}</span>;
}
function time(value?: string) { return value ? format(new Date(value), 'MMM d, HH:mm:ss') : '—'; }

export default function App() {
  const [incidents, setIncidents] = useState<Incident[]>([]);
  const [metrics, setMetrics] = useState<Record<string, number | null> | null>(null);
  const [provider, setProvider] = useState<Provider | null>(null);
  const [telemetry, setTelemetry] = useState<TelemetryStatus | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const [connection, setConnection] = useState<'connecting' | 'live' | 'reconnecting'>('connecting');

  const refresh = useCallback(async () => {
    try {
      const tr = await axios.get(`${API}/system/telemetry`);
      setTelemetry(tr.data);
      const [ir, mr, pr] = await Promise.all([axios.get(`${API}/incidents`), axios.get(`${API}/services/${tr.data.service_id}/metrics`), axios.get(`${API}/system/provider`)]);
      setIncidents(ir.data);
      setMetrics(mr.data.current);
      setProvider(pr.data);
      setTelemetry(tr.data);
      setError('');
    } catch (err) {
      setMetrics(null);
      setError(axios.isAxiosError(err) ? err.response?.data?.detail || 'Backend unavailable. Check the API connection.' : 'Unable to load Sentinel data.');
    }
  }, []);

  useEffect(() => { void refresh(); const id = window.setInterval(() => void refresh(), 5000); return () => window.clearInterval(id); }, [refresh]);

  async function changeProvider(value: string) {
    setBusy(true);
    try { await axios.post(`${API}/system/provider`, { provider: value }); await refresh(); }
    catch (err) { setError(axios.isAxiosError(err) ? err.response?.data?.detail || 'Could not switch provider.' : 'Could not switch provider.'); }
    finally { setBusy(false); }
  }
  async function trigger(path: string) {
    setBusy(true);
    try { const result = await axios.post(`${API}/demo/scenarios/${path}`); await refresh(); if (result.data.incident_id) setSelected(result.data.incident_id); }
    catch (err) { setError(axios.isAxiosError(err) ? err.response?.data?.detail || 'Scenario trigger failed.' : 'Scenario trigger failed.'); }
    finally { setBusy(false); }
  }
  async function reset() {
    setBusy(true);
    try { await axios.post(`${API}/demo/reset`); await refresh(); }
    catch (err) { setError(axios.isAxiosError(err) ? err.response?.data?.detail || 'Simulator reset failed.' : 'Simulator reset failed.'); }
    finally { setBusy(false); }
  }
  async function triggerMonitoredPoolExhaustion() {
    setBusy(true);
    try {
      await axios.post(`${API}/monitored/scenarios/db-pool-exhaustion`);
      setNotice('Real pool exhaustion requests started. Sentinel will detect the incident from Prometheus and Loki telemetry.');
      window.setTimeout(() => setNotice(''), 6000);
    } catch (err) {
      setError(axios.isAxiosError(err) ? err.response?.data?.detail || 'Could not trigger the monitored-app scenario.' : 'Could not trigger the monitored-app scenario.');
    } finally { setBusy(false); }
  }

  const selectedIncident = incidents.find(i => i.id === selected);
  return <div className="app-shell">
    <nav className="topbar"><div className="brand"><span className="brand-mark">S</span><span>SENTINEL</span><span className="tag">RELIABILITY OPS</span></div><div className="nav-state"><span className="live-dot" /> {telemetry?.provider === 'prometheus_loki' ? 'Prometheus + Loki monitoring' : 'Simulator environment'}</div></nav>
    <main className="content">
      {error && <div className="error-banner"><AlertTriangle size={17}/>{error}<button onClick={() => setError('')} aria-label="Dismiss error">×</button></div>}
      {notice && <div className="notice-banner"><CheckCircle2 size={17}/>{notice}<button onClick={() => setNotice('')} aria-label="Dismiss notice">×</button></div>}
      {selected && selectedIncident ? <IncidentPage incident={selectedIncident} back={() => setSelected(null)} onChanged={refresh} connection={connection} setConnection={setConnection} /> : <>
        <div className="page-heading"><div><div className="eyebrow">OPERATIONS / OVERVIEW</div><h1>Service overview</h1><p>{telemetry?.provider === 'prometheus_loki' ? 'Real monitored-app telemetry and incident response.' : 'Production simulator telemetry and incident response.'}</p></div><div className="provider-control"><label htmlFor="provider">Agent provider</label><select id="provider" value={provider?.active || 'demo'} disabled={busy || !provider} onChange={e => void changeProvider(e.target.value)}><option value="demo">Deterministic demo agent</option><option value="nemotron" disabled={!provider?.nemotron_available}>OpenAI-compatible model{provider?.nemotron_available ? ` (${provider.model})` : ' · configure credentials'}</option></select></div></div>
        <section className="panel" style={{display:'flex',justifyContent:'space-between',alignItems:'center',gap:16,flexWrap:'wrap'}}><div><div className="eyebrow">MONITORING ENVIRONMENT</div><strong>{telemetry?.service_name || 'Loading service…'}</strong><div className="muted">Telemetry source: {telemetry?.provider === 'prometheus_loki' ? 'Prometheus + Loki' : 'Sentinel simulator'} · AI: {telemetry?.ai_provider || provider?.active || 'loading'}</div></div>{telemetry?.provider === 'prometheus_loki' && <div style={{display:'flex',gap:8,flexWrap:'wrap'}}><StatusBadge name="Application" active={telemetry.service.ready}/><StatusBadge name="Prometheus" active={telemetry.prometheus.available === true}/><StatusBadge name="Loki" active={telemetry.loki.available === true}/></div>}</section>
        <div className="metric-grid">
          <Metric title="HTTP 5xx rate" value={typeof metrics?.http_5xx_rate === 'number' ? `${(metrics.http_5xx_rate * 100).toFixed(1)}%` : '—'} icon={<Activity/>} danger={typeof metrics?.http_5xx_rate === 'number' && metrics.http_5xx_rate > 0.1}/>
          <Metric title="DB pool utilization" value={typeof metrics?.db_pool_utilization === 'number' ? `${(metrics.db_pool_utilization * 100).toFixed(1)}%` : '—'} icon={<Database/>} danger={typeof metrics?.db_pool_utilization === 'number' && metrics.db_pool_utilization > 0.9}/>
          <Metric title="Request latency" value={typeof metrics?.request_latency_ms === 'number' ? `${metrics.request_latency_ms.toFixed(0)} ms` : '—'} icon={<Clock3/>}/>
          <Metric title="Worker health" value={telemetry?.provider === 'prometheus_loki' ? 'N/A' : metrics?.worker_healthy ? 'ONLINE' : metrics ? 'OFFLINE' : '—'} icon={<Radio/>} danger={telemetry?.provider !== 'prometheus_loki' && !!metrics && !metrics.worker_healthy}/>
        </div>
        {telemetry?.provider === 'prometheus_loki' ? <section className="panel demo-controls"><div><div className="eyebrow">LOCAL MONITORED-APP SCENARIO</div><h2>Inject real pool exhaustion</h2><p>Sends concurrent requests that hold actual SQLAlchemy connections; Prometheus and Loki provide the resulting evidence.</p></div><div className="button-row"><button className="button danger" disabled={busy || incidents.some(i => activeStatuses.has(i.status)) || telemetry.service.ready === false} onClick={() => void triggerMonitoredPoolExhaustion()}><Database size={16}/>{busy ? 'Starting…' : 'Exhaust monitored DB pool'}</button></div></section> : <section className="panel demo-controls"><div><div className="eyebrow">DEMO CONTROLS</div><h2>Inject a controlled failure</h2><p>The simulator generates the logs and metrics Sentinel investigates.</p></div><div className="button-row"><button className="button secondary" disabled={busy} onClick={() => void reset()}><RefreshCw size={16}/>Reset simulator</button><button className="button danger" disabled={busy || incidents.some(i => activeStatuses.has(i.status))} onClick={() => void trigger('db-pool-exhaustion')}><Database size={16}/>DB pool exhaustion</button><button className="button amber" disabled={busy || incidents.some(i => activeStatuses.has(i.status))} onClick={() => void trigger('worker-failure')}><AlertTriangle size={16}/>Worker failure</button></div></section>}
        <section className="panel incidents-panel"><div className="section-heading"><div><div className="eyebrow">INCIDENTS</div><h2>Incident history</h2></div><button className="icon-button" onClick={() => void refresh()} title="Refresh"><RefreshCw size={16}/></button></div>
          {incidents.length === 0 ? <div className="empty-state"><CheckCircle2/><strong>No incidents recorded</strong><span>Trigger a demo scenario to start an investigation.</span></div> : <div className="incident-list">{incidents.map(item => <button key={item.id} className="incident-row" onClick={() => setSelected(item.id)}><span className="severity-mark"/><span className="incident-copy"><strong>{item.title}</strong><small>{item.trigger}</small></span><Badge value={item.status}/><time>{time(item.created_at)}</time><span className="row-arrow">→</span></button>)}</div>}
        </section>
        <div className="footnote">All times from persisted incident data · {provider?.active === 'demo' ? 'DEMO AGENT ACTIVE' : 'NEMOTRON PROVIDER ACTIVE'}</div>
      </>}
    </main>
  </div>;
}
function StatusBadge({ name, active }: { name: string; active: boolean }) {
  return <span className={`badge ${active ? 'good' : 'bad'}`}>{name}: {active ? 'connected' : 'unavailable'}</span>;
}
function Metric({ title, value, icon, danger = false }: { title: string; value: string; icon: ReactNode; danger?: boolean }) {
  return <div className={`metric-card ${danger ? 'metric-danger' : ''}`}><div className="metric-top"><span className="metric-icon">{icon}</span><span className={`metric-health ${danger ? 'text-red' : 'text-green'}`}>{danger ? 'DEGRADED' : 'NORMAL'}</span></div><span className="metric-title">{title}</span><strong>{value}</strong></div>;
}
function IncidentPage({ incident, back, onChanged, connection, setConnection }: { incident: Incident; back: () => void; onChanged: () => Promise<void>; connection: string; setConnection: (s: 'connecting'|'live'|'reconnecting') => void }) {
  const [events, setEvents] = useState<Event[]>([]);
  const [evidence, setEvidence] = useState<any[]>([]);
  const [proposals, setProposals] = useState<Proposal[]>([]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const fetchDetail = useCallback(async () => {
    try {
      const [er, vr, pr] = await Promise.all([axios.get(`${API}/incidents/${incident.id}/events`), axios.get(`${API}/incidents/${incident.id}/evidence`), axios.get(`${API}/incidents/${incident.id}/remediations`)]);
      setEvents(er.data); setEvidence(vr.data); setProposals(pr.data);
    } catch { setError('Unable to load all incident details.'); }
  }, [incident.id]);
  useEffect(() => {
    void fetchDetail();
    const stream = new EventSource(`${API}/incidents/${incident.id}/stream`);
    stream.onopen = () => setConnection('live');
    stream.onerror = () => setConnection('reconnecting');
    stream.addEventListener('update', event => {
      const parsed = JSON.parse((event as MessageEvent).data) as Event;
      setEvents(current => current.some(item => item.id === parsed.id) ? current : [...current, parsed]);
      void fetchDetail(); void onChanged();
    });
    return () => stream.close();
  }, [incident.id, fetchDetail, onChanged, setConnection]);
  async function investigate() {
    setBusy(true); setError('');
    try { await axios.post(`${API}/incidents/${incident.id}/investigate`, {}); await fetchDetail(); }
    catch (err) { setError(axios.isAxiosError(err) ? err.response?.data?.detail || 'Investigation could not start.' : 'Investigation could not start.'); }
    finally { setBusy(false); }
  }
  async function decide(proposal: Proposal, approve: boolean) {
    setBusy(true); setError('');
    try {
      const data = { proposal_id: proposal.id, idempotency_key: crypto.randomUUID(), parameters: proposal.parameters };
      await axios.post(`${API}/incidents/${incident.id}/${approve ? 'approve' : 'reject'}`, approve ? data : { ...data, reason: 'Rejected by operator' });
      await fetchDetail(); await onChanged();
    } catch (err) { setError(axios.isAxiosError(err) ? err.response?.data?.detail || 'Decision request failed.' : 'Decision request failed.'); }
    finally { setBusy(false); }
  }
  const pending = proposals.find(p => p.status === 'pending_approval');
  return <div className="incident-detail">
    <button className="back-button" onClick={back}><ChevronLeft size={17}/>All incidents</button>
    {error && <div className="error-banner"><AlertTriangle size={16}/>{error}</div>}
    <div className="detail-heading"><div><div className="eyebrow">INCIDENT / {incident.id.slice(0, 8)}</div><h1>{incident.title}</h1><p>Detected {time(incident.created_at)} · {incident.trigger}</p></div><div className="detail-badges"><Badge value={incident.severity}/><Badge value={incident.status}/></div></div>
    <div className="detail-layout"><div className="main-column">
      <section className="panel"><div className="section-heading"><div><div className="eyebrow">PERSISTED SSE EVENT STREAM</div><h2>Investigation timeline</h2></div><span className={`connection ${connection}`}><i/>{connection}</span></div>
        {events.length === 0 ? <div className="empty-state compact"><span>Waiting for investigation events…</span>{incident.status === 'active' && <button className="button primary" disabled={busy} onClick={() => void investigate()}>{busy ? 'Starting…' : 'Start investigation'}</button>}</div> : <ol className="timeline">{events.map(event => <li key={event.id}><span className="timeline-node"/><time>{time(event.timestamp)}</time><strong>{event.event_type.replaceAll('_', ' ')}</strong>{event.payload?.tool && <code>{event.payload.tool}({JSON.stringify(event.payload.arguments || {})})</code>}{event.event_type === 'EVIDENCE_RECORDED' ? <p><b>{event.payload.evidence_id}</b> · {event.payload.summary}</p> : event.event_type === 'TOOL_CALL_COMPLETED' ? <details><summary>View actual tool result</summary><pre>{JSON.stringify(event.payload.result, null, 2)}</pre></details> : event.event_type === 'HYPOTHESIS_CREATED' ? <p>{event.payload.explanation}<br/><small>Evidence: {(event.payload.supporting_evidence || []).join(', ') || 'none'} · stated confidence {(event.payload.confidence * 100).toFixed(0)}%</small></p> : event.payload?.reason ? <p>{event.payload.reason}</p> : null}</li>)}</ol>}
      </section>
      {incident.root_cause && <section className="panel diagnosis"><div className="eyebrow">AGENT DIAGNOSIS · HYPOTHESIS, NOT OBSERVED FACT</div><h2>{incident.root_cause.root_cause || 'Inconclusive'}</h2><p>{incident.root_cause.explanation}</p><div className="diagnosis-grid"><div><small>Stated confidence</small><strong>{(incident.root_cause.confidence * 100).toFixed(0)}%</strong></div><div><small>Supporting evidence</small><strong>{incident.root_cause.supporting_evidence?.join(', ') || 'None'}</strong></div><div><small>Missing information</small><strong>{incident.root_cause.missing_information?.join(', ') || 'None reported'}</strong></div></div></section>}
      {pending && <section className="panel approval"><div className="eyebrow">OPERATOR ACTION REQUIRED · {pending.risk.toUpperCase()} RISK</div><h2>{pending.action.replaceAll('_', ' ')}</h2><p>{pending.rationale}</p><div className="proposal-params"><b>Parameters</b><code>{JSON.stringify(pending.parameters)}</code><small>Expected: {pending.expected_outcome}</small></div><div className="button-row"><button className="button secondary" disabled={busy} onClick={() => void decide(pending, false)}><X size={16}/>Reject</button><button className="button success" disabled={busy} onClick={() => void decide(pending, true)}><Check size={16}/>Approve & execute</button></div></section>}
      {proposals.map(p => <section className="panel" key={p.id}><div className="section-heading"><div><div className="eyebrow">REMEDIATION / {p.risk.toUpperCase()} RISK</div><h2>{p.action.replaceAll('_', ' ')}</h2></div><Badge value={p.status}/></div><p>{p.expected_outcome}</p>{p.execution && <div className="execution-report"><h3>Execution · {p.execution.status}</h3><div className="report-grid"><div><small>Before action</small><pre>{JSON.stringify(p.execution.pre_metrics, null, 2)}</pre></div><div><small>Fresh verification metrics</small><pre>{JSON.stringify(p.execution.post_metrics, null, 2)}</pre></div></div><p>{p.execution.result?.status || p.execution.result?.error || ''}</p></div>}</section>)}
    </div><aside className="side-column"><section className="panel"><div className="eyebrow">EVIDENCE LEDGER</div><h2>Observed facts</h2>{evidence.length ? evidence.map(item => <article className="evidence-card" key={item.id}><div><code>{item.id}</code><span>{item.source_tool}</span></div><p>{item.summary}</p><details><summary>Observation</summary><pre>{JSON.stringify(item.observation, null, 2)}</pre></details></article>) : <p className="muted">Evidence will appear as tools return results.</p>}</section><section className="panel"><div className="eyebrow">TRIGGER TELEMETRY</div><h2>Detection measurements</h2><pre>{JSON.stringify(incident.trigger_metrics, null, 2)}</pre></section><div className="security-note"><Shield size={17}/><span>Agent suggestions cannot execute arbitrary code. The policy engine validates every action.</span></div></aside></div>
  </div>;
}
