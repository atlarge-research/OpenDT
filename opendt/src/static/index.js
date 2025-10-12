const $ = s => document.querySelector(s);
const modeKey = 'opendt-view-mode';

// ---- helpers -------------------------------------------------
function fmt(n, d=1){ return (n===null||n===undefined||isNaN(n)) ? '—' : Number(n).toFixed(d); }
function pct(n, d=1){ return (n===null||n===undefined||isNaN(n)) ? '—' : (Number(n)*100).toFixed(d) + '%'; }

// Parse "Window 1: 121 tasks, 1155 fragments"
function parseWindowCounts(info){
  const m = /:\s*([\d,]+)\s*tasks?\s*,\s*([\d,]+)\s*fragments?/i.exec(info || "");
  if (!m) return { tasks: null, frags: null };
  const toInt = s => parseInt(String(s).replace(/,/g,''), 10);
  return { tasks: toInt(m[1]), frags: toInt(m[2]) };
}

// ---- UI mode / button ripple --------------------------------
document.addEventListener('click', e => {
  const b = e.target.closest('.btn');
  if (!b) return;
  const r = b.getBoundingClientRect();
  b.style.setProperty('--press-x', (e.clientX - r.left) + 'px');
  b.style.setProperty('--press-y', (e.clientY - r.top) + 'px');
});

function setMode(mode){
  document.body.classList.toggle('mode-json', mode === 'json');
  $('#btnModeUI')?.classList.toggle('active', mode === 'ui');
  $('#btnModeJSON')?.classList.toggle('active', mode === 'json');
  localStorage.setItem(modeKey, mode);
}

function applyStatus(status){
  const up = (status||'').toLowerCase() === 'running';
  const btn = $('#toggleBtn');
  if (btn){
    btn.className = 'btn ' + (up ? 'btn-danger' : 'btn-primary');
    btn.innerHTML = up
      ? `<span class="icon" style="--icon-url:var(--sym-stop)"></span>&nbsp;Stop`
      : `<span class="icon" style="--icon-url:var(--sym-play)"></span>&nbsp;Start`;
  }
  const pill = $('#statusPill');
  pill?.classList.toggle('running', up);
  pill?.classList.toggle('stopped', !up);
  $('#statusText') && ($('#statusText').textContent = (status || '—').toUpperCase());
}

async function toggleSystem(){
  const status = ($('#statusText')?.textContent || '').toUpperCase();
  const endpoint = (status === 'RUNNING' || status === 'STARTING') ? '/api/stop' : '/api/start';
  try{ await fetch(endpoint, {method:'POST'}); }catch(e){}
}

// ---- Renders -------------------------------------------------
function renderMetrics(s){
  const { tasks, frags } = parseWindowCounts(s.current_window);
  $('#mCycles')      && ($('#mCycles').textContent = s.cycle_count ?? 0);
  $('#mTasks')       && ($('#mTasks').textContent  = (tasks ?? s.total_tasks ?? 0));
  $('#mFragments')   && ($('#mFragments').textContent = (frags ?? s.total_fragments ?? 0));

  const sim = s.last_simulation || {};
  $('#mEnergy')      && ($('#mEnergy').textContent  = fmt(sim.energy_kwh, 2));
  $('#mCPU')         && ($('#mCPU').textContent     = pct(sim.cpu_utilization ?? 0, 1));
  $('#mRuntime')     && ($('#mRuntime').textContent = fmt(sim.runtime_hours, 1) + 'h');
  $('#mTopoUpdates') && ($('#mTopoUpdates').textContent = s.topology_updates ?? 0);

  const opt = s.last_optimization || {};
  const action = opt.action_taken || (Array.isArray(opt.action_type) ? opt.action_type[0] : null) || '—';
  $('#mLLMAction') && ($('#mLLMAction').textContent = action);
  const typ = (opt.type || '—').replace('_',' ');
  const llmTypeEl = $('#mLLMType');
  if (llmTypeEl) llmTypeEl.innerHTML = typ !== '—' ? `<span class="badge">${typ}</span>` : '—';
}

function renderOpenDC(sim,s){
  $('#kvEnergy')   && ($('#kvEnergy').textContent   = fmt(sim.energy_kwh, 2));
  $('#kvCPU')      && ($('#kvCPU').textContent      = pct(sim.cpu_utilization, 1));
  $('#kvRuntime')  && ($('#kvRuntime').textContent  = fmt(sim.runtime_hours, 1));
  $('#kvMaxPower') && ($('#kvMaxPower').textContent = fmt(sim.max_power_draw, 0));
  $('#kvSimType')  && ($('#kvSimType').textContent  = fmt(s.cycle_count_opt) ?? '—');

  const pre = $('#simJSON');
  const txt = JSON.stringify(sim || null, null, 2);
  if (pre && pre.textContent !== txt) pre.textContent = txt;
}

function renderLLM(opt){
  $('#kvLLMAction')   && ($('#kvLLMAction').textContent = opt.action_taken || '—');
  $('#kvLLMReason')   && ($('#kvLLMReason').textContent = opt.reason || '—');
  $('#kvLLMPriority') && ($('#kvLLMPriority').textContent = opt.priority || '—');
  $('#kvLLMType')     && ($('#kvLLMType').textContent     = (opt.type || '—').replace('_',' '));

  const list = $('#kvLLMRecs');
  const recs = opt.recommendations || opt.llm_recommendations || [];
  if (list) list.innerHTML = recs.length ? recs.map(r=>`<li>${r}</li>`).join('') : '<li>—</li>';

  const pre = $('#llmJSON');
  const txt = JSON.stringify(opt || null, null, 2);
  if (pre && pre.textContent !== txt) pre.textContent = txt;
}

function renderTopoTable(topo){
  const body = $('#topoTable tbody');
  if (!body) return;
  const rows = [];
  try{
    const clusters = (topo && topo.clusters) || [];
    clusters.forEach(c=>{
      (c.hosts || []).forEach(h=>{
        rows.push(`
          <tr>
            <td>${c.name || '—'}</td>
            <td>${h.name || '—'}</td>
            <td>${h.count ?? '—'}</td>
            <td>${h.cpu?.coreCount ?? '—'}</td>
            <td>${h.cpu?.coreSpeed ?? '—'}</td>
            <td>${Math.round((h.memory?.memorySize || 0)/1073741824) || '—'}</td>
          </tr>
        `);
      });
    });
  }catch(e){}
  body.innerHTML = rows.length ? rows.join('') : '<tr><td colspan="6">No topology</td></tr>';

  const pre = $('#topologyJSON');
  const txt = JSON.stringify(topo || null, null, 2);
  if (pre && pre.textContent !== txt) pre.textContent = txt;
}

function renderBest(best){
  const badge = $('#bestScore');
  if (badge){
    if (best && best.score !== undefined && best.score !== null){
      badge.textContent = `Score: ${Number(best.score).toFixed(2)}`;
    } else {
      badge.textContent = 'Score: —';
    }
  }
  const pre = $('#bestJSON');
  const payload = best ? (best.config ? best : {config: best}) : null;
  const txt = JSON.stringify(payload, null, 2);
  if (pre && pre.textContent !== txt) pre.textContent = txt;
}

// ----- time normalization helpers (fix 1970) -----
function _isBadEpoch(v){
  const d = new Date(v);
  return !isFinite(d) || d.getFullYear() < 2000;
}
function _genTimeline(count, stepMs = 5*60*1000, anchorMs = Date.now()){
  const start = anchorMs - (count - 1) * stepMs;
  const arr = new Array(count);
  for (let i = 0; i < count; i++) arr[i] = new Date(start + i*stepMs).toISOString();
  return arr;
}
function _normalizeX(x, yLen, stepMs = 5*60*1000, anchorMs = Date.now()){
  if (Array.isArray(x) && x.length === yLen && x.length > 0) {
    const first = x.find(v => v != null);
    if (first && !_isBadEpoch(first)) return x;          // already real timestamps
  }
  return _genTimeline(yLen, stepMs, anchorMs);           // fabricate sane "now" timeline
}

// ---- Recommendation Action -------------------------------------------------
async function acceptRecommendation() {
  try {
    const btn = $('#btnAcceptRec');
    if (btn) btn.disabled = true;
    
    const response = await fetch('/api/accept_recommendation', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      }
    });
    
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    
    // Refresh the UI
    await poll();
  } catch(e) {
    console.error('Failed to accept recommendation:', e);
  } finally {
    const btn = $('#btnAcceptRec');
    if (btn) btn.disabled = false;
  }
}

// ---- polling -------------------------------------------------
async function poll(){
  try{
    const r = await fetch('/api/status', {cache:'no-store'});
    const s = await r.json();

    applyStatus(s.status);

    // NEW: right-side window pill text
    const wp = document.querySelector('#windowPill');
    if (wp) {
      const n = s.cycle_count ?? null;                 // window number
      const info = s.current_window || '';             // "Window 1: …"
      wp.textContent = n ? `Window ${n}` : (info || '—');
    }

    renderMetrics(s);
    renderOpenDC(s.last_optimization || {},s);
    renderLLM(s.last_optimization || {});
    renderTopoTable(s.current_topology || null);
    renderBest(s.best_config || null);
  }catch(e){}
}

setMode(localStorage.getItem(modeKey) || 'ui');
poll();
setInterval(poll, 2000);

// ---- SLO submit handler -------------------------------------------------
async function submitSLO() {
  const btn = document.getElementById('submit_slo');
  if (!btn) return;

  try {
    const energy = parseFloat(document.getElementById('energy_input').value);
    const runtime = parseFloat(document.getElementById('runtime_input').value);

    if (isNaN(energy) || isNaN(runtime)) {
      console.error('Invalid input values');
      btn.classList.add('btn-danger');
      setTimeout(() => {
        btn.classList.remove('btn-danger');
        btn.classList.add('btn-primary');
      }, 1000);
      return;
    }

    btn.disabled = true;
    btn.classList.remove('btn-primary');
    btn.classList.add('btn-ghost');

    // send SLO to backend (adjust endpoint/body to your API if needed)
    await fetch('/api/submit_slo', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ energy, runtime })
    });

    // refresh UI after submit
    await poll();
  } catch(e) {
    console.error('Failed to submit SLO:', e);
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.classList.remove('btn-ghost');
      btn.classList.add('btn-primary');
    }
  }
}


// ===== Plotly config & layout =====
const PLOTLY_CONFIG = {
  responsive: true,
  displaylogo: false,
  displayModeBar: 'hover',    // <— show only on hover
  modeBarButtonsToRemove: ['lasso2d','select2d','autoScale2d','toggleSpikelines'],
  toImageButtonOptions: { format: 'png', filename: 'opendt-chart', height: 720, width: 1280, scale: 2 }
};


// Keep zoom/selection when re-rendering
let UIREVISION = 'persist-zoom';

// Decide if x looks like real timestamps (very loose check)
function looksLikeDates(arr) {
  if (!arr || !arr.length) return false;
  const v = arr[0];
  return typeof v === 'string' && /\d{4}-\d{2}-\d{2}.*\d{2}:\d{2}/.test(v);
}


// Colors (match your theme)
const COLORS = {
  real: '#56B4E9',
  sim:  '#D55E00',
  grid: 'rgba(255,255,255,0.12)'
};

// Build layout with optional date features
function layoutFor(title, opts = {}) {
  const { yTickformat = null } = opts;
  return {
    title,
    uirevision: UIREVISION,
    paper_bgcolor: 'rgba(0,0,0,0)',
    plot_bgcolor:  'rgba(0,0,0,0)',
    margin: { l: 70, r: 140, t: 60, b: 70 },
    font: { color: 'rgba(236,240,241,0.95)' },

    legend: {
      x: 1.02, y: 1, xanchor: 'left', yanchor: 'top',
      bgcolor: 'rgba(0,0,0,0)'
    },

    xaxis: {
      title: { text: 'Time', standoff: 8 },
      type: 'date',
      showgrid: true, gridcolor: COLORS.grid,
      showline: true, linecolor: '#cbd5e1', linewidth: 1.2,
      ticks: 'outside', tickcolor: '#cbd5e1', ticklen: 6,
      rangeslider: { visible: true, bgcolor: 'rgba(255,255,255,0.05)' },
      rangeselector: {
        x: 0.02, xanchor: 'left', y: 1.15, yanchor: 'top',
        buttons: [
          { step: 'minute', stepmode: 'backward', count: 30, label: '30m' },
          { step: 'hour',   stepmode: 'backward', count: 2,  label: '2h' },
          { step: 'day',    stepmode: 'backward', count: 1,  label: '1d' },
          { step: 'all', label: 'All' },
        ]
      }
    },

    yaxis: {
      showgrid: true, gridcolor: COLORS.grid,
      showline: true, linecolor: '#cbd5e1', linewidth: 1.2,
      ticks: 'outside', tickcolor: '#cbd5e1', ticklen: 6,
      tickformat: yTickformat || undefined
    }
  };
}


// ===== Draw charts with real + sim (if present) =====
async function fetchTS() {
  const r = await fetch('/api/sim/timeseries', { cache: 'no-store' });
  if (!r.ok) throw new Error('timeseries fetch failed');
  return r.json();
}

async function drawCharts(){
  const d = await fetchTS();
  if (d.status !== 'ok') return;

  // anchor charts to "latest write time" or now
  const ANCHOR_MS = (d.mtime ? d.mtime * 1000 : Date.now());
  const STEP_MS = 5 * 60 * 1000; // 5 minutes

  // ===== CPU
  {
    const yR = d.host?.cpu_utilization || [];
    const yS = d.host_sim?.cpu_utilization || [];
    const xR = _normalizeX(d.host?.x,      yR.length, STEP_MS, ANCHOR_MS);
    const xS = _normalizeX(d.host_sim?.x,  yS.length, STEP_MS, ANCHOR_MS);

    const traces = [];
    if (yR.length) traces.push({ x: xR, y: yR, mode:'lines', name:'Real CPU', line:{ color: COLORS.real, width: 2 } });
    if (yS.length) traces.push({ x: xS, y: yS, mode:'lines', name:'Sim CPU',  line:{ color: COLORS.sim,  width: 2, dash:'dash' } });

    Plotly.react('chart-cpu', traces, layoutFor('CPU Utilization — Real vs Sim', { yTickformat: ',.0%' }), PLOTLY_CONFIG);
  }

  // ===== Power
  {
    const yR = d.power?.power_draw || [];
    const yS = d.power_sim?.power_draw || [];
    const xR = _normalizeX(d.power?.x,      yR.length, STEP_MS, ANCHOR_MS);
    const xS = _normalizeX(d.power_sim?.x,  yS.length, STEP_MS, ANCHOR_MS);

    const traces = [];
    if (yR.length) traces.push({ x: xR, y: yR, mode:'lines', name:'Real Power (W)', line:{ color: COLORS.real, width: 2 } });
    if (yS.length) traces.push({ x: xS, y: yS, mode:'lines', name:'Sim Power (W)',  line:{ color: COLORS.sim,  width: 2, dash:'dash' } });

    Plotly.react('chart-power', traces, layoutFor('Power — Real vs Sim'), PLOTLY_CONFIG);
  }

  // ===== Cumulative Energy
  {
    const yR = d.power?.energy_kwh_cum || [];
    const yS = d.power_sim?.energy_kwh_cum || [];
    const xR = _normalizeX(d.power?.x,      yR.length, STEP_MS, ANCHOR_MS);
    const xS = _normalizeX(d.power_sim?.x,  yS.length, STEP_MS, ANCHOR_MS);

    const traces = [];
    if (yR.length) traces.push({ x: xR, y: yR, mode:'lines', name:'Real Cumulative (kWh)', line:{ color: COLORS.real, width: 2 } });
    if (yS.length) traces.push({ x: xS, y: yS, mode:'lines', name:'Sim Cumulative (kWh)',  line:{ color: COLORS.sim,  width: 2, dash:'dash' } });

    Plotly.react('chart-energy', traces, layoutFor('Cumulative Energy — Real vs Sim'), PLOTLY_CONFIG);
  }
}


// Optional SSE refresh (if your backend emits it)
function startSse(){
  try{
    const es = new EventSource('/api/stream');
    es.onmessage = () => drawCharts();
    es.onerror = () => es.close();
  }catch(_){}
}

// Boot
document.addEventListener('DOMContentLoaded', ()=>{
  drawCharts();
  startSse();
  // safety poll every 5s
  setInterval(()=>{ drawCharts().catch(()=>{}); }, 5000);
});


