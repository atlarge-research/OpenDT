const $ = s => document.querySelector(s);
const modeKey = 'opendt-view-mode';

// ---- helpers -------------------------------------------------
function fmt(n, d = 1) { return (n === null || n === undefined || isNaN(n)) ? '—' : Number(n).toFixed(d); }
function pct(n, d = 1) { return (n === null || n === undefined || isNaN(n)) ? '—' : (Number(n) * 100).toFixed(d) + '%'; }

// Parse "Window 1: 121 tasks, 1155 fragments"
function parseWindowCounts(info){
  const m = /:\s*([\d,]+)\s*tasks?\s*,\s*([\d,]+)\s*fragments?/i.exec(info || "");
  if (!m) return { tasks: null, frags: null };
  const toInt = s => parseInt(String(s).replace(/,/g,''), 10);
  return { tasks: toInt(m[1]), frags: toInt(m[2]) };
}


// Minimal, soft palette for dark UI
const COLORS = {
  real: '#7dd3fc',  // soft cyan (Real)
  sim:  '#fbbf24',  // soft amber (Sim)
};


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

function renderOpenDC(sim){
  $('#kvEnergy')   && ($('#kvEnergy').textContent   = fmt(sim.energy_kwh, 2));
  $('#kvCPU')      && ($('#kvCPU').textContent      = pct(sim.cpu_utilization, 1));
  $('#kvRuntime')  && ($('#kvRuntime').textContent  = fmt(sim.runtime_hours, 1));
  $('#kvMaxPower') && ($('#kvMaxPower').textContent = fmt(sim.max_power_draw, 0));
  $('#kvSimType')  && ($('#kvSimType').textContent  = sim.status || '—');

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

// ---- polling -------------------------------------------------
async function poll(){
  try{
    const r = await fetch('/api/status', {cache:'no-store'});
    const s = await r.json();

    applyStatus(s.status);

    // right-side window pill text
    const wp = document.querySelector('#windowPill');
    if (wp) {
      const n = s.cycle_count ?? null;
      const info = s.current_window || '';
      wp.textContent = n ? `Window ${n}` : (info || '—');
    }

    renderMetrics(s);
    renderOpenDC(s.last_simulation || {});
    renderLLM(s.last_optimization || {});
    renderTopoTable(s.current_topology || null);
    renderBest(s.best_config || null);
  }catch(e){}
}

setMode(localStorage.getItem(modeKey) || 'ui');
poll();
setInterval(poll, 2000);

// ===== Mock charts (client-side) =====
function mkTimeline(points = 24*12, stepMin = 5) {
  const now = Date.now();
  const step = stepMin * 60 * 1000;
  const xs = [];
  for (let i = points - 1; i >= 0; i--) xs.push(new Date(now - i * step));
  return xs;
}

function mockSeries() {
  const x = mkTimeline();
  const n = x.length;
  const cpuReal = [], cpuSim = [], eReal = [], eSim = [];

  for (let i = 0; i < n; i++) {
    const t = i / n;
    const base = 55 + 25 * Math.sin(t * Math.PI * 4);
    const jitter = (a) => a + (Math.random() - 0.5) * 8;
    const cr = Math.max(0, Math.min(100, jitter(base)));
    const cs = Math.max(0, Math.min(100, cr * 0.97 + (Math.random() - 0.5) * 5));
    cpuReal.push(cr);
    cpuSim.push(cs);
    const er = Math.max(0, cr/100 * 0.70/12 + (Math.random() - 0.5) * 0.008);
    const es = Math.max(0, cs/100 * 0.68/12 + (Math.random() - 0.5) * 0.008);
    eReal.push(er); eSim.push(es);
  }
  const pReal = eReal.map(v => v * 12);
  const pSim  = eSim.map(v => v * 12);
  let crCum = 0, csCum = 0;
  const cumReal = eReal.map(v => (crCum += v));
  const cumSim  = eSim.map(v => (csCum += v));
  return { x, cpuReal, cpuSim, pReal, pSim, cumReal, cumSim };
}

// -------- Plotly styling & interactivity helpers --------
const PLOTLY_CONFIG = {
  responsive: true,
  displaylogo: false,
  scrollZoom: true,
  doubleClick: 'reset',
};

function themeFromPage() {
  const bodyStyles = window.getComputedStyle(document.body);
  const fg = bodyStyles.color || '#e5e7eb';
  return {
    paperBg: 'rgba(0,0,0,0)',
    plotBg: 'rgba(0,0,0,0)',
    fg,
    grid: 'rgba(128,128,128,0.25)',
    accent: '#22d3ee',
  };
}

function layoutFor(title) {
  const t = themeFromPage();
  return {
    title,
    paper_bgcolor: t.paperBg,
    plot_bgcolor: t.plotBg,
    font: { color: t.fg },
    margin: { l: 60, r: 24, t: 42, b: 40 },
    hovermode: 'x unified',
    uirevision: 'keep',
    hoverlabel: {
      bgcolor: 'rgba(0,0,0,0.65)',
      bordercolor: 'transparent',
      font: { color: t.fg }
    },
    xaxis: {
      showspikes: true,
      spikemode: 'across',
      spikecolor: t.accent,
      spikethickness: 1,
      gridcolor: t.grid,
      // Darken the slider + selector (no more white)
      rangeslider: { visible: true, thickness: 0.12, bgcolor: 'rgba(255,255,255,0.06)' },
      rangeselector: {
        bgcolor: 'rgba(255,255,255,0.06)',
        activecolor: t.accent,
        bordercolor: 'rgba(255,255,255,0.15)',
        buttons: [
          { count: 30, step: 'minute', stepmode: 'backward', label: '30m' },
          { count: 2,  step: 'hour',   stepmode: 'backward', label: '2h'  },
          { count: 6,  step: 'hour',   stepmode: 'backward', label: '6h'  },
          { count: 1,  step: 'day',    stepmode: 'backward', label: '1d'  },
          { step: 'all', label: 'All' }
        ]
      }
    },
    yaxis: { gridcolor: t.grid, zerolinecolor: t.grid },
    legend: { bgcolor: 'rgba(0,0,0,0)' }
  };
}


function plotMockCharts() {
  const { x, cpuReal, cpuSim, pReal, pSim, cumReal, cumSim } = mockSeries();

  Plotly.react(
    'chart-cpu',
    [
      { x, y: cpuReal, mode: 'lines', name: 'Real CPU (%)',
        line: { color: COLORS.real, width: 2 } },
      { x, y: cpuSim,  mode: 'lines', name: 'Sim CPU (%)',
        line: { color: COLORS.sim,  width: 2, dash: 'dash' } },
    ],
    layoutFor('CPU Utilization — Real vs Sim'),
    PLOTLY_CONFIG
  );


  Plotly.react(
    'chart-power',
    [
      { x, y: pReal, mode: 'lines', name: 'Real Power (kW)',
        line: { color: COLORS.real, width: 2 } },
      { x, y: pSim,  mode: 'lines', name: 'Sim Power (kW)',
        line: { color: COLORS.sim,  width: 2, dash: 'dash' } },
    ],
    layoutFor('Power — Real vs Sim (5-min avg)'),
    PLOTLY_CONFIG
  );


  Plotly.react(
    'chart-energy',
    [
      { x, y: cumReal, mode: 'lines', name: 'Real Cumulative (kWh)',
        line: { color: COLORS.real, width: 2 } },
      { x, y: cumSim,  mode: 'lines', name: 'Sim Cumulative (kWh)',
        line: { color: COLORS.sim,  width: 2, dash: 'dash' } },
    ],
    layoutFor('Cumulative Energy — Real vs Sim'),
    PLOTLY_CONFIG
  );

}

// --- responsive resize helpers ---
function debounce(fn, ms) {
  let t;
  return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), ms); };
}

function resizePlots() {
  ['chart-cpu','chart-power','chart-energy'].forEach(id => {
    const el = document.getElementById(id);
    if (el) Plotly.Plots.resize(el);
  });
}
window.addEventListener('resize', debounce(resizePlots, 120));

// ===== Real-time charts via SSE =====
async function fetchTS() {
  const r = await fetch('/api/sim/timeseries', { cache: 'no-store' });
  if (!r.ok) throw new Error('timeseries fetch failed');
  return r.json();
}

async function drawReal() {
  const d = await fetchTS();
  if (d.status !== 'ok') return;

  if (d.host) {
  const traces = [
    { x: d.host.x, y: d.host.cpu_utilization, mode: 'lines',
      name: 'Real CPU (%)', line: { color: COLORS.real, width: 2 } }
  ];
  if (d.host_sim) {
    traces.push({
      x: d.host_sim.x, y: d.host_sim.cpu_utilization, mode: 'lines',
      name: 'Sim CPU (%)', line: { color: COLORS.sim, width: 2, dash: 'dash' }
    });
  }
  Plotly.react('chart-cpu', traces, layoutFor('CPU Utilization — Real vs Sim'), PLOTLY_CONFIG);
}

if (d.power) {
  const traces = [
    { x: d.power.x, y: d.power.power_draw, mode: 'lines',
      name: 'Real Power (kW)', line: { color: COLORS.real, width: 2 } }
  ];
  if (d.power_sim) {
    traces.push({
      x: d.power_sim.x, y: d.power_sim.power_draw, mode: 'lines',
      name: 'Sim Power (kW)', line: { color: COLORS.sim, width: 2, dash: 'dash' }
    });
  }
  Plotly.react('chart-power', traces, layoutFor('Power — Real vs Sim (5-min avg)'), PLOTLY_CONFIG);

  const eTraces = [
    { x: d.power.x, y: d.power.energy_kwh_cum, mode: 'lines',
      name: 'Real Cumulative (kWh)', line: { color: COLORS.real, width: 2 } }
  ];
  if (d.power_sim) {
    eTraces.push({
      x: d.power_sim.x, y: d.power_sim.energy_kwh_cum, mode: 'lines',
      name: 'Sim Cumulative (kWh)', line: { color: COLORS.sim, width: 2, dash: 'dash' }
    });
  }
  Plotly.react('chart-energy', eTraces, layoutFor('Cumulative Energy — Real vs Sim'), PLOTLY_CONFIG);
}

}

// ---- boot the charts ----
document.addEventListener('DOMContentLoaded', () => {
  // draw mock immediately so the page isn’t empty
  try { plotMockCharts(); } catch(_) {}

  // draw real data once, then keep it refreshed
  try { drawReal(); } catch(_) {}

  // start file-change events if the server provides SSE
  try { startSse(); } catch(_) {}

  // safety refresh every few seconds (optional)
  setInterval(() => { try { drawReal(); } catch(_) {} }, 5000);
});
