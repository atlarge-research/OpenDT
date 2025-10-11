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
    
    // Validate inputs
    if (isNaN(energy) || isNaN(runtime)) {
      console.error('Invalid input values');
      btn.classList.add('btn-danger');
      setTimeout(() => {
        btn.classList.remove('btn-danger');
        btn.classList.add('btn-primary');
      }, 1000);
      return;
    }

    // Disable button and store class
    btn.disabled = true;
    btn.classList.remove('btn-primary');
    btn.classList.add('btn-ghost');

<<<<<<< HEAD
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

  // --- CPU (Real vs Sim)
  if (d.host || d.host_sim) {
    const traces = [];
    if (d.host) {
      traces.push({
        x: d.host.x, y: d.host.cpu_utilization, mode: 'lines',
        name: 'Real CPU (%)', line: { color: COLORS.real, width: 2 }
      });
    }
    if (d.host_sim) {
      traces.push({
        x: d.host_sim.x, y: d.host_sim.cpu_utilization, mode: 'lines',
        name: 'Sim CPU (%)', line: { color: COLORS.sim, width: 2, dash: 'dash' }
      });
    }
    Plotly.react('chart-cpu', traces, layoutFor('CPU Utilization — Real vs Sim'), PLOTLY_CONFIG);
  }

  // --- Power (Real vs Sim) — show kW
  if (d.power || d.power_sim) {
    const pTraces = [];
    if (d.power) {
      pTraces.push({
        x: d.power.x, y: (d.power.power_draw || []).map(v => v == null ? null : v / 1000),
        mode: 'lines', name: 'Real Power (kW)', line: { color: COLORS.real, width: 2 }
      });
    }
    if (d.power_sim) {
      pTraces.push({
        x: d.power_sim.x, y: (d.power_sim.power_draw || []).map(v => v == null ? null : v / 1000),
        mode: 'lines', name: 'Sim Power (kW)', line: { color: COLORS.sim, width: 2, dash: 'dash' }
      });
    }
    Plotly.react('chart-power', pTraces, layoutFor('Power — Real vs Sim (5-min avg)'), PLOTLY_CONFIG);
  }

  // --- Cumulative Energy (kWh) — only if present
  if ((d.power && Array.isArray(d.power.energy_kwh_cum)) ||
      (d.power_sim && Array.isArray(d.power_sim.energy_kwh_cum))) {
    const eTraces = [];
    if (d.power && d.power.energy_kwh_cum) {
      eTraces.push({
        x: d.power.x, y: d.power.energy_kwh_cum, mode: 'lines',
        name: 'Real Cumulative (kWh)', line: { color: COLORS.real, width: 2 }
      });
    }
    if (d.power_sim && d.power_sim.energy_kwh_cum) {
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
  try { plotMockCharts(); } catch(_) {}
  try { drawReal(); } catch(_) {}
  try { startSse(); } catch(_) {}
  setInterval(() => { try { drawReal(); } catch(_) {} }, 5000);
});
=======
    const response = await fetch('/api/set_slo', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        energy_target: energy,
        runtime_target: runtime
      })
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    // Update pill sigils
    const energySigil = document.querySelector('#energy_input').closest('.pill-input').querySelector('.pill-sigil');
    const runtimeSigil = document.querySelector('#runtime_input').closest('.pill-input').querySelector('.pill-sigil');
    if (energySigil) energySigil.textContent = fmt(energy, 2);
    if (runtimeSigil) runtimeSigil.textContent = fmt(runtime, 1);

    // Success feedback
    btn.classList.remove('btn-ghost');
    btn.classList.add('btn-success');
    setTimeout(() => {
      btn.classList.remove('btn-success');
      btn.classList.add('btn-primary');
    }, 1000);

  } catch (error) {
    console.error('Failed to submit SLO:', error);
    btn.classList.remove('btn-ghost');
    btn.classList.add('btn-danger');
    setTimeout(() => {
      btn.classList.remove('btn-danger');
      btn.classList.add('btn-primary');
    }, 1000);
  } finally {
    btn.disabled = false;
  }
}
>>>>>>> 658302df2e3dabd73f38e9019164cdda7b59332f
