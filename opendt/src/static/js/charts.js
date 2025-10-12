// charts.js

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

function startSse(){
  try{
    const es = new EventSource('/api/stream');
    es.onmessage = () => drawCharts();
    es.onerror = () => es.close();
  }catch(_){}
}

window.drawCharts = drawCharts;
window.startSse   = startSse;
