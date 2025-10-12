// charts.js

// ===== Draw charts with digital twin data =====
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
    const y = d.host_sim?.cpu_utilization || [];
    const x = _normalizeX(d.host_sim?.x, y.length, STEP_MS, ANCHOR_MS);

    const traces = [];
    if (y.length) traces.push({
      x: x,
      y: y,
      mode: 'lines',
      name: 'CPU',
      line: { color: COLORS.sim, width: 2 }
    });

    Plotly.react('chart-cpu', traces, layoutFor('CPU Utilization', { yTickformat: ',.0%' }), PLOTLY_CONFIG);
  }

  // ===== Power
  {
    const y = d.power_sim?.power_draw || [];
    const x = _normalizeX(d.power_sim?.x, y.length, STEP_MS, ANCHOR_MS);

    const traces = [];
    if (y.length) traces.push({
      x: x,
      y: y,
      mode: 'lines',
      name: 'Power (W)',
      line: { color: COLORS.sim, width: 2 }
    });

    Plotly.react('chart-power', traces, layoutFor('Power'), PLOTLY_CONFIG);
  }

  // ===== Cumulative Energy
  {
    const y = d.power_sim?.energy_kwh_cum || [];
    const x = _normalizeX(d.power_sim?.x, y.length, STEP_MS, ANCHOR_MS);

    const traces = [];
    if (y.length) traces.push({
      x: x,
      y: y,
      mode: 'lines',
      name: 'Cumulative (kWh)',
      line: { color: COLORS.sim, width: 2 }
    });

    Plotly.react('chart-energy', traces, layoutFor('Cumulative Energy'), PLOTLY_CONFIG);
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
