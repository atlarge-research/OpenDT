(function(){
  const panel = document.getElementById('datalakePanel');
  if (!panel) return;

  const tableBody = panel.querySelector('#datalakeTable tbody');
  const metricsRoot = panel.querySelector('#datalakeMetrics');
  const topologyPre = document.getElementById('datalakeTopology');
  const powerChartEl = document.getElementById('datalakePowerChart');
  const cpuChartEl = document.getElementById('datalakeCPUChart');
  const serviceChartEl = document.getElementById('datalakeServiceChart');

  let currentRun = null;

  const PLOT_CONFIG = { displaylogo: false, responsive: true, modeBarButtonsToRemove: ['lasso2d','select2d'] };

  function resolvePalette(){
    const styles = getComputedStyle(document.documentElement);
    const read = (token, fallback) => {
      const raw = styles.getPropertyValue(token);
      return raw && raw.trim() ? raw.trim() : fallback;
    };
    return {
      text: read('--ink', '#0f172a'),
      subtle: read('--ink-muted', '#64748b'),
      grid: read('--hairline', 'rgba(148,163,184,0.25)'),
      accent: read('--blue', '#3b82f6'),
    };
  }

  function open(){
    panel.classList.add('open');
    panel.setAttribute('aria-hidden', 'false');
    refresh();
  }

  function close(){
    panel.classList.remove('open');
    panel.setAttribute('aria-hidden', 'true');
  }

  function refresh(){
    fetch('/api/datalake/index')
      .then(res => res.ok ? res.json() : Promise.reject(res))
      .then(payload => {
        const runs = (payload && Array.isArray(payload.runs)) ? payload.runs : [];
        renderIndex(runs);
        if (runs.length && (!currentRun || !runs.find(r => r.run_id === currentRun))){
          loadRun(runs[0].run_id);
        }
      })
      .catch(err => {
        console.error('Failed to load data lake index', err);
        renderIndex([]);
      });
  }

  function loadRun(runId){
    if (!runId) return;
    fetch(`/api/datalake/run/${encodeURIComponent(runId)}`)
      .then(res => res.ok ? res.json() : Promise.reject(res))
      .then(record => {
        currentRun = runId;
        highlightRow(runId);
        renderDetail(record);
      })
      .catch(err => {
        console.error('Failed to load run detail', err);
      });
  }

  function renderIndex(runs){
    if (!tableBody) return;
    if (!runs.length){
      tableBody.innerHTML = '<tr><td colspan="4">No runs recorded yet</td></tr>';
      return;
    }
    const rows = runs.map(run => {
      const stamp = formatTimestamp(run.timestamp);
      const type = run.run_type ? run.run_type.toUpperCase() : '—';
      const energy = fmt(run.energy_kwh, 2);
      const runtime = fmt(run.runtime_hours, 2);
      return `<tr data-run-id="${run.run_id}"><td>${stamp}</td><td>${type}</td><td>${energy}</td><td>${runtime}</td></tr>`;
    }).join('');
    tableBody.innerHTML = rows;
    Array.from(tableBody.querySelectorAll('tr')).forEach(row => {
      row.addEventListener('click', () => loadRun(row.dataset.runId));
      if (row.dataset.runId === currentRun){
        row.classList.add('active');
      }
    });
  }

  function highlightRow(runId){
    Array.from(tableBody.querySelectorAll('tr')).forEach(row => {
      row.classList.toggle('active', row.dataset.runId === runId);
    });
  }

  function renderDetail(record){
    if (!record || !metricsRoot) return;
    const metrics = record.metrics || {};
    const summary = record.summary || {};
    const merged = Object.assign({}, summary, metrics);

    metricsRoot.querySelectorAll('[data-key]').forEach(el => {
      const key = el.getAttribute('data-key');
      let value = merged[key];
      if (key === 'cpu_utilization'){ value = pct(value ?? null, 1); }
      else if (key === 'status'){ value = value || 'unknown'; }
      else if (value !== undefined && value !== null && !Number.isNaN(Number(value))){
        const decimals = key === 'runtime_hours' ? 2 : 2;
        value = fmt(value, decimals);
      } else if (value === undefined || value === null){
        value = '—';
      }
      el.textContent = value;
    });

    if (record.topology){
      topologyPre.textContent = JSON.stringify(record.topology, null, 2);
    } else {
      topologyPre.textContent = 'No topology captured for this run.';
    }

    const series = record.timeseries || {};
    plotSeries(powerChartEl, series.power_draw, 'Power [kW]', '#5bd1ff');
    plotSeries(cpuChartEl, normalizeCpuSeries(series.cpu_utilization), 'CPU Utilization', '#9d8bff', { yaxis: { tickformat: '.0%' } });
    plotSeries(serviceChartEl, normalizeServiceSeries(series.service_timestamps), 'Service timeline', '#f7c779');
  }

  function normalizeCpuSeries(series){
    if (!Array.isArray(series)) return series;
    return series.map(entry => {
      if (!entry) return entry;
      const clone = Object.assign({}, entry);
      if (typeof clone.value === 'number'){
        clone.value = Math.min(Math.max(clone.value, 0), 1);
      }
      return clone;
    });
  }

  function normalizeServiceSeries(series){
    if (!Array.isArray(series)) return series;
    return series.map((entry, idx) => ({
      timestamp: entry && entry.timestamp ? entry.timestamp : idx,
      value: idx + 1,
    }));
  }

  function plotSeries(el, series, label, color, layoutExtras){
    if (!el || !window.Plotly){
      return;
    }
    if (!Array.isArray(series) || !series.length){
      Plotly.purge(el);
      el.textContent = 'No data available';
      return;
    }
    el.textContent = '';
    const x = series.map(point => point && point.timestamp !== undefined ? point.timestamp : null);
    const y = series.map(point => point && point.value !== undefined ? point.value : null);
    const cleanX = _normalizeX(x, y.length);
    const palette = resolvePalette();
    const trace = {
      type: 'scatter',
      mode: 'lines+markers',
      name: label,
      x: cleanX,
      y: y,
      line: { color: color || palette.accent, width: 2.5 },
      marker: { size: 5, color: color || palette.accent },
    };
    const baseLayout = {
      margin: { l: 54, r: 18, b: 38, t: 12 },
      paper_bgcolor: 'rgba(0,0,0,0)',
      plot_bgcolor: 'rgba(0,0,0,0)',
      hovermode: 'closest',
      font: { color: palette.text, size: 12 },
      xaxis: {
        tickfont: { color: palette.subtle },
        gridcolor: palette.grid,
        zeroline: false,
        linecolor: palette.grid,
      },
      yaxis: {
        tickfont: { color: palette.subtle },
        gridcolor: palette.grid,
        zeroline: false,
        linecolor: palette.grid,
      },
    };

    if (layoutExtras && typeof layoutExtras === 'object'){
      Object.entries(layoutExtras).forEach(([key, value]) => {
        if (key === 'xaxis' || key === 'yaxis'){
          baseLayout[key] = Object.assign({}, baseLayout[key] || {}, value);
        } else {
          baseLayout[key] = value;
        }
      });
    }

    Plotly.react(el, [trace], baseLayout, PLOT_CONFIG);
  }

  function formatTimestamp(value){
    if (!value) return '—';
    try {
      const d = new Date(value);
      if (!Number.isNaN(d.getTime())){
        return d.toLocaleString();
      }
    } catch (_err) {
      // ignore
    }
    return value;
  }

  document.addEventListener('keydown', evt => {
    if (evt.key === 'Escape' && panel.classList.contains('open')){
      close();
    }
  });

  panel.addEventListener('click', evt => {
    if (evt.target === panel){
      close();
    }
  });

  window.dataLake = {
    open,
    close,
    refresh,
  };
})();
