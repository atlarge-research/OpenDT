const PLOTLY_CONFIG = {
  responsive: true,
  displaylogo: false,
  displayModeBar: false,
  modeBarButtonsToRemove: ['lasso2d','select2d','autoScale2d','toggleSpikelines']
};

let UIREVISION = 'persist-zoom';

function resolveThemePalette(){
  const styles = getComputedStyle(document.documentElement);
  const read = (token, fallback) => {
    const raw = styles.getPropertyValue(token);
    return raw && raw.trim() ? raw.trim() : fallback;
  };
  return {
    text: read('--ink', '#0f172a'),
    subtle: read('--ink-muted', '#64748b'),
    grid: read('--hairline', 'rgba(148,163,184,0.28)'),
    accent: read('--blue', '#56B4E9'),
    surface: read('--surface', 'rgba(255,255,255,0.9)'),
  };
}

function layoutFor(_title) {
  const palette = resolveThemePalette();
  return {
    paper_bgcolor: 'rgba(0,0,0,0)',
    plot_bgcolor:  'rgba(0,0,0,0)',
    margin: { l: 60, r: 120, t: 24, b: 78 },
    font: { color: palette.text },
    uirevision: UIREVISION,

    legend: {
      x: 1.02, y: 1, xanchor: 'left', yanchor: 'top',
      bgcolor: 'rgba(0,0,0,0)', bordercolor: 'rgba(0,0,0,0)',
      borderwidth: 0, orientation: 'v', font: { size: 13, color: palette.subtle }
    },

    xaxis: {
      title: { text: 'Time', font: { color: palette.subtle } },
      tickfont: { color: palette.subtle },
      type: 'date',
      gridcolor: palette.grid,
      zeroline: false,
      linecolor: palette.grid,
      rangeslider: { visible: false }
    },

    yaxis: {
      gridcolor: palette.grid,
      zeroline: false,
      linecolor: palette.grid,
      tickfont: { color: palette.subtle },
      title: { font: { color: palette.subtle } }
    }
  };
}

async function fetchTS() {
  const r = await fetch('/api/sim/timeseries', { cache: 'no-store' });
  if (!r.ok) throw new Error('timeseries fetch failed');
  return r.json();
}

const PLOTS = {
  cpu_usages: {
    range: [0, 100],
    autorange: false,
    title: 'Average CPU Utilization [%]',
    legend: 'Average CPU Utilization [%]',
    transform: value => value * 100,
  },
  power_usages: {
    autorange: true,
    title: 'Power [kWh]',
    legend: 'Power [kWh]',
  }
};

function mapValues(values, transform) {
  if (!Array.isArray(values)) return [];
  if (typeof transform !== 'function') return values;
  return values.map(v => {
    if (v === null || v === undefined) return null;
    const numeric = Number(v);
    if (!Number.isFinite(numeric)) return null;
    return transform(numeric);
  });
}

function drawPlot(plot_name, x, y, extraConfigLayout, isNew = false) {
  const palette = resolveThemePalette();
  const trace = {
    x, y,
    mode: 'lines',
    name: (extraConfigLayout && extraConfigLayout.legend) || plot_name,
    line: { color: palette.accent, width: 2.6 }
  };

  // base layout
  const layout = layoutFor('');

  // always anchor plots to zero on the y-axis
  layout.yaxis = {
    ...layout.yaxis,
    rangemode: 'tozero',
  };

  // apply y-axis config from plot-specific settings
  if (extraConfigLayout) {
    layout.yaxis = {
      ...layout.yaxis,
      ...(extraConfigLayout.range ? { range: extraConfigLayout.range } : {}),
      ...(extraConfigLayout.autorange !== undefined ? { autorange: extraConfigLayout.autorange } : {})
    };
    if (extraConfigLayout.title) {
      layout.yaxis.title = {
        ...(layout.yaxis.title || {}),
        text: '',
      };
    }
  }

  const el = document.getElementById(plot_name);
  if (!el) {
    console.warn(`drawPlot: missing <div id="${plot_name}">`);
    return;
  }

  const data = [trace]; // <-- MUST be an array
  if (isNew) {
    Plotly.newPlot(el, data, layout, PLOTLY_CONFIG);
  } else {
    Plotly.react(el, data, layout, PLOTLY_CONFIG);
  }
}

function init_graphs() {
  Object.keys(PLOTS).forEach(plot_name => {
    drawPlot(plot_name, [], [], PLOTS[plot_name], true);
  });
}

async function drawCharts() {
  try {
    const d = await fetchTS();
   
    const xValues = Array.isArray(d.timestamps) ? d.timestamps : [];
    Object.keys(PLOTS).forEach(plot_name => {
      const cfg = PLOTS[plot_name] || {};
      const yValues = mapValues(d[plot_name], cfg.transform);
      drawPlot(plot_name, xValues, yValues, cfg);
    });
  } catch (err) {
    console.error('drawCharts error:', err);
  }
}

function startSse() {
  try {
    const es = new EventSource('/api/stream');
    es.onmessage = () => drawCharts();
    es.onerror = () => es.close();
  } catch (_) {}
}

// Make sure DOM is ready and the divs exist
document.addEventListener('DOMContentLoaded', () => {
  init_graphs();
  drawCharts();
  // startSse(); // enable if your SSE is live
});

window.init_graphs = init_graphs;
window.drawCharts = drawCharts;
window.startSse = startSse;
