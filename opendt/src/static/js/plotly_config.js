// plotly_config.js

// ===== Plotly config & layout =====
const PLOTLY_CONFIG = {
  responsive: true,
  displaylogo: false,
  displayModeBar: 'hover',
  modeBarButtonsToRemove: ['lasso2d','select2d','autoScale2d','toggleSpikelines'],
  toImageButtonOptions: { format: 'png', filename: 'opendt-chart', height: 720, width: 1280, scale: 2 }
};

// Keep zoom/selection when re-rendering
let UIREVISION = 'persist-zoom';

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
