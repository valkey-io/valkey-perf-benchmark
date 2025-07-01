/* app.js — Valkey benchmark dashboard for all recorded commits
   Features:
   • Fetch commit SHAs from completed_commits.json
     (ignoring entries with status "in_progress")
   • Load metrics.json for each commit in parallel
   • Filter by cluster_mode and tls
   • Display separate trend charts for each command over the available commits
*/

/* global React, ReactDOM, Recharts */

const COMPLETED_URL = "../completed_commits.json";
const RESULT_URL = sha => `../results/${sha}/metrics.json`;
const COMMIT_URL = sha => `https://github.com/valkey-io/valkey/commit/${sha}`;

const {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  Brush
} = Recharts;

// Utility fetch ---------------------------------------------------------
async function fetchJSON(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`${url}: ${r.status}`);
  return r.json();
}

// React root component --------------------------------------------------
function Dashboard() {
  const [commits, setCommits] = React.useState([]);  // full sha[]
  const [metrics, setMetrics] = React.useState([]);  // flat metrics rows
  const [commitTimes, setCommitTimes] = React.useState({});

  const [cluster, setCluster]         = React.useState("all"); // all/true/false
  const [tls, setTLS]                 = React.useState("all"); // all/true/false
  const [metricKey, setMetricKey]     = React.useState("rps");
  const [pipeline, setPipeline]       = React.useState("all");
  const [dataSize, setDataSize]       = React.useState("all");
  const [selectedCommands, setSelectedCommands] = React.useState(new Set());
  const [fromDate, setFromDate]       = React.useState("");
  const [toDate, setToDate]           = React.useState("");
  const [fromDateUser, setFromDateUser] = React.useState(false);
  const [toDateUser, setToDateUser]     = React.useState(false);
  const [brushRange, setBrushRange]   = React.useState(null);

  function toggleCommand(cmd) {
    setSelectedCommands(prev => {
      const next = new Set(prev);
      if (next.has(cmd)) next.delete(cmd); else next.add(cmd);
      return next;
    });
  }

  // 1) load commit list once on mount
  React.useEffect(() => {
    async function refresh() {
      try {
        const raw = await fetchJSON(COMPLETED_URL);

        const list = [];
        const times = {};
        raw.forEach(c => {
          if (typeof c === 'object' && c.status === 'in_progress') return;
          const sha = typeof c === 'string'
            ? c
            : (c.sha || c.commit || c.full);
          if (!sha) return;
          list.push(sha);
          if (c.timestamp) times[sha] = c.timestamp;
        });

        setCommitTimes(prev => ({ ...prev, ...times }));
        setCommits(prev => {
          const same = prev.length === list.length &&
            prev.every((sha, i) => sha === list[i]);
          return same ? prev : list;
        });
      } catch (err) {
        console.error('Failed to load commit list:', err);
      }
    }
    refresh();
  }, []);

  const loadMetrics = React.useCallback(async () => {
    if (!commits.length) return;
    const all = [];
    const times = { ...commitTimes };
    await Promise.all(commits.map(async sha => {
      try {
        const rows = await fetchJSON(RESULT_URL(sha));
        rows.forEach(r => all.push({ ...r, sha }));
        if (rows[0] && rows[0].timestamp && !times[sha]) times[sha] = rows[0].timestamp;
      } catch (err) {
        console.error(`Failed to load metrics for ${sha}:`, err);
      }
    }));
    const ordered = [...commits].sort((a, b) =>
      new Date(times[a] || 0) - new Date(times[b] || 0)
    );
    const orderMap = Object.fromEntries(ordered.map((s, i) => [s, i]));
    all.sort((a, b) => orderMap[a.sha] - orderMap[b.sha]);
    setCommitTimes(times);
    // avoid triggering the effect again if order didn't change
    const isSameOrder = ordered.length === commits.length &&
      ordered.every((sha, i) => sha === commits[i]);
    if (!isSameOrder) setCommits(ordered);
    setMetrics(all);
  }, [commits, commitTimes]);

  // 2) fetch metrics for each commit
  React.useEffect(() => { if (commits.length) loadMetrics(); }, [commits, loadMetrics]);

  // ensure the date range includes all commit times
  React.useEffect(() => {
    if (!commits.length) return;
    const times = commits
      .map(s => commitTimes[s])
      .filter(Boolean)
      .map(t => new Date(t));
    if (!times.length) return;
    const min = new Date(Math.min.apply(null, times));
    const max = new Date(Math.max.apply(null, times));
    const minStr = min.toISOString().slice(0, 10);
    const maxStr = max.toISOString().slice(0, 10);
    if (!fromDateUser) {
      setFromDate(d => (!d || new Date(d) > min) ? minStr : d);
    }
    if (!toDateUser) {
      setToDate(d => (!d || new Date(d) < max) ? maxStr : d);
    }
  }, [commits, commitTimes, fromDateUser, toDateUser]);

  // unique values for filters
  const commands = React.useMemo(() => [...new Set(metrics.map(m => m.command))].sort(), [metrics]);
  const pipelines = React.useMemo(
    () => [...new Set(metrics.map(m => m.pipeline))].sort((a,b)=>a-b),
    [metrics]
  );
  const dataSizes = React.useMemo(
    () => [...new Set(metrics.map(m => m.data_size))].sort((a,b)=>a-b),
    [metrics]
  );

  const filteredCommits = React.useMemo(() =>
    commits.filter(sha => {
      const ts = commitTimes[sha];
      if (!ts) return false;
      if (fromDate && new Date(ts) < new Date(fromDate)) return false;
      if (toDate && new Date(ts) > new Date(toDate)) return false;
      return true;
    }),
    [commits, commitTimes, fromDate, toDate]
  );

  // reset brush range whenever the user changes the date filters so the
  // full range of the new selection is displayed
  React.useEffect(() => {
    setBrushRange(null);
  }, [fromDate, toDate]);

  React.useEffect(() => {
    if (!filteredCommits.length) {
      setBrushRange(null);
      return;
    }
    setBrushRange(range => {
      if (!range) return { startIndex: 0, endIndex: filteredCommits.length - 1 };
      const start = Math.max(0, Math.min(range.startIndex, filteredCommits.length - 1));
      const end = Math.max(start, Math.min(range.endIndex, filteredCommits.length - 1));
      return { startIndex: start, endIndex: end };
    });
  }, [filteredCommits]);

  const brushStartDate = React.useMemo(() => {
    if (!brushRange || !filteredCommits.length) return null;
    const sha = filteredCommits[brushRange.startIndex];
    const ts = commitTimes[sha];
    return ts ? String(ts).slice(0, 10) : null;
  }, [brushRange, filteredCommits, commitTimes]);

  const brushEndDate = React.useMemo(() => {
    if (!brushRange || !filteredCommits.length) return null;
    const sha = filteredCommits[brushRange.endIndex];
    const ts = commitTimes[sha];
    return ts ? String(ts).slice(0, 10) : null;
  }, [brushRange, filteredCommits, commitTimes]);

  // when command list changes keep existing selections but avoid
  // re-enabling commands the user unchecked
  React.useEffect(() => {
    setSelectedCommands(prev => {
      if (!prev.size) return new Set(commands);
      return new Set([...prev].filter(c => commands.includes(c)));
    });
  }, [commands]);

  // regroup per command → series of commit metrics
  const seriesByCommand = React.useMemo(() => {
    const map = {};
    commands.forEach(cmd => {
      const rows = metrics.filter(r =>
        r.command === cmd &&
        (cluster  === "all" || r.cluster_mode === (cluster === "true")) &&
        (tls      === "all" || r.tls          === (tls === "true")) &&
        (pipeline === "all" || r.pipeline    === Number(pipeline)) &&
        (dataSize === "all" || r.data_size   === Number(dataSize))
      );
      map[cmd] = filteredCommits.map(sha => {
        const row = rows.find(r => r.sha === sha);
        return {
          sha: sha.slice(0,8),
          full: sha,
          timestamp: commitTimes[sha],
          value: row ? row[metricKey] : null
        };
      });
    });
    return map;
  }, [metrics, commands, filteredCommits, cluster, tls, pipeline, dataSize, metricKey, commitTimes]);

  // The full seriesByCommand data is fed into each chart so the Brush component
  // can control the visible range.  Recharts will automatically display only
  // the selected slice based on `startIndex` and `endIndex` provided to Brush.

  const children = [
    // Controls -----------------------------------------------------------
    React.createElement('div', {className:'flex flex-wrap gap-4 justify-center text-center'},
      labelSel('Cluster', cluster, setCluster, ['all','true','false']),
      labelSel('TLS',     tls,     setTLS,     ['all','true','false']),
      labelSel('Pipeline', pipeline, setPipeline, ['all', ...pipelines.map(p=>String(p))]),
      labelSel('Data Size', dataSize, setDataSize, ['all', ...dataSizes.map(d=>String(d))]),
      labelSel('Metric',  metricKey,setMetricKey, ['rps','avg_latency_ms','p95_latency_ms','p99_latency_ms']),
      labelDate('From', fromDate, v => { setFromDate(v); setFromDateUser(true); }, brushStartDate),
      labelDate('To',   toDate,   v => { setToDate(v);   setToDateUser(true);   }, brushEndDate)
    ),
    React.createElement('div', {className:'flex flex-wrap gap-2 justify-center text-center'},
      ...commands.map(cmd => React.createElement('label', {key:cmd, className:'flex items-center'},
        React.createElement('input', {
          type:'checkbox',
          className:'mr-1',
          checked: selectedCommands.has(cmd),
          onChange:()=>toggleCommand(cmd)
        }),
        cmd
      ))
    ),
    // One chart per command ---------------------------------------------
    ...commands.filter(c=>selectedCommands.has(c)).map(cmd => React.createElement('div', {key:cmd, className:'bg-white rounded shadow p-2 w-full max-w-4xl'},
      React.createElement('div', {className:'font-semibold mb-2'}, cmd),
      React.createElement(ResponsiveContainer, {width:'100%', height:400},
        React.createElement(LineChart, {data: seriesByCommand[cmd]},
          React.createElement(CartesianGrid, {strokeDasharray:'3 3'}),
          React.createElement(XAxis, {
            dataKey:'sha',
            interval:0,
            height:70,
            tick: ShaTick
          }),
          React.createElement(YAxis),
          React.createElement(Tooltip, {content: CustomTooltip}),
          (filteredCommits.length > 0) && React.createElement(Brush, {
            dataKey:'timestamp',
            startIndex: brushRange ? brushRange.startIndex : 0,
            endIndex: brushRange ? brushRange.endIndex : (filteredCommits.length - 1),
            tickFormatter: t => t ? String(t).slice(0,10) : '',
            onChange: r => setBrushRange(r)
          }),
          React.createElement(Line, {type:'monotone', dataKey:'value', stroke:'#3b82f6', dot:false, name: metricKey })
        )
      )
    ))
  ];

  return React.createElement('div', {className:'space-y-6 w-full flex flex-col items-center text-center'}, ...children);
}

function labelSel(label, val, setter, opts){
  return React.createElement('label', {className:'font-medium inline-flex items-center'}, `${label}:`,
    React.createElement('select', {className:'border rounded p-1 ml-2', value:val, onChange:e=>setter(e.target.value)},
      opts.map(o=>React.createElement('option',{key:o,value:o},o))
    )
  );
}

function labelDate(label, val, setter, brushVal){
  const displayVal = brushVal || val || '';
  return React.createElement('label', {className:'font-medium inline-flex items-center'}, `${label}:`,
    React.createElement('input', {
      type:'date',
      className:'border rounded p-1 ml-2',
      value: displayVal,
      onChange:e=>setter(e.target.value)
    })
  );
}

function ShaTick(props) {
  const {x, y, payload} = props;
  const sha = payload.value;
  const full = payload.payload && payload.payload.full ? payload.payload.full : sha;
  return React.createElement('g', {transform:`translate(${x},${y})`},
    React.createElement('a', {href: COMMIT_URL(full), target:'_blank', rel:'noopener noreferrer'},
      React.createElement('text', {x:0, y:0, dy:16, textAnchor:'end', transform:'rotate(-45)', style:{cursor:'pointer'}}, sha)
    )
  );
}

function CustomTooltip(props) {
  const {active, payload} = props;
  if (!active || !payload || !payload.length) return null;
  const data = payload[0].payload || {};
  const time = data.timestamp ? new Date(data.timestamp).toLocaleString() : '';
  const name = payload[0].name;
  const value = payload[0].value;
  return React.createElement('div', {className:'bg-white p-2 border rounded shadow text-sm'},
    React.createElement('div', null, time),
    React.createElement('div', null, `${name}: ${value}`)
  );
}

// boot ------------------------------------------------------------------
document.addEventListener('DOMContentLoaded', () => {
  const rootEl = document.getElementById('chartRoot');
  if (ReactDOM.createRoot) {
    ReactDOM.createRoot(rootEl).render(React.createElement(Dashboard));
  } else {
    ReactDOM.render(React.createElement(Dashboard), rootEl);
  }
});
