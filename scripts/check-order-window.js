// Property test of OrderWindow (compiled from the front-end source) against an independent model.
// usage: node check-order-window.js <compiled order-window.js> [seeds]
// Model: the server applies events in order; a subscription registered at event index s receives
// every event after s, in order; a snapshot taken at index t (>= s when the join waits for the
// acknowledgement) returns the page computed from the server state at t; the HTTP response is
// delivered after an arbitrary number of the socket messages. Expected view = page recomputed
// from the final server state (live: newest P keys; historical: keys inside the page's range).
const { OrderWindow } = require(require('path').resolve(process.argv[2]));
const seeds = Number(process.argv[3] || 3000);
let rng = 1;
const rand = () => ((rng = (rng * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
const pick = n => Math.floor(rand() * n);
const STATUSES = ['Z', '0', '1', '2', '4', '6'];

function serverRun(nEvents, initial) {
  const state = new Map(); let next = 1; const events = [];
  for (let i = 0; i < initial; i++) { const r = { id: next++, st: 'Z', v: 0 }; state.set(r.id, r); }
  for (let i = 0; i < nEvents; i++) {
    let row;
    if (state.size === 0 || rand() < 0.4) row = { id: next++, st: 'Z' };
    else { const id = 1 + pick(next - 1); row = { id, st: STATUSES[pick(STATUSES.length)] }; }
    row.v = i + 1; events.push(row);
  }
  return { events, initial };
}
function stateAt(run, t) {
  const m = new Map();
  for (let id = 1; id <= run.initial; id++) m.set(id, { id, st: 'Z', v: 0 });
  for (let i = 0; i < t; i++) m.set(run.events[i].id, run.events[i]);
  return m;
}
function page(state, view, P) {
  const keys = [...state.keys()].sort((a, b) => a - b);
  let sel;
  if (view.kind === 'latest') sel = keys.slice(-P).reverse();
  else if (view.kind === 'before') sel = keys.filter(k => k < view.id).slice(-P).reverse();
  else sel = keys.filter(k => k > view.id).slice(0, P);
  return sel.map(k => state.get(k));
}
let failures = 0, cases = 0, maxSize = 0, unsafeFailures = 0, unsafeCases = 0;
for (let seed = 1; seed <= seeds; seed++) {
  rng = seed;
  const P = 1 + pick(12), n = pick(80), initial = pick(40);
  const run = serverRun(n, initial);
  const kinds = ['latest', 'before', 'after'];
  const kind = kinds[pick(3)];
  const maxId = initial + n + 1;
  const view = kind === 'latest' ? { kind } : { kind, id: 1 + pick(maxId) };
  const s = pick(n + 1);                       // subscription registered after event s
  const safe = rand() < 0.8;                   // safe: snapshot after acknowledgement (t >= s)
  const t = safe ? s + pick(n - s + 1) : pick(s + 1);
  const snapshot = page(stateAt(run, t), view, P);
  const stream = run.events.slice(s);          // delivered in order
  const deliverAt = pick(stream.length + 1);   // response arrives after this many socket messages
  const w = new OrderWindow(r => r.id, P, 3, 1000);
  w.begin();
  for (let i = 0; i < stream.length; i++) {
    if (i === deliverAt) w.apply(snapshot, kind === 'latest');
    w.update(stream[i]);
    maxSize = Math.max(maxSize, w.size);
    if (w.size > P + 3 + 1000) throw new Error('unbounded');
  }
  if (deliverAt === stream.length) w.apply(snapshot, kind === 'latest');
  const final = stateAt(run, n);
  const got = w.view();
  // Expected: live view tracks the newest P keys; a historical page keeps its range.
  let expected;
  if (kind === 'latest') expected = page(final, view, P);
  else {
    const ks = snapshot.map(r => r.id);
    if (!ks.length) expected = [];
    else {
      const lo = Math.min(...ks), hi = Math.max(...ks);
      expected = [...final.keys()].filter(k => k >= lo && k <= hi).sort((a, b) => b - a).map(k => final.get(k));
    }
  }
  const ids = got.map(r => r.id);
  const ok = new Set(ids).size === ids.length && JSON.stringify(got.map(r => [r.id, r.st, r.v])) === JSON.stringify(expected.map(r => [r.id, r.st, r.v]));
  if (safe) { cases++; if (!ok) { failures++; if (failures < 5) console.error('FAIL', { seed, P, kind, view, s, t, deliverAt, got: ids, expected: expected.map(r => r.id) }); } }
  else { unsafeCases++; if (!ok) unsafeFailures++; }
}
const out = { safe_cases: cases, safe_failures: failures, unsafe_cases: unsafeCases, unsafe_failures_expected_nonzero: unsafeFailures, max_rows_held_in_window: maxSize };
console.log(JSON.stringify(out));
process.exit(failures ? 1 : 0);
