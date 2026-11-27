/**
 * A bounded, keyed view of one page of orders that merges WebSocket updates.
 *
 * Join protocol (see OrderReadComponent): subscribe with ?hello=1, wait for the server's
 * {"subscribed":true} frame, then begin() and request one page. Updates that arrive while the
 * page is in flight are held (latest per key) and replayed after the page, so an update that
 * the snapshot missed is never overwritten by the older snapshot row.
 *
 * Live view: the newest `capacity` orders by key; a new order above the oldest shown row is
 * inserted and the oldest row leaves the view. Historical view: only rows inside the page's
 * key range change; orders above it only set `newer`.
 * Storage is bounded by capacity + slack rows and, during a snapshot, `maxHeld` keys.
 */
export class OrderWindow<T> {
  private rows = new Map<number, T>();
  private held: Map<number, T> | null = null;
  private min = Infinity;
  private max = -Infinity;
  private cached: T[] | null = [];
  /** True when a historical page is shown and an order above it was updated or created. */
  newer = false;
  /** Highest key seen in an update while a historical page is shown. */
  newestSeen = -Infinity;
  live = true;

  constructor(private readonly key: (row: T) => number | undefined, readonly capacity: number,
              private readonly slack = 64, readonly maxHeld = 20000) { }

  get syncing(): boolean { return this.held !== null; }
  get size(): number { return this.rows.size; }
  get oldest(): number | undefined { return this.rows.size ? this.min : undefined; }
  get newest(): number | undefined { return this.rows.size ? this.max : undefined; }

  /** Hold updates until the next page is applied. */
  begin(): void {
    this.held = new Map();
  }

  /** Replace the rows with one page and replay the updates held since begin(). */
  apply(page: T[], live: boolean): void {
    const held = this.held;
    this.held = null;
    this.live = live;
    this.newer = false;
    this.newestSeen = -Infinity;
    this.rows = new Map();
    this.min = Infinity;
    this.max = -Infinity;
    for (const row of page) {
      const k = this.key(row);
      if (k === undefined || k === null || !isFinite(k)) continue;
      this.put(k, row);
    }
    // A historical page fixes its key range; the live page grows upward from its newest row.
    if (held) held.forEach((row, k) => this.merge(k, row));
    if (this.live) this.trim(this.capacity);
    this.cached = null;
  }

  /** Merge one update. Returns 'overflow' when too many updates were held during a snapshot. */
  update(row: T): 'changed' | 'unchanged' | 'held' | 'overflow' {
    const k = this.key(row);
    if (k === undefined || k === null || !isFinite(k)) return 'unchanged';
    if (this.held) {
      this.held.delete(k); // keep the latest per key; arrival order within a key is server order
      this.held.set(k, row);
      return this.held.size > this.maxHeld ? 'overflow' : 'held';
    }
    return this.merge(k, row) ? 'changed' : 'unchanged';
  }

  /** Visible rows, newest key first. Rebuilt at most once per change. */
  view(): T[] {
    if (this.cached === null) {
      if (this.live) this.trim(this.capacity);
      const keys = Array.from(this.rows.keys()).sort((a, b) => b - a);
      this.cached = keys.map(k => this.rows.get(k) as T);
    }
    return this.cached;
  }

  private merge(k: number, row: T): boolean {
    if (this.rows.has(k)) {
      this.rows.set(k, row);
      this.cached = null;
      return true;
    }
    if (this.live) {
      if (this.rows.size >= this.capacity && k < this.min) return false;
      this.put(k, row);
      if (this.rows.size > this.capacity + this.slack) this.trim(this.capacity);
      this.cached = null;
      return true;
    }
    if (this.rows.size && k > this.min && k < this.max) {
      // Inside a historical page's range but absent from it: show it rather than lose it.
      this.put(k, row);
      this.cached = null;
      return true;
    }
    if (!this.rows.size || k > this.max) {
      const changed = !this.newer;
      this.newer = true;
      this.newestSeen = Math.max(this.newestSeen, k);
      return changed;
    }
    return false;
  }

  private put(k: number, row: T): void {
    this.rows.set(k, row);
    if (k < this.min) this.min = k;
    if (k > this.max) this.max = k;
  }

  private trim(size: number): void {
    if (this.rows.size <= size) return;
    const keys = Array.from(this.rows.keys()).sort((a, b) => b - a);
    for (let i = size; i < keys.length; i++) this.rows.delete(keys[i]);
    this.min = keys[size - 1];
    this.max = keys[0];
    this.cached = null;
  }
}
