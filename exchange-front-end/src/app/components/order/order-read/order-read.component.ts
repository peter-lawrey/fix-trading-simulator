import { Component, NgZone, OnDestroy, OnInit } from '@angular/core';
import { Order } from './../order.model';
import { OrderPageQuery, OrderService } from './../order.service';
import { OrderWebSocketService } from './../order.websocket.service';
import { OrderWindow } from './../order-window';

type View = { kind: 'latest' } | { kind: 'before', id: number } | { kind: 'after', id: number };

/**
 * Shows one bounded page of orders, newest public OrderID first, and merges WebSocket updates by
 * OrderID (which can differ from the broker's ClOrdID shown as OrigClOrdID).
 *
 * First page: the newest orders (GET /orders?order=desc&limit=100, by OrderID), live: new orders are added at
 * the top and the oldest row leaves the page. "Older" and "Newer" read the next page by cursor
 * and stop live insertion; "Latest" returns to the live page.
 * Join: the socket is opened with ?hello=1 and the page is requested only after the server's
 * subscription frame, so every update after the snapshot is delivered; updates that arrive while
 * the page is in flight are held and replayed after it. Reconnect: back off, resubscribe, then
 * read the same view again. Socket and HTTP callbacks run outside Angular; one change detection
 * per animation frame renders all updates merged since the previous frame.
 */
@Component({
  selector: 'app-order-read',
  templateUrl: './order-read.component.html',
  styleUrls: ['./order-read.component.css']
})
export class OrderReadComponent implements OnInit, OnDestroy {

  /** Columns displayed in the table. Columns IDs can be added, removed, or reordered. */
  displayedColumns = ['origClOrdID', 'sideText', 'symbol', 'ordStatusText', 'price', 'orderQty', 'cumQty', 'action'];

  readonly pageSize = 100;

  orders: Order[] = [];
  state = 'connecting';
  live = true;
  newer = false;

  private readonly window = new OrderWindow<Order>(order => order.orderID, this.pageSize);
  private view: View = { kind: 'latest' };
  private socket: WebSocket = null;
  private subscribed = false;
  private everSubscribed = false;
  private attempts = 0;
  private request = 0;
  private frame = 0;
  private retry: any = null;
  private announce = false;
  private destroyed = false;

  trackByOrder = (index: number, order: Order) => order.orderID;

  constructor(
    private orderService: OrderService,
    private orderWebSocketService: OrderWebSocketService,
    private zone: NgZone) { }

  ngOnInit() {
    this.zone.runOutsideAngular(() => this.connect());
  }

  ngOnDestroy(): void {
    this.destroyed = true;
    clearTimeout(this.retry);
    if (this.frame) cancelAnimationFrame(this.frame);
    if (this.socket) this.socket.close();
    this.socket = null;
  }

  readOrders(showMessage: boolean) {
    this.announce = showMessage;
    this.show(this.view);
  }

  latest() {
    this.show({ kind: 'latest' });
  }

  older() {
    const id = this.window.oldest;
    if (id !== undefined) this.show({ kind: 'before', id });
  }

  newerOrders() {
    const id = this.window.newest;
    this.show(id === undefined ? { kind: 'latest' } : { kind: 'after', id });
  }

  get range(): string {
    if (!this.orders.length) return 'No orders';
    return `${this.orders[this.orders.length - 1].orderID}–${this.orders[0].orderID}`;
  }

  private show(view: View) {
    this.view = view;
    this.zone.runOutsideAngular(() => this.load());
  }

  private connect() {
    if (this.destroyed) return;
    this.subscribed = false;
    const socket = this.orderWebSocketService.openFeed();
    this.socket = socket;
    socket.onmessage = event => this.receive(socket, event.data);
    socket.onclose = () => this.disconnected(socket);
    this.render();
  }

  private receive(socket: WebSocket, data: string) {
    if (socket !== this.socket) return;
    let message: any;
    try {
      message = JSON.parse(data);
    } catch (e) {
      return;
    }
    if (!this.subscribed) {
      if (message && message.subscribed === true) {
        this.subscribed = true;
        this.everSubscribed = true;
        this.attempts = 0;
        this.load();
      }
      return;
    }
    const outcome = this.window.update(message as Order);
    if (outcome === 'overflow') this.load();
    else if (outcome === 'changed') this.render();
  }

  private disconnected(socket: WebSocket) {
    if (socket !== this.socket || this.destroyed) return;
    this.socket = null;
    this.subscribed = false;
    this.request++; // a page in flight may miss updates sent while disconnected
    this.render();
    clearTimeout(this.retry);
    this.retry = setTimeout(() => this.connect(), Math.min(5000, 250 * Math.pow(2, this.attempts++)));
  }

  private load() {
    clearTimeout(this.retry);
    if (!this.subscribed) return; // the next subscription acknowledgement reads this view
    const request = ++this.request;
    const view = this.view;
    const query: OrderPageQuery = view.kind === 'latest' ? { order: 'desc', limit: this.pageSize }
      : view.kind === 'before' ? { before: view.id, limit: this.pageSize } : { after: view.id, limit: this.pageSize };
    this.window.begin();
    this.render();
    this.orderService.readPage(query).subscribe(rows => {
      if (request !== this.request) return;
      if (view.kind === 'after' && rows.length < this.pageSize) {
        this.view = { kind: 'latest' }; // reached the newest orders: show the live page
        this.load();
        return;
      }
      this.window.apply(rows, view.kind === 'latest');
      this.render();
      if (this.announce) {
        this.announce = false;
        this.zone.run(() => this.orderService.showMessage('Refresh done'));
      }
    }, () => {
      if (request !== this.request) return;
      this.zone.run(() => this.orderService.showMessage('Something wrong happened!', true));
      this.retry = setTimeout(() => this.load(), 2000);
    });
  }

  private render() {
    if (this.frame) return;
    this.frame = requestAnimationFrame(() => {
      this.frame = 0;
      this.zone.run(() => {
        this.orders = this.window.view();
        this.live = this.window.live;
        this.newer = this.window.newer;
        this.state = !this.subscribed ? (this.everSubscribed ? 'reconnecting' : 'connecting')
          : this.window.syncing ? 'loading' : this.live ? 'live' : 'page';
      });
    });
  }

}
