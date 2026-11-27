import { catchError, map } from 'rxjs/operators';
import { Order } from './order.model';
import { Injectable } from '@angular/core';
import { MatSnackBar } from '@angular/material/snack-bar';
import { HttpClient, HttpParams } from '@angular/common/http';
import { EMPTY, Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class OrderService {

  baseUrl = 'http://localhost:8080/orders';

  constructor(private snackBar: MatSnackBar,
    private http: HttpClient) { }

  showMessage(msg: string, isError: boolean = false): void {
    this.snackBar.open(msg, 'X', {
      duration: 3000,
      horizontalPosition: "right",
      verticalPosition: "top",
      panelClass: isError ? ['msg-error'] : ['msg-success']
    });
  }

  create(order: Order): Observable<Order> {
    return this.http.post<Order>(this.baseUrl, order).pipe(
      map((obj) => obj),
      catchError((error) => this.errorHandler(error))
    );
  }

  read(): Observable<Order[]> {
    return this.http.get<Order[]>(this.baseUrl).pipe(
      map((obj) => obj),
      catchError((error) => this.errorHandler(error))
    )
  }

  readById(clOrdID: number): Observable<Order> {
    const url = `${this.baseUrl}/${clOrdID}`;
    return this.http.get<Order>(url).pipe(
      map((obj) => obj),
      catchError((error) => this.errorHandler(error))
    )
  }

  cancel(clOrdID: number): Observable<Order> {
    const url = `${this.baseUrl}/${clOrdID}`;
    return this.http.delete<Order>(url).pipe(
      map((obj) => obj),
      catchError((error) => this.errorHandler(error))
    )
  }

  /**
   * One bounded page. {order: 'desc'} is the newest orders first, {before} the next older ones,
   * {after} the next newer ones in ascending order. Errors reach the caller, which retries.
   */
  readPage(query: OrderPageQuery): Observable<Order[]> {
    let params = new HttpParams().set('limit', String(query.limit));
    if (query.after !== undefined) params = params.set('after', String(query.after));
    if (query.before !== undefined) params = params.set('before', String(query.before));
    if (query.order !== undefined) params = params.set('order', query.order);
    return this.http.get<Order[]>(this.baseUrl, { params });
  }

  errorHandler(e: any): Observable<any> {
    this.showMessage('Something wrong happened!', true);
    return EMPTY;
  }

}

export interface OrderPageQuery {
  limit: number;
  after?: number;
  before?: number;
  order?: 'asc' | 'desc';
}
