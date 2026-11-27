import { catchError, map } from 'rxjs/operators';
import { Injectable } from "@angular/core";
import { EMPTY, Observable } from "rxjs";
import { webSocket, WebSocketSubject } from 'rxjs/webSocket';
import { Order } from './order.model';
import { MatSnackBar } from '@angular/material/snack-bar';

@Injectable({
    providedIn: 'root'
})
export class OrderWebSocketService {

    myWebSocket: WebSocketSubject<Order> = webSocket('ws://localhost:8090/socket/order');

    /** The server sends {"subscribed":true} first, once this socket receives every later update. */
    feedUrl = 'ws://localhost:8090/socket/order?hello=1';

    /** A native socket for the bounded order view; the caller owns its handlers and closing. */
    public openFeed(): WebSocket {
        return new WebSocket(this.feedUrl);
    }

    constructor(private snackBar: MatSnackBar) { }

    public connect(): Observable<Order> {
        return this.myWebSocket.asObservable().pipe(
            map((obj) => obj),
            catchError((error) => this.errorHandler(error))
        );
    }

    public close(): void {
        this.myWebSocket.complete();
    }

    errorHandler(e: any): Observable<any> {
        this.showMessage('Something wrong happened with websocket!', true);
        return EMPTY;
    }

    showMessage(msg: string, isError: boolean = false): void {
        this.snackBar.open(msg, 'X', {
            duration: 3000,
            horizontalPosition: "right",
            verticalPosition: "top",
            panelClass: isError ? ['msg-error'] : ['msg-success']
        });
    }
    
}