import { LogService } from './../log.service';
import { LogMessage } from './../log-message.model';
import { Component, Input, OnInit, SimpleChanges } from '@angular/core';

/**
 * One bounded page of the raw FIX history, newest message first (GET ?limit=100). Message IDs are
 * stable positions, so "Older" reads ?before=<oldest shown> and "Newer" ?before=<newest shown + 101>.
 * Refresh reads the same page again; switching direction returns to the newest page.
 */
@Component({
  selector: 'app-log-message',
  templateUrl: './log-message.component.html',
  styleUrls: ['./log-message.component.css']
})
export class LogMessageComponent implements OnInit {

  displayedColumns = ['id', 'time', 'text'];

  @Input() mode: string = "";

  title: string = "";

  readonly pageSize = 100;

  logMessages: LogMessage[] = [];

  /** Exclusive upper message ID of the page shown; undefined for the newest page. */
  before: number | undefined = undefined;

  latestPage = true;

  private loaded = false;
  private request = 0;

  trackById = (index: number, message: LogMessage) => message.id;

  constructor(private logService: LogService) {
  }

  ngOnInit() {
    if (!this.loaded) this.readMessages(false);
  }

  ngOnChanges(change: SimpleChanges) {
    if (change.mode) {
      this.before = undefined;
      this.readMessages(false);
    }
  }

  latest() {
    this.before = undefined;
    this.readMessages(false);
  }

  older() {
    if (!this.logMessages.length) return;
    this.before = this.logMessages[this.logMessages.length - 1].id;
    this.readMessages(false);
  }

  newer() {
    if (!this.logMessages.length) return this.latest();
    this.before = this.logMessages[0].id + this.pageSize + 1;
    this.readMessages(false);
  }

  get range(): string {
    if (!this.logMessages.length) return 'No messages';
    return `${this.logMessages[this.logMessages.length - 1].id}–${this.logMessages[0].id}`;
  }

  readMessages(showMessage: boolean) {
    this.loaded = true;
    this.title = `Message Log ${this.mode}`;

    let url = "";

    if (this.mode === "Incoming") {
      url = "incoming";
    }
    else {
      url = "outgoing";
    }

    const request = ++this.request;
    const before = this.before;
    this.logService.readMessagePage(url, before, this.pageSize).subscribe(logMessages => {
      if (request !== this.request) return;
      if (showMessage) {
        this.logService.showMessage("Refresh done");
      }
      this.logMessages = logMessages;
      // The newest page is the one asked without a cursor, or a "Newer" page that reached the end.
      this.latestPage = before === undefined || !logMessages.length || logMessages[0].id < before - 1;
      if (this.latestPage) this.before = undefined;
    })
  }

}
