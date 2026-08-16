import { fromEvent, of, timer, merge, NEVER, interval } from 'rxjs';
import { fromFetch } from 'rxjs/fetch';
import {
  catchError,
  exhaustMap,
  mapTo,
  mergeMap,
  retry,
  startWith,
  switchMap,
  tap,
  pluck,
  throttleTime,
} from 'rxjs/operators';

import {
  fetchButton,
  stopButton,
  clearError,
  clearFacts,
  addFacts,
  setError,
} from './utilities';

// const endpoint = 'http://localhost:3333/api/facts';
const endpoint = 'http://localhost:3333/api/facts?delay=2000&chaos=1&flakiness=0';

const fetchData$ = fromFetch(endpoint).pipe(
  mergeMap((res) => {
    if (res.ok) {
      return res.json();
    } else {
      throw new Error('Something went wrong!');
    }
  }),
  retry(4),
  catchError((err) => {
    console.error(err);
    return of({ error: 'The stream caught an error. Cool, right?' });
  }),
);

const fetch$ = fromEvent(fetchButton, 'click').pipe(mapTo(true));
const stop$ = fromEvent(stopButton, 'click').pipe(mapTo(true));

const fetchStream$ = merge(fetch$, stop$).pipe(
  startWith(false),
  switchMap(shouldfetch => {
    if (shouldfetch) {
      return timer(0, 5000).pipe(
        tap(() => clearError),
        tap(() => clearFacts()),
        exhaustMap(() => fetchData$)
      )
    } else {
      return NEVER
    }
  })
)

fetchStream$.subscribe(addFacts);
