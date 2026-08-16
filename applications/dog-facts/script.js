import { fromEvent, of, timer, merge, NEVER, concat, interval } from 'rxjs';
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
const endpoint = 'http://localhost:3333/api/facts?delay=3000&amp;chaos=1';

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

const fetch$ = fromEvent(fetchButton, 'click').pipe(
  tap(() => clearError()),
  exhaustMap(() => fetchData$),
);

fetch$.subscribe(addFacts);
