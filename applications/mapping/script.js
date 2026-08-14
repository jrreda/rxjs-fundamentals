import { of, from, interval, fromEvent, merge, NEVER } from 'rxjs';
import {
  pluck,
  concatMap,
  take,
  map,
  mapTo,
  startWith,
  tap,
  switchMap,
  mergeMap,
  shareReplay,
} from 'rxjs/operators';

import {
  getCharacter,
  render,
  startButton,
  pauseButton,
  setStatus,
} from './utilities';

const character$ = from(getCharacter(1)).pipe(pluck('name'));
character$.subscribe(render);

// const characters$ = interval(1000).pipe(mergeMap(getCharacter));
const characters$ = interval(1000).pipe(mergeMap(getCharacter), shareReplay(0));

const start$ = fromEvent(startButton, 'click').pipe(mapTo(true));
const pause$ = fromEvent(pauseButton, 'click').pipe(mapTo(false));
const isRunning$ = merge(start$, pause$).pipe(
  startWith(false),
  tap(setStatus),
  switchMap((isRunning) => (isRunning ? characters$ : NEVER)),
  tap(render),
);
isRunning$.subscribe();
