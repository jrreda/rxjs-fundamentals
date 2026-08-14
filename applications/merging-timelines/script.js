import { fromEvent, merge, interval, concat, race, forkJoin } from 'rxjs';
import { mapTo, startWith, take, map } from 'rxjs/operators';
import {
  labelWith,
  startButton,
  pauseButton,
  setStatus,
  bootstrap,
} from './utilities';

const start$ = fromEvent(startButton, 'click').pipe(mapTo(true));
const pause$ = fromEvent(pauseButton, 'click').pipe(mapTo(false));

const isRunning$ = merge(start$, pause$).pipe(startWith(false));
isRunning$.subscribe(setStatus);

const first$ = interval(1500).pipe(map(labelWith('First')), take(4));
const second$ = interval(1000).pipe(map(labelWith('Second')), take(4));
// const combined$ = merge(first$, second$);
// const combined$ = concat(first$, second$);
// const combined$ = race(first$, second$);
const combined$ = forkJoin(first$, second$);

bootstrap({ first$, second$, combined$ });
