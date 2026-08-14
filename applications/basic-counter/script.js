import {
  fromEvent,
  interval,
  mapTo,
  merge,
  NEVER,
  scan,
  skipUntil,
  switchMap,
  takeUntil,
} from 'rxjs';
import { setCount, startButton, pauseButton } from './utilities';

// const start$ = fromEvent(startButton, 'click');
// const pause$ = fromEvent(pauseButton, 'click');

// let interval$ = interval(1000);
// let subscription;

// start$.subscribe(() => subscription = interval$.subscribe(setCount));
// pause$.subscribe(() => subscription.unsubscribe());

// --------------

// const counter$ = interval(1000).pipe(
//   skipUntil(start$),
//   scan(total => total + 1, 0),
//   takeUntil(pause$)
// )

// counter$.subscribe(setCount)

// --------------

const start$ = fromEvent(startButton, 'click').pipe(mapTo(true));
const pause$ = fromEvent(pauseButton, 'click').pipe(mapTo(false));

const counter$ = merge(start$, pause$).pipe(
  switchMap((shouldIBeRunning) => {
    if (shouldIBeRunning) {
      return interval(1000);
    } else {
      return NEVER;
    }
  }),
  scan(total => total + 1, 0)
);

counter$.subscribe(setCount);
