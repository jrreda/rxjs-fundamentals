import { fromEvent, interval, merge, NEVER, scan, skipUntil, takeUntil } from 'rxjs';
import { setCount, startButton, pauseButton } from './utilities';

const start$ = fromEvent(startButton, 'click');
const pause$ = fromEvent(pauseButton, 'click');

// let interval$ = interval(1000);
// let subscription;

// start$.subscribe(() => subscription = interval$.subscribe(setCount));

// pause$.subscribe(() => subscription.unsubscribe());

const counter$ = interval(1000).pipe(
  skipUntil(start$),
  scan(total => total + 1, 0),
  takeUntil(pause$)
)

counter$.subscribe(setCount)
