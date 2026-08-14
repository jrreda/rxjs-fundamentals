import { fromEvent, interval } from 'rxjs';
import {
  throttleTime,
  debounceTime,
  delay,
  debounce,
  throttle,
  scan,
  map,
  tap,
} from 'rxjs/operators';

import {
  button,
  panicButton,
  addMessageToDOM,
  deepThoughtInput,
  setTextArea,
  setStatus,
} from './utilities';

// const buttonClicks$ = fromEvent(button, 'click').pipe(
//   throttleTime(2000),
//   // delay(2000),
// );

const panicButtonClicks$ = fromEvent(panicButton, 'click');
const buttonClicks$ = fromEvent(button, 'click').pipe(
  debounce(() => panicButtonClicks$),
);

buttonClicks$.subscribe(addMessageToDOM);
