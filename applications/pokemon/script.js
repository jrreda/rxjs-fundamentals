import {
  debounceTime,
  distinctUntilChanged,
  fromEvent,
  map,
  mergeMap,
  switchMap,
  tap,
  of,
  merge,
  from,
  filter,
  catchError,
  concat,
  take,
  EMPTY,
  pluck,
  mergeAll,
  exhaustMap,
} from 'rxjs';

import { fromFetch } from 'rxjs/fetch';

import {
  addResults,
  addResult,
  clearResults,
  endpoint,
  endpointFor,
  search,
  form,
  renderPokemon,
} from '../pokemon/utilities';
import { identity } from 'lodash';

// const endpoint = 'http://localhost:3333/api/pokemon/search/';

const getPokemon = (searchTerm) =>
  fromFetch(endpoint + searchTerm + '?delay=1000&chaos=true').pipe(
    mergeMap((res) => res.json()),
  );

const getAdditionalData = (pokemon) =>
  fromFetch(endpointFor(pokemon.id)).pipe(mergeMap((res) => res.json()));

const search$ = fromEvent(form, 'submit').pipe(
  // debounceTime(300),
  map(() => search.value),
  // distinctUntilChanged(),
  exhaustMap((seachTerm) =>
    getPokemon(seachTerm).pipe(
      pluck('pokemon'),
      mergeMap(identity),
      take(1),
      switchMap((pokemon) => {
        const pokemon$ = of(pokemon);
        const additionalData$ = getAdditionalData(pokemon).pipe(
          map((data) => ({ ...pokemon, data })),
        );
        return merge(pokemon$, additionalData$);
      }),
    ),
  ),
  tap(renderPokemon),
);

search$.subscribe(console.log);
