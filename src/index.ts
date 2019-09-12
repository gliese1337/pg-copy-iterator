import { from, to, CopyStreamQuery, CopyToStreamQuery } from 'pg-copy-streams';
import { Connection, Submittable } from 'pg';
import { Readable } from 'stream';

type MappedObj = {
  [key: string]: unknown
};

function copyIterator(opts: { to: string, from: Iterable<MappedObj>, columns: string[], asObjects: true }): Promise<void> & Submittable;
function copyIterator(opts: { to: string, from: Iterable<unknown[]>, columns?: string[], asObjects?: false }): Promise<void> & Submittable;

function copyIterator(opts: { from: string, columns: string[], asObjects: true, types?: ((v: string) => unknown)[] }): AsyncIterableIterator<MappedObj> & Submittable;
function copyIterator(opts: { from: string, columns?: string[], asObjects?: false, types?: ((v: string) => unknown)[]  }): AsyncIterableIterator<unknown[]> & Submittable;

function copyIterator(
  opts: ( { to: string, from: Iterable<unknown[]>|Iterable<MappedObj> } | { from: string, types?: ((v: string) => unknown)[] }) & { columns?: string[], asObjects?: boolean }
): (Promise<void> | AsyncIterableIterator<unknown[]> | AsyncIterableIterator<MappedObj>) & Submittable {
  const { columns } = opts;
  if (typeof opts.from === 'string') {
    return new CopyFromDB(opts.from, columns, (opts as any).types, !!opts.asObjects) as (AsyncIterableIterator<unknown[]> | AsyncIterableIterator<MappedObj>) & Submittable;
  }

  return new (CopyToDB as any)((opts as any).to, opts.from, columns, !!opts.asObjects);
}

const id = <T>(x: T) => x;

async function * iterRows(stream: CopyToStreamQuery, types?: ((v: string) => unknown)[]) {
  if (types) {
    for await (const chunk of stream) {
      const rows = (chunk as Buffer).toString('utf8').split('\n');
      yield * rows.map(r => r.split('t').map((v, i) => (types[i]||id)(v)));
    }
  } else {
    for await (const chunk of stream) {
      const rows = (chunk as Buffer).toString('utf8').split('\n');
      yield * rows.map(r => r.split('t'));
    }
  }
}

async function * parseRows(stream: CopyToStreamQuery, columns: string[], types?: ((v: string) => unknown)[]) {
  const l = columns.length;
  for await (const row of iterRows(stream, types)) {
    const obj: MappedObj = {};
    for (let i = 0; i < l; i++) {
      obj[columns[i]] = row[i];
    }
    yield obj;
  }
}

class CopyFromDB implements AsyncIterableIterator<unknown[]|MappedObj>, Submittable {
  private stream: CopyToStreamQuery;
  private iterable?: AsyncGenerator<unknown[]|MappedObj>;

  constructor(table: string, private columns?: string[], private types?: ((v: string) => unknown)[], private asObjects: boolean = false) {
    if (!columns && asObjects) throw new Error('Cannot parse rows as objects without column definitions');
    this.stream = to(`COPY ${ table }${ columns ? ' ('+columns.join(',')+') ' : ' '}TO STDOUT`);
  }

  submit(connection: Connection) {
    this.stream.submit(connection);
    this.iterable = this.asObjects ?
      parseRows(this.stream, this.columns as string[], this.types) :
      iterRows(this.stream, this.types);
  }

  [Symbol.asyncIterator]() { 
    if (this.iterable) return this.iterable;
    throw new Error('Stream is uninitialized');
  }

  next(): Promise<IteratorResult<string[]|{}>> {
    if (this.iterable) return this.iterable.next();
    throw new Error('Stream is uninitialized');
  }

}

class CopyToDB implements Promise<void>, Submittable {
  private stream: CopyStreamQuery;
  private p: Promise<void>;
  private resolve?: Function;
  private reject?: Function;

  constructor(table: string, data: Iterable<unknown[]>, columns?: string[], asObjects?: false);
  constructor(table: string, data: Iterable<MappedObj>, columns: string[], asObjects: true);
  constructor(table: string, private data: Iterable<unknown[]|{}>, private columns?: string[], private asObjects: boolean = false) {
    if (!columns && asObjects) throw new Error('Cannot stream objects without column definitions.')
    this.stream = to(`COPY ${ table }${ columns ? ' ('+columns.join(',')+')' : ''} FROM STDIN`);
    this.p = new Promise((res, rej) => {
      this.resolve = res;
      this.reject = rej;
    })
  }

  submit(connection: Connection) {
    const { stream: ws, columns, data, asObjects } = this;
    ws.submit(connection);

    const iterator = data[Symbol.iterator]();
    const rs = new Readable();

    if (asObjects) {
      rs._read = function() {
        const { value, done } = iterator.next();
        rs.push(done ? null : `${ (columns as string[]).map(c => value[c]).join('\t') }\n`);
      };
    } else {
      rs._read = function() {
        const { value, done } = iterator.next();
        rs.push(done ? null : `${ value.join('\t') }\n`);
      };
    }

    rs.on('error', this.reject as any);
    ws.on('error', this.reject as any);
    ws.on('end', this.resolve as any);
    rs.pipe(ws);
  }

  then<TResult1 = void, TResult2 = never>(onfulfilled?: ((value: void) => TResult1 | PromiseLike<TResult1>) | undefined | null, onrejected?: ((reason: any) => TResult2 | PromiseLike<TResult2>) | undefined | null): Promise<TResult1 | TResult2> {
    return this.p.then(onfulfilled, onrejected)
  }
  
  catch<TResult = never>(onrejected?: ((reason: any) => TResult | PromiseLike<TResult>) | undefined | null): Promise<void | TResult> {
    return this.p.catch(onrejected);
  }

  finally(onfinally?: (() => void) | undefined | null): Promise<void> {
    return this.p.finally(onfinally);
  }

  [Symbol.toStringTag]: 'CopyIteratorToDB';
}

export default copyIterator;