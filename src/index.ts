import type {
  DocumentByName,
  ExpressionOrValue,
  FilterBuilder,
  GenericDatabaseReader,
  GenericDataModel,
  GenericTableInfo,
  IndexNames,
  IndexRange,
  IndexRangeBuilder,
  NamedIndex,
  NamedTableInfo,
  TableNamesInDataModel,
} from 'convex/server';
import type { GenericId } from 'convex/values';

type Simplify<T> = {
  [K in keyof T]: T[K];
} & {};

export type AppTable<DataModel extends GenericDataModel> =
  TableNamesInDataModel<DataModel>;
export type AppDoc<
  DataModel extends GenericDataModel,
  Table extends AppTable<DataModel>,
> = DocumentByName<DataModel, Table>;
export type UserIndex<
  DataModel extends GenericDataModel,
  Table extends AppTable<DataModel>,
> = Exclude<
  IndexNames<NamedTableInfo<DataModel, Table>>,
  'by_creation_time'
> &
  string;
type RawIndexFields<
  DataModel extends GenericDataModel,
  Table extends AppTable<DataModel>,
  IndexName extends UserIndex<DataModel, Table>,
> = NamedIndex<NamedTableInfo<DataModel, Table>, IndexName>;
type IndexFields<
  DataModel extends GenericDataModel,
  Table extends AppTable<DataModel>,
  IndexName extends UserIndex<DataModel, Table>,
> = Exclude<
  RawIndexFields<DataModel, Table, IndexName>[number],
  '_creationTime'
>;
type StripCreationTime<Fields extends readonly string[]> = Fields extends readonly [
  ...infer Rest extends readonly string[],
  '_creationTime',
]
  ? Rest
  : Fields;
type UserIndexFields<
  DataModel extends GenericDataModel,
  Table extends AppTable<DataModel>,
  IndexName extends UserIndex<DataModel, Table>,
> = StripCreationTime<RawIndexFields<DataModel, Table, IndexName>>;
type SingleIndexField<
  DataModel extends GenericDataModel,
  Table extends AppTable<DataModel>,
  IndexName extends UserIndex<DataModel, Table>,
> = UserIndexFields<DataModel, Table, IndexName> extends readonly [
  infer Field extends string,
]
    ? Field
    : never;
type TuplePrefixArgs<
  DataModel extends GenericDataModel,
  Table extends AppTable<DataModel>,
  Fields extends readonly string[],
  Seen extends readonly string[] = [],
  SeenValues extends readonly unknown[] = [],
> = Fields extends readonly [
  infer Head extends string,
  ...infer Tail extends readonly string[],
]
  ?
      | [...SeenValues, AppDoc<DataModel, Table>[Head]]
      | TuplePrefixArgs<
          DataModel,
          Table,
          Tail,
          [...Seen, Head],
          [...SeenValues, AppDoc<DataModel, Table>[Head]]
        >
  : never;
type PositionalIndexArgs<
  DataModel extends GenericDataModel,
  Table extends AppTable<DataModel>,
  IndexName extends UserIndex<DataModel, Table>,
> = TuplePrefixArgs<DataModel, Table, UserIndexFields<DataModel, Table, IndexName>>;
export type RootIndexValueArg<
  DataModel extends GenericDataModel,
  Table extends AppTable<DataModel>,
  IndexName extends UserIndex<DataModel, Table>,
> =
  | PositionalIndexArgs<DataModel, Table, IndexName>
  | (SingleIndexField<DataModel, Table, IndexName> extends never
      ? never
      : AppDoc<DataModel, Table>[SingleIndexField<DataModel, Table, IndexName>]);
export type StrictRootIndexValueArg<
  DataModel extends GenericDataModel,
  Table extends AppTable<DataModel>,
  IndexName extends UserIndex<DataModel, Table>,
  Value extends RootIndexValueArg<DataModel, Table, IndexName>,
> =
  Value;
type TableIndexName<DataModel extends GenericDataModel, Table extends AppTable<DataModel>> =
  | UserIndex<DataModel, Table>
  | 'by_id';
type UserIndexArg<
  DataModel extends GenericDataModel,
  Table extends AppTable<DataModel>,
  IndexName extends UserIndex<DataModel, Table>,
> = SingleIndexField<DataModel, Table, IndexName> extends never
  ? PositionalIndexArgs<DataModel, Table, IndexName>
  : AppDoc<DataModel, Table>[SingleIndexField<DataModel, Table, IndexName>];
type TableIndexValueArg<
  DataModel extends GenericDataModel,
  Table extends AppTable<DataModel>,
  IndexName extends TableIndexName<DataModel, Table>,
> = IndexName extends 'by_id'
  ? GenericId<Table>
  : IndexName extends UserIndex<DataModel, Table>
    ? UserIndexArg<DataModel, Table, IndexName>
    : never;
type StrictTableIndexValueArg<
  DataModel extends GenericDataModel,
  Table extends AppTable<DataModel>,
  IndexName extends TableIndexName<DataModel, Table>,
  Value extends TableIndexValueArg<DataModel, Table, IndexName>,
> = Value;
type TableIndexInvocationArgs<
  DataModel extends GenericDataModel,
  Table extends AppTable<DataModel>,
  IndexName extends TableIndexName<DataModel, Table>,
> = IndexName extends 'by_id'
  ? [GenericId<Table>]
  : IndexName extends UserIndex<DataModel, Table>
    ? PositionalIndexArgs<DataModel, Table, IndexName>
    : never;

type DbReader<DataModel extends GenericDataModel> = GenericDatabaseReader<DataModel>;

type IdTargetTable<DataModel extends GenericDataModel, Value> =
  Value extends GenericId<infer Table extends AppTable<DataModel>> ? Table : never;

type QueryNode<Output> = PromiseLike<Output> & {
  readonly _executeRoot: () => Promise<Output>;
};
type TableInfo<
  DataModel extends GenericDataModel,
  Table extends AppTable<DataModel>,
> = NamedTableInfo<DataModel, Table>;
type QueryPlanHandle<
  DataModel extends GenericDataModel,
  Table extends AppTable<DataModel>,
> = {
  readonly _plan: QueryPlan;
  readonly _table: Table;
};

type WithSpec = Record<string, unknown>;
type DeferredBuilder = <Output>(
  load: () => Promise<Output> | Output,
) => QueryNode<Output>;
type BaseWithContext = {
  defer: DeferredBuilder;
};
type WithContext<SourceItem = never> = BaseWithContext &
  ([SourceItem] extends [never] ? {} : { source: SourceItem });
type WithBuilder<
  ParentItem,
  Context = BaseWithContext,
  Spec extends WithSpec | undefined = WithSpec | undefined,
> = (
  parent: ParentItem,
  context: Context,
) => Spec;
type AnyWithBuilder<ParentItem, Context = BaseWithContext> = WithBuilder<
  ParentItem,
  Context,
  WithSpec | undefined
>;
type BuiltWithSpec<Builder> = Builder extends (...args: any[]) => infer Spec
  ? Spec
  : never;
type ExpandWith<ParentItem, Builder> = Simplify<
  ParentItem &
    (BuiltWithSpec<Builder> extends Record<string, unknown>
      ? {
          [K in keyof BuiltWithSpec<Builder>]: BuiltWithSpec<Builder>[K] extends QueryNode<
            infer Output
          >
            ? Output
            : BuiltWithSpec<Builder>[K];
        }
      : {})
>;

type PaginationOptions = {
  numItems: number;
  cursor: string | null;
};

type PaginatedResult<Item> = {
  page: Item[];
  isDone: boolean;
  continueCursor: string;
};

type QueryFilter<TableInfo extends GenericTableInfo> = (
  q: FilterBuilder<TableInfo>,
) => ExpressionOrValue<boolean>;
type QueryModifier = (query: any) => any;
const RESERVED_PROMISE_KEYS = new Set(['then', 'catch', 'finally']);
type IndexSelector<
  DataModel extends GenericDataModel,
  Table extends AppTable<DataModel>,
  IndexName extends UserIndex<DataModel, Table>,
> = (
  q: IndexRangeBuilder<
    AppDoc<DataModel, Table>,
    NamedIndex<TableInfo<DataModel, Table>, IndexName>
  >,
) => IndexRange;
type SingleNodeKind<Nullable extends boolean> = Nullable extends true
  ? 'nullableSingle'
  : 'single';
type SingleQueryBuilder<Item, Nullable extends boolean> = QueryNode<
  Nullable extends true ? Item | null : Item
> &
  ThroughNodeHandle<Item, SingleNodeKind<Nullable>>;
type ExpandableSingleQueryBuilder<Item, Nullable extends boolean> = SingleQueryBuilder<
  Item,
  Nullable
> & {
  with<Builder extends AnyWithBuilder<Item>>(
    withBuilder: Builder,
  ): ExpandableSingleQueryBuilder<ExpandWith<Item, Builder>, Nullable>;
};

type UniqueQueryBuilder<Item> = SingleQueryBuilder<Item, false>;
type UniqueOrNullQueryBuilder<Item> = SingleQueryBuilder<Item, true>;
type FirstQueryBuilder<Item> = SingleQueryBuilder<Item, false>;
type FirstOrNullQueryBuilder<Item> = SingleQueryBuilder<Item, true>;
type FindQueryBuilder<Item> = ExpandableSingleQueryBuilder<Item, false>;
type FindOrNullQueryBuilder<Item> = ExpandableSingleQueryBuilder<Item, true>;

type ManyQueryBuilder<Item> = QueryNode<Item[]> & ThroughNodeHandle<Item, 'many'>;
type BatchQueryBuilder<Item> = ManyQueryBuilder<Item>;

type BatchQueryFacade<Item> = {
  with<Builder extends AnyWithBuilder<Item>>(
    withBuilder: Builder,
  ): BatchQueryFacade<ExpandWith<Item, Builder>>;
  many(): BatchQueryBuilder<Item>;
};
type ThroughPair<SourceItem, TargetItem> = {
  source: SourceItem;
  target: TargetItem;
};

type ThroughSourceNodeKind = 'many' | 'single' | 'nullableSingle';
type ThroughNodeHandle<SourceItem, Kind extends ThroughSourceNodeKind> = {
  readonly _throughSourceKind: Kind;
  readonly _throughSourceType?: SourceItem;
};
type AnyManySourceNode<SourceItem> = QueryNode<SourceItem[]> &
  ThroughNodeHandle<SourceItem, 'many'>;
type AnySingleSourceNode<SourceItem, Nullable extends boolean> = QueryNode<
  Nullable extends true ? SourceItem | null : SourceItem
> &
  ThroughNodeHandle<SourceItem, SingleNodeKind<Nullable>>;
type ThroughSourceField<
  DataModel extends GenericDataModel,
  TargetTable extends AppTable<DataModel>,
  SourceItem,
> = {
  [Field in Extract<keyof SourceItem, string>]: IdTargetTable<
    DataModel,
    SourceItem[Field]
  > extends TargetTable
    ? Field
    : never;
}[Extract<keyof SourceItem, string>];
type ManyThroughQueryBuilder<Item, SourceItem = unknown> = QueryNode<Item[]> &
  ThroughNodeHandle<SourceItem, 'many'> & {
    with<Builder extends AnyWithBuilder<Item, WithContext<SourceItem>>>(
      withBuilder: Builder,
    ): ManyThroughQueryBuilder<ExpandWith<Item, Builder>, SourceItem>;
  };
type SingleThroughQueryBuilder<
  Item,
  SourceItem,
  Nullable extends boolean,
> = QueryNode<Nullable extends true ? Item | null : Item> &
  ThroughNodeHandle<SourceItem, Nullable extends true ? 'nullableSingle' : 'single'> & {
    with<Builder extends AnyWithBuilder<Item, WithContext<SourceItem>>>(
      withBuilder: Builder,
    ): SingleThroughQueryBuilder<ExpandWith<Item, Builder>, SourceItem, Nullable>;
  };

type TableQueryFacade<
  DataModel extends GenericDataModel,
  Table extends AppTable<DataModel>,
  Item = AppDoc<DataModel, Table>,
> = {
  with<Builder extends AnyWithBuilder<Item>>(
    withBuilder: Builder,
  ): TableQueryFacade<DataModel, Table, ExpandWith<Item, Builder>>;
  order(direction: 'asc' | 'desc'): TableQueryFacade<DataModel, Table, Item>;
  filter(
    filterer: QueryFilter<TableInfo<DataModel, Table>>,
  ): TableQueryFacade<DataModel, Table, Item>;
  unique(): UniqueQueryBuilder<Item>;
  uniqueOrNull(): UniqueOrNullQueryBuilder<Item>;
  first(): FirstQueryBuilder<Item>;
  firstOrNull(): FirstOrNullQueryBuilder<Item>;
  take(count: number): ManyQueryBuilder<Item>;
  paginate(opts: PaginationOptions): Promise<PaginatedResult<Item>>;
  many(): ManyQueryBuilder<Item>;
} & QueryPlanHandle<DataModel, Table>;

type TableRangeQueryFacade<
  DataModel extends GenericDataModel,
  Table extends AppTable<DataModel>,
  Item = AppDoc<DataModel, Table>,
> = {
  with<Builder extends AnyWithBuilder<Item>>(
    withBuilder: Builder,
  ): TableRangeQueryFacade<DataModel, Table, ExpandWith<Item, Builder>>;
  order(direction: 'asc' | 'desc'): TableRangeQueryFacade<DataModel, Table, Item>;
  filter(
    filterer: QueryFilter<TableInfo<DataModel, Table>>,
  ): TableRangeQueryFacade<DataModel, Table, Item>;
  unique(): UniqueQueryBuilder<Item>;
  uniqueOrNull(): UniqueOrNullQueryBuilder<Item>;
  first(): FirstQueryBuilder<Item>;
  firstOrNull(): FirstOrNullQueryBuilder<Item>;
  take(count: number): ManyQueryBuilder<Item>;
  paginate(opts: PaginationOptions): Promise<PaginatedResult<Item>>;
  many(): ManyQueryBuilder<Item>;
} & QueryPlanHandle<DataModel, Table>;

type TableBatchQueryFacade<
  DataModel extends GenericDataModel,
  Table extends AppTable<DataModel>,
  Item = AppDoc<DataModel, Table>,
> = {
  with<Builder extends AnyWithBuilder<Item>>(
    withBuilder: Builder,
  ): TableBatchQueryFacade<DataModel, Table, ExpandWith<Item, Builder>>;
  many(): BatchQueryBuilder<Item>;
};

type ThroughQueryFacade<
  DataModel extends GenericDataModel,
  TargetTable extends AppTable<DataModel>,
  SourceItem,
  Item = AppDoc<DataModel, TargetTable>,
> = {
  with<Builder extends AnyWithBuilder<Item, WithContext<SourceItem>>>(
    withBuilder: Builder,
  ): ThroughQueryFacade<DataModel, TargetTable, SourceItem, ExpandWith<Item, Builder>>;
  unique(): UniqueQueryBuilder<Item>;
  uniqueOrNull(): UniqueOrNullQueryBuilder<Item>;
  first(): FirstQueryBuilder<Item>;
  firstOrNull(): FirstOrNullQueryBuilder<Item>;
  many(): ManyQueryBuilder<Item>;
};
type ThroughCollectionSource<
  DataModel extends GenericDataModel,
  SourceTable extends AppTable<DataModel>,
  SourceItem = AppDoc<DataModel, SourceTable>,
> =
  | TableQueryFacade<DataModel, SourceTable, SourceItem>
  | TableRangeQueryFacade<DataModel, SourceTable, SourceItem>;
type TableNamespace<
  DataModel extends GenericDataModel,
  Table extends AppTable<DataModel>,
> = {
  find<const Id extends GenericId<Table>>(
    id: Id,
  ): FindQueryBuilder<AppDoc<DataModel, Table>>;
  findOrNull<const Id extends GenericId<Table>>(
    id: Id,
  ): FindOrNullQueryBuilder<AppDoc<DataModel, Table>>;
  in<const Id extends GenericId<Table>>(
    ids: Id[],
  ): TableBatchQueryFacade<DataModel, Table>;
  through: {
    <const SourceTable extends AppTable<DataModel>, SourceItem, const TargetField extends ThroughSourceField<
      DataModel,
      Table,
      SourceItem
    >>(
      sourceQuery: ThroughCollectionSource<DataModel, SourceTable, SourceItem>,
      targetField: TargetField,
    ): ThroughQueryFacade<DataModel, Table, SourceItem>;
    <SourceItem, const TargetField extends ThroughSourceField<DataModel, Table, SourceItem>>(
      sourceQuery: AnyManySourceNode<SourceItem>,
      targetField: TargetField,
    ): ManyThroughQueryBuilder<AppDoc<DataModel, Table>, SourceItem>;
    <SourceItem, const TargetField extends ThroughSourceField<DataModel, Table, SourceItem>>(
      sourceQuery: AnySingleSourceNode<SourceItem, false>,
      targetField: TargetField,
    ): SingleThroughQueryBuilder<AppDoc<DataModel, Table>, SourceItem, false>;
    <SourceItem, const TargetField extends ThroughSourceField<DataModel, Table, SourceItem>>(
      sourceQuery: AnySingleSourceNode<SourceItem, true>,
      targetField: TargetField,
    ): SingleThroughQueryBuilder<AppDoc<DataModel, Table>, SourceItem, true>;
  };
} & TableRangeQueryFacade<DataModel, Table> & {
    [IndexName in TableIndexName<DataModel, Table>]: {
      (
        selector: IndexName extends UserIndex<DataModel, Table>
          ? IndexSelector<DataModel, Table, IndexName>
          : never,
      ): TableQueryFacade<DataModel, Table>;
      <const Args extends TableIndexInvocationArgs<DataModel, Table, IndexName>>(
        ...args: Args
      ): TableQueryFacade<DataModel, Table>;
      (): TableRangeQueryFacade<DataModel, Table>;
      in<const Value extends TableIndexValueArg<DataModel, Table, IndexName>>(
        values: StrictTableIndexValueArg<DataModel, Table, IndexName, Value>[],
      ): TableBatchQueryFacade<DataModel, Table>;
    };
  };

export type QueryFacade<DataModel extends GenericDataModel> = {
  [Table in AppTable<DataModel>]: TableNamespace<DataModel, Table>;
};

type QuerySourcePlan =
  | {
      kind: 'id';
      table: string;
      id: GenericId<any>;
    }
  | {
      kind: 'query';
      table: string;
      index?: string;
      selector?: unknown;
    }
  | {
      kind: 'batch';
      table: string;
      index: string;
      values: unknown[];
    };

type QueryPlan = {
  source: QuerySourcePlan;
  modifiers: QueryModifier[];
  expanders: AnyWithBuilder<any, any>[];
};

type ThroughCollectionPlan = {
  targetTable: string;
  targetField: string;
  sourcePlan: QueryPlan;
  expanders: AnyWithBuilder<any, any>[];
};

type ThroughSourceNodePlan<
  SourceItem,
  Kind extends ThroughSourceNodeKind,
> = {
  targetTable: string;
  targetField: string;
  sourceNode: QueryNode<
    Kind extends 'many'
      ? SourceItem[]
      : Kind extends 'nullableSingle'
        ? SourceItem | null
        : SourceItem
  > &
    ThroughNodeHandle<SourceItem, Kind>;
  expanders: AnyWithBuilder<any, any>[];
};

type PlanRuntime<RawItem, Item> = {
  findOrNull?: () => Promise<RawItem | null>;
  unique?: () => Promise<RawItem | null>;
  first?: () => Promise<RawItem | null>;
  many?: () => Promise<RawItem[]>;
  take?: (count: number) => Promise<RawItem[]>;
  paginate?: (opts: PaginationOptions) => Promise<PaginatedResult<RawItem>>;
  mapOne: (rawItem: RawItem) => Promise<Item>;
  mapMany: (rawItems: RawItem[]) => Promise<Item[]>;
  missingMessages: {
    find?: string;
    unique?: string;
    first?: string;
  };
  normalizeUniqueError?: (error: unknown) => unknown;
};

function createQueryNode<Output>(executeRoot: () => Promise<Output>): QueryNode<Output> {
  return {
    _executeRoot: executeRoot,
    then<TResult1 = Output, TResult2 = never>(
      onfulfilled?: ((value: Output) => TResult1 | PromiseLike<TResult1>) | null,
      onrejected?: ((reason: any) => TResult2 | PromiseLike<TResult2>) | null,
    ): PromiseLike<TResult1 | TResult2> {
      return executeRoot().then(onfulfilled, onrejected);
    },
  };
}

function createDeferredNode<Output>(
  load: () => Promise<Output> | Output,
): QueryNode<Output> {
  return createQueryNode(async () => await load());
}

function isQueryNode(value: unknown): value is QueryNode<unknown> {
  return (
    typeof value === 'object' &&
    value !== null &&
    '_executeRoot' in value &&
    typeof (value as { _executeRoot?: unknown })._executeRoot === 'function'
  );
}

async function expandDoc<
  ParentItem,
  Context,
  Builder extends AnyWithBuilder<ParentItem, Context>,
>(
  parent: ParentItem,
  withBuilder: Builder,
  context: Context,
) {
  const spec = withBuilder(parent, context) ?? {};
  const entries = await Promise.all(
    Object.entries(spec).map(async ([key, value]) => [
      key,
      isQueryNode(value) ? await value._executeRoot() : value,
    ]),
  );

  return {
    ...parent,
    ...Object.fromEntries(entries),
  };
}

async function applyExpanders<Item, Context = {}>(
  item: Item,
  expanders: AnyWithBuilder<any, any>[],
  context: Context,
): Promise<Item> {
  let current: any = item;
  for (const expander of expanders) {
    current = await expandDoc(current, expander, context);
  }
  return current;
}

async function applyExpandersToMany<Item, Context = {}>(
  items: Item[],
  expanders: AnyWithBuilder<any, any>[],
  getContext: (_item: Item, _index: number) => Context,
): Promise<Item[]> {
  return await Promise.all(
    items.map((item, index) => applyExpanders(item, expanders, getContext(item, index))),
  );
}

function buildQuery(makeQuery: () => any, modifiers: QueryModifier[]) {
  return modifiers.reduce((query, modifier) => modifier(query), makeQuery());
}

function createWithContext<SourceItem = never>(
  source?: SourceItem,
): WithContext<SourceItem> {
  return {
    defer: createDeferredNode,
    ...(source === undefined ? {} : { source }),
  } as WithContext<SourceItem>;
}

function createPlan(source: QuerySourcePlan): QueryPlan {
  return {
    source,
    modifiers: [],
    expanders: [],
  };
}

function withModifier(plan: QueryPlan, modifier: QueryModifier): QueryPlan {
  return {
    ...plan,
    modifiers: [...plan.modifiers, modifier],
  };
}

function withExpander(
  plan: QueryPlan,
  expander: AnyWithBuilder<any, any>,
): QueryPlan {
  return {
    ...plan,
    expanders: [...plan.expanders, expander],
  };
}

function normalizeIndexValues(index: string, value: unknown) {
  if (Array.isArray(value)) {
    const fieldNames = inferFieldNamesFromIndex(index);
    return Object.fromEntries(fieldNames.map((field, index) => [field, value[index]]));
  }

  if (isPlainObject(value)) {
    return value as Record<string, unknown>;
  }

  return {
    [inferFieldNameFromIndex(index)]: value,
  };
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function applyIndexValues(query: any, values: Record<string, unknown>) {
  let current = query;
  for (const [field, value] of Object.entries(values)) {
    current = current.eq(field, value);
  }
  return current;
}

function inferFieldNameFromIndex(index: string) {
  return inferFieldNamesFromIndex(index)[0]!;
}

function inferFieldNamesFromIndex(index: string) {
  if (index === 'by_id') {
    return ['_id'];
  }
  if (index.startsWith('by') && index.length > 2) {
    return index
      .slice(2)
      .split('And')
      .map((part) => `${part[0]!.toLowerCase()}${part.slice(1)}`);
  }
  throw new Error(`Cannot infer field name from index ${index}`);
}

function createIndexedQuery<DataModel extends GenericDataModel>(
  db: DbReader<DataModel>,
  table: AppTable<DataModel>,
  index?: string,
  selector?: unknown,
) {
  const baseQuery = db.query(table);
  if (index === undefined) {
    return baseQuery;
  }
  if (selector === undefined) {
    return baseQuery.withIndex(index as any);
  }
  if (typeof selector === 'function') {
    return baseQuery.withIndex(index as any, selector as any);
  }
  if (index === 'by_id') {
    return baseQuery.withIndex(index as any, (q: any) => q.eq('_id', selector));
  }
  return baseQuery.withIndex(index as any, (q: any) =>
    applyIndexValues(q, normalizeIndexValues(index, selector)),
  );
}

function sourceDescription(plan: QueryPlan) {
  switch (plan.source.kind) {
    case 'id':
      return `${plan.source.table} with id ${plan.source.id}`;
    case 'batch':
      return `${plan.source.table} via ${plan.source.index}`;
    case 'query':
      return plan.source.index
        ? `${plan.source.table} with index ${plan.source.index}`
        : plan.source.table;
  }
}

async function resolveThroughPair<
  DataModel extends GenericDataModel,
  TargetTable extends AppTable<DataModel>,
  SourceItem,
>(
  db: DbReader<DataModel>,
  targetField: string,
  source: SourceItem,
): Promise<ThroughPair<SourceItem, AppDoc<DataModel, TargetTable>> | null> {
  const id = (source as Record<string, unknown>)[targetField] as
    | GenericId<TargetTable>
    | null
    | undefined;
  if (!id) {
    return null;
  }

  const target = await db.get(id);
  return target ? { source, target } : null;
}

async function collectThroughPairs<
  DataModel extends GenericDataModel,
  TargetTable extends AppTable<DataModel>,
  SourceItem,
>(
  db: DbReader<DataModel>,
  targetField: string,
  sourceItems: SourceItem[],
): Promise<ThroughPair<SourceItem, AppDoc<DataModel, TargetTable>>[]> {
  const pairs = await Promise.all(
    sourceItems.map(async (source) => await resolveThroughPair(db, targetField, source)),
  );

  return pairs.filter((pair) => pair !== null) as ThroughPair<
    SourceItem,
    AppDoc<DataModel, TargetTable>
  >[];
}

async function* iterateDecoratedSourceItems<DataModel extends GenericDataModel>(
  db: DbReader<DataModel>,
  plan: QueryPlan,
) {
  if (plan.source.kind !== 'query') {
    for (const item of await executeMany(db, plan)) {
      yield item;
    }
    return;
  }

  const runtime = createPlanRuntime(db, plan);
  const source = plan.source;
  const query = buildQuery(
    () => createIndexedQuery(db, source.table, source.index, source.selector),
    plan.modifiers,
  );

  for await (const rawItem of query as AsyncIterable<any>) {
    yield await decorateItem(plan, rawItem, await runtime.mapOne(rawItem));
  }
}

async function collectThroughPairsUntil<
  DataModel extends GenericDataModel,
  TargetTable extends AppTable<DataModel>,
  SourceItem,
>(
  db: DbReader<DataModel>,
  targetField: string,
  sourceItems: AsyncIterable<SourceItem>,
  count: number,
): Promise<ThroughPair<SourceItem, AppDoc<DataModel, TargetTable>>[]> {
  const pairs: ThroughPair<SourceItem, AppDoc<DataModel, TargetTable>>[] = [];

  for await (const source of sourceItems) {
    const pair = await resolveThroughPair(db, targetField, source);
    if (!pair) {
      continue;
    }

    pairs.push(pair);
    if (pairs.length >= count) {
      break;
    }
  }

  return pairs;
}

function createPlanRuntime<DataModel extends GenericDataModel>(
  db: DbReader<DataModel>,
  plan: QueryPlan,
): PlanRuntime<any, any> {
  const source = plan.source;

  switch (source.kind) {
    case 'id':
      return {
        findOrNull: async () => await db.get(source.id),
        mapOne: async (rawItem) => rawItem,
        mapMany: async (rawItems) => rawItems,
        missingMessages: {
          find: `Could not find ${source.table} with id ${source.id}`,
        },
      };
    case 'batch':
      return {
        many: async () =>
          (
            await Promise.all(
              source.values.map(
                async (value: unknown) =>
                  await queryUniqueByIndex(db, source.table, source.index, value),
              ),
            )
          ).filter(
            (
              doc: DocumentByName<DataModel, AppTable<DataModel>> | null,
            ): doc is DocumentByName<DataModel, AppTable<DataModel>> => doc !== null,
          ),
        mapOne: async (rawItem) => rawItem,
        mapMany: async (rawItems) => rawItems,
        missingMessages: {},
      };
    case 'query': {
      const runQuery = () =>
        buildQuery(
          () => createIndexedQuery(db, source.table, source.index, source.selector),
          plan.modifiers,
        );

      return {
        unique: async () => await runQuery().unique(),
        first: async () => await runQuery().first(),
        many: async () => await runQuery().collect(),
        take: async (count) => await runQuery().take(count),
        paginate: async (opts) => await runQuery().paginate(opts),
        mapOne: async (rawItem) => rawItem,
        mapMany: async (rawItems) => rawItems,
        missingMessages: {
          unique: source.index
            ? `Could not find ${source.table} with index ${source.index}`
            : `Could not find ${source.table}`,
          first: `Could not find first ${source.table}`,
        },
      };
    }
  }
}

async function decorateItem(plan: QueryPlan, rawItem: any, item: any) {
  let output = item;
  if (plan.expanders.length > 0) {
    output = await applyExpanders(output, plan.expanders, createWithContext());
  }
  return output;
}

async function decorateItems(plan: QueryPlan, rawItems: any[], items: any[]) {
  let output = items;
  if (plan.expanders.length > 0) {
    output = await applyExpandersToMany(output, plan.expanders, () =>
      createWithContext(),
    );
  }
  return output;
}

async function executeFind<DataModel extends GenericDataModel>(
  db: DbReader<DataModel>,
  plan: QueryPlan,
) {
  const runtime = createPlanRuntime(db, plan);
  const rawItem = await runtime.findOrNull?.();
  if (rawItem == null) {
    throw new Error(runtime.missingMessages.find);
  }
  return await decorateItem(plan, rawItem, await runtime.mapOne(rawItem));
}

async function executeFindOrNull<DataModel extends GenericDataModel>(
  db: DbReader<DataModel>,
  plan: QueryPlan,
) {
  const runtime = createPlanRuntime(db, plan);
  const rawItem = await runtime.findOrNull?.();
  if (rawItem == null) {
    return null;
  }
  return await decorateItem(plan, rawItem, await runtime.mapOne(rawItem));
}

async function executeUnique<DataModel extends GenericDataModel>(
  db: DbReader<DataModel>,
  plan: QueryPlan,
) {
  const runtime = createPlanRuntime(db, plan);
  let rawItem: any;
  try {
    rawItem = await runtime.unique?.();
  } catch (error) {
    throw runtime.normalizeUniqueError ? runtime.normalizeUniqueError(error) : error;
  }
  if (rawItem == null) {
    throw new Error(runtime.missingMessages.unique);
  }
  return await decorateItem(plan, rawItem, await runtime.mapOne(rawItem));
}

async function executeUniqueOrNull<DataModel extends GenericDataModel>(
  db: DbReader<DataModel>,
  plan: QueryPlan,
) {
  const runtime = createPlanRuntime(db, plan);
  let rawItem: any;
  try {
    rawItem = await runtime.unique?.();
  } catch (error) {
    throw runtime.normalizeUniqueError ? runtime.normalizeUniqueError(error) : error;
  }
  if (rawItem == null) {
    return null;
  }
  return await decorateItem(plan, rawItem, await runtime.mapOne(rawItem));
}

async function executeFirst<DataModel extends GenericDataModel>(
  db: DbReader<DataModel>,
  plan: QueryPlan,
) {
  const runtime = createPlanRuntime(db, plan);
  const rawItem = await runtime.first?.();
  if (rawItem == null) {
    throw new Error(runtime.missingMessages.first);
  }
  return await decorateItem(plan, rawItem, await runtime.mapOne(rawItem));
}

async function executeFirstOrNull<DataModel extends GenericDataModel>(
  db: DbReader<DataModel>,
  plan: QueryPlan,
) {
  const runtime = createPlanRuntime(db, plan);
  const rawItem = await runtime.first?.();
  if (rawItem == null) {
    return null;
  }
  return await decorateItem(plan, rawItem, await runtime.mapOne(rawItem));
}

async function executeMany<DataModel extends GenericDataModel>(
  db: DbReader<DataModel>,
  plan: QueryPlan,
) {
  const runtime = createPlanRuntime(db, plan);
  const rawItems = await runtime.many?.();
  return await decorateItems(plan, rawItems ?? [], await runtime.mapMany(rawItems ?? []));
}

async function executeTake<DataModel extends GenericDataModel>(
  db: DbReader<DataModel>,
  plan: QueryPlan,
  count: number,
) {
  const runtime = createPlanRuntime(db, plan);
  const rawItems = await runtime.take?.(count);
  return await decorateItems(plan, rawItems ?? [], await runtime.mapMany(rawItems ?? []));
}

async function executePaginate<DataModel extends GenericDataModel>(
  db: DbReader<DataModel>,
  plan: QueryPlan,
  opts: PaginationOptions,
) {
  const runtime = createPlanRuntime(db, plan);
  const result = await runtime.paginate?.(opts);
  const rawItems = result?.page ?? [];

  return {
    page: await decorateItems(plan, rawItems, await runtime.mapMany(rawItems)),
    isDone: result?.isDone ?? true,
    continueCursor: result?.continueCursor ?? opts.cursor ?? '',
  };
}

function createSingleQueryBuilder<Item, Nullable extends boolean>(
  executeRoot: () => Promise<Nullable extends true ? Item | null : Item>,
  nullable: Nullable,
): SingleQueryBuilder<Item, Nullable> {
  return {
    ...createQueryNode(executeRoot),
    _throughSourceKind: nullable ? 'nullableSingle' : 'single',
  } as SingleQueryBuilder<Item, Nullable>;
}

function createManyQueryBuilder<Item>(
  executeRoot: () => Promise<Item[]>,
): ManyQueryBuilder<Item> {
  return {
    ...createQueryNode(executeRoot),
    _throughSourceKind: 'many',
  } as ManyQueryBuilder<Item>;
}

async function decorateThroughItem<Item, SourceItem>(
  expanders: AnyWithBuilder<any, any>[],
  pair: ThroughPair<SourceItem, Item>,
) {
  let output: any = pair.target;
  if (expanders.length > 0) {
    output = await applyExpanders(
      output,
      expanders,
      createWithContext(pair.source),
    );
  }
  return output;
}

async function decorateThroughItems<Item, SourceItem>(
  expanders: AnyWithBuilder<any, any>[],
  pairs: ThroughPair<SourceItem, Item>[],
) {
  let output: any[] = pairs.map(({ target }) => target);
  if (expanders.length > 0) {
    output = await applyExpandersToMany(
      output,
      expanders,
      (_item, index) => createWithContext(pairs[index]!.source),
    );
  }
  return output;
}

async function executeThroughCollectionMany<DataModel extends GenericDataModel>(
  db: DbReader<DataModel>,
  plan: ThroughCollectionPlan,
) {
  const sourceItems = await executeMany(db, plan.sourcePlan);
  const pairs = await collectThroughPairs<DataModel, any, any>(
    db,
    plan.targetField,
    sourceItems,
  );
  return await decorateThroughItems(plan.expanders, pairs);
}

async function executeThroughCollectionFirst<DataModel extends GenericDataModel>(
  db: DbReader<DataModel>,
  plan: ThroughCollectionPlan,
) {
  const pair = (
    await collectThroughPairsUntil<DataModel, any, any>(
      db,
      plan.targetField,
      iterateDecoratedSourceItems(db, plan.sourcePlan),
      1,
    )
  )[0];

  if (!pair) {
    throw new Error(`Could not find first ${plan.targetTable} through ${sourceDescription(plan.sourcePlan)}`);
  }

  return await decorateThroughItem(plan.expanders, pair);
}

async function executeThroughCollectionFirstOrNull<DataModel extends GenericDataModel>(
  db: DbReader<DataModel>,
  plan: ThroughCollectionPlan,
) {
  const pair = (
    await collectThroughPairsUntil<DataModel, any, any>(
      db,
      plan.targetField,
      iterateDecoratedSourceItems(db, plan.sourcePlan),
      1,
    )
  )[0];

  return pair
    ? await decorateThroughItem(plan.expanders, pair)
    : null;
}

async function executeThroughCollectionUnique<DataModel extends GenericDataModel>(
  db: DbReader<DataModel>,
  plan: ThroughCollectionPlan,
) {
  const pairs = await collectThroughPairsUntil<DataModel, any, any>(
    db,
    plan.targetField,
    iterateDecoratedSourceItems(db, plan.sourcePlan),
    2,
  );

  if (pairs.length > 1) {
    throw new Error('unique() returned more than one result');
  }

  const pair = pairs[0];
  if (!pair) {
    throw new Error(`Could not find ${plan.targetTable} through ${sourceDescription(plan.sourcePlan)}`);
  }

  return await decorateThroughItem(plan.expanders, pair);
}

async function executeThroughCollectionUniqueOrNull<DataModel extends GenericDataModel>(
  db: DbReader<DataModel>,
  plan: ThroughCollectionPlan,
) {
  const pairs = await collectThroughPairsUntil<DataModel, any, any>(
    db,
    plan.targetField,
    iterateDecoratedSourceItems(db, plan.sourcePlan),
    2,
  );

  if (pairs.length > 1) {
    throw new Error('unique() returned more than one result');
  }

  return pairs[0]
    ? await decorateThroughItem(plan.expanders, pairs[0])
    : null;
}

async function executeThroughManyNode<
  DataModel extends GenericDataModel,
  SourceItem,
>(
  db: DbReader<DataModel>,
  plan: ThroughSourceNodePlan<SourceItem, 'many'>,
) {
  const sourceItems = await plan.sourceNode._executeRoot();
  const pairs = await collectThroughPairs<DataModel, any, SourceItem>(
    db,
    plan.targetField,
    sourceItems,
  );
  return await decorateThroughItems(plan.expanders, pairs);
}

async function executeThroughSingleNode<
  DataModel extends GenericDataModel,
  SourceItem,
  Nullable extends boolean,
>(
  db: DbReader<DataModel>,
  plan: ThroughSourceNodePlan<
    SourceItem,
    Nullable extends true ? 'nullableSingle' : 'single'
  >,
  nullable: Nullable,
) {
  const sourceItem = await plan.sourceNode._executeRoot();
  if (sourceItem == null) {
    if (nullable) {
      return null as Nullable extends true ? any : never;
    }
    throw new Error(`Could not find ${plan.targetTable} through source query`);
  }

  const pair = await resolveThroughPair<DataModel, any, SourceItem>(
    db,
    plan.targetField,
    sourceItem as SourceItem,
  );
  if (!pair) {
    if (nullable) {
      return null as Nullable extends true ? any : never;
    }
    throw new Error(`Could not find ${plan.targetTable} through source query`);
  }

  return await decorateThroughItem(plan.expanders, pair);
}

function withThroughCollectionExpander(
  plan: ThroughCollectionPlan,
  expander: AnyWithBuilder<any, any>,
): ThroughCollectionPlan {
  return {
    ...plan,
    expanders: [...plan.expanders, expander],
  };
}

function withThroughNodeExpander<
  SourceItem,
  Kind extends ThroughSourceNodeKind,
>(
  plan: ThroughSourceNodePlan<SourceItem, Kind>,
  expander: AnyWithBuilder<any, any>,
): ThroughSourceNodePlan<SourceItem, Kind> {
  return {
    ...plan,
    expanders: [...plan.expanders, expander],
  };
}

function createThroughCollectionFacade<
  DataModel extends GenericDataModel,
  TargetTable extends AppTable<DataModel>,
  SourceItem,
  Item = AppDoc<DataModel, TargetTable>,
>(
  db: DbReader<DataModel>,
  plan: ThroughCollectionPlan,
): ThroughQueryFacade<DataModel, TargetTable, SourceItem, Item> {
  return {
    with<Builder extends AnyWithBuilder<Item, WithContext<SourceItem>>>(
      withBuilder: Builder,
    ) {
      return createThroughCollectionFacade<
        DataModel,
        TargetTable,
        SourceItem,
        ExpandWith<Item, Builder>
      >(db, withThroughCollectionExpander(plan, withBuilder));
    },
    unique() {
      return createSingleQueryBuilder(
        async () => await executeThroughCollectionUnique(db, plan),
        false,
      );
    },
    uniqueOrNull() {
      return createSingleQueryBuilder(
        async () => await executeThroughCollectionUniqueOrNull(db, plan),
        true,
      );
    },
    first() {
      return createSingleQueryBuilder(
        async () => await executeThroughCollectionFirst(db, plan),
        false,
      );
    },
    firstOrNull() {
      return createSingleQueryBuilder(
        async () => await executeThroughCollectionFirstOrNull(db, plan),
        true,
      );
    },
    many() {
      return createManyQueryBuilder(
        async () => await executeThroughCollectionMany(db, plan),
      );
    },
  };
}

function createThroughManyQueryBuilder<
  DataModel extends GenericDataModel,
  SourceItem,
  Item = unknown,
>(
  db: DbReader<DataModel>,
  plan: ThroughSourceNodePlan<SourceItem, 'many'>,
): ManyThroughQueryBuilder<Item, SourceItem> {
  return {
    ...createQueryNode(async () => await executeThroughManyNode(db, plan)),
    _throughSourceKind: 'many',
    with<Builder extends AnyWithBuilder<Item, WithContext<SourceItem>>>(
      withBuilder: Builder,
    ) {
      return createThroughManyQueryBuilder<DataModel, SourceItem, ExpandWith<Item, Builder>>(
        db,
        withThroughNodeExpander(plan, withBuilder),
      );
    },
  } as ManyThroughQueryBuilder<Item, SourceItem>;
}

function createThroughSingleQueryBuilder<
  DataModel extends GenericDataModel,
  SourceItem,
  Item,
  Nullable extends boolean,
>(
  db: DbReader<DataModel>,
  plan: ThroughSourceNodePlan<
    SourceItem,
    Nullable extends true ? 'nullableSingle' : 'single'
  >,
  nullable: Nullable,
): SingleThroughQueryBuilder<Item, SourceItem, Nullable> {
  return {
    ...createQueryNode(async () => await executeThroughSingleNode(db, plan, nullable)),
    _throughSourceKind: nullable ? 'nullableSingle' : 'single',
    with<Builder extends AnyWithBuilder<Item, WithContext<SourceItem>>>(
      withBuilder: Builder,
    ) {
      return createThroughSingleQueryBuilder<
        DataModel,
        SourceItem,
        ExpandWith<Item, Builder>,
        Nullable
      >(db, withThroughNodeExpander(plan, withBuilder), nullable);
    },
  } as SingleThroughQueryBuilder<Item, SourceItem, Nullable>;
}

function createExpandableSingleFromPlan<Item, Nullable extends boolean>(
  db: DbReader<any>,
  plan: QueryPlan,
  nullable: Nullable,
): ExpandableSingleQueryBuilder<Item, Nullable> {
  const execute = async () =>
    nullable
      ? ((await executeFindOrNull(db, plan)) as Nullable extends true
          ? Item | null
          : Item)
      : ((await executeFind(db, plan)) as Nullable extends true ? Item | null : Item);

  return {
    ...createSingleQueryBuilder(execute, nullable),
    with<Builder extends AnyWithBuilder<Item>>(withBuilder: Builder) {
      return createExpandableSingleFromPlan<ExpandWith<Item, Builder>, Nullable>(
        db,
        withExpander(plan, withBuilder),
        nullable,
      );
    },
  };
}

function createBatchFacade<Item>(
  db: DbReader<any>,
  plan: QueryPlan,
): BatchQueryFacade<Item> {
  return {
    with<Builder extends AnyWithBuilder<Item>>(withBuilder: Builder) {
      return createBatchFacade<ExpandWith<Item, Builder>>(
        db,
        withExpander(plan, withBuilder),
      );
    },
    many() {
      return createManyQueryBuilder(async () => await executeMany(db, plan));
    },
  };
}

function createCollectionFacade<Item>(
  db: DbReader<any>,
  plan: QueryPlan,
):
  | TableRangeQueryFacade<any, any, Item>
  | TableQueryFacade<any, any, Item> {
  const facade: any = {
    _plan: plan,
    _table: plan.source.table,
    with<Builder extends AnyWithBuilder<Item>>(withBuilder: Builder) {
      return createCollectionFacade<ExpandWith<Item, Builder>>(
        db,
        withExpander(plan, withBuilder),
      );
    },
    order(direction: 'asc' | 'desc') {
      return createCollectionFacade<Item>(
        db,
        withModifier(plan, (query) => query.order(direction)),
      );
    },
    filter(filterer: any) {
      return createCollectionFacade<Item>(
        db,
        withModifier(plan, (query) => query.filter(filterer)),
      );
    },
    unique() {
      return createSingleQueryBuilder(async () => await executeUnique(db, plan), false);
    },
    uniqueOrNull() {
      return createSingleQueryBuilder(
        async () => await executeUniqueOrNull(db, plan),
        true,
      );
    },
    first() {
      return createSingleQueryBuilder(async () => await executeFirst(db, plan), false);
    },
    firstOrNull() {
      return createSingleQueryBuilder(
        async () => await executeFirstOrNull(db, plan),
        true,
      );
    },
    take(count: number) {
      return createManyQueryBuilder(async () => await executeTake(db, plan, count));
    },
    paginate(opts: PaginationOptions) {
      return executePaginate(db, plan, opts);
    },
    many() {
      return createManyQueryBuilder(async () => await executeMany(db, plan));
    },
  };

  return facade;
}

function createIdPlan(table: string, id: GenericId<any>): QueryPlan {
  return createPlan({
    kind: 'id',
    table,
    id,
  });
}

function createQueryPlan(table: string, index?: string, selector?: unknown): QueryPlan {
  return createPlan({
    kind: 'query',
    table,
    index,
    selector,
  });
}

function createBatchPlan(table: string, index: string, values: unknown[]): QueryPlan {
  return createPlan({
    kind: 'batch',
    table,
    index,
    values,
  });
}

function isCollectionSource(value: unknown): value is QueryPlanHandle<any, any> {
  return (
    typeof value === 'object' &&
    value !== null &&
    '_plan' in value &&
    '_table' in value
  );
}

function isManySourceNode(value: unknown): value is AnyManySourceNode<unknown> {
  return (
    typeof value === 'object' &&
    value !== null &&
    '_executeRoot' in value &&
    (value as { _throughSourceKind?: unknown })._throughSourceKind === 'many'
  );
}

function isSingleSourceNode(
  value: unknown,
): value is AnySingleSourceNode<unknown, false> | AnySingleSourceNode<unknown, true> {
  return (
    typeof value === 'object' &&
    value !== null &&
    '_executeRoot' in value &&
    ((value as { _throughSourceKind?: unknown })._throughSourceKind === 'single' ||
      (value as { _throughSourceKind?: unknown })._throughSourceKind ===
        'nullableSingle')
  );
}

function normalizeIndexSelectorArgs(args: unknown[]) {
  if (args.length === 0) {
    return undefined;
  }
  if (args.length === 1) {
    return args[0];
  }
  return args;
}

function createTableNamespace<
  DataModel extends GenericDataModel,
  const Table extends AppTable<DataModel>,
>(
  db: DbReader<DataModel>,
  table: Table,
): TableNamespace<DataModel, Table> {
  const rootFacade = createCollectionFacade(db, createQueryPlan(table));
  const target = {
    ...rootFacade,
    find(id: GenericId<Table>) {
      return createExpandableSingleFromPlan(db, createIdPlan(table, id), false);
    },
    findOrNull(id: GenericId<Table>) {
      return createExpandableSingleFromPlan(db, createIdPlan(table, id), true);
    },
    in(ids: GenericId<Table>[]) {
      return createBatchFacade(db, createBatchPlan(table, 'by_id', ids as never[]));
    },
    through(sourceQuery: unknown, targetField: string) {
      if (isCollectionSource(sourceQuery)) {
        return createThroughCollectionFacade(db, {
          targetTable: table,
          targetField,
          sourcePlan: sourceQuery._plan,
          expanders: [],
        });
      }

      if (isManySourceNode(sourceQuery)) {
        return createThroughManyQueryBuilder(db, {
          targetTable: table,
          targetField,
          sourceNode: sourceQuery,
          expanders: [],
        });
      }

      if (isSingleSourceNode(sourceQuery)) {
        return createThroughSingleQueryBuilder(
          db,
          {
            targetTable: table,
            targetField,
            sourceNode: sourceQuery as AnySingleSourceNode<any, any>,
            expanders: [],
          },
          sourceQuery._throughSourceKind === 'nullableSingle',
        );
      }

      throw new Error('through() requires a query facade or lazy query node');
    },
  } as any;

  return new Proxy(target, {
    get(currentTarget, prop, receiver) {
      if (typeof prop !== 'string' || prop in currentTarget) {
        return Reflect.get(currentTarget, prop, receiver);
      }
      if (RESERVED_PROMISE_KEYS.has(prop) || prop === 'all') {
        return undefined;
      }

      const indexMethod = ((...args: unknown[]) =>
        createCollectionFacade(
          db,
          createQueryPlan(
            table,
            prop as TableIndexName<DataModel, Table>,
            normalizeIndexSelectorArgs(args),
          ),
        )) as ((...args: unknown[]) => unknown) & {
        in: (values: unknown[]) => unknown;
      };

      indexMethod.in = (values: unknown[]) =>
        createBatchFacade(
          db,
          createBatchPlan(table, prop as TableIndexName<DataModel, Table>, values),
        );

      return indexMethod;
    },
  }) as TableNamespace<DataModel, Table>;
}

export function createQueryFacade<DataModel extends GenericDataModel>(
  db: GenericDatabaseReader<DataModel>,
): QueryFacade<DataModel> {
  return new Proxy(
    {},
    {
      get(_target, prop) {
        if (typeof prop !== 'string' || RESERVED_PROMISE_KEYS.has(prop)) {
          return undefined;
        }
        return createTableNamespace(db, prop as AppTable<DataModel>);
      },
    },
  ) as QueryFacade<DataModel>;
}

async function queryUniqueByIndex<DataModel extends GenericDataModel>(
  db: DbReader<DataModel>,
  table: AppTable<DataModel>,
  index: string,
  value: unknown,
) {
  if (index === 'by_id') {
    return await (db.query(table) as any)
      .withIndex('by_id', (q: any) => q.eq('_id', value))
      .unique();
  }

  return await db
    .query(table)
    .withIndex(index as any, (q: any) =>
      applyIndexValues(q, normalizeIndexValues(index, value)),
    )
    .unique();
}
