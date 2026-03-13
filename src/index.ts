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
type JoinTargetTable<
  DataModel extends GenericDataModel,
  JoinTable extends AppTable<DataModel>,
  TargetField extends Extract<keyof AppDoc<DataModel, JoinTable>, string>,
> = IdTargetTable<DataModel, AppDoc<DataModel, JoinTable>[TargetField]>;

type QueryNode<Output> = PromiseLike<Output> & {
  readonly _executeRoot: () => Promise<Output>;
};
type TableInfo<
  DataModel extends GenericDataModel,
  Table extends AppTable<DataModel>,
> = NamedTableInfo<DataModel, Table>;

type WithSpec = Record<string, QueryNode<any>>;
type WithBuilder<ParentItem, Spec extends WithSpec | undefined = WithSpec | undefined> = (
  parent: ParentItem,
) => Spec;
type AnyWithBuilder<ParentItem> = WithBuilder<ParentItem, WithSpec | undefined>;
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
            : never;
        }
      : {})
>;
type AttachSource<ParentItem, SourceItem, SourceKey extends string> = Simplify<
  ParentItem & { [K in SourceKey]: SourceItem }
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
type ExpandableSingleQueryBuilder<Item, Nullable extends boolean> = QueryNode<
  Nullable extends true ? Item | null : Item
> & {
  with<Builder extends AnyWithBuilder<Item>>(
    withBuilder: Builder,
  ): ExpandableSingleQueryBuilder<ExpandWith<Item, Builder>, Nullable>;
};

type SingleQueryBuilder<Item, Nullable extends boolean> = QueryNode<
  Nullable extends true ? Item | null : Item
>;

type UniqueQueryBuilder<Item> = SingleQueryBuilder<Item, false>;
type UniqueOrNullQueryBuilder<Item> = SingleQueryBuilder<Item, true>;
type FirstQueryBuilder<Item> = SingleQueryBuilder<Item, false>;
type FirstOrNullQueryBuilder<Item> = SingleQueryBuilder<Item, true>;
type FindQueryBuilder<Item> = ExpandableSingleQueryBuilder<Item, false>;
type FindOrNullQueryBuilder<Item> = ExpandableSingleQueryBuilder<Item, true>;

type ManyQueryBuilder<Item> = QueryNode<Item[]>;
type BatchQueryBuilder<Item> = QueryNode<Item[]>;

type BatchQueryFacade<Item> = {
  with<Builder extends AnyWithBuilder<Item>>(
    withBuilder: Builder,
  ): BatchQueryFacade<ExpandWith<Item, Builder>>;
  many(): BatchQueryBuilder<Item>;
};

type ViaPair<
  DataModel extends GenericDataModel,
  TargetTable extends AppTable<DataModel>,
  JoinTable extends AppTable<DataModel>,
> = {
  doc: AppDoc<DataModel, TargetTable>;
  link: AppDoc<DataModel, JoinTable>;
};

type ManyViaQueryBuilder<Item> = QueryNode<Item[]>;

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
  take(count: number): Promise<Item[]>;
  paginate(opts: PaginationOptions): Promise<PaginatedResult<Item>>;
  many(): ManyQueryBuilder<Item>;
};

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
  take(count: number): Promise<Item[]>;
  paginate(opts: PaginationOptions): Promise<PaginatedResult<Item>>;
  many(): ManyQueryBuilder<Item>;
};

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

type ViaQueryFacade<
  DataModel extends GenericDataModel,
  TargetTable extends AppTable<DataModel>,
  JoinTable extends AppTable<DataModel>,
  Item = AppDoc<DataModel, TargetTable>,
> = {
  with<Builder extends AnyWithBuilder<Item>>(
    withBuilder: Builder,
  ): ViaQueryFacade<DataModel, TargetTable, JoinTable, ExpandWith<Item, Builder>>;
  order(direction: 'asc' | 'desc'): ViaQueryFacade<DataModel, TargetTable, JoinTable, Item>;
  filter(
    filterer: QueryFilter<TableInfo<DataModel, JoinTable>>,
  ): ViaQueryFacade<DataModel, TargetTable, JoinTable, Item>;
  withSource<const SourceKey extends string>(
    key: SourceKey,
  ): ViaQueryFacade<
    DataModel,
    TargetTable,
    JoinTable,
    AttachSource<Item, AppDoc<DataModel, JoinTable>, SourceKey>
  >;
  unique(): UniqueQueryBuilder<Item>;
  uniqueOrNull(): UniqueOrNullQueryBuilder<Item>;
  first(): FirstQueryBuilder<Item>;
  firstOrNull(): FirstOrNullQueryBuilder<Item>;
  take(count: number): Promise<Item[]>;
  paginate(opts: PaginationOptions): Promise<PaginatedResult<Item>>;
  many(): ManyViaQueryBuilder<Item>;
};

type ValidViaTargetField<
  DataModel extends GenericDataModel,
  JoinTable extends AppTable<DataModel>,
  TargetTable extends AppTable<DataModel>,
> = {
  [Field in Extract<keyof AppDoc<DataModel, JoinTable>, string>]: JoinTargetTable<
    DataModel,
    JoinTable,
    Field
  > extends TargetTable
    ? Field
    : never;
}[Extract<keyof AppDoc<DataModel, JoinTable>, string>];

type ViaIndexNamespace<
  DataModel extends GenericDataModel,
  TargetTable extends AppTable<DataModel>,
  JoinTable extends AppTable<DataModel>,
> = {
  [IndexName in UserIndex<DataModel, JoinTable>]: {
    (): ViaQueryFacade<DataModel, TargetTable, JoinTable>;
    <const Args extends PositionalIndexArgs<DataModel, JoinTable, IndexName>>(
      ...args: Args
    ): ViaQueryFacade<DataModel, TargetTable, JoinTable>;
    (
      selector: IndexSelector<DataModel, JoinTable, IndexName>,
    ): ViaQueryFacade<DataModel, TargetTable, JoinTable>;
  };
};

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
  via: <
    const JoinTable extends AppTable<DataModel>,
    const TargetField extends ValidViaTargetField<DataModel, JoinTable, Table>,
  >(
    joinTable: JoinTable,
    targetField: TargetField,
  ) => ViaIndexNamespace<DataModel, Table, JoinTable>;
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
    }
  | {
      kind: 'via';
      targetTable: string;
      joinTable: string;
      targetField: string;
      index: string;
      selector?: unknown;
    };

type QueryPlan = {
  source: QuerySourcePlan;
  modifiers: QueryModifier[];
  expanders: AnyWithBuilder<any>[];
  sourceKey?: string;
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

async function expandDoc<ParentItem, Builder extends AnyWithBuilder<ParentItem>>(
  parent: ParentItem,
  withBuilder: Builder,
) {
  const spec = withBuilder(parent) ?? {};
  const entries = await Promise.all(
    Object.entries(spec).map(
      async ([key, query]) => [key, await query._executeRoot()] as const,
    ),
  );

  return {
    ...parent,
    ...Object.fromEntries(entries),
  };
}

async function applyExpanders<Item>(
  item: Item,
  expanders: AnyWithBuilder<any>[],
): Promise<Item> {
  let current: any = item;
  for (const expander of expanders) {
    current = await expandDoc(current, expander);
  }
  return current;
}

async function applyExpandersToMany<Item>(
  items: Item[],
  expanders: AnyWithBuilder<any>[],
): Promise<Item[]> {
  return await Promise.all(items.map((item) => applyExpanders(item, expanders)));
}

function buildQuery(makeQuery: () => any, modifiers: QueryModifier[]) {
  return modifiers.reduce((query, modifier) => modifier(query), makeQuery());
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

function withExpander(plan: QueryPlan, expander: AnyWithBuilder<any>): QueryPlan {
  return {
    ...plan,
    expanders: [...plan.expanders, expander],
  };
}

function withSourceKey(plan: QueryPlan, sourceKey: string): QueryPlan {
  return {
    ...plan,
    sourceKey,
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

function createViaQuery<DataModel extends GenericDataModel>(
  db: DbReader<DataModel>,
  joinTable: AppTable<DataModel>,
  index: string,
  selector?: unknown,
) {
  const baseQuery = db.query(joinTable);
  if (selector === undefined) {
    return baseQuery.withIndex(index as any);
  }
  if (typeof selector === 'function') {
    return baseQuery.withIndex(index as any, selector as any);
  }
  return baseQuery.withIndex(index as any, (q: any) =>
    applyIndexValues(q, normalizeIndexValues(index, selector)),
  );
}

async function collectViaPairs<
  DataModel extends GenericDataModel,
  TargetTable extends AppTable<DataModel>,
  JoinTable extends AppTable<DataModel>,
>(
  db: DbReader<DataModel>,
  targetField: string,
  links: AppDoc<DataModel, JoinTable>[],
): Promise<ViaPair<DataModel, TargetTable, JoinTable>[]> {
  const pairs = await Promise.all(
    links.map(async (link) => {
      const id = link[targetField] as GenericId<TargetTable> | null | undefined;
      const doc = id ? await db.get(id) : null;
      return doc ? { doc, link } : null;
    }),
  );

  return pairs.filter((pair) => pair !== null) as ViaPair<
    DataModel,
    TargetTable,
    JoinTable
  >[];
}

async function collectViaPairsUntil<
  DataModel extends GenericDataModel,
  TargetTable extends AppTable<DataModel>,
  JoinTable extends AppTable<DataModel>,
>(
  db: DbReader<DataModel>,
  targetField: string,
  query: AsyncIterable<AppDoc<DataModel, JoinTable>>,
  count: number,
): Promise<ViaPair<DataModel, TargetTable, JoinTable>[]> {
  const pairs: ViaPair<DataModel, TargetTable, JoinTable>[] = [];

  for await (const link of query) {
    const id = link[targetField] as GenericId<TargetTable> | null | undefined;
    if (!id) continue;

    const doc = await db.get(id);
    if (!doc) continue;

    pairs.push({ doc, link });
    if (pairs.length >= count) {
      break;
    }
  }

  return pairs;
}

function normalizeViaUniqueError(
  error: unknown,
  targetTable: string,
  joinTable: string,
  index: string,
) {
  if (
    error instanceof Error &&
    error.message === 'unique() returned more than one result'
  ) {
    return new Error(`Expected unique ${targetTable} via ${joinTable}.${index}`);
  }

  return error;
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
    case 'via': {
      const runQuery = () =>
        buildQuery(
          () => createViaQuery(db, source.joinTable, source.index, source.selector),
          plan.modifiers,
        );

      return {
        unique: async () => {
          const pairs = await collectViaPairsUntil(
            db,
            source.targetField,
            runQuery(),
            2,
          );
          if (pairs.length > 1) {
            throw new Error('unique() returned more than one result');
          }
          return pairs[0] ?? null;
        },
        first: async () => {
          const pairs = await collectViaPairsUntil(
            db,
            source.targetField,
            runQuery(),
            1,
          );
          return pairs[0] ?? null;
        },
        many: async () =>
          await collectViaPairs(db, source.targetField, await runQuery().collect()),
        take: async (count) =>
          await collectViaPairs(db, source.targetField, await runQuery().take(count)),
        paginate: async (opts) => {
          const result = await runQuery().paginate(opts);
          return {
            page: await collectViaPairs(db, source.targetField, result.page),
            isDone: result.isDone,
            continueCursor: result.continueCursor,
          };
        },
        mapOne: async (rawItem) => rawItem.doc,
        mapMany: async (rawItems) => rawItems.map((pair) => pair.doc),
        missingMessages: {
          unique: `Could not find ${source.targetTable} via ${source.joinTable}.${source.index}`,
          first: `Could not find first ${source.targetTable} via ${source.joinTable}.${source.index}`,
        },
        normalizeUniqueError: (error) =>
          normalizeViaUniqueError(
            error,
            source.targetTable,
            source.joinTable,
            source.index,
          ),
      };
    }
  }
}

async function decorateItem(plan: QueryPlan, rawItem: any, item: any) {
  let output = item;
  if (plan.source.kind === 'via' && plan.sourceKey) {
    output = { ...output, [plan.sourceKey]: rawItem.link };
  }
  if (plan.expanders.length > 0) {
    output = await applyExpanders(output, plan.expanders);
  }
  return output;
}

async function decorateItems(plan: QueryPlan, rawItems: any[], items: any[]) {
  let output = items;
  if (plan.source.kind === 'via' && plan.sourceKey) {
    output = output.map((item, index) => ({
      ...item,
      [plan.sourceKey!]: rawItems[index]!.link,
    }));
  }
  if (plan.expanders.length > 0) {
    output = await applyExpandersToMany(output, plan.expanders);
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
    ...createQueryNode(execute),
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
      return createQueryNode(async () => await executeMany(db, plan));
    },
  };
}

function createCollectionFacade<Item>(
  db: DbReader<any>,
  plan: QueryPlan,
):
  | TableRangeQueryFacade<any, any, Item>
  | TableQueryFacade<any, any, Item>
  | ViaQueryFacade<any, any, any, Item> {
  const facade: any = {
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
      return createQueryNode(async () => await executeUnique(db, plan));
    },
    uniqueOrNull() {
      return createQueryNode(async () => await executeUniqueOrNull(db, plan));
    },
    first() {
      return createQueryNode(async () => await executeFirst(db, plan));
    },
    firstOrNull() {
      return createQueryNode(async () => await executeFirstOrNull(db, plan));
    },
    take(count: number) {
      return executeTake(db, plan, count);
    },
    paginate(opts: PaginationOptions) {
      return executePaginate(db, plan, opts);
    },
    many() {
      return createQueryNode(async () => await executeMany(db, plan));
    },
  };

  if (plan.source.kind === 'via') {
    facade.withSource = (key: string) =>
      createCollectionFacade(db, withSourceKey(plan, key));
  }

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

function createViaPlan(
  targetTable: string,
  joinTable: string,
  targetField: string,
  index: string,
  selector?: unknown,
): QueryPlan {
  return createPlan({
    kind: 'via',
    targetTable,
    joinTable,
    targetField,
    index,
    selector,
  });
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
    via(joinTable: AppTable<DataModel>, targetField: string) {
      return createViaNamespace(db, table, joinTable, targetField);
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

function createViaNamespace<
  DataModel extends GenericDataModel,
  const TargetTable extends AppTable<DataModel>,
  const JoinTable extends AppTable<DataModel>,
>(
  db: DbReader<DataModel>,
  targetTable: TargetTable,
  joinTable: JoinTable,
  targetField: string,
): ViaIndexNamespace<DataModel, TargetTable, JoinTable> {
  return new Proxy(
    {},
    {
      get(_target, prop) {
        if (typeof prop !== 'string' || RESERVED_PROMISE_KEYS.has(prop)) {
          return undefined;
        }

        return (...args: unknown[]) =>
          createCollectionFacade(
            db,
            createViaPlan(
              targetTable,
              joinTable,
              targetField as never,
              prop as UserIndex<DataModel, JoinTable>,
              normalizeIndexSelectorArgs(args),
            ),
          );
      },
    },
  ) as ViaIndexNamespace<DataModel, TargetTable, JoinTable>;
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

export function compute<Output = unknown>(
  load: () => Promise<Output> | Output,
): QueryNode<Output> {
  return createQueryNode(async () => await load());
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
