import type {
  DataModelFromSchemaDefinition,
  GenericDatabaseReader,
} from 'convex/server';
import { describe, expect, expectTypeOf, test } from 'vitest';
import { compute, createQueryFacade, type QueryFacade } from '../src/index';
import schema from './schema';

type DataModel = DataModelFromSchemaDefinition<typeof schema>;

class FakeIndexBuilder {
  conditions: Array<{ field: string; value: unknown }> = [];

  eq(field: string, value: unknown) {
    this.conditions.push({ field, value });
    return this;
  }
}

class FakeQuery {
  constructor(
    private readonly rows: Record<string, any>[],
    private readonly db: FakeDb,
  ) {}

  withIndex(_index: string, selector?: (builder: FakeIndexBuilder) => FakeIndexBuilder) {
    if (!selector) {
      return this;
    }

    const builder = selector(new FakeIndexBuilder());
    const filtered = this.rows.filter((row) =>
      builder.conditions.every(({ field, value }) => row[field] === value),
    );
    return new FakeQuery(filtered, this.db);
  }

  filter(filterer: (query: any) => boolean) {
    return new FakeQuery(
      this.rows.filter((row) =>
        filterer({
          eq: (left: unknown, right: unknown) => left === right,
          neq: (left: unknown, right: unknown) => left !== right,
          field: (field: string) => row[field],
        }),
      ),
      this.db,
    );
  }

  order(_direction: 'asc' | 'desc') {
    return this;
  }

  async unique() {
    if (this.rows.length > 1) {
      throw new Error('unique() returned more than one result');
    }
    return this.rows[0] ?? null;
  }

  async first() {
    return this.rows[0] ?? null;
  }

  async collect() {
    return [...this.rows];
  }

  async take(count: number) {
    return this.rows.slice(0, count);
  }

  async paginate(opts: { numItems: number; cursor: string | null }) {
    const offset = opts.cursor ? Number(opts.cursor) : 0;
    const page = this.rows.slice(offset, offset + opts.numItems);
    const nextOffset = offset + page.length;
    return {
      page,
      isDone: nextOffset >= this.rows.length,
      continueCursor: String(nextOffset),
    };
  }

  async *[Symbol.asyncIterator]() {
    for (const row of this.rows) {
      yield row;
    }
  }
}

class FakeDb {
  constructor(private readonly tables: Record<string, Record<string, any>[]>) {}

  query(table: string) {
    return new FakeQuery(this.tables[table] ?? [], this);
  }

  async get(id: string) {
    for (const rows of Object.values(this.tables)) {
      const found = rows.find((row) => row._id === id);
      if (found) {
        return found;
      }
    }
    return null;
  }
}

describe('convex-query type surface', () => {
  test('infers indexed and expanded queries', () => {
    const qDb = null as never as GenericDatabaseReader<DataModel>;
    const q = createQueryFacade<DataModel>(qDb);

    const foundUser = q.users.byClerkId('clerk-1').with((user) => ({
      tags: q.tags.byOwnerId(user._id).many(),
    })).unique();

    expectTypeOf<Awaited<typeof foundUser>['tags'][number]['code']>().toEqualTypeOf<string>();

    const maybeTag = q.tags.findOrNull('tag-id' as any).with((tag) => ({
      owner: q.users.find(tag.ownerId),
      tapped: q.tags
        .via('taps', 'tagId')
        .byUserIdAndTagId({ userId: 'user-id' as any, tagId: tag._id })
        .withSource('tap')
        .firstOrNull(),
    }));

    expectTypeOf<NonNullable<Awaited<typeof maybeTag>>['owner']['username']>()
      .toEqualTypeOf<string>();
    expectTypeOf<
      NonNullable<NonNullable<Awaited<typeof maybeTag>>['tapped']>['tap']['userId']
    >().toEqualTypeOf<DataModel['taps']['document']['userId']>();

    const maybeScore = compute(async () => {
      const scoreQ: QueryFacade<DataModel> = createQueryFacade(qDb);
      return (await scoreQ.tags.many()).length;
    });

    expectTypeOf<Awaited<typeof maybeScore>>().toEqualTypeOf<number>();

    // @ts-expect-error invalid index on tags
    void q.tags.byUserId('user-id');
  });
});

describe('convex-query runtime behavior', () => {
  test('supports indexed, expanded, and computed queries', async () => {
    const userId = 'user_1';
    const tagId = 'tag_1';
    const tapId = 'tap_1';
    const ctx = {
      db: new FakeDb({
        users: [
          {
            _id: userId,
            _creationTime: 1,
            clerkId: 'clerk-1',
            username: 'ducky',
          },
        ],
        tags: [
          {
            _id: tagId,
            _creationTime: 2,
            code: 'tag-1',
            ownerId: userId,
          },
        ],
        taps: [
          {
            _id: tapId,
            _creationTime: 3,
            tagId,
            userId,
          },
        ],
      }) as never as GenericDatabaseReader<DataModel>,
    };

    const q = createQueryFacade<DataModel>(ctx.db);
    const user = await q.users.byClerkId('clerk-1').with((foundUser) => ({
      tags: q.tags.byOwnerId(foundUser._id).many(),
      tagCount: compute(async () => {
        const tags = await q.tags.byOwnerId(foundUser._id).many();
        return tags.length;
      }),
    })).unique();

    expect(user.username).toBe('ducky');
    expect(user.tags.map((tag) => tag.code)).toEqual(['tag-1']);
    expect(user.tagCount).toBe(1);

    const linkedTag = await q.tags
      .via('taps', 'tagId')
      .byUserId(userId as any)
      .withSource('tap')
      .first();

    expect(linkedTag._id).toBe(tagId);
    expect(linkedTag.tap.userId).toBe(userId);
  });
});
