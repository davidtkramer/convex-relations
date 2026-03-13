import type { DataModelFromSchemaDefinition, GenericDatabaseReader } from 'convex/server';
import { assertType, describe, expectTypeOf, test } from 'vitest';
import { compute, createQueryFacade, type QueryFacade } from '../src/index';
import schema from './schema';

type DataModel = DataModelFromSchemaDefinition<typeof schema>;
type PostId = DataModel['posts']['document']['_id'];
type TagId = DataModel['tags']['document']['_id'];

declare const db: GenericDatabaseReader<DataModel>;
declare const postId: PostId;
declare const tagId: TagId;

const q = createQueryFacade<DataModel>(db);

describe('convex-relations type surface', () => {
  test('direct id builders support staged composition', () => {
    const foundPost = q.posts.find(postId).with((post) => ({
      author: q.authors.find(post.authorId),
    }));
    expectTypeOf<Awaited<typeof foundPost>['author']>().toEqualTypeOf<
      DataModel['authors']['document']
    >();

    const chainedFoundPost = q.posts.find(postId).with((post) => ({
      author: q.authors.find(post.authorId).with((author) => ({
        latestPost: q.posts.byAuthorId(author._id).order('desc').firstOrNull(),
      })),
    }));
    expectTypeOf<
      Awaited<typeof chainedFoundPost>['author']['latestPost']
    >().toEqualTypeOf<DataModel['posts']['document'] | null>();

    const maybeFoundPost = q.posts.findOrNull(postId).with((post) => ({
      author: q.authors.find(post.authorId),
    }));
    expectTypeOf<
      NonNullable<Awaited<typeof maybeFoundPost>>['author']
    >().toEqualTypeOf<DataModel['authors']['document']>();
  });

  test('table range builders support staged composition', () => {
    const maybeOnlyPost = q.posts.uniqueOrNull();
    expectTypeOf<Awaited<typeof maybeOnlyPost>>().toEqualTypeOf<
      DataModel['posts']['document'] | null
    >();

    const expandedUnique = q.posts
      .with((post) => ({ author: q.authors.find(post.authorId) }))
      .unique();
    expectTypeOf<Awaited<typeof expandedUnique>['author']>().toEqualTypeOf<
      DataModel['authors']['document']
    >();

    const scopedPosts = q.posts
      .with((post) => ({ author: q.authors.find(post.authorId) }))
      .with((post) => ({
        latestComment: q.comments.byPostId(post._id).order('desc').firstOrNull(),
      }))
      .order('desc')
      .filter((query) => query.eq(query.field('status'), 'published'))
      .many();
    expectTypeOf<Awaited<typeof scopedPosts>[number]['author']>().toEqualTypeOf<
      DataModel['authors']['document']
    >();
    expectTypeOf<
      Awaited<typeof scopedPosts>[number]['latestComment']
    >().toEqualTypeOf<DataModel['comments']['document'] | null>();

    const postsByIds = q.posts
      .in([postId])
      .with((post) => ({ author: q.authors.find(post.authorId) }))
      .many();
    expectTypeOf<Awaited<typeof postsByIds>[number]['author']>().toEqualTypeOf<
      DataModel['authors']['document']
    >();
  });

  test('indexed builders support value range selector and batch entrypoints', () => {
    const maybeAuthor = q.authors.bySlug('ada').uniqueOrNull();
    expectTypeOf<Awaited<typeof maybeAuthor>>().toEqualTypeOf<
      DataModel['authors']['document'] | null
    >();

    const expandedIndexedUnique = q.authors
      .bySlug('ada')
      .with((author) => ({ posts: q.posts.byAuthorId(author._id).many() }))
      .unique();
    expectTypeOf<Awaited<typeof expandedIndexedUnique>['posts']>().toEqualTypeOf<
      DataModel['posts']['document'][]
    >();

    const indexedRangeComments = q.comments
      .byPostId()
      .with((comment) => ({ author: q.authors.find(comment.authorId) }))
      .filter((query) => query.eq(query.field('postId'), postId))
      .order('desc')
      .many();
    expectTypeOf<Awaited<typeof indexedRangeComments>[number]['author']>().toEqualTypeOf<
      DataModel['authors']['document']
    >();

    const approvedComments = q.comments
      .byPostIdAndStatus({ postId, status: 'approved' })
      .many();
    expectTypeOf<Awaited<typeof approvedComments>>().toEqualTypeOf<
      DataModel['comments']['document'][]
    >();

    const authorsAbove = q.authors.byReputation((query) => query.gt('reputation', 0)).many();
    expectTypeOf<Awaited<typeof authorsAbove>>().toEqualTypeOf<
      DataModel['authors']['document'][]
    >();

    const tagsBySlug = q.tags.bySlug.in(['news']).many();
    expectTypeOf<Awaited<typeof tagsBySlug>>().toEqualTypeOf<
      DataModel['tags']['document'][]
    >();

    const tagsByInternalId = q.tags.by_id.in([tagId]).many();
    expectTypeOf<Awaited<typeof tagsByInternalId>>().toEqualTypeOf<
      DataModel['tags']['document'][]
    >();
  });

  test('via builders support value zero-arg selector and source composition', () => {
    const postTags = q.tags
      .via('postsTags', 'tagId')
      .byPostId(postId)
      .withSource('link')
      .many();
    assertType<TagId>(null as any as Awaited<typeof postTags>[number]['_id']);
    expectTypeOf<Awaited<typeof postTags>[number]['link']>().toEqualTypeOf<
      DataModel['postsTags']['document']
    >();

    const taggedPosts = q.posts
      .via('postsTags', 'postId')
      .byTagId(tagId)
      .withSource('link')
      .many();
    expectTypeOf<Awaited<typeof taggedPosts>[number]['link']>().toEqualTypeOf<
      DataModel['postsTags']['document']
    >();

    const expandedViaFirst = q.posts
      .via('postsTags', 'postId')
      .byTagId(tagId)
      .withSource('link')
      .with((post) => ({ author: q.authors.find(post.authorId) }))
      .first();
    expectTypeOf<Awaited<typeof expandedViaFirst>['link']>().toEqualTypeOf<
      DataModel['postsTags']['document']
    >();
    expectTypeOf<Awaited<typeof expandedViaFirst>['author']>().toEqualTypeOf<
      DataModel['authors']['document']
    >();

    const expandedViaUniqueOrNull = q.tags
      .via('postsTags', 'tagId')
      .byPostIdAndTagId({ postId, tagId })
      .withSource('link')
      .uniqueOrNull();
    expectTypeOf<
      NonNullable<Awaited<typeof expandedViaUniqueOrNull>>['link']
    >().toEqualTypeOf<DataModel['postsTags']['document']>();

    const foundTagsViaZeroArg = q.tags
      .via('postsTags', 'tagId')
      .byPostId()
      .filter((query) => query.eq(query.field('postId'), postId))
      .order('desc')
      .many();
    expectTypeOf<Awaited<typeof foundTagsViaZeroArg>>().toEqualTypeOf<
      DataModel['tags']['document'][]
    >();

    const foundTagsViaSelector = q.tags
      .via('postsTags', 'tagId')
      .byPostId((query) => query.eq('postId', postId))
      .order('desc')
      .many();
    expectTypeOf<Awaited<typeof foundTagsViaSelector>>().toEqualTypeOf<
      DataModel['tags']['document'][]
    >();
  });

  test('compute lifts arbitrary async work into the query tree', () => {
    const maybeScore = compute(async () => {
      const scoreQ: QueryFacade<DataModel> = createQueryFacade(db);
      return (await scoreQ.posts.many()).length;
    });

    expectTypeOf<Awaited<typeof maybeScore>>().toEqualTypeOf<number>();
  });

  test('rejects invalid query shapes and post-terminal chaining', () => {
    // @ts-expect-error invalid table
    void q.nope;
    // @ts-expect-error invalid index
    void q.posts.byPostId(postId);
    // @ts-expect-error all was removed in favor of direct table-scoped queries
    void q.posts.all();
    // @ts-expect-error compound indexes do not support scalar shorthand
    void q.comments.byPostIdAndStatus(postId);
    // @ts-expect-error wrong value type for by_id batch lookup
    void q.tags.by_id.in(['not-a-tag-id']);
    // @ts-expect-error wrong value type for indexed batch lookup
    void q.tags.bySlug.in([123]);
    // @ts-expect-error terminals no longer accept order
    void q.comments.byPostId(postId).first().order('desc');
    // @ts-expect-error terminals no longer accept take
    void q.comments.byPostId(postId).many().take(5);
    // @ts-expect-error terminals no longer accept paginate
    void q.comments.byPostId(postId).many().paginate({ cursor: null, numItems: 5 });
    // @ts-expect-error terminals no longer accept with
    void q.posts.unique().with((post) => ({ author: q.authors.find(post.authorId) }));
    // @ts-expect-error terminals no longer accept with
    void q.posts.first().with((post) => ({ author: q.authors.find(post.authorId) }));
    // @ts-expect-error many builders no longer accept with
    void q.posts.many().with((post) => ({ author: q.authors.find(post.authorId) }));
    const batchPosts = q.posts.in([postId]).many();
    // @ts-expect-error batch builders no longer accept with
    void batchPosts.with(() => ({}));
    const viaManyTags = q.tags.via('postsTags', 'tagId').byPostId(postId).many();
    // @ts-expect-error via many builders no longer accept with
    void viaManyTags.with(() => ({}));
    // @ts-expect-error via many builders no longer accept withSource
    void q.tags.via('postsTags', 'tagId').byPostId(postId).many().withSource('link');
    // @ts-expect-error invalid via target field for tags
    void q.tags.via('postsTags', 'postId');
    // @ts-expect-error invalid join index
    void q.tags.via('postsTags', 'tagId').bySlug('news');
  });
});
