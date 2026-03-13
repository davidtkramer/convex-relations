import type {
  DataModelFromSchemaDefinition,
  GenericDatabaseReader,
} from "convex/server";
import { assertType, describe, expectTypeOf, test } from "vitest";
import { createQueryFacade } from "../src/index";
import schema from "./schema";

type DataModel = DataModelFromSchemaDefinition<typeof schema>;
type IsAny<T> = 0 extends 1 & T ? true : false;
type PostId = DataModel["posts"]["document"]["_id"];
type TagId = DataModel["tags"]["document"]["_id"];

declare const db: GenericDatabaseReader<DataModel>;
declare const postId: PostId;
declare const tagId: TagId;

const q = createQueryFacade<DataModel>(db, schema);

describe("convex-relations type surface", () => {
  test("direct id builders support staged composition", () => {
    const foundPost = q.posts.find(postId).with((post) => ({
      author: q.authors.find(post.authorId),
    }));
    expectTypeOf<Awaited<typeof foundPost>["author"]>().toEqualTypeOf<
      DataModel["authors"]["document"]
    >();

    const chainedFoundPost = q.posts.find(postId).with((post) => ({
      author: q.authors.find(post.authorId).with((author) => ({
        latestPost: q.posts.byAuthorId(author._id).order("desc").firstOrNull(),
      })),
    }));
    expectTypeOf<
      Awaited<typeof chainedFoundPost>["author"]["latestPost"]
    >().toEqualTypeOf<DataModel["posts"]["document"] | null>();

    const maybeFoundPost = q.posts.findOrNull(postId).with((post) => ({
      author: q.authors.find(post.authorId),
    }));
    expectTypeOf<
      NonNullable<Awaited<typeof maybeFoundPost>>["author"]
    >().toEqualTypeOf<DataModel["authors"]["document"]>();
  });

  test("table range builders support staged composition", () => {
    const maybeOnlyPost = q.posts.uniqueOrNull();
    expectTypeOf<Awaited<typeof maybeOnlyPost>>().toEqualTypeOf<
      DataModel["posts"]["document"] | null
    >();

    const expandedUnique = q.posts
      .with((post) => ({ author: q.authors.find(post.authorId) }))
      .unique();
    expectTypeOf<Awaited<typeof expandedUnique>["author"]>().toEqualTypeOf<
      DataModel["authors"]["document"]
    >();

    const scopedPosts = q.posts
      .with((post) => ({ author: q.authors.find(post.authorId) }))
      .with((post) => ({
        latestComment: q.comments
          .byPostId(post._id)
          .order("desc")
          .firstOrNull(),
      }))
      .order("desc")
      .filter((query) => query.eq(query.field("status"), "published"))
      .many();
    expectTypeOf<Awaited<typeof scopedPosts>[number]["author"]>().toEqualTypeOf<
      DataModel["authors"]["document"]
    >();
    expectTypeOf<
      Awaited<typeof scopedPosts>[number]["latestComment"]
    >().toEqualTypeOf<DataModel["comments"]["document"] | null>();

    const postsByIds = q.posts
      .in([postId])
      .with((post) => ({ author: q.authors.find(post.authorId) }))
      .many();
    expectTypeOf<Awaited<typeof postsByIds>[number]["author"]>().toEqualTypeOf<
      DataModel["authors"]["document"]
    >();
  });

  test("indexed builders support value range selector and batch entrypoints", () => {
    const maybeAuthor = q.authors.bySlug("ada").uniqueOrNull();
    expectTypeOf<Awaited<typeof maybeAuthor>>().toEqualTypeOf<
      DataModel["authors"]["document"] | null
    >();

    const expandedIndexedUnique = q.authors
      .bySlug("ada")
      .with((author) => ({ posts: q.posts.byAuthorId(author._id).many() }))
      .unique();
    expectTypeOf<
      Awaited<typeof expandedIndexedUnique>["posts"]
    >().toEqualTypeOf<DataModel["posts"]["document"][]>();

    const indexedRangeComments = q.comments
      .byPostId()
      .with((comment) => ({ author: q.authors.find(comment.authorId) }))
      .filter((query) => query.eq(query.field("postId"), postId))
      .order("desc")
      .many();
    expectTypeOf<
      Awaited<typeof indexedRangeComments>[number]["author"]
    >().toEqualTypeOf<DataModel["authors"]["document"]>();

    const approvedComments = q.comments
      .byPostIdAndStatus(postId, "approved")
      .many();
    expectTypeOf<Awaited<typeof approvedComments>>().toEqualTypeOf<
      DataModel["comments"]["document"][]
    >();

    const authorsAbove = q.authors
      .byReputation((query) => query.gt("reputation", 0))
      .many();
    expectTypeOf<Awaited<typeof authorsAbove>>().toEqualTypeOf<
      DataModel["authors"]["document"][]
    >();

    q.authors.byReputation((query) => {
      assertType<false>(null as any as IsAny<typeof query>);
      return query.gt("reputation", 0);
    });

    const tagsBySlug = q.tags.bySlug.in(["news"]).many();
    expectTypeOf<Awaited<typeof tagsBySlug>>().toEqualTypeOf<
      DataModel["tags"]["document"][]
    >();

    const tagsByInternalId = q.tags.by_id.in([tagId]).many();
    expectTypeOf<Awaited<typeof tagsByInternalId>>().toEqualTypeOf<
      DataModel["tags"]["document"][]
    >();
  });

  test("through builders support collection sources, source nodes, and source composition", () => {
    const postTags = q.tags
      .through(q.postsTags.byPostId(postId), "tagId")
      .with((tag, { source }) => ({ link: source }))
      .many();
    assertType<TagId>(null as any as Awaited<typeof postTags>[number]["_id"]);
    expectTypeOf<Awaited<typeof postTags>[number]["link"]>().toEqualTypeOf<
      DataModel["postsTags"]["document"]
    >();

    const taggedPosts = q.posts
      .through(q.postsTags.byTagId(tagId), "postId")
      .with((post, { source }) => ({ link: source }))
      .many();
    expectTypeOf<Awaited<typeof taggedPosts>[number]["link"]>().toEqualTypeOf<
      DataModel["postsTags"]["document"]
    >();

    const expandedThroughFirst = q.posts
      .through(q.postsTags.byTagId(tagId), "postId")
      .with((post, { source }) => ({
        link: source,
        author: q.authors.find(post.authorId),
      }))
      .first();
    expectTypeOf<Awaited<typeof expandedThroughFirst>["link"]>().toEqualTypeOf<
      DataModel["postsTags"]["document"]
    >();
    expectTypeOf<Awaited<typeof expandedThroughFirst>["author"]>().toEqualTypeOf<
      DataModel["authors"]["document"]
    >();

    const expandedThroughUniqueOrNull = q.tags
      .through(q.postsTags.byPostIdAndTagId(postId, tagId), "tagId")
      .with((tag, { source }) => ({ link: source }))
      .uniqueOrNull();
    expectTypeOf<
      NonNullable<Awaited<typeof expandedThroughUniqueOrNull>>["link"]
    >().toEqualTypeOf<DataModel["postsTags"]["document"]>();

    const foundTagsThroughZeroArg = q.tags
      .through(
        q.postsTags
          .byPostId()
          .filter((query) => query.eq(query.field("postId"), postId))
          .order("desc"),
        "tagId",
      )
      .many();
    expectTypeOf<Awaited<typeof foundTagsThroughZeroArg>>().toEqualTypeOf<
      DataModel["tags"]["document"][]
    >();

    const foundTagsThroughSelector = q.tags
      .through(
        q.postsTags.byPostId((query) => query.eq("postId", postId)).order("desc"),
        "tagId",
      )
      .many();
    expectTypeOf<Awaited<typeof foundTagsThroughSelector>>().toEqualTypeOf<
      DataModel["tags"]["document"][]
    >();

    const throughSingleSource = q.authors
      .through(q.posts.bySlug("hello-world").unique(), "authorId")
      .with((author, { source }) => ({
        post: source,
        latestPost: q.posts.byAuthorId(author._id).firstOrNull(),
      }));
    expectTypeOf<Awaited<typeof throughSingleSource>["post"]>().toEqualTypeOf<
      DataModel["posts"]["document"]
    >();
    expectTypeOf<
      Awaited<typeof throughSingleSource>["latestPost"]
    >().toEqualTypeOf<DataModel["posts"]["document"] | null>();

    const throughManySource = q.tags
      .through(q.postsTags.byPostId(postId).take(1), "tagId")
      .with((tag, { source }) => ({ link: source }));
    expectTypeOf<
      Awaited<typeof throughManySource>[number]["link"]
    >().toEqualTypeOf<DataModel["postsTags"]["document"]>();
  });

  test("defer lifts arbitrary async work into the query tree", () => {
    const postScore = q.posts.find(postId).with((post, { defer }) => ({
      score: defer(async () => (await q.comments.byPostId(post._id).many()).length),
    }));

    expectTypeOf<Awaited<typeof postScore>["score"]>().toEqualTypeOf<number>();
  });

  test("rejects invalid query shapes and post-terminal chaining", () => {
    // @ts-expect-error invalid table
    void q.nope;
    // @ts-expect-error invalid index
    void q.posts.byPostId(postId);
    // @ts-expect-error all was removed in favor of direct table-scoped queries
    void q.posts.all();
    // @ts-expect-error compound indexes use positional arguments, not an object bag
    void q.comments.byPostIdAndStatus({ postId, status: "approved" });
    // @ts-expect-error wrong value type for by_id batch lookup
    void q.tags.by_id.in(["not-a-tag-id"]);
    // @ts-expect-error wrong value type for indexed batch lookup
    void q.tags.bySlug.in([123]);
    // @ts-expect-error terminals no longer accept order
    void q.comments.byPostId(postId).first().order("desc");
    // @ts-expect-error terminals no longer accept take
    void q.comments.byPostId(postId).many().take(5);
    void q.comments
      .byPostId(postId)
      .many()
      // @ts-expect-error terminals no longer accept paginate
      .paginate({ cursor: null, numItems: 5 });
    void q.posts.find(postId).with((post) => ({
      recentComments: q.comments.byPostId(post._id).take(5),
    }));
    void q.posts
      .unique()
      // @ts-expect-error terminals no longer accept with
      .with((post) => ({ author: q.authors.find(post.authorId) }));
    void q.posts
      .first()
      // @ts-expect-error terminals no longer accept with
      .with((post) => ({ author: q.authors.find(post.authorId) }));
    void q.posts
      .many()
      // @ts-expect-error many builders no longer accept with
      .with((post) => ({ author: q.authors.find(post.authorId) }));
    const batchPosts = q.posts.in([postId]).many();
    // @ts-expect-error batch builders no longer accept with
    void batchPosts.with(() => ({}));
    const throughManyTags = q.tags.through(q.postsTags.byPostId(postId), "tagId").many();
    // @ts-expect-error through many builders no longer accept with
    void throughManyTags.with(() => ({}));
    // @ts-expect-error invalid through target field for tags
    void q.tags.through(q.postsTags.byPostId(postId), "postId");
    // @ts-expect-error invalid through source field type for tags
    void q.tags.through(q.posts.bySlug("news"), "authorId");
    void q.posts
      // @ts-expect-error source context is only available on through builders
      .with((post, { source }) => ({ source }));
    void q.posts.with((post, { defer }) => ({
      score: defer(async () => (await q.comments.byPostId(post._id).many()).length),
    }));
    void q.posts
      // @ts-expect-error with callbacks must stay synchronous
      .with(async (post) => ({ author: q.authors.find(post.authorId) }));
    // @ts-expect-error order stays on the source query passed to through
    void q.tags.through(q.postsTags.byPostId(postId), "tagId").order("desc");
    // @ts-expect-error filter stays on the source query passed to through
    void q.tags.through(q.postsTags.byPostId(postId), "tagId").filter((query) =>
      query.eq(query.field("slug"), "news"),
    );
    // @ts-expect-error take stays on the source query passed to through
    void q.tags.through(q.postsTags.byPostId(postId), "tagId").take(1);
    void q.tags
      .through(q.postsTags.byPostId(postId), "tagId")
      // @ts-expect-error paginate stays on the source query passed to through
      .paginate({ cursor: null, numItems: 5 });
  });

  test("filter callbacks are not any", () => {
    q.authors
      .byReputation()
      .filter((query) => {
        assertType<false>(null as any as IsAny<typeof query>);
        return query.eq(query.field("reputation"), 10);
      })
      .many();
  });
});
