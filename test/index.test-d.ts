import type {
  DataModelFromSchemaDefinition,
  GenericDatabaseReader,
} from "convex/server";
import { assertType, describe, expectTypeOf, test } from "vitest";
import { createQueryFacade, type RootIndexValueArg } from "../src/index";
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

    const tagMetrics = {
      slug: {
        index: "bySlug",
        field: "slug",
      },
      url: {
        index: "byURL",
        field: "url",
      },
    } as const;
    const tagMetric = null as any as keyof typeof tagMetrics;
    const m = tagMetrics[tagMetric];

    const dynamicIndexedTags = q.tags.withIndex(m.index, (query) =>
      query.eq(m.field, "value"),
    ).many();
    expectTypeOf<Awaited<typeof dynamicIndexedTags>>().toEqualTypeOf<
      DataModel["tags"]["document"][]
    >();

    q.tags.bySlug((query) => {
      // @ts-expect-error wrong field for bySlug selector
      return query.eq("url", "value");
    });
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

  test("in-memory filter narrows with a type predicate and keeps the node shape", () => {
    type Comment = DataModel["comments"]["document"];
    type Author = DataModel["authors"]["document"];

    const withAuthor = q.comments
      .byPostId(postId)
      .with((comment) => ({ author: q.authors.findOrNull(comment.authorId) }))
      .many()
      .filter(
        (comment): comment is typeof comment & { author: Author } =>
          comment.author !== null,
      )
      .sort((a, b) => b.author.reputation - a.author.reputation);
    expectTypeOf<Awaited<typeof withAuthor>[number]["author"]>().toEqualTypeOf<Author>();
    expectTypeOf<Awaited<typeof withAuthor>[number]["status"]>().toEqualTypeOf<
      Comment["status"]
    >();

    const unnarrowed = q.comments
      .byPostId(postId)
      .take(5)
      .filter((comment) => comment.status === "approved");
    expectTypeOf<Awaited<typeof unnarrowed>>().toEqualTypeOf<Comment[]>();

    const throughShaped = q.authors
      .through(
        q.comments
          .byPostId(postId)
          .many()
          .filter((comment) => comment.status === "approved"),
        "authorId",
      )
      .with((author, { source }) => ({ comment: source }));
    expectTypeOf<Awaited<typeof throughShaped>[number]["comment"]>().toEqualTypeOf<Comment>();
  });

  test("index equality on a union table narrows to the variants it can return", () => {
    type Activity = DataModel["activities"]["document"];
    type View = Extract<Activity, { kind: "view" }>;
    type Share = Extract<Activity, { kind: "share" }>;
    type Moderation = Extract<Activity, { kind: "flag" | "report" }>;

    // A literal discriminant picks the variant.
    const views = q.activities.byKind("view").many();
    expectTypeOf<Awaited<typeof views>>().toEqualTypeOf<View[]>();

    // A literal that several variants can hold keeps all of them.
    const flags = q.activities.byPostIdAndKind(postId, "flag").many();
    expectTypeOf<Awaited<typeof flags>>().toEqualTypeOf<Moderation[]>();

    // A field only some variants carry narrows by presence, whatever the value.
    const pending = q.activities.byPostIdAndStatus(postId, "pending").many();
    expectTypeOf<Awaited<typeof pending>>().toEqualTypeOf<Moderation[]>();
    const status = null as any as "pending" | "resolved";
    const byStatus = q.activities.byStatus(status).first();
    expectTypeOf<Awaited<typeof byStatus>>().toEqualTypeOf<Moderation>();

    // A prefix that stops before the discriminant does not narrow.
    const byPost = q.activities.byPostIdAndKind(postId).many();
    expectTypeOf<Awaited<typeof byPost>>().toEqualTypeOf<Activity[]>();

    // eq on an optional field with undefined can return rows without the field.
    const noNote = q.activities.byNote(undefined).many();
    expectTypeOf<Awaited<typeof noNote>>().toEqualTypeOf<Activity[]>();
    const noted = q.activities.byNote("spam").many();
    expectTypeOf<Awaited<typeof noted>>().toEqualTypeOf<Moderation[]>();

    // A field some variants lack holds undefined on them, so eq(undefined)
    // is accepted, as Convex's q.eq accepts it, and returns only those variants.
    const noStatus = q.activities.byStatus(undefined).many();
    expectTypeOf<Awaited<typeof noStatus>>().toEqualTypeOf<(View | Share)[]>();

    // Batch entrypoints narrow on the union of their values.
    const taps = q.activities.byKind.in(["view", "share"]).many();
    expectTypeOf<Awaited<typeof taps>>().toEqualTypeOf<(View | Share)[]>();

    // Batch elements may be readonly, bare values for the leading field, or
    // readonly prefix tuples.
    const kinds = ["view", "share"] as const;
    const fromConst = q.activities.byKind.in(kinds).many();
    expectTypeOf<Awaited<typeof fromConst>>().toEqualTypeOf<(View | Share)[]>();
    const scalarPrefix = q.activities.byPostIdAndKind.in([postId]).many();
    expectTypeOf<Awaited<typeof scalarPrefix>>().toEqualTypeOf<Activity[]>();
    const tuplePrefix = q.activities.byPostIdAndKind
      .in(kinds.map((kind) => [postId, kind] as const))
      .many();
    expectTypeOf<Awaited<typeof tuplePrefix>>().toEqualTypeOf<(View | Share)[]>();
    // @ts-expect-error a bare value must fit the leading field
    q.activities.byPostIdAndKind.in(["view"]);

    // Relations attach to the narrowed item.
    const shares = q.activities
      .byKind("share")
      .with((activity) => ({ post: q.posts.find(activity.postId) }))
      .many();
    expectTypeOf<Awaited<typeof shares>[number]["channel"]>().toEqualTypeOf<string>();

    // Variant-only index fields are typed by the variants that carry them.
    // @ts-expect-error status is a literal union, not a number
    q.activities.byStatus(123);
    // @ts-expect-error kind never holds this value
    q.activities.byKind("like");

    // Non-union tables are unchanged.
    const approved = q.comments.byPostIdAndStatus(postId, "approved").many();
    expectTypeOf<Awaited<typeof approved>>().toEqualTypeOf<
      DataModel["comments"]["document"][]
    >();
  });

  test("dot-path index fields type as the nested field", () => {
    type Profile = DataModel["profiles"]["document"];
    type AuthorId = DataModel["authors"]["document"]["_id"];
    const authorId = null as any as AuthorId;

    expectTypeOf<RootIndexValueArg<DataModel, "profiles", "byTheme">>().toEqualTypeOf<
      [string] | string
    >();
    const dark = q.profiles.byTheme("dark").many();
    expectTypeOf<Awaited<typeof dark>>().toEqualTypeOf<Profile[]>();
    const prefix = q.profiles.byAuthorIdAndTheme(authorId).many();
    expectTypeOf<Awaited<typeof prefix>>().toEqualTypeOf<Profile[]>();
    const compound = q.profiles.byAuthorIdAndTheme(authorId, "dark").firstOrNull();
    expectTypeOf<Awaited<typeof compound>>().toEqualTypeOf<Profile | null>();
    const batch = q.profiles.byTheme.in(["dark", "light"]).many();
    expectTypeOf<Awaited<typeof batch>>().toEqualTypeOf<Profile[]>();

    // An optional nested field also accepts undefined, as q.eq does.
    q.profiles.byDigest("weekly");
    q.profiles.byDigest(undefined);
    q.profiles.byDigest((range) => range.eq("settings.digest", undefined));

    // @ts-expect-error theme is a string
    q.profiles.byTheme(123);
    // @ts-expect-error theme is required, so it never holds undefined
    q.profiles.byTheme(undefined);
    // @ts-expect-error theme is a string
    q.profiles.byAuthorIdAndTheme(authorId, 123);
    // @ts-expect-error the leading field is an author id
    q.profiles.byAuthorIdAndTheme("dark", "dark");
    // @ts-expect-error digest is a string
    q.profiles.byDigest(123);
  });

  test("dot-path index fields on a union table type and narrow by variant", () => {
    type Notification = DataModel["notifications"]["document"];
    type Announcement = Extract<Notification, { kind: "announcement" }>;
    type NewFollower = Extract<Notification, { kind: "new_follower" }>;
    type NewComment = Extract<Notification, { kind: "new_comment" }>;
    type AuthorId = DataModel["authors"]["document"]["_id"];
    const authorId = null as any as AuthorId;

    // A top-level field whose type differs across variants accepts the union
    // of the variant types, and narrows to the variants that can hold the value.
    const forAuthor = q.notifications.byToAndSentAt(authorId).many();
    expectTypeOf<Awaited<typeof forAuthor>>().toEqualTypeOf<Notification[]>();
    const broadcast = q.notifications.byToAndSentAt("all").many();
    expectTypeOf<Awaited<typeof broadcast>>().toEqualTypeOf<Announcement[]>();
    const at = q.notifications.byToAndSentAt(authorId, 1).many();
    expectTypeOf<Awaited<typeof at>>().toEqualTypeOf<Notification[]>();
    // @ts-expect-error `to` is an author id or 'all'
    q.notifications.byToAndSentAt("everyone");
    // @ts-expect-error sentAt is a number
    q.notifications.byToAndSentAt(authorId, "now");

    // A dot path only one variant carries: the prefix keeps the union, the
    // full equality narrows to the variant with the path.
    const toAuthor = q.notifications.byToAndFollowerId(authorId).many();
    expectTypeOf<Awaited<typeof toAuthor>>().toEqualTypeOf<Notification[]>();
    const follow = q.notifications.byToAndFollowerId(authorId, authorId).uniqueOrNull();
    expectTypeOf<Awaited<typeof follow>>().toEqualTypeOf<NewFollower | null>();
    const batchFollows = q.notifications.byToAndFollowerId
      .in([[authorId, authorId]])
      .many();
    expectTypeOf<Awaited<typeof batchFollows>>().toEqualTypeOf<NewFollower[]>();

    // The positional form accepts what Convex's q.eq accepts for the path:
    // the nested type, plus undefined for the variants without it.
    expectTypeOf<
      RootIndexValueArg<DataModel, "notifications", "byToAndFollowerId">
    >().toEqualTypeOf<[AuthorId | "all"] | [AuthorId | "all", AuthorId | undefined]>();
    q.notifications.byToAndFollowerId((range) =>
      range.eq("to", authorId).eq("payload.followerId", undefined),
    );
    q.notifications.byToAndFollowerId((range) =>
      // @ts-expect-error q.eq rejects a number for the path too
      range.eq("to", authorId).eq("payload.followerId", 123),
    );
    // @ts-expect-error followerId is an author id
    q.notifications.byToAndFollowerId(authorId, 123);
    // @ts-expect-error followerId is an author id, not a post id
    q.notifications.byToAndFollowerId(authorId, postId);

    // eq(undefined) matches the rows WITHOUT the path, never the ones with it.
    const notFollows = q.notifications.byToAndFollowerId(authorId, undefined).many();
    expectTypeOf<Awaited<typeof notFollows>>().toEqualTypeOf<
      (Announcement | NewComment)[]
    >();
    // A value that may be undefined can match every variant.
    const maybeFollowerId = null as any as AuthorId | undefined;
    const maybeFollows = q.notifications.byToAndFollowerId(authorId, maybeFollowerId).many();
    expectTypeOf<Awaited<typeof maybeFollows>>().toEqualTypeOf<Notification[]>();
    // 'all' and a followerId never occur on the same variant.
    const impossible = q.notifications.byToAndFollowerId("all", authorId).many();
    expectTypeOf<Awaited<typeof impossible>>().toEqualTypeOf<never[]>();

    // A discriminant and a dot path in one index narrow together.
    const comments = q.notifications.byKindAndPostId("new_comment", postId).many();
    expectTypeOf<Awaited<typeof comments>>().toEqualTypeOf<NewComment[]>();
    const byKind = q.notifications.byKindAndPostId("announcement").many();
    expectTypeOf<Awaited<typeof byKind>>().toEqualTypeOf<Announcement[]>();
    const kind = null as any as Notification["kind"];
    const aboutPost = q.notifications.byKindAndPostId(kind, postId).many();
    expectTypeOf<Awaited<typeof aboutPost>>().toEqualTypeOf<
      (Announcement | NewComment)[]
    >();
    const followerWithPost = q.notifications.byKindAndPostId("new_follower", postId).many();
    expectTypeOf<Awaited<typeof followerWithPost>>().toEqualTypeOf<never[]>();
    const followerNoPost = q.notifications.byKindAndPostId("new_follower", undefined).many();
    expectTypeOf<Awaited<typeof followerNoPost>>().toEqualTypeOf<NewFollower[]>();

    // The dot path may lead the index; the discriminant narrows further.
    const byPost = q.notifications.byPostIdAndKind(postId).many();
    expectTypeOf<Awaited<typeof byPost>>().toEqualTypeOf<(Announcement | NewComment)[]>();
    const announced = q.notifications.byPostIdAndKind(postId, "announcement").many();
    expectTypeOf<Awaited<typeof announced>>().toEqualTypeOf<Announcement[]>();
    const batchByPost = q.notifications.byPostIdAndKind.in([postId]).many();
    expectTypeOf<Awaited<typeof batchByPost>>().toEqualTypeOf<
      (Announcement | NewComment)[]
    >();

    // Relations attach to the narrowed item, including its nested payload.
    const withFollower = q.notifications
      .byToAndFollowerId(authorId, authorId)
      .with((notification) => ({
        follower: q.authors.find(notification.payload.followerId),
      }))
      .many();
    expectTypeOf<Awaited<typeof withFollower>>().items.toHaveProperty("follower");

    // The selector-function form still returns the full union.
    const selected = q.notifications
      .byToAndFollowerId((range) =>
        range.eq("to", authorId).eq("payload.followerId", authorId),
      )
      .many();
    expectTypeOf<Awaited<typeof selected>>().toEqualTypeOf<Notification[]>();
  });
});
