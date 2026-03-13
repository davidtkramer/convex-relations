import { convexTest } from "convex-test";
import type { GenericId } from "convex/values";
import { beforeEach, describe, expect, test } from "vitest";
import { createQueryFacade } from "../src/index";
import {
  rewindFactories,
  seedAuthor,
  seedComment,
  seedPost,
  seedPostTag,
  seedTag,
} from "./factories";
import schema from "./schema";

// @ts-ignore
const modules = import.meta.glob(["./**/*.ts", "../convex/**/*.ts"]);

type PostId = GenericId<"posts">;

beforeEach(() => {
  rewindFactories();
});

describe("convex-relations direct id builders", () => {
  test("support find, findOrNull, repeated with composition, and defer", async () => {
    const t = convexTest(schema, modules);

    await t.run(async (ctx) => {
      const q = createQueryFacade(ctx.db, schema);
      const authorId = await seedAuthor(ctx, { slug: "ada" });
      const postId = await seedPost(ctx, { authorId, slug: "hello-world" });

      const found = await q.posts
        .find(postId)
        .with((post) => ({ author: q.authors.find(post.authorId) }))
        .with((post, { defer }) => ({
          authorPosts: q.posts.byAuthorId(post.author._id).many(),
          commentCount: defer(
            async () => (await q.comments.byPostId(post._id).many()).length,
          ),
        }));
      const maybeFound = await q.posts.findOrNull(postId).with((post) => ({
        author: q.authors.find(post.authorId),
      }));
      const missing = await q.posts.findOrNull("missing-post" as PostId);

      await expect(q.posts.find("missing-post" as PostId)).rejects.toThrow(
        /Could not find posts/,
      );
      expect(found.author._id).toBe(authorId);
      expect(found.authorPosts.map((post) => post._id)).toEqual([postId]);
      expect(found.commentCount).toBe(0);
      expect(maybeFound?.author._id).toBe(authorId);
      expect(missing).toBeNull();
    });
  });

  test("supports lazy take inside nested with expansions", async () => {
    const t = convexTest(schema, modules);

    await t.run(async (ctx) => {
      const q = createQueryFacade(ctx.db, schema);
      const authorId = await seedAuthor(ctx, { slug: "nested-take-author" });
      const commenterId = await seedAuthor(ctx, {
        slug: "nested-take-commenter",
      });
      const postId = await seedPost(ctx, {
        authorId,
        slug: "nested-take-post",
      });

      await seedComment(ctx, { postId, authorId: commenterId, body: "older" });
      const newestCommentId = await seedComment(ctx, {
        postId,
        authorId: commenterId,
        body: "newer",
      });

      const post = await q.posts.find(postId).with((foundPost) => ({
        recentComments: q.comments
          .byPostId(foundPost._id)
          .order("desc")
          .take(1),
      }));

      expect(post.recentComments).toHaveLength(1);
      expect(post.recentComments[0]?._id).toBe(newestCommentId);
    });
  });
});

describe("convex-relations table range builders", () => {
  test("support many, first, firstOrNull, order, filter, and staged with composition", async () => {
    const t = convexTest(schema, modules);

    await t.run(async (ctx) => {
      const q = createQueryFacade(ctx.db, schema);
      const authorId = await seedAuthor(ctx, { slug: "grace" });
      const alphaId = await seedPost(ctx, { authorId, slug: "alpha" });
      const betaId = await seedPost(ctx, { authorId, slug: "beta" });
      const deletedId = await seedPost(ctx, { authorId, slug: "deleted" });

      await ctx.db.delete(deletedId);

      const first = await q.posts
        .order("desc")
        .filter((query) => query.neq(query.field("slug"), "alpha"))
        .first();
      const firstOrNull = await q.posts
        .filter((query) => query.eq(query.field("slug"), "missing"))
        .firstOrNull();
      const many = await q.posts
        .with((post) => ({ author: q.authors.find(post.authorId) }))
        .with((post) => ({
          authorPosts: q.posts.byAuthorId(post.author._id).many(),
        }))
        .order("desc")
        .filter((query) => query.neq(query.field("slug"), "alpha"))
        .filter((query) => query.eq(query.field("status"), "published"))
        .many();

      expect(first._id).toBe(betaId);
      expect(firstOrNull).toBeNull();
      expect(many).toHaveLength(1);
      expect(many[0]?.author._id).toBe(authorId);
      expect(many[0]?.authorPosts.map((post) => post._id)).toEqual([
        alphaId,
        betaId,
      ]);
    });
  });

  test("support direct batch lookups with with and skip missing rows", async () => {
    const t = convexTest(schema, modules);

    await t.run(async (ctx) => {
      const q = createQueryFacade(ctx.db, schema);
      const authorId = await seedAuthor(ctx, { slug: "linus" });
      const alphaId = await seedPost(ctx, { authorId, slug: "batch-a" });
      const betaId = await seedPost(ctx, { authorId, slug: "batch-b" });
      const deletedId = await seedPost(ctx, {
        authorId,
        slug: "batch-deleted",
      });

      await ctx.db.delete(deletedId);

      const batched = await q.posts
        .in([alphaId, deletedId, betaId])
        .with((post) => ({ author: q.authors.find(post.authorId) }))
        .many();

      expect(batched.map((post) => post._id)).toEqual([alphaId, betaId]);
      expect(batched[0]?.author._id).toBe(authorId);
    });
  });

  test("support take and paginate", async () => {
    const t = convexTest(schema, modules);

    await t.run(async (ctx) => {
      const q = createQueryFacade(ctx.db, schema);
      const authorId = await seedAuthor(ctx, { slug: "margaret" });
      await seedPost(ctx, { authorId, slug: "take-a" });
      await seedPost(ctx, { authorId, slug: "take-b" });

      const taken = await q.posts.take(1);
      const page = await q.posts.paginate({ cursor: null, numItems: 1 });

      expect(taken).toHaveLength(1);
      expect(page.page).toHaveLength(1);
      expect(page.isDone).toBe(false);
    });
  });

  test("support unique and uniqueOrNull with and without expansions", async () => {
    const t = convexTest(schema, modules);

    await t.run(async (ctx) => {
      const q = createQueryFacade(ctx.db, schema);

      expect(await q.posts.uniqueOrNull()).toBeNull();
      await expect(q.posts.unique()).rejects.toThrow(/Could not find posts/);

      const authorId = await seedAuthor(ctx, { slug: "donald" });
      const onlyPostId = await seedPost(ctx, { authorId, slug: "only-post" });

      const unique = await q.posts.unique();
      const maybeUnique = await q.posts.uniqueOrNull();
      const expandedUnique = await q.posts
        .with((post) => ({ author: q.authors.find(post.authorId) }))
        .unique();
      const expandedMaybeUnique = await q.posts
        .with((post) => ({ author: q.authors.find(post.authorId) }))
        .uniqueOrNull();

      expect(unique._id).toBe(onlyPostId);
      expect(maybeUnique?._id).toBe(onlyPostId);
      expect(expandedUnique.author._id).toBe(authorId);
      expect(expandedMaybeUnique?.author._id).toBe(authorId);

      await seedPost(ctx, { authorId, slug: "second-post" });

      await expect(q.posts.unique()).rejects.toThrow();
      await expect(q.posts.uniqueOrNull()).rejects.toThrow();
    });
  });

  test("namespaces are not thenable", async () => {
    const t = convexTest(schema, modules);

    await t.run(async (ctx) => {
      const q = createQueryFacade(ctx.db, schema);
      const table = q.posts;

      expect((table as any).then).toBeUndefined();
      await expect(Promise.resolve(table)).resolves.toBe(table);
    });
  });
});

describe("convex-relations indexed builders", () => {
  test("support unique and uniqueOrNull with missing duplicates and expansions", async () => {
    const t = convexTest(schema, modules);

    await t.run(async (ctx) => {
      const q = createQueryFacade(ctx.db, schema);
      const slug = "octavia";
      const authorId = await seedAuthor(ctx, { slug });
      const postId = await seedPost(ctx, { authorId, slug: "indexed-post" });
      const author = await q.authors.bySlug(slug).unique();
      const expandedAuthor = await q.authors
        .bySlug(slug)
        .with((foundAuthor) => ({
          posts: q.posts.byAuthorId(foundAuthor._id).many(),
        }))
        .unique();
      const maybeExpandedAuthor = await q.authors
        .bySlug(slug)
        .with((foundAuthor) => ({
          posts: q.posts.byAuthorId(foundAuthor._id).many(),
        }))
        .uniqueOrNull();
      const maybeMissing = await q.authors.bySlug("missing").uniqueOrNull();

      await seedAuthor(ctx, { slug: "dupe" });
      await seedAuthor(ctx, { slug: "dupe" });

      expect(author._id).toBe(authorId);
      expect(expandedAuthor._id).toBe(authorId);
      expect(expandedAuthor.posts).toHaveLength(1);
      expect(expandedAuthor.posts[0]?._id).toBe(postId);
      expect(maybeExpandedAuthor?._id).toBe(authorId);
      expect(maybeExpandedAuthor?.posts).toHaveLength(1);
      expect(maybeExpandedAuthor?.posts[0]?._id).toBe(postId);
      expect(maybeMissing).toBeNull();
      await expect(q.authors.bySlug("missing").unique()).rejects.toThrow(
        /Could not find authors with index bySlug/,
      );
      await expect(q.authors.bySlug("dupe").unique()).rejects.toThrow();
    });
  });

  test("support first, firstOrNull, many, take, and paginate with filter, order, and with", async () => {
    const t = convexTest(schema, modules);

    await t.run(async (ctx) => {
      const q = createQueryFacade(ctx.db, schema);
      const postAuthorId = await seedAuthor(ctx, { slug: "ursula" });
      const commentAuthorA = await seedAuthor(ctx, { slug: "commenter-a" });
      const commentAuthorB = await seedAuthor(ctx, { slug: "commenter-b" });
      const postId = await seedPost(ctx, {
        authorId: postAuthorId,
        slug: "comments-post",
      });
      const alphaId = await seedComment(ctx, {
        postId,
        authorId: commentAuthorA,
        body: "first",
      });
      const betaId = await seedComment(ctx, {
        postId,
        authorId: commentAuthorB,
        body: "second",
      });

      const first = await q.comments
        .byPostId(postId)
        .with((comment) => ({ author: q.authors.find(comment.authorId) }))
        .order("desc")
        .first();
      const maybeMissing = await q.comments
        .byPostId("missing-post" as PostId)
        .firstOrNull();
      const many = await q.comments
        .byPostId(postId)
        .with((comment) => ({ author: q.authors.find(comment.authorId) }))
        .order("desc")
        .filter((query) => query.neq(query.field("authorId"), commentAuthorA))
        .many();
      const taken = await q.comments.byPostId(postId).order("desc").take(1);
      const page = await q.comments.byPostId(postId).order("desc").paginate({
        cursor: null,
        numItems: 1,
      });

      expect(first.author._id).toBe(commentAuthorB);
      expect(first._id).toBe(betaId);
      expect(maybeMissing).toBeNull();
      expect(many).toHaveLength(1);
      expect(many[0]?.author._id).toBe(commentAuthorB);
      expect(taken).toHaveLength(1);
      expect(page.page).toHaveLength(1);
      expect(page.isDone).toBe(false);

      expect(alphaId).toBeDefined();
    });
  });

  test("support selector and zero-arg range entrypoints", async () => {
    const t = convexTest(schema, modules);

    await t.run(async (ctx) => {
      const q = createQueryFacade(ctx.db, schema);
      const withReputationId = await seedAuthor(ctx, {
        slug: "experienced",
        reputation: 10,
      });
      await seedAuthor(ctx, {
        slug: "newcomer",
        reputation: 0,
      });

      const authorsAbove = await q.authors
        .byReputation((query) => query.gt("reputation", 0))
        .many();
      const rangeAuthors = await q.authors
        .byReputation()
        .filter((query) => query.eq(query.field("reputation"), 10))
        .many();

      expect(authorsAbove.map((author) => author._id)).toEqual([
        withReputationId,
      ]);
      expect(rangeAuthors.map((author) => author._id)).toEqual([
        withReputationId,
      ]);
    });
  });

  test("support indexed batch lookups through custom and built-in indexes", async () => {
    const t = convexTest(schema, modules);

    await t.run(async (ctx) => {
      const q = createQueryFacade(ctx.db, schema);
      const firstTagId = await seedTag(ctx, { slug: "tag-a" });
      const secondTagId = await seedTag(ctx, { slug: "tag-b" });

      const bySlug = await q.tags.bySlug.in(["tag-a", "tag-b"]).many();
      const byInternalId = await q.tags.by_id
        .in([firstTagId, secondTagId])
        .many();

      expect(bySlug.map((tag) => tag._id)).toEqual([firstTagId, secondTagId]);
      expect(byInternalId.map((tag) => tag._id)).toEqual([
        firstTagId,
        secondTagId,
      ]);
    });
  });

  test("support shorthand lookups for custom index names and acronym fields", async () => {
    const t = convexTest(schema, modules);

    await t.run(async (ctx) => {
      const q = createQueryFacade(ctx.db, schema);
      const firstTagId = await seedTag(ctx, {
        slug: "lookup-a",
        url: "https://example.com/a",
      });
      const secondTagId = await seedTag(ctx, {
        slug: "lookup-b",
        url: "https://example.com/b",
      });

      const byCustomName = await q.tags.lookupSlug("lookup-a").many();
      const byCustomNameBatch = await q.tags.lookupSlug
        .in(["lookup-a", "lookup-b"])
        .many();
      const byAcronymField = await q.tags.byURL("https://example.com/a").many();
      const byAcronymFieldBatch = await q.tags.byURL
        .in(["https://example.com/a", "https://example.com/b"])
        .many();

      expect(byCustomName.map((tag: { _id: string }) => tag._id)).toEqual([
        firstTagId,
      ]);
      expect(byCustomNameBatch.map((tag: { _id: string }) => tag._id)).toEqual([
        firstTagId,
        secondTagId,
      ]);
      expect(byAcronymField.map((tag: { _id: string }) => tag._id)).toEqual([
        firstTagId,
      ]);
      expect(
        byAcronymFieldBatch.map((tag: { _id: string }) => tag._id),
      ).toEqual([firstTagId, secondTagId]);
    });
  });

  test("support indexed batch lookups on non-unique indexes", async () => {
    const t = convexTest(schema, modules);

    await t.run(async (ctx) => {
      const q = createQueryFacade(ctx.db, schema);
      const firstAuthorId = await seedAuthor(ctx, { slug: "batch-author-a" });
      const secondAuthorId = await seedAuthor(ctx, { slug: "batch-author-b" });
      const firstPostId = await seedPost(ctx, {
        authorId: firstAuthorId,
        slug: "batch-author-a-1",
      });
      const secondPostId = await seedPost(ctx, {
        authorId: firstAuthorId,
        slug: "batch-author-a-2",
      });
      const thirdPostId = await seedPost(ctx, {
        authorId: secondAuthorId,
        slug: "batch-author-b-1",
      });

      const posts = await q.posts.byAuthorId
        .in([firstAuthorId, secondAuthorId])
        .many();

      expect(posts.map((post) => post._id)).toEqual([
        firstPostId,
        secondPostId,
        thirdPostId,
      ]);
    });
  });
});

describe("convex-relations through builders", () => {
  test("support collection sources, source context, and expansions", async () => {
    const t = convexTest(schema, modules);

    await t.run(async (ctx) => {
      const q = createQueryFacade(ctx.db, schema);
      const authorId = await seedAuthor(ctx, { slug: "jane" });
      const postId = await seedPost(ctx, { authorId, slug: "tagged-post" });
      const secondPostId = await seedPost(ctx, {
        authorId,
        slug: "tagged-post-2",
      });
      const tagId = await seedTag(ctx, { slug: "featured" });
      const secondTagId = await seedTag(ctx, { slug: "news" });
      const firstLinkId = await seedPostTag(ctx, { postId, tagId });
      await seedPostTag(ctx, { postId, tagId: secondTagId });
      await seedPostTag(ctx, { postId: secondPostId, tagId });

      const postTags = await q.tags
        .through(q.postsTags.byPostId(postId), "tagId")
        .with((tag, { source }) => ({ link: source }))
        .many();
      const taggedPosts = await q.posts
        .through(q.postsTags.byTagId(tagId), "postId")
        .with((post, { source }) => ({ link: source }))
        .many();
      const expandedThroughFirst = await q.posts
        .through(q.postsTags.byTagId(tagId), "postId")
        .with((post, { source }) => ({
          link: source,
          author: q.authors.find(post.authorId),
        }))
        .first();
      const expandedThroughUniqueOrNull = await q.tags
        .through(q.postsTags.byPostIdAndTagId(postId, tagId), "tagId")
        .with((tag, { source }) => ({ link: source }))
        .uniqueOrNull();
      const throughZeroArg = await q.tags
        .through(
          q.postsTags
            .byPostId()
            .filter((query) => query.eq(query.field("postId"), postId))
            .order("desc"),
          "tagId",
        )
        .many();
      const throughSelector = await q.tags
        .through(
          q.postsTags
            .byPostId((query) => query.eq("postId", postId))
            .order("desc"),
          "tagId",
        )
        .many();
      const throughSingleSource = await q.authors
        .through(q.posts.bySlug("tagged-post").unique(), "authorId")
        .with((author, { source }) => ({
          post: source,
          latestPost: q.posts
            .byAuthorId(author._id)
            .order("desc")
            .firstOrNull(),
        }));
      const throughTakenSource = await q.tags
        .through(q.postsTags.byPostId(postId).order("desc").take(1), "tagId")
        .with((tag, { source }) => ({ link: source }));

      expect(postTags).toHaveLength(2);
      expect(postTags[0]?.link._id).toBe(firstLinkId);
      expect(taggedPosts).toHaveLength(2);
      expect(expandedThroughFirst.author._id).toBe(authorId);
      expect(expandedThroughUniqueOrNull?.link.postId).toBe(postId);
      expect(throughZeroArg.map((tag) => tag._id)).toEqual([
        secondTagId,
        tagId,
      ]);
      expect(throughSelector.map((tag) => tag._id)).toEqual([
        secondTagId,
        tagId,
      ]);
      expect(throughSingleSource.post._id).toBe(postId);
      expect(throughSingleSource.latestPost?._id).toBe(secondPostId);
      expect(throughTakenSource).toHaveLength(1);
      expect(throughTakenSource[0]?.link.postId).toBe(postId);
    });
  });
});
