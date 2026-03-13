import { convexTest } from 'convex-test';
import type { GenericId } from 'convex/values';
import { describe, expect, test } from 'vitest';
import { compute, createQueryFacade } from '../src/index';
import schema from './schema';

// @ts-ignore
const modules = import.meta.glob(['./**/*.ts', '../convex/**/*.ts']);

type AuthorId = GenericId<'authors'>;
type PostId = GenericId<'posts'>;
type CommentId = GenericId<'comments'>;
type TagId = GenericId<'tags'>;

async function seedAuthor(
  ctx: any,
  slug: string = crypto.randomUUID(),
  reputation = 0,
) {
  return await ctx.db.insert('authors', {
    slug,
    name: slug,
    reputation,
  });
}

async function seedPost(
  ctx: any,
  authorId: AuthorId,
  slug: string,
  title = slug,
  status: 'draft' | 'published' = 'published',
) {
  return await ctx.db.insert('posts', {
    slug,
    title,
    body: `${slug}-body`,
    status,
    authorId,
  });
}

async function seedComment(
  ctx: any,
  postId: PostId,
  authorId: AuthorId,
  body: string,
  status: 'pending' | 'approved' = 'approved',
) {
  return await ctx.db.insert('comments', {
    postId,
    authorId,
    body,
    status,
  });
}

async function seedTag(ctx: any, slug: string) {
  return await ctx.db.insert('tags', {
    slug,
    name: slug,
  });
}

async function seedPostTag(ctx: any, postId: PostId, tagId: TagId) {
  return await ctx.db.insert('postsTags', {
    postId,
    tagId,
  });
}

describe('convex-relations direct id builders', () => {
  test('support find, findOrNull, repeated with composition, and compute', async () => {
    const t = convexTest(schema, modules);

    await t.run(async (ctx) => {
      const q = createQueryFacade(ctx.db);
      const authorId = await seedAuthor(ctx, 'ada');
      const postId = await seedPost(ctx, authorId, 'hello-world');

      const found = await q.posts
        .find(postId)
        .with((post) => ({ author: q.authors.find(post.authorId) }))
        .with((post) => ({
          authorPosts: q.posts.byAuthorId(post.author._id).many(),
          commentCount: compute(async () => (await q.comments.byPostId(post._id).many()).length),
        }));
      const maybeFound = await q.posts.findOrNull(postId).with((post) => ({
        author: q.authors.find(post.authorId),
      }));
      const missing = await q.posts.findOrNull('missing-post' as PostId);

      await expect(q.posts.find('missing-post' as PostId)).rejects.toThrow(/Could not find posts/);
      expect(found.author._id).toBe(authorId);
      expect(found.authorPosts.map((post) => post._id)).toEqual([postId]);
      expect(found.commentCount).toBe(0);
      expect(maybeFound?.author._id).toBe(authorId);
      expect(missing).toBeNull();
    });
  });
});

describe('convex-relations table range builders', () => {
  test('support many, first, firstOrNull, order, filter, and staged with composition', async () => {
    const t = convexTest(schema, modules);

    await t.run(async (ctx) => {
      const q = createQueryFacade(ctx.db);
      const authorId = await seedAuthor(ctx, 'grace');
      const alphaId = await seedPost(ctx, authorId, 'alpha', 'A');
      const betaId = await seedPost(ctx, authorId, 'beta', 'B');
      const deletedId = await seedPost(ctx, authorId, 'deleted', 'C');

      await ctx.db.delete(deletedId);

      const first = await q.posts
        .order('desc')
        .filter((query) => query.neq(query.field('slug'), 'alpha'))
        .first();
      const firstOrNull = await q.posts
        .filter((query) => query.eq(query.field('slug'), 'missing'))
        .firstOrNull();
      const many = await q.posts
        .with((post) => ({ author: q.authors.find(post.authorId) }))
        .with((post) => ({ authorPosts: q.posts.byAuthorId(post.author._id).many() }))
        .order('desc')
        .filter((query) => query.neq(query.field('slug'), 'alpha'))
        .filter((query) => query.eq(query.field('status'), 'published'))
        .many();

      expect(first._id).toBe(betaId);
      expect(firstOrNull).toBeNull();
      expect(many).toHaveLength(1);
      expect(many[0]?.author._id).toBe(authorId);
      expect(many[0]?.authorPosts.map((post) => post._id)).toEqual([alphaId, betaId]);
    });
  });

  test('support direct batch lookups with with and skip missing rows', async () => {
    const t = convexTest(schema, modules);

    await t.run(async (ctx) => {
      const q = createQueryFacade(ctx.db);
      const authorId = await seedAuthor(ctx, 'linus');
      const alphaId = await seedPost(ctx, authorId, 'batch-a', 'A');
      const betaId = await seedPost(ctx, authorId, 'batch-b', 'B');
      const deletedId = await seedPost(ctx, authorId, 'batch-deleted', 'C');

      await ctx.db.delete(deletedId);

      const batched = await q.posts
        .in([alphaId, deletedId, betaId])
        .with((post) => ({ author: q.authors.find(post.authorId) }))
        .many();

      expect(batched.map((post) => post._id)).toEqual([alphaId, betaId]);
      expect(batched[0]?.author._id).toBe(authorId);
    });
  });

  test('support take and paginate', async () => {
    const t = convexTest(schema, modules);

    await t.run(async (ctx) => {
      const q = createQueryFacade(ctx.db);
      const authorId = await seedAuthor(ctx, 'margaret');
      await seedPost(ctx, authorId, 'take-a', 'A');
      await seedPost(ctx, authorId, 'take-b', 'B');

      const taken = await q.posts.take(1);
      const page = await q.posts.paginate({ cursor: null, numItems: 1 });

      expect(taken).toHaveLength(1);
      expect(page.page).toHaveLength(1);
      expect(page.isDone).toBe(false);
    });
  });

  test('support unique and uniqueOrNull with and without expansions', async () => {
    const t = convexTest(schema, modules);

    await t.run(async (ctx) => {
      const q = createQueryFacade(ctx.db);

      expect(await q.posts.uniqueOrNull()).toBeNull();
      await expect(q.posts.unique()).rejects.toThrow(/Could not find posts/);

      const authorId = await seedAuthor(ctx, 'donald');
      const onlyPostId = await seedPost(ctx, authorId, 'only-post', 'only');

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

      await seedPost(ctx, authorId, 'second-post', 'second');

      await expect(q.posts.unique()).rejects.toThrow();
      await expect(q.posts.uniqueOrNull()).rejects.toThrow();
    });
  });

  test('namespaces are not thenable', async () => {
    const t = convexTest(schema, modules);

    await t.run(async (ctx) => {
      const q = createQueryFacade(ctx.db);
      const table = q.posts;

      expect((table as any).then).toBeUndefined();
      await expect(Promise.resolve(table)).resolves.toBe(table);
    });
  });
});

describe('convex-relations indexed builders', () => {
  test('support unique and uniqueOrNull with missing duplicates and expansions', async () => {
    const t = convexTest(schema, modules);

    await t.run(async (ctx) => {
      const q = createQueryFacade(ctx.db);
      const slug = 'octavia';
      const authorId = await seedAuthor(ctx, slug);
      const postId = await seedPost(ctx, authorId, 'indexed-post');
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
      const maybeMissing = await q.authors.bySlug('missing').uniqueOrNull();

      await seedAuthor(ctx, 'dupe');
      await seedAuthor(ctx, 'dupe');

      expect(author._id).toBe(authorId);
      expect(expandedAuthor._id).toBe(authorId);
      expect(expandedAuthor.posts).toHaveLength(1);
      expect(expandedAuthor.posts[0]?._id).toBe(postId);
      expect(maybeExpandedAuthor?._id).toBe(authorId);
      expect(maybeExpandedAuthor?.posts).toHaveLength(1);
      expect(maybeExpandedAuthor?.posts[0]?._id).toBe(postId);
      expect(maybeMissing).toBeNull();
      await expect(q.authors.bySlug('missing').unique()).rejects.toThrow(
        /Could not find authors with index bySlug/,
      );
      await expect(q.authors.bySlug('dupe').unique()).rejects.toThrow();
    });
  });

  test('support first, firstOrNull, many, take, and paginate with filter, order, and with', async () => {
    const t = convexTest(schema, modules);

    await t.run(async (ctx) => {
      const q = createQueryFacade(ctx.db);
      const postAuthorId = await seedAuthor(ctx, 'ursula');
      const commentAuthorA = await seedAuthor(ctx, 'commenter-a');
      const commentAuthorB = await seedAuthor(ctx, 'commenter-b');
      const postId = await seedPost(ctx, postAuthorId, 'comments-post');
      const alphaId = await seedComment(ctx, postId, commentAuthorA, 'first');
      const betaId = await seedComment(ctx, postId, commentAuthorB, 'second');

      const first = await q.comments
        .byPostId(postId)
        .with((comment) => ({ author: q.authors.find(comment.authorId) }))
        .order('desc')
        .first();
      const maybeMissing = await q.comments.byPostId('missing-post' as PostId).firstOrNull();
      const many = await q.comments
        .byPostId(postId)
        .with((comment) => ({ author: q.authors.find(comment.authorId) }))
        .order('desc')
        .filter((query) => query.neq(query.field('authorId'), commentAuthorA))
        .many();
      const taken = await q.comments.byPostId(postId).order('desc').take(1);
      const page = await q.comments.byPostId(postId).order('desc').paginate({
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

  test('support selector and zero-arg range entrypoints', async () => {
    const t = convexTest(schema, modules);

    await t.run(async (ctx) => {
      const q = createQueryFacade(ctx.db);
      const withReputationId = await seedAuthor(ctx, 'experienced', 10);
      await seedAuthor(ctx, 'newcomer', 0);

      const authorsAbove = await q.authors
        .byReputation((query) => query.gt('reputation', 0))
        .many();
      const rangeAuthors = await q.authors
        .byReputation()
        .filter((query) => query.eq(query.field('reputation'), 10))
        .many();

      expect(authorsAbove.map((author) => author._id)).toEqual([withReputationId]);
      expect(rangeAuthors.map((author) => author._id)).toEqual([withReputationId]);
    });
  });

  test('support indexed batch lookups through custom and built-in indexes', async () => {
    const t = convexTest(schema, modules);

    await t.run(async (ctx) => {
      const q = createQueryFacade(ctx.db);
      const firstTagId = await seedTag(ctx, 'tag-a');
      const secondTagId = await seedTag(ctx, 'tag-b');

      const bySlug = await q.tags.bySlug.in(['tag-a', 'tag-b']).many();
      const byInternalId = await q.tags.by_id.in([firstTagId, secondTagId]).many();

      expect(bySlug.map((tag) => tag._id)).toEqual([firstTagId, secondTagId]);
      expect(byInternalId.map((tag) => tag._id)).toEqual([firstTagId, secondTagId]);
    });
  });
});

describe('convex-relations via builders', () => {
  test('support value, zero-arg, selector, withSource, and expansions', async () => {
    const t = convexTest(schema, modules);

    await t.run(async (ctx) => {
      const q = createQueryFacade(ctx.db);
      const authorId = await seedAuthor(ctx, 'jane');
      const postId = await seedPost(ctx, authorId, 'tagged-post');
      const secondPostId = await seedPost(ctx, authorId, 'tagged-post-2');
      const tagId = await seedTag(ctx, 'featured');
      const secondTagId = await seedTag(ctx, 'news');
      const firstLinkId = await seedPostTag(ctx, postId, tagId);
      await seedPostTag(ctx, postId, secondTagId);
      await seedPostTag(ctx, secondPostId, tagId);

      const postTags = await q.tags
        .via('postsTags', 'tagId')
        .byPostId(postId)
        .withSource('link')
        .many();
      const taggedPosts = await q.posts
        .via('postsTags', 'postId')
        .byTagId(tagId)
        .withSource('link')
        .many();
      const expandedViaFirst = await q.posts
        .via('postsTags', 'postId')
        .byTagId(tagId)
        .withSource('link')
        .with((post) => ({ author: q.authors.find(post.authorId) }))
        .first();
      const expandedViaUniqueOrNull = await q.tags
        .via('postsTags', 'tagId')
        .byPostIdAndTagId({ postId, tagId })
        .withSource('link')
        .uniqueOrNull();
      const viaZeroArg = await q.tags
        .via('postsTags', 'tagId')
        .byPostId()
        .filter((query) => query.eq(query.field('postId'), postId))
        .order('desc')
        .many();
      const viaSelector = await q.tags
        .via('postsTags', 'tagId')
        .byPostId((query) => query.eq('postId', postId))
        .order('desc')
        .many();

      expect(postTags).toHaveLength(2);
      expect(postTags[0]?.link._id).toBe(firstLinkId);
      expect(taggedPosts).toHaveLength(2);
      expect(expandedViaFirst.author._id).toBe(authorId);
      expect(expandedViaUniqueOrNull?.link.postId).toBe(postId);
      expect(viaZeroArg.map((tag) => tag._id)).toEqual([secondTagId, tagId]);
      expect(viaSelector.map((tag) => tag._id)).toEqual([secondTagId, tagId]);
    });
  });
});
