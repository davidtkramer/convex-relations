import { createFactory, type EntityFactory } from 'typical-data';
import type { GenericId } from 'convex/values';
import type { Doc, Id, TableNames } from '../convex/_generated/dataModel';
import type { DatabaseWriter } from '../convex/_generated/server';

type AuthorId = GenericId<'authors'>;
type PostId = GenericId<'posts'>;
type TagId = GenericId<'tags'>;

type InsertValue<Table extends TableNames> = Omit<Doc<Table>, '_id' | '_creationTime'>;

type SeedCtx = {
  db: DatabaseWriter;
};

export const authorFactory = createFactory<InsertValue<'authors'>>({
  slug: ({ sequence }) => `author-${sequence}`,
  name: ({ entity }) => entity.slug,
  reputation: 0,
});

export const postFactory = createFactory<InsertValue<'posts'>>({
  slug: ({ sequence }) => `post-${sequence}`,
  title: ({ entity }) => entity.slug,
  body: ({ entity }) => `${entity.slug}-body`,
  status: 'published',
  authorId: null as never as AuthorId,
});

export const commentFactory = createFactory<InsertValue<'comments'>>({
  postId: null as never as PostId,
  authorId: null as never as AuthorId,
  body: ({ sequence }) => `comment-${sequence}`,
  status: 'approved',
});

export const tagFactory = createFactory<InsertValue<'tags'>>({
  slug: ({ sequence }) => `tag-${sequence}`,
  name: ({ entity }) => entity.slug,
  url: undefined,
});

export const postTagFactory = createFactory<InsertValue<'postsTags'>>({
  postId: null as never as PostId,
  tagId: null as never as TagId,
});

export function rewindFactories() {
  authorFactory.rewindSequence();
  postFactory.rewindSequence();
  commentFactory.rewindSequence();
  tagFactory.rewindSequence();
  postTagFactory.rewindSequence();
}

export function createSeed<Table extends TableNames>(
  table: Table,
  factory: EntityFactory<InsertValue<Table>, any, any>,
) {
  return async (
    ctx: SeedCtx,
    ...args: Parameters<typeof factory.build>
  ): Promise<Id<Table>> => {
    const value = factory.build(...args);
    return await ctx.db.insert(table, value as never);
  };
}

export const seedAuthor = createSeed('authors', authorFactory);
export const seedPost = createSeed('posts', postFactory);
export const seedComment = createSeed('comments', commentFactory);
export const seedTag = createSeed('tags', tagFactory);
export const seedPostTag = createSeed('postsTags', postTagFactory);
