<div align="center">
  <img width="120" height="120" alt="image" src="https://github.com/user-attachments/assets/05619751-ea3a-4dd9-9bd7-3c231bd11d83" />

  <h1>Convex Relations</h1>
</div>

`convex-relations` is a server-side query facade for Convex. It lets you write
data loading code as a typed result tree instead of manually coordinating
lookups, parallelization, and response shaping by hand.

## Example

With `convex-relations`, a nested API-ready query can look like this:

```ts
import { query } from "./_generated/server";

export const getPost = query({
  args: {},
  handler: async (ctx) => {
    const post = await ctx.q.posts
      .bySlug("hello-world")
      .with((post) => ({
        author: ctx.q.authors.find(post.authorId),
        recentComments: ctx.q.comments
          .byPostId(post._id)
          .order("desc")
          .with((comment) => ({
            author: ctx.q.authors.find(comment.authorId),
          }))
          .take(10),
        categories: ctx.q.categories
          .through(ctx.q.postCategories.byPostId(post._id), "categoryId")
          .many(),
      }))
      .unique();

    // post.author is an author document
    console.log(post.author.name);

    // post.recentComments is a list of comments with nested authors
    console.log(post.recentComments[0]?.author.name);

    // post.categories is already shaped as related category documents
    console.log(post.categories.map((category) => category.slug));

    return post;
  },
});
```

This example shows the core model:

- table-scoped access through `q.posts`, `q.comments`, and `q.categories`
- indexes as first-class query methods like `.bySlug(...)` and `.byPostId(...)`
- nested relation expansion with `.with(...)` inside `.with(...)`
- reference traversal with `.through(...)`
- parallel nested loading within each `with(...)`
- a final strongly typed, API-ready result from one expression

## Equivalent Convex Code

Without `convex-relations`, you end up assembling the same result shape by hand (or by agent 😉):

```ts
const post = await ctx.db
  .query("posts")
  .withIndex("bySlug", (q) => q.eq("slug", args.slug))
  .unique();

if (!post) {
  throw new Error("Post not found");
}

const [author, recentComments, postCategoryLinks] = await Promise.all([
  ctx.db.get(post.authorId),
  ctx.db
    .query("comments")
    .withIndex("byPostId", (q) => q.eq("postId", post._id))
    .order("desc")
    .take(10),
  ctx.db
    .query("postCategories")
    .withIndex("byPostId", (q) => q.eq("postId", post._id))
    .collect(),
]);

const recentCommentsWithAuthors = await Promise.all(
  recentComments.map(async (comment) => ({
    ...comment,
    author: await ctx.db.get(comment.authorId),
  })),
);

const categories = (
  await Promise.all(
    postCategoryLinks.map((link) => ctx.db.get(link.categoryId)),
  )
).filter((category) => category !== null);

return {
  ...post,
  author,
  recentComments: recentCommentsWithAuthors,
  categories,
};
```

That works, but you are responsible for:

- deciding what should run in parallel
- remembering to manually `Promise.all(...)` nested relationships
- traversing join tables by hand
- assembling the final tree shape yourself for API responses

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Core Concepts](#core-concepts)
- [API](#api)
- [Table Access Patterns](#table-access-patterns)
- [Relation Expansion with `with(...)`](#relation-expansion-with-with)
- [Reference Traversal with `through(...)`](#reference-traversal-with-through)
- [Terminals](#terminals)
- [Error Semantics](#error-semantics)
- [Performance Characteristics](#performance-characteristics)
- [Comparison to `convex-helpers/server/relationships`](#comparison-to-convex-helpersserverrelationships)
- [Type Notes](#type-notes)
- [License](#license)

## Installation

```bash
npm install @davidtkramer/convex-relations
```

```bash
pnpm add @davidtkramer/convex-relations
```

```bash
bun add @davidtkramer/convex-relations
```

```bash
yarn add @davidtkramer/convex-relations
```

## Quick Start

Most apps expose the facade on `ctx.q` through `convex-helpers` custom function
wrappers. A minimal setup looks like this:

```ts
// convex/lib/functions.ts
import { customCtx, customQuery } from "convex-helpers/server/customFunctions";
import { query as baseQuery } from "./_generated/server";
import type { DataModel } from "./_generated/dataModel";
import { createQueryFacade } from "@davidtkramer/convex-relations";

export const query = customQuery(
  baseQuery,
  customCtx((ctx: { db: any }) => ({
    q: createQueryFacade<DataModel>(ctx.db),
  })),
);
```

Once you do that, usage looks like this:

```ts
const post = await ctx.q.posts
  .bySlug("hello-world")
  .with((post) => ({
    author: ctx.q.authors.find(post.authorId),
    comments: ctx.q.comments.byPostId(post._id).order("desc").take(10),
  }))
  .unique();
```

## Core Concepts

### Table namespaces

Every table becomes a namespace on the returned facade:

```ts
await ctx.q.posts.many();
await ctx.q.authors.bySlug("ada-lovelace").unique();
await ctx.q.comments.byPostId(postId).order("desc").take(20);
```

### Indexes become methods

For example, imagine these index definitions:

```ts
authors: defineTable({
  slug: v.string(),
  name: v.string(),
}).index("bySlug", ["slug"]);

comments: defineTable({
  postId: v.id("posts"),
  authorId: v.id("authors"),
  status: v.union(v.literal("pending"), v.literal("approved")),
  body: v.string(),
})
  .index("byPostId", ["postId"])
  .index("byPostIdAndStatus", ["postId", "status"]);
```

```ts
const author = await ctx.q.authors.bySlug("ada-lovelace").unique();
const comments = await ctx.q.comments.byPostId(postId).many();
const approvedComments = await ctx.q.comments
  .byPostIdAndStatus(postId, "approved")
  .many();
```

Single-field indexes accept a scalar. Compound indexes accept positional
arguments in index order. Zero-argument calls give you the indexed range so you
can filter, sort, paginate, or take a subset.

### `with(...)` builds nested result shapes

`with(...)` lets you attach additional fields to every document in a query. The
callback receives the current document plus a small helper context, and returns
an object whose values can be plain sync values, other query nodes, or deferred
work via `defer(...)`.

```ts
const post = await ctx.q.posts
  .bySlug("hello-world")
  .with((post, { defer }) => ({
    author: ctx.q.authors.find(post.authorId),
    comments: ctx.q.comments.byPostId(post._id).take(10),
    readingTimeMinutes: defer(() =>
      Math.ceil(post.body.split(/\s+/).length / 200),
    ),
  }))
  .unique();
```

That returns a single object shaped like:

```ts
{
  ...post,
  author,
  comments,
}
```

This is the core idea behind the library: compose the data you want as a tree,
and `convex-relations` resolves and assembles that tree for you.

Within a single `with(...)`, sibling fields are resolved in parallel. If you
attach both `author` and `comments`, those branches start loading at the same
time. Nested `with(...)` calls preserve that behavior recursively, so each level
of the result tree parallelizes across its sibling fields.

## API

### `createQueryFacade<DataModel>(db)`

Creates a typed facade over your Convex `db`.

```ts
import { createQueryFacade } from "@davidtkramer/convex-relations";
import type { DataModel } from "./_generated/dataModel";

const q = createQueryFacade<DataModel>(ctx.db);
```

### `with(..., { defer })`

The `with(...)` callback context includes `defer`, which wraps arbitrary async
or sync work into the same lazy result tree as your relation queries.

```ts
const post = await q.posts
  .bySlug("hello-world")
  .with((post, { defer }) => ({
    readingTimeMinutes: defer(() =>
      Math.ceil(post.body.split(/\s+/).length / 200),
    ),
  }))
  .unique();
```

## Table Access Patterns

### `find(id)` and `findOrNull(id)`

Direct `_id` lookup.

```ts
const post = await q.posts.find(postId);
const maybePost = await q.posts.findOrNull(postId);
```

`find(...)` throws if the document is missing. `findOrNull(...)` returns `null`.

### Full table or index range queries

Zero-argument table or index access creates a range query.

```ts
const latestPosts = await q.posts.order("desc").take(20);

const authorPosts = await q.posts
  .byAuthorId()
  .filter((query) => query.eq(query.field("authorId"), authorId))
  .order("desc")
  .many();
```

### Indexed lookup by value

Single-field indexes accept a scalar:

```ts
const author = await q.authors.bySlug("ada-lovelace").unique();
```

Compound indexes accept leading positional arguments:

```ts
const comments = await q.comments
  .byPostIdAndStatus(postId)
  .order("desc")
  .take(20);

const exactOrPrefix = await q.comments
  .byPostIdAndStatus(postId, "approved")
  .many();
```

### Indexed lookup by selector function

You can also pass Convex's index selector callback:

```ts
const approvedComments = await q.comments
  .byPostIdAndStatus((q) => q.eq("postId", postId).eq("status", "approved"))
  .many();
```

### Batch lookup with `.in(...)`

Available on `_id` and indexed entrypoints.

```ts
const posts = await q.posts.in(postIds).many();

const categories = await q.categories.bySlug
  .in(["typescript", "convex"])
  .many();
```

Batch lookups skip missing rows.

## Relation Expansion with `with(...)`

`with(...)` lets you attach related data or computed fields before a terminal.

```ts
const post = await q.posts
  .bySlug("hello-world")
  .with((post, { defer }) => ({
    author: q.authors.find(post.authorId),
    comments: q.comments.byPostId(post._id).order("desc").take(10),
    commentCount: defer(async () => {
      const comments = await q.comments.byPostId(post._id).many();
      return comments.length;
    }),
  }))
  .unique();
```

You can chain `with(...)` calls:

```ts
const post = await q.posts
  .bySlug("hello-world")
  .with((post) => ({
    author: q.authors.find(post.authorId),
  }))
  .with((post) => ({
    otherPostsByAuthor: q.posts.byAuthorId(post.author._id).many(),
  }))
  .unique();
```

Each `with(...)` stage sees fields added by earlier stages.

## Reference Traversal with `through(...)`

Use `through(sourceQuery, foreignKeyField)` when another query already produces
rows that point at the table you want.

Given `postCategories { postId, categoryId }`, you can fetch categories for a post:

```ts
const categories = await q.categories
  .through(q.postCategories.byPostId(postId), "categoryId")
  .many();
```

This is essentially syntactic sugar for "run the source query, extract ids from
that field, then load the target rows for you."

You can also attach the source row through the normal `with(...)` callback:

```ts
const categories = await q.categories
  .through(q.postCategories.byPostId(postId).order("desc"), "categoryId")
  .with((category, { source }) => ({ link: source }))
  .many();

categories[0]?.link.postId;
categories[0]?.link.categoryId;
```

This is useful when the source table stores metadata like ordering, role, or
timestamps.

`through(...)` is not limited to join tables. Any compatible source query works:

```ts
const author = await q.authors
  .through(q.posts.bySlug("hello-world"), "authorId")
  .with((author, { source }) => ({ post: source }))
  .unique();

author.post.slug;
```

Source-query shaping lives inside the `through(...)` argument:

```ts
const tags = await q.tags
  .through(
    q.postTags
      .byPostId(postId)
      .filter((query) => query.eq(query.field("kind"), "primary"))
      .order("desc")
      .take(10),
    "tagId",
  )
  .with((tag, { source }) => ({ link: source }))
  .many();
```

After `through(...)`, you can keep shaping the target result with `with(...)`
and then choose a terminal like `many()` or `first()`.

## Terminals

### `unique()` / `uniqueOrNull()`

Use when the query should match at most one document.

```ts
const author = await q.authors.bySlug("ada-lovelace").unique();
const maybeAuthor = await q.authors.bySlug("missing").uniqueOrNull();
```

### `first()` / `firstOrNull()`

Use when you want the first result from an ordered or filtered range query.

```ts
const latestComment = await q.comments.byPostId(postId).order("desc").first();
const maybeLatestComment = await q.comments
  .byPostId(postId)
  .order("desc")
  .firstOrNull();
```

### `many()`

Collects all matching rows.

```ts
const comments = await q.comments.byPostId(postId).many();
```

### `take(count)`

Collects up to `count` rows.

```ts
const comments = await q.comments.byPostId(postId).order("desc").take(20);
```

### `paginate(opts)`

Returns Convex-style pagination output.

```ts
const page = await q.posts.byAuthorId(authorId).paginate({
  cursor: null,
  numItems: 25,
});
```

## Error Semantics

- `find(...)` throws if the document is missing
- `unique()` throws if there is no match
- `unique()` also throws if there are multiple matches
- `first()` throws if there is no match
- `findOrNull()`, `uniqueOrNull()`, and `firstOrNull()` return `null` instead

## Performance Characteristics

### What runs in parallel

Within a single `with(...)` stage, every field in the returned object runs in parallel.

```ts
const post = await q.posts.find(postId).with((post, { defer }) => ({
  author: q.authors.find(post.authorId),
  comments: q.comments.byPostId(post._id).take(10),
  categoryCount: defer(async () => {
    const categories = await q.categories
      .through(q.postCategories.byPostId(post._id), "categoryId")
      .many();
    return categories.length;
  }),
}));
```

Those three branches are executed concurrently.

For collection queries, expansion also runs in parallel across items:

- the query fetches the base rows
- each row is expanded concurrently
- each field inside a single expansion stage is also concurrent

### What runs sequentially

Chained `with(...)` stages are sequential by design.

```ts
q.posts
  .with((post) => ({ author: q.authors.find(post.authorId) }))
  .with((post) => ({ otherPosts: q.posts.byAuthorId(post.author._id).many() }));
```

The second stage waits for the first stage, because it depends on fields added earlier.

`through(...)` currently resolves target documents by fetching the source rows
first, then loading each target document individually. This is correct and
predictable, but it is not a single batched join at the database level.

### Practical guidance

- Prefer one `with(...)` stage when fields are independent
- Split into multiple `with(...)` stages only when later fields depend on earlier expansions
- Use `take(...)` or `paginate(...)` instead of `many()` on large collections
- Use indexed entrypoints whenever possible
- Use `.in(...)` when you already have a set of ids or indexed values

## Comparison to `convex-helpers/server/relationships`

If you started from Convex relationship helpers like `getOneFrom`, `getManyFrom`, or `getManyVia`, this library aims to provide the same kind of relational navigation with better composition.

This:

```ts
const categories = await q.categories
  .through(q.postCategories.byPostId(postId), "categoryId")
  .many();
```

replaces patterns like:

```ts
const categories = await getManyVia(
  db,
  "postCategories",
  "categoryId",
  "postId",
  postId,
);
```

but also composes naturally with `with(...)`, `take(...)`, `firstOrNull()`, and typed nested traversal.

## Type Notes

- The facade is generic over your generated `DataModel`
- Table names, `_id` types, index names, and compound index prefixes are inferred
- Invalid table names and invalid index names are rejected at compile time
- Scalar shorthand is only allowed for single-field indexes
- Compound indexes use leading positional arguments

## License

MIT
