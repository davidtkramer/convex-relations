import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

const schema = defineSchema({
  authors: defineTable({
    slug: v.string(),
    name: v.string(),
    reputation: v.number(),
  })
    .index("bySlug", ["slug"])
    .index("byReputation", ["reputation"]),
  posts: defineTable({
    slug: v.string(),
    title: v.string(),
    body: v.string(),
    status: v.union(v.literal("draft"), v.literal("published")),
    authorId: v.id("authors"),
  })
    .index("bySlug", ["slug"])
    .index("byAuthorId", ["authorId"]),
  comments: defineTable({
    postId: v.id("posts"),
    authorId: v.id("authors"),
    body: v.string(),
    status: v.union(v.literal("pending"), v.literal("approved")),
  })
    .index("byPostId", ["postId"])
    .index("byPostIdAndStatus", ["postId", "status"]),
  tags: defineTable({
    slug: v.string(),
    name: v.string(),
    url: v.optional(v.string()),
  })
    .index("bySlug", ["slug"])
    .index("lookupSlug", ["slug"])
    .index("byURL", ["url"]),
  postsTags: defineTable({
    postId: v.id("posts"),
    tagId: v.id("tags"),
  })
    .index("byPostId", ["postId"])
    .index("byPostIdAndTagId", ["postId", "tagId"])
    .index("byTagId", ["tagId"]),
});

export default schema;
