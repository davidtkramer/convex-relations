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
    .index("byPostIdAndStatus", ["postId", "status"])
    .index("byPostIdAndStatusAndAuthorId", ["postId", "status", "authorId"]),
  tags: defineTable({
    slug: v.string(),
    name: v.string(),
    url: v.optional(v.string()),
  })
    .index("bySlug", ["slug"])
    .index("lookupSlug", ["slug"])
    .index("byURL", ["url"]),
  // A union table: 'view' and 'share' are activity taps, 'flag' / 'report'
  // are moderation rows carrying a status the other variants lack.
  activities: defineTable(
    v.union(
      v.object({ postId: v.id("posts"), kind: v.literal("view") }),
      v.object({ postId: v.id("posts"), kind: v.literal("share"), channel: v.string() }),
      v.object({
        postId: v.id("posts"),
        kind: v.union(v.literal("flag"), v.literal("report")),
        status: v.union(v.literal("pending"), v.literal("resolved")),
        note: v.optional(v.string()),
      }),
    ),
  )
    .index("byKind", ["kind"])
    .index("byPostIdAndKind", ["postId", "kind"])
    .index("byStatus", ["status"])
    .index("byPostIdAndStatus", ["postId", "status"])
    .index("byNote", ["note"]),
  // Index fields that are dot paths into a nested object.
  profiles: defineTable({
    authorId: v.id("authors"),
    settings: v.object({ theme: v.string(), digest: v.optional(v.string()) }),
  })
    .index("byTheme", ["settings.theme"])
    .index("byAuthorIdAndTheme", ["authorId", "settings.theme"])
    .index("byDigest", ["settings.digest"]),
  // A union table whose variants nest different payloads, so a dot-path index
  // field exists on only some of them. `to` also differs across variants.
  notifications: defineTable(
    v.union(
      v.object({
        kind: v.literal("announcement"),
        to: v.union(v.id("authors"), v.literal("all")),
        sentAt: v.number(),
        payload: v.object({ postId: v.id("posts") }),
      }),
      v.object({
        kind: v.literal("new_follower"),
        to: v.id("authors"),
        sentAt: v.number(),
        payload: v.object({ followerId: v.id("authors") }),
      }),
      v.object({
        kind: v.literal("new_comment"),
        to: v.id("authors"),
        sentAt: v.number(),
        payload: v.object({ postId: v.id("posts"), commentId: v.id("comments") }),
      }),
    ),
  )
    .index("byToAndSentAt", ["to", "sentAt"])
    .index("byToAndFollowerId", ["to", "payload.followerId"])
    .index("byKindAndPostId", ["kind", "payload.postId"])
    .index("byPostIdAndKind", ["payload.postId", "kind"]),
  postsTags: defineTable({
    postId: v.id("posts"),
    tagId: v.id("tags"),
  })
    .index("byPostId", ["postId"])
    .index("byPostIdAndTagId", ["postId", "tagId"])
    .index("byTagId", ["tagId"]),
});

export default schema;
