# `@davidtkramer/convex-query`

Typed query facade helpers for Convex backends.

## Install

```bash
pnpm add @davidtkramer/convex-query convex
```

## Usage

```ts
import { createQueryFacade, compute, type QueryFacade } from '@davidtkramer/convex-query';
import type { DataModel } from './convex/_generated/dataModel';

type AppQueryFacade = QueryFacade<DataModel>;

export const q = createQueryFacade<DataModel>(db);

const enriched = await q.users.byClerkId('clerk-id').with((user) => ({
  tags: q.tags.byOwnerId(user._id).many(),
  score: compute(async () => {
    const taps = await createQueryFacade<DataModel>(db).taps.byUserId(user._id).many();
    return taps.length;
  }),
})).unique();
```

The package is generic over your Convex `DataModel`, so you can bind it once in your app and keep the rest of your backend strongly typed.
